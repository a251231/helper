"""
WeChat 4.1 HTTP + WebSocket Gateway Server (server_v4.py)
Powered by Prometheus-first/helper (NativeTextService / text_bridge.dll)

Drop-in replacement for server.py (WeChatFerry 3.9) with 100% backward compatibility
for LangBot's WeChatFerry adapter and whale_persona_plugin, plus WeChat 4.1 V4 native
enhancements:
  - Native GIF Emoticon (Type 47) sending via /api/send_image
  - Native Voice (SILK v3 / WAV / MP3) sending via /api/send_voice
  - Native WeChat Moments (朋友圈) publishing via /api/send_moment
  - Automatic process discovery & reconnect when WeChat 4.1 logs in or switches accounts
"""

from __future__ import annotations

import asyncio
import base64
import html
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# ── Ensure both gateway root and native/ are importable ──────────────
BASE_DIR = Path(__file__).resolve().parent
NATIVE_DIR = BASE_DIR / "native"
for _p in (str(BASE_DIR), str(NATIVE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from text_service import NativeTextService, inspect_profile  # noqa: E402
from wxbot_framework.mentions import parse_at_members  # noqa: E402
from wxbot_framework.file_metadata import parse_file_metadata  # noqa: E402
from wxbot_framework.forwarded import ForwardedError, parse_forwarded  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("wcf-gateway-v4")

# ── Configuration ────────────────────────────────────────────────────
HTTP_PORT = int(os.environ.get("WCF_PORT", "8080"))
PREFERRED_ACCOUNT = os.environ.get("WXBOT_ACCOUNT", "").strip() or None

RUNTIME_DIR = BASE_DIR / ".runtime"
GATEWAY_MEDIA_DIR = RUNTIME_DIR / "gateway_media"
GATEWAY_MEDIA_DIR.mkdir(parents=True, exist_ok=True)


# ── Subclass NativeTextService to also deliver non-file Type 49 XML ──
class GatewayNativeTextService(NativeTextService):
    """Extends NativeTextService so that non-file, non-forwarded Type 49
    appmsg XML payloads (such as <type>57</type> quote replies, <type>5</type>
    shared web/Bilibili/Xiaohongshu link cards, <type>33/36</type> mini-program
    cards, or <type>19</type> cards that failed structured parsing) are
    delivered to msg_handlers instead of being silently dropped.
    """

    def _map_message(self, message):
        res = super()._map_message(message)
        if res is None and getattr(message, "type", 0) == 49:
            body = getattr(message, "image_xml", b"").decode("utf-8", errors="replace")
            is_async_file = False
            try:
                is_async_file = parse_file_metadata(body) is not None
            except Exception:
                is_async_file = False

            is_async_fwd = False
            try:
                is_async_fwd = parse_forwarded(body) is not None
            except ForwardedError:
                is_async_fwd = False
            except Exception:
                is_async_fwd = False

            if not is_async_file and not is_async_fwd:
                data = self.router.convert(
                    message.sender.decode("utf-8", errors="replace"),
                    message.receiver.decode("utf-8", errors="replace"),
                    body,
                    sequence=message.sequence,
                    msg_type=message.type,
                    thread_id=message.thread_id,
                )
                if data:
                    data["native_message_id"] = str(getattr(message, "native_message_id", 0) or "")
                    name = self._sender_wechat_name(data["sender_id"]) if data["sender_id"] else None
                    if name:
                        data["sender_wechat_name"] = name
                    source = getattr(message, "mention_source", b"").decode("utf-8", errors="replace")
                    data["at_members"] = parse_at_members(source)
                    data["is_at_self"] = bool(data["room_wxid"] and self.self_wxid in data["at_members"])
                    return data
        return res


# ── Global State ─────────────────────────────────────────────────────
_svc: Optional[GatewayNativeTextService] = None
_svc_lock = threading.RLock()
_receiving = False
_stop_worker = threading.Event()
_ws_clients: set[WebSocket] = set()
_msg_count = 0
_last_msg_summary = "None"
_main_loop: Optional[asyncio.AbstractEventLoop] = None

# ── Caches for contact/group display names ───────────────────────────
_room_members_cache: dict[str, tuple[float, dict[str, str]]] = {}
_contact_name_cache: dict[str, str] = {}
_self_profile_cache: dict[str, str] = {}
_CACHE_TTL = 300.0  # 5 minutes


def _is_service_active(svc: Optional[GatewayNativeTextService]) -> bool:
    if svc is None:
        return False
    return bool(
        getattr(svc, "_owns_bridge", False)
        and not getattr(svc, "_stop", threading.Event()).is_set()
        and getattr(svc, "_channel", None) is not None
        and getattr(svc, "self_wxid", "")
    )


def _get_cached_self_profile(svc: Optional[GatewayNativeTextService]) -> dict:
    """Fast, lock-free memory lookup for /api/status and dashboard."""
    if svc is None:
        return {}
    wxid = getattr(svc, "self_wxid", "") or ""
    if not wxid:
        return {}
    return {"wxid": wxid, "name": _self_profile_cache.get(wxid, "")}


def _refresh_self_profile(svc: GatewayNativeTextService) -> dict:
    wxid = svc.self_wxid or ""
    if not wxid:
        return {}
    name = _self_profile_cache.get(wxid, "")
    if not name and svc.pid:
        try:
            prof = inspect_profile(svc.pid)
            if prof and prof.get("name"):
                name = prof["name"]
                _self_profile_cache[wxid] = name
        except Exception:
            pass
    if not name:
        try:
            name = svc._sender_wechat_name(wxid) or ""
            if name:
                _self_profile_cache[wxid] = name
        except Exception:
            pass
    return {"wxid": wxid, "name": name}


def _get_room_members(roomid: str) -> dict[str, str]:
    """Return {wxid: display_name} for a chatroom, with caching (including negative cache)."""
    if not roomid:
        return {}
    now = time.time()
    cached = _room_members_cache.get(roomid)
    if cached:
        ttl = _CACHE_TTL if cached[1] else 60.0
        if (now - cached[0]) < ttl:
            return cached[1]
    svc = _svc
    if not _is_service_active(svc):
        return cached[1] if cached else {}
    try:
        rows = svc.group_members(roomid) or []
        members: dict[str, str] = {}
        for row in rows:
            wxid = row.get("wxid", "")
            if not wxid:
                continue
            dname = (
                row.get("display_name")
                or row.get("room_nickname")
                or row.get("nickname")
                or wxid
            )
            members[wxid] = dname
            if row.get("nickname"):
                _contact_name_cache[wxid] = row["nickname"]
        _room_members_cache[roomid] = (now, members)
        return members
    except Exception as e:
        logger.debug(f"group_members({roomid}) error: {e}")
        _room_members_cache[roomid] = (now, cached[1] if cached else {})
        return cached[1] if cached else {}


def _resolve_sender_name(
    sender: str,
    roomid: str = "",
    fallback_name: str = "",
    *,
    fast_only: bool = False,
) -> str:
    """Resolve human-readable display name for a sender."""
    if not sender:
        return ""
    if fallback_name and fallback_name != sender:
        _contact_name_cache[sender] = fallback_name
    if roomid:
        cached_room = _room_members_cache.get(roomid)
        if cached_room and sender in cached_room[1] and cached_room[1][sender]:
            return cached_room[1][sender]
        if not fast_only:
            members = _get_room_members(roomid)
            if sender in members and members[sender]:
                return members[sender]
    if fallback_name:
        return fallback_name
    if sender in _contact_name_cache:
        return _contact_name_cache[sender]
    if not fast_only:
        svc = _svc
        if _is_service_active(svc):
            try:
                name = svc._sender_wechat_name(sender)
                if name:
                    _contact_name_cache[sender] = name
                    return name
            except Exception:
                pass
    return sender


def _extract_xml_sender(raw_xml: str) -> str:
    """Extract sender wxid from WeChat XML when group prefix was absent."""
    if not raw_xml:
        return ""
    m = re.search(r'\bfromusername\s*=\s*"([^"]+)"', raw_xml)
    if m and m.group(1).strip():
        return m.group(1).strip()
    m2 = re.search(r"<fromusername><!\[CDATA\[(.*?)\]\]></fromusername>", raw_xml)
    if m2 and m2.group(1).strip():
        return m2.group(1).strip()
    m3 = re.search(r"<fromusername>([^<]+)</fromusername>", raw_xml)
    if m3 and m3.group(1).strip():
        return m3.group(1).strip()
    return ""


def _detect_image_mime_and_ext(data: bytes) -> tuple[str, str]:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif", ".gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp", ".webp"
    if data.startswith(b"BM"):
        return "image/bmp", ".bmp"
    return "image/jpeg", ".jpg"


def _build_forwarded_xml(data: dict) -> str:
    """Construct a WeChat-compatible Type 49 <type>19</type> XML string from
    NativeTextService's parsed forwarded_records so whale_listener.py's
    existing chat history parser works without changes.
    """
    fwd_title = str(data.get("forwarded_title") or "群聊的聊天记录")
    records = data.get("forwarded_records") or []
    des_lines: list[str] = []
    dataitems: list[str] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        sname = str(rec.get("sender_name") or "未知")
        stime = str(rec.get("source_time") or "")
        kind = rec.get("kind", "text")
        rtext = str(rec.get("text") or "").strip()
        if kind == "image":
            img_path = rec.get("image_path")
            if img_path and os.path.exists(str(img_path)):
                rtext = rtext or "[图片]"
            else:
                rtext = rtext or "[图片]"
        if not rtext:
            rtext = f"[{kind}]"
        des_lines.append(f"{sname}: {rtext}")
        dataitems.append(
            f'<dataitem datatype="1">'
            f"<sourcename>{html.escape(sname)}</sourcename>"
            f"<sourcetime>{html.escape(stime)}</sourcetime>"
            f"<datadesc>{html.escape(rtext)}</datadesc>"
            f"</dataitem>"
        )
    des_summary = "\n".join(des_lines)
    return (
        f"<msg><appmsg>"
        f"<title>{html.escape(fwd_title)}</title>"
        f"<des>{html.escape(des_summary)}</des>"
        f"<type>19</type>"
        f"<recorditem><recordinfo><datalist>{''.join(dataitems)}</datalist></recordinfo></recorditem>"
        f"</appmsg></msg>"
    )


def _on_native_message(pid: int, msg_type_code: int, data: dict, service: GatewayNativeTextService):
    """Callback invoked by NativeTextService for text, image, file, and forwarded events."""
    global _msg_count, _last_msg_summary

    try:
        if not isinstance(data, dict):
            return

        # Ignore bot-generated / self-sent messages
        if data.get("bot_generated") or data.get("is_self"):
            return

        wx_type = int(data.get("wx_type") or 1)
        # Skip pure system / recall / friend-request events that aren't chat messages
        if wx_type in (37, 10000, 10002):
            return

        roomid = str(data.get("room_wxid") or "")
        chat_id = str(data.get("chat_id") or "")
        is_group = bool(roomid or chat_id.endswith("@chatroom"))
        if is_group and not roomid:
            roomid = chat_id

        raw_xml = str(data.get("raw_content") or "")
        sender = str(data.get("sender_id") or data.get("from_wxid") or "").strip()
        if not sender and raw_xml:
            sender = _extract_xml_sender(raw_xml)
        if not sender and not is_group:
            sender = chat_id

        self_wxid = str(data.get("self_wxid") or getattr(service, "self_wxid", "") or "")
        if self_wxid and sender == self_wxid:
            return

        # Ensure group messages always have a non-empty sender so LangBot's wechatferry.py doesn't drop them
        if is_group and not sender:
            sender = roomid

        fallback_sname = str(data.get("sender_wechat_name") or "")
        sender_name = _resolve_sender_name(
            sender,
            roomid if is_group else "",
            fallback_sname,
            fast_only=bool(fallback_sname),
        )

        is_at = bool(data.get("is_at_self") or data.get("mentioned_self"))
        content = str(data.get("content") or data.get("msg") or "")
        extra = ""
        out_type = wx_type

        # ── 1. Image message (wx_type == 3) ──────────────────────────
        if wx_type == 3:
            img_path = data.get("image_path")
            if img_path and os.path.exists(str(img_path)):
                try:
                    with open(str(img_path), "rb") as f:
                        img_bytes = f.read()
                    mime, _ = _detect_image_mime_and_ext(img_bytes)
                    b64 = base64.b64encode(img_bytes).decode("ascii")
                    content = f"<img_b64>data:{mime};base64,{b64}</img_b64>"
                    extra = str(img_path)
                    logger.info(f"Encoded inbound V4 image ({len(img_bytes)} bytes) -> b64 ({len(b64)} chars)")
                except Exception as e:
                    logger.warning(f"Failed to read inbound V4 image {img_path}: {e}")
                    content = raw_xml or "[图片]"
            else:
                # Avoid delivering monitor_only placeholder before ImageReceiver finishes
                if data.get("monitor_only"):
                    return
                content = raw_xml or "[图片]"

        # ── 2. AppMsg / File / Forwarded / Quote / Link (wx_type == 49) ──
        elif wx_type == 49:
            if data.get("file_status") is not None:
                # Delivered via _deliver_file
                fpath = data.get("file_path")
                fname = str(data.get("file_name") or "")
                if not fname and fpath:
                    fname = os.path.basename(str(fpath))
                fname = fname or "attachment"
                if fpath and os.path.exists(str(fpath)):
                    try:
                        fsize = os.path.getsize(str(fpath))
                        if 0 < fsize <= 15 * 1024 * 1024:
                            with open(str(fpath), "rb") as f:
                                fb64 = base64.b64encode(f.read()).decode("ascii")
                            esc_name = html.escape(fname)
                            content = (
                                f'<msg><appmsg file_b64="{fb64}">'
                                f"<type>6</type><title>{esc_name}</title>"
                                f"</appmsg></msg>"
                                f"<file_b64>{fb64}</file_b64><file_name>{esc_name}</file_name>"
                            )
                            extra = str(fpath)
                            logger.info(f"Encoded inbound V4 file '{fname}' ({fsize} bytes) -> b64")
                        else:
                            esc_name = html.escape(fname)
                            content = f"<msg><appmsg><type>6</type><title>{esc_name}</title></appmsg></msg>"
                            extra = str(fpath)
                    except Exception as e:
                        logger.warning(f"Failed to encode inbound V4 file {fpath}: {e}")
                        content = raw_xml or f"<msg><appmsg><type>6</type><title>{html.escape(fname)}</title></appmsg></msg>"
                else:
                    content = raw_xml or f"<msg><appmsg><type>6</type><title>{html.escape(fname)}</title></appmsg></msg>"

            elif data.get("forwarded_records") is not None or data.get("app_type") == 19:
                # Delivered via _deliver_forwarded or fallback
                if data.get("forwarded_records"):
                    content = _build_forwarded_xml(data)
                else:
                    content = raw_xml or content
            else:
                # Quote reply (<type>57</type>), shared link card (<type>5</type>), mini-program, etc.
                if raw_xml:
                    content = raw_xml

                # Also check if this is a quote reply referencing an earlier text/image and @mentioning self
                if self_wxid and not is_at and raw_xml and f"<chatusr>{self_wxid}</chatusr>" in raw_xml:
                    is_at = True

        # ── 3. Animated Emoticon / Sticker (wx_type == 47) ───────────
        elif wx_type == 47:
            content = raw_xml or "[动画表情]"

        # ── 4. Voice message (wx_type == 34) ─────────────────────────
        elif wx_type == 34:
            content = raw_xml or "[语音]"

        # ── 5. Text message (wx_type == 1) ───────────────────────────
        elif wx_type == 1:
            # Fallback @mention check if user typed @BotName manually without native mention_source
            if is_group and not is_at and self_wxid:
                prof = _refresh_self_profile(service)
                bot_name = prof.get("name", "")
                if bot_name and f"@{bot_name}" in content:
                    is_at = True

        msg_id = int(data.get("native_message_id") or data.get("sequence") or int(time.time() * 1000))
        payload = {
            "id": msg_id,
            "type": out_type,
            "is_group": is_group,
            "roomid": roomid if is_group else "",
            "sender": sender,
            "sender_name": sender_name,
            "content": content,
            "is_at": is_at,
            "ts": int(time.time()),
            "extra": extra,
        }

        _msg_count += 1
        preview = content[:80].replace("\n", " ")
        _last_msg_summary = (
            f"[{'群:' + roomid if is_group else '私聊'}] "
            f"{sender_name}({sender}): {preview}"
        )
        logger.info(f"Inbound V4 msg #{_msg_count} (type={out_type}, is_at={is_at}): {_last_msg_summary}")

        if _main_loop and not _main_loop.is_closed():
            asyncio.run_coroutine_threadsafe(_broadcast_ws(payload), _main_loop)
    except Exception as e:
        logger.error(f"Error processing native V4 message: {e}", exc_info=True)


async def _broadcast_ws(payload: dict):
    """Send message payload to all connected WebSocket clients."""
    dead: set[WebSocket] = set()
    for ws in list(_ws_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    _ws_clients.difference_update(dead)


def _service_connection_worker():
    """Background daemon that manages NativeTextService connection and
    auto-reconnects if WeChat 4.1 starts up later or switches accounts.
    """
    global _svc, _receiving
    logger.info("Starting WeChat 4.1 NativeTextService connection worker...")
    while not _stop_worker.is_set():
        svc = _svc
        if not _is_service_active(svc):
            _receiving = False
            with _svc_lock:
                if _svc is not None:
                    try:
                        _svc.stop()
                    except Exception:
                        pass
                    _svc = None

                candidate = GatewayNativeTextService(
                    account=PREFERRED_ACCOUNT,
                    media_directory=RUNTIME_DIR / "images",
                    file_directory=RUNTIME_DIR / "files",
                    emoticon_directory=RUNTIME_DIR / "emoticons",
                    voice_directory=RUNTIME_DIR / "voices",
                )
                candidate.register_msg_handler(_on_native_message)
                try:
                    candidate.start()
                    _svc = candidate
                    _receiving = True
                    prof = _refresh_self_profile(candidate)
                    logger.info(
                        f"Connected to WeChat 4.1! self_wxid={candidate.self_wxid}, "
                        f"name={prof.get('name', '')}, pid={candidate.pid}"
                    )
                except Exception as e:
                    logger.debug(f"WeChat 4.1 connection attempt waiting: {e}")
                    try:
                        candidate.stop()
                    except Exception:
                        pass
                    _svc = None
                    _receiving = False

        _stop_worker.wait(5.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _main_loop, _svc, _receiving
    _main_loop = asyncio.get_running_loop()
    _stop_worker.clear()
    worker = threading.Thread(target=_service_connection_worker, name="WcfV4Connector", daemon=True)
    worker.start()
    yield
    logger.info("Shutting down WeChat 4.1 Gateway Server...")
    _stop_worker.set()
    with _svc_lock:
        if _svc is not None:
            try:
                _svc.stop()
                logger.info("NativeTextService stopped and bridge unloaded cleanly.")
            except Exception as e:
                logger.warning(f"Error stopping NativeTextService: {e}")
            _svc = None
            _receiving = False


app = FastAPI(title="WeChat 4.1 Native Gateway (v4)", lifespan=lifespan)


# ── Outgoing Text / Media Helpers ────────────────────────────────────
def _normalize_outgoing_text(text: str) -> str:
    """Clean literal escaped newlines and stray markdown symbols before sending to WeChat."""
    if not text:
        return text
    if "\\n" in text and "\n" not in text:
        text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    text = text.replace("\\t", " ")
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"(?m)^#{1,6}\s+", "", text)
    return text


def _split_utf8_chunks(text: str, max_bytes: int = 950) -> list[str]:
    """Split text into chunks whose UTF-8 byte length is <= max_bytes (under text_bridge.dll's 1023-byte limit)."""
    if not text:
        return []
    if len(text.encode("utf-8")) <= max_bytes:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining.encode("utf-8")) <= max_bytes:
            chunks.append(remaining)
            break
        # Find safe character prefix within max_bytes
        low, high = 1, len(remaining)
        best = 1
        while low <= high:
            mid = (low + high) // 2
            if len(remaining[:mid].encode("utf-8")) <= max_bytes:
                best = mid
                low = mid + 1
            else:
                high = mid - 1
        candidate = remaining[:best]
        # Prefer splitting at last newline if reasonably far into the chunk
        nl_idx = candidate.rfind("\n")
        if nl_idx > best // 3:
            split_at = nl_idx + 1
        else:
            split_at = best
        chunk = remaining[:split_at].rstrip("\n")
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].lstrip("\n")
    return chunks or [text[:200]]


def _materialize_media_input(
    *,
    path: str = "",
    url: str = "",
    b64_str: str = "",
    filename: str = "",
    default_ext: str = ".bin",
    auto_image_ext: bool = False,
) -> tuple[Optional[Path], bool, Optional[str]]:
    """Resolve path, URL, or base64 into a local file inside GATEWAY_MEDIA_DIR.
    Returns (local_path, is_temp_file, error_msg).
    """
    raw_bytes: Optional[bytes] = None
    ext = os.path.splitext(filename)[1].lower() if filename else ""

    if path and os.path.exists(path):
        # Copy into GATEWAY_MEDIA_DIR so _no_links validation never hits symlinked temp dirs
        try:
            with open(path, "rb") as f:
                raw_bytes = f.read()
            if not ext:
                ext = os.path.splitext(path)[1].lower()
        except Exception as e:
            return None, False, f"Failed to read local file {path}: {e}"
    elif b64_str:
        try:
            clean_b64 = b64_str
            if "," in clean_b64 and clean_b64.lstrip().startswith("data:"):
                header, clean_b64 = clean_b64.split(",", 1)
                if not ext:
                    if "gif" in header:
                        ext = ".gif"
                    elif "png" in header:
                        ext = ".png"
                    elif "webp" in header:
                        ext = ".webp"
                    elif "jpeg" in header or "jpg" in header:
                        ext = ".jpg"
                    elif "mp3" in header or "mpeg" in header:
                        ext = ".mp3"
                    elif "wav" in header:
                        ext = ".wav"
            raw_bytes = base64.b64decode(clean_b64)
        except Exception as e:
            return None, False, f"Failed to decode base64 media: {e}"
    elif url:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw_bytes = resp.read()
            if not ext:
                url_path = url.split("?", 1)[0]
                ext = os.path.splitext(url_path)[1].lower()
        except Exception as e:
            return None, False, f"Failed to download media URL: {e}"
    else:
        return None, False, "No valid path, url, or base64 provided"

    if not raw_bytes:
        return None, False, "Media payload is empty"

    if auto_image_ext:
        _, detected_ext = _detect_image_mime_and_ext(raw_bytes)
        ext = detected_ext
    elif not ext:
        ext = default_ext

    safe_stem = Path(filename).stem if filename else f"media_{uuid.uuid4().hex[:10]}"
    safe_stem = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", safe_stem).strip(" .") or "media"
    target_path = GATEWAY_MEDIA_DIR / f"{safe_stem}_{uuid.uuid4().hex[:8]}{ext}"
    try:
        with open(target_path, "wb") as f:
            f.write(raw_bytes)
        return target_path, True, None
    except Exception as e:
        return None, False, f"Failed to write media file: {e}"


def _convert_audio_to_wav24k(src_path: Path) -> Path:
    """Convert an audio file (.mp3, .wav, .ogg, .m4a, etc.) into a 24kHz mono 16-bit PCM WAV
    capped at 59 seconds so wxbot_framework.voices.prepare_voice accepts it cleanly.
    """
    ffmpeg_exe = None
    try:
        import imageio_ffmpeg
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        ffmpeg_exe = shutil.which("ffmpeg")
    if not ffmpeg_exe:
        raise RuntimeError("ffmpeg / imageio-ffmpeg is not available for audio transcoding")

    wav_path = GATEWAY_MEDIA_DIR / f"voice_{uuid.uuid4().hex[:10]}.wav"
    cmd = [
        ffmpeg_exe,
        "-y",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(src_path),
        "-t",
        "59",
        "-ac",
        "1",
        "-ar",
        "24000",
        "-acodec",
        "pcm_s16le",
        "-bitexact",
        str(wav_path),
    ]
    proc = subprocess.run(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if proc.returncode != 0 or not wav_path.exists() or wav_path.stat().st_size == 0:
        err_text = proc.stderr.decode("utf-8", errors="replace").strip()
        wav_path.unlink(missing_ok=True)
        raise RuntimeError(f"ffmpeg audio conversion failed: {err_text}")
    return wav_path


def _normalize_moment_image(src_path: Path) -> Path:
    """Ensure moment image is a standard .jpg or .png <= 5MB as required by moments_transport."""
    with open(src_path, "rb") as f:
        header = f.read(16)
    if header.startswith(b"\xff\xd8\xff") and src_path.suffix.lower() in (".jpg", ".jpeg"):
        return src_path
    if header.startswith(b"\x89PNG\r\n\x1a\n") and src_path.suffix.lower() == ".png":
        return src_path

    # Convert GIF (first frame), WebP, BMP, or mismatched extension to JPEG
    from PIL import Image

    out_path = GATEWAY_MEDIA_DIR / f"moment_{uuid.uuid4().hex[:10]}.jpg"
    with Image.open(src_path) as img:
        img.seek(0)
        rgb = img.convert("RGB")
        rgb.save(out_path, format="JPEG", quality=92)
    return out_path


# ── Request Models ───────────────────────────────────────────────────
class SendTextRequest(BaseModel):
    receiver: str
    msg: str
    aters: str = ""


class SendImageRequest(BaseModel):
    receiver: str
    path: str = ""
    url: str = ""
    base64: str = ""


class SendFileRequest(BaseModel):
    receiver: str
    path: str = ""
    url: str = ""
    base64: str = ""
    filename: str = ""


class SendRichTextRequest(BaseModel):
    receiver: str
    name: str = ""
    account: str = ""
    title: str = ""
    digest: str = ""
    url: str = ""
    thumburl: str = ""


class SendVoiceRequest(BaseModel):
    receiver: str
    path: str = ""
    url: str = ""
    base64: str = ""
    filename: str = ""


class SendMomentRequest(BaseModel):
    content: str = ""
    images: list[str] = []
    visibility: str = "public"


# ── HTTP & WebSocket Endpoints ───────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    svc = _svc
    active = _is_service_active(svc)
    wxid = svc.self_wxid if active else ""
    prof = _get_cached_self_profile(svc) if active else {}
    nickname = prof.get("name") or "N/A" if active else "Not Logged In"
    status_color = "#10b981" if active else "#ef4444"
    status_text = f"Connected — {nickname} ({wxid})" if active else "Waiting for WeChat 4.1 Login..."

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>WeChat 4.1 Native Gateway (v4)</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               background: #0f172a; color: #e2e8f0; display: flex; justify-content: center;
               align-items: center; min-height: 100vh; margin: 0; }}
        .card {{ background: #1e293b; border-radius: 16px; padding: 32px; max-width: 560px;
                width: 90%; box-shadow: 0 20px 60px rgba(0,0,0,0.5); }}
        h1 {{ margin-top: 0; font-size: 24px; color: #38bdf8; }}
        .badge {{ display: inline-block; padding: 6px 14px; border-radius: 9999px;
                 background: {status_color}22; color: {status_color}; font-weight: 600;
                 border: 1px solid {status_color}; margin-bottom: 16px; }}
        .stat {{ background: #0f172a; padding: 12px 16px; border-radius: 8px; margin: 8px 0;
                font-family: monospace; font-size: 13px; word-break: break-all; }}
        a {{ color: #38bdf8; text-decoration: none; }}
    </style>
</head>
<body>
    <div class="card">
        <h1>💬 WeChat 4.1 Native Gateway (v4)</h1>
        <div class="badge">{status_text}</div>
        <div class="stat">Engine: WeChat 4.1 Native Bridge (v4) | PID: {getattr(svc, 'pid', None) if active else 'N/A'}</div>
        <div class="stat">Receiving Messages: {_receiving}</div>
        <div class="stat">WebSocket Clients (LangBot): {len(_ws_clients)}</div>
        <div class="stat">Messages Processed: {_msg_count}</div>
        <div class="stat">Last Message: {html.escape(_last_msg_summary)}</div>
        <p style="font-size:13px; color:#94a3b8; margin-top:16px;">
            API: <a href="/api/status">/api/status</a> |
            Send: <code>POST /api/send</code> |
            Voice: <code>POST /api/send_voice</code> |
            Moment: <code>POST /api/send_moment</code> |
            WS: <code>ws://localhost:{HTTP_PORT}/ws</code>
        </p>
    </div>
</body>
</html>"""


@app.get("/qrcode", response_class=HTMLResponse)
async def qrcode_page():
    return """<html><body style="font-family:sans-serif;background:#0f172a;color:#e2e8f0;display:flex;justify-content:center;align-items:center;height:100vh;">
    <div style="background:#1e293b;padding:32px;border-radius:16px;max-width:480px;text-align:center;">
        <h2 style="color:#38bdf8;">WeChat 4.1 Native Desktop Mode</h2>
        <p>Please open WeChat 4.1 on the Windows desktop and scan the QR code or click Log In directly in the WeChat client.</p>
        <p><code>server_v4.py</code> will automatically detect the logged-in process and attach within 5 seconds.</p>
        <p><a href="/" style="color:#38bdf8;">Return to Dashboard</a></p>
    </div></body></html>"""


@app.get("/api/status")
async def api_status():
    svc = _svc
    active = _is_service_active(svc)
    wxid = svc.self_wxid if active else ""
    user_info = _get_cached_self_profile(svc) if active else {}
    return {
        "is_login": active,
        "self_wxid": wxid,
        "user_info": user_info,
        "is_receiving_msg": _receiving,
        "msg_count": _msg_count,
        "last_msg": _last_msg_summary,
        "ws_clients": len(_ws_clients),
        "engine": "v4",
    }


@app.post("/api/send")
async def api_send(req: SendTextRequest):
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "message": "WeChat 4.1 is not connected"}, status_code=503)

    try:
        def _do_send() -> tuple[bool, int, str]:
            msg_text = _normalize_outgoing_text(req.msg)
            at_list: list[str] = []
            if req.aters and "@chatroom" in req.receiver:
                at_list = [a.strip() for a in req.aters.split(",") if a.strip()]
                prefix_parts: list[str] = []
                for wxid in at_list:
                    dname = _resolve_sender_name(wxid, req.receiver)
                    if f"@{dname}" not in msg_text:
                        prefix_parts.append(f"@{dname}\u2005")
                if prefix_parts:
                    msg_text = "".join(prefix_parts) + msg_text

            chunks = _split_utf8_chunks(msg_text, max_bytes=950)
            if not chunks:
                return True, 0, msg_text

            for idx, chunk in enumerate(chunks):
                if idx == 0 and at_list and "@chatroom" in req.receiver:
                    try:
                        ok = svc.send_at_text(req.receiver, chunk, at_list[:8])
                        if ok:
                            continue
                    except Exception as e:
                        logger.warning(f"send_at_text failed ({e}), falling back to send_text")
                ok = svc.send_text(req.receiver, chunk)
                if not ok:
                    return False, len(chunks), msg_text
                if len(chunks) > 1 and idx < len(chunks) - 1:
                    time.sleep(0.25)
            return True, len(chunks), msg_text

        ok, num_chunks, final_text = await asyncio.to_thread(_do_send)
        logger.info(
            f"Sent V4 text to {req.receiver} (aters={req.aters}, chunks={num_chunks}, ok={ok}): "
            f"{final_text[:60]}..."
        )
        return {"status": 0 if ok else -1, "message": "ok" if ok else "send_text returned False"}
    except Exception as e:
        logger.error(f"Error sending V4 text: {e}", exc_info=True)
        return JSONResponse({"status": -1, "message": str(e)}, status_code=500)


@app.post("/api/send_image")
async def api_send_image(req: SendImageRequest):
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "message": "WeChat 4.1 is not connected"}, status_code=503)

    img_path, is_temp, err = await asyncio.to_thread(
        lambda: _materialize_media_input(
            path=req.path,
            url=req.url,
            b64_str=req.base64,
            default_ext=".jpg",
            auto_image_ext=True,
        )
    )
    if err or img_path is None:
        return JSONResponse({"status": -1, "message": err or "Invalid image"}, status_code=400)

    try:
        def _do_send_img() -> tuple[bool, str]:
            # If the image is a GIF, try sending as a native Type 47 animated emoticon first!
            if img_path.suffix.lower() == ".gif":
                try:
                    if svc.send_emoticon(req.receiver, img_path):
                        return True, "emoticon"
                except Exception as emo_err:
                    logger.info(f"GIF send_emoticon fallback to send_image ({emo_err})")
            ok = svc.send_image(req.receiver, img_path)
            return bool(ok), "image"

        ok, mode = await asyncio.to_thread(_do_send_img)
        logger.info(f"Sent V4 {mode} to {req.receiver} (path={img_path.name}, ok={ok})")
        return {"status": 0 if ok else -1, "mode": mode, "message": "ok" if ok else "send_image failed"}
    except Exception as e:
        logger.error(f"Error sending V4 image: {e}", exc_info=True)
        return JSONResponse({"status": -1, "message": str(e)}, status_code=500)
    finally:
        if is_temp and img_path is not None:
            try:
                img_path.unlink(missing_ok=True)
            except Exception:
                pass


@app.post("/api/send_file")
async def api_send_file(req: SendFileRequest):
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "message": "WeChat 4.1 is not connected"}, status_code=503)

    file_path, is_temp, err = await asyncio.to_thread(
        lambda: _materialize_media_input(
            path=req.path,
            url=req.url,
            b64_str=req.base64,
            filename=req.filename,
            default_ext=".mp3",
            auto_image_ext=False,
        )
    )
    if err or file_path is None:
        return JSONResponse({"status": -1, "message": err or "Invalid file"}, status_code=400)

    # Preserve clean display filename if requested
    send_target_path = file_path
    if req.filename:
        clean_name = os.path.basename(req.filename)
        named_dir = GATEWAY_MEDIA_DIR / f"f_{uuid.uuid4().hex[:8]}"
        named_dir.mkdir(parents=True, exist_ok=True)
        named_path = named_dir / clean_name
        try:
            shutil.copy2(file_path, named_path)
            send_target_path = named_path
        except Exception:
            send_target_path = file_path

    try:
        ok = await asyncio.to_thread(svc.send_file, req.receiver, send_target_path)
        logger.info(f"Sent V4 file to {req.receiver} (file={send_target_path.name}, ok={ok})")
        return {"status": 0 if ok else -1, "message": "ok" if ok else "send_file failed"}
    except Exception as e:
        logger.error(f"Error sending V4 file: {e}", exc_info=True)
        return JSONResponse({"status": -1, "message": str(e)}, status_code=500)
    finally:
        if is_temp and file_path is not None:
            try:
                file_path.unlink(missing_ok=True)
            except Exception:
                pass
        if send_target_path != file_path:
            try:
                send_target_path.unlink(missing_ok=True)
                send_target_path.parent.rmdir()
            except Exception:
                pass


@app.post("/api/send_rich_text")
async def api_send_rich_text(req: SendRichTextRequest):
    """WeChat 4.1 text_bridge does not have a raw XML rich-card export, so we
    format a clean music/link text card and send it via send_text.
    """
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "message": "WeChat 4.1 is not connected"}, status_code=503)

    lines: list[str] = []
    if req.title:
        lines.append(f"🎵 {req.title}")
    if req.digest:
        lines.append(f"🎤 {req.digest}")
    if req.url:
        lines.append(f"🔗 {req.url}")
    card_text = "\n".join(lines) if lines else (req.url or req.title or "[分享卡片]")

    try:
        chunks = _split_utf8_chunks(card_text, max_bytes=950)
        ok = False
        for chunk in chunks:
            ok = await asyncio.to_thread(svc.send_text, req.receiver, chunk)
        logger.info(f"Sent V4 rich_text fallback to {req.receiver} (title={req.title}, ok={ok})")
        return {"status": 0 if ok else -1, "message": "ok" if ok else "send_rich_text failed"}
    except Exception as e:
        logger.error(f"Error in V4 send_rich_text: {e}", exc_info=True)
        return JSONResponse({"status": -1, "message": str(e)}, status_code=500)


@app.post("/api/send_voice")
async def api_send_voice(req: SendVoiceRequest):
    """Send a native WeChat voice bubble (Type 34 SILK v3).
    Automatically transcodes MP3 / arbitrary WAV into 24kHz mono 16-bit PCM WAV (<=59s)
    before passing to NativeTextService.send_voice.
    """
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "message": "WeChat 4.1 is not connected"}, status_code=503)

    src_path, is_temp, err = await asyncio.to_thread(
        lambda: _materialize_media_input(
            path=req.path,
            url=req.url,
            b64_str=req.base64,
            filename=req.filename,
            default_ext=".mp3",
            auto_image_ext=False,
        )
    )
    if err or src_path is None:
        return JSONResponse({"status": -1, "message": err or "Invalid voice input"}, status_code=400)

    wav_path: Optional[Path] = None
    try:
        def _do_send_voice() -> bool:
            nonlocal wav_path
            # First check if src_path is already a valid SILK or conforming mono PCM WAV <= 60s
            try:
                return bool(svc.send_voice(req.receiver, src_path))
            except Exception:
                # Transcode via ffmpeg to 24kHz mono 16-bit PCM WAV (capped at 59s)
                wav_path = _convert_audio_to_wav24k(src_path)
                return bool(svc.send_voice(req.receiver, wav_path))

        ok = await asyncio.to_thread(_do_send_voice)
        logger.info(f"Sent V4 native voice to {req.receiver} (ok={ok})")
        return {"status": 0 if ok else -1, "message": "ok" if ok else "send_voice failed"}
    except Exception as e:
        logger.error(f"Error sending V4 voice: {e}", exc_info=True)
        return JSONResponse({"status": -1, "message": str(e)}, status_code=500)
    finally:
        if is_temp and src_path is not None:
            try:
                src_path.unlink(missing_ok=True)
            except Exception:
                pass
        if wav_path is not None:
            try:
                wav_path.unlink(missing_ok=True)
            except Exception:
                pass


@app.post("/api/send_moment")
async def api_send_moment(req: SendMomentRequest):
    """Publish a post (text and/or up to 9 images) to real WeChat 4.1 Moments (朋友圈)."""
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "message": "WeChat 4.1 is not connected"}, status_code=503)

    temp_files: list[Path] = []
    try:
        def _do_publish_moment() -> dict:
            norm_images: list[str] = []
            for raw_img in (req.images or [])[:9]:
                if not raw_img or not str(raw_img).strip():
                    continue
                s = str(raw_img).strip()
                if os.path.exists(s):
                    p, is_tmp, err = _materialize_media_input(path=s, auto_image_ext=True)
                elif s.startswith(("http://", "https://")):
                    p, is_tmp, err = _materialize_media_input(url=s, auto_image_ext=True)
                else:
                    p, is_tmp, err = _materialize_media_input(b64_str=s, auto_image_ext=True)
                if err or p is None:
                    logger.warning(f"Skipping invalid moment image: {err}")
                    continue
                if is_tmp:
                    temp_files.append(p)
                norm_p = _normalize_moment_image(p)
                if norm_p != p:
                    temp_files.append(norm_p)
                norm_images.append(str(norm_p))

            content_str = (req.content or "").strip()
            if not content_str and not norm_images:
                return {"_validation_error": "Moment requires non-empty content or at least one image"}

            payload = {
                "content": content_str,
                "images": norm_images,
                "visibility": req.visibility or "public",
            }
            res = svc.publish_moments(payload)
            return {
                "result": res,
                "num_images": len(norm_images),
                "visibility": payload["visibility"],
                "content_preview": content_str[:50],
            }

        out = await asyncio.to_thread(_do_publish_moment)
        if "_validation_error" in out:
            return JSONResponse({"status": -1, "message": out["_validation_error"]}, status_code=400)

        result = out["result"]
        status_str = result.get("status", "") if isinstance(result, dict) else ""
        ok = status_str == "published"
        logger.info(
            f"Published V4 WeChat Moment (images={out['num_images']}, visibility={out['visibility']}, "
            f"result={result}): {out['content_preview']}..."
        )
        return {
            "status": 0 if ok else -1,
            "result": result,
            "message": "ok" if ok else f"publish_moments status={status_str}",
        }
    except Exception as e:
        logger.error(f"Error publishing V4 moment: {e}", exc_info=True)
        return JSONResponse({"status": -1, "message": str(e)}, status_code=500)
    finally:
        for tf in temp_files:
            try:
                tf.unlink(missing_ok=True)
            except Exception:
                pass


@app.get("/api/contacts")
async def api_contacts():
    """Return contact list compatible with server.py."""
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "contacts": [], "message": "WeChat 4.1 is not connected"}, status_code=503)
    try:
        snapshot = await asyncio.to_thread(svc.contacts)
        raw_items = snapshot.get("contacts", []) if isinstance(snapshot, dict) else []
        cleaned: list[dict] = []
        for c in raw_items:
            if not isinstance(c, dict):
                continue
            wxid = c.get("wxid", "")
            name = c.get("display_name") or c.get("nickname") or c.get("remark") or wxid
            if wxid and name:
                _contact_name_cache[wxid] = name
            cleaned.append(
                {
                    "wxid": wxid,
                    "name": name,
                    "remark": c.get("remark", ""),
                    "code": c.get("alias", ""),
                }
            )
        return {"status": 0, "contacts": cleaned}
    except Exception as e:
        logger.warning(f"Error in V4 /api/contacts: {e}")
        return {"status": -1, "contacts": [], "message": str(e)}


@app.get("/api/chatroom_members")
async def api_chatroom_members(roomid: str):
    """Return {wxid: display_name} map for a group chatroom."""
    svc = _svc
    if not _is_service_active(svc):
        return JSONResponse({"status": -1, "members": {}, "message": "WeChat 4.1 is not connected"}, status_code=503)
    try:
        members = await asyncio.to_thread(_get_room_members, roomid)
        return {"status": 0, "roomid": roomid, "members": members}
    except Exception as e:
        return JSONResponse({"status": -1, "members": {}, "message": str(e)}, status_code=500)


@app.get("/api/user_name")
async def api_user_name(wxid: str, roomid: str = ""):
    """Query display name for a single wxid (optionally within a chatroom)."""
    name = await asyncio.to_thread(_resolve_sender_name, wxid, roomid)
    return {"status": 0, "wxid": wxid, "name": name}


@app.post("/api/inbound")
async def api_inbound(payload: dict):
    """Manual inbound message injection endpoint (for testing)."""
    global _msg_count, _last_msg_summary
    _msg_count += 1
    _last_msg_summary = f"[Manual] {payload.get('sender', '?')}: {str(payload.get('content', ''))[:60]}"
    await _broadcast_ws(payload)
    return {"status": 0, "clients": len(_ws_clients)}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    logger.info(f"LangBot WebSocket client connected to V4 gateway (total: {len(_ws_clients)})")
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"WebSocket closed: {e}")
    finally:
        _ws_clients.discard(ws)
        logger.info(f"LangBot WebSocket client disconnected (remaining: {len(_ws_clients)})")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
