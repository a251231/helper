# helper

Windows 微信机器人 Python 插件框架，提供本地网页管理台、控制台入口以及 **WeChat 4.1 V4 原生桥接网关 (`server_v4.py`)**。

[在线文档](https://prometheus-first.github.io/helper/) · [快速开始](docs/guide/quick-start.md) · [插件开发](docs/development/plugins.md)

**使用许可：允许个人自用，禁止商业使用。** 允许个人学习研究及非商业修改、免费分享，分享时须保留协议和版权声明。完整条款见 [LICENSE](LICENSE)，第三方组件继续适用各自的许可证。

## 功能

- 多账号管理：发现已登录微信，按账号启停，独立保存插件配置与数据。
- 联系人：扫描当前账号的好友，搜索及按标签筛选，查看昵称、备注、微信号、地区和签名。
- 消息收发：文本、图片、文件、表情包、语音条，支持真实群艾特与群成员身份查询。
- 消息监控：显示时间、会话 ID、发送者及正文，展开合并转发记录，保留撤回标记。
- 群聊插件：进退群通知、成员昵称绑定、聊天总结、人格切换、成语游戏和图库取图。
- 模型与语音：支持 OpenAI 兼容的 Chat Completions API、Fish Audio 合成和按群切换音色。
- 插件二开：在网页修改业务函数与配置，保存生效，支持恢复默认代码和配置。

自用版另有原生红包自动领取功能，不包含在此公开仓库；需要可联系交流群群主。

防撤回分为文本（含表情包）、图片、文件三个独立插件，均默认开启。好友自动通过默认关闭。完整指令见[群聊功能](docs/guide/features.md)和[控制台命令](docs/guide/commands.md)。

## 快速开始

运行条件：Windows 10/11 x64、64 位 Python（已验证 3.11 / 3.13）、Microsoft Visual C++ x64 运行库。

**当前适配微信 4.1.15.13 / 4.1.15.56 的指定构建。** 程序按核心 DLL 指纹校验，未知构建会拒绝加载。日常使用仓库内的两个桥接 DLL，无需 Visual Studio、Ghidra 或重新编译。各接口的实测范围见 [native/README.md](native/README.md)。

```powershell
python -m pip install -r requirements.txt
```

登录电脑微信后，双击 `start_web.bat`，打开 `http://127.0.0.1:8765`，点击账号行的“启动”；也可以勾选多个账号批量启动。首次使用无需手填 wxid。顶部账号选择用于切换正在编辑的配置。

需要控制台操作时运行：

```powershell
python main.py --check
python main.py
```

缺失的本地配置会自动补齐，已有配置保留。`--check` 检查配置与插件语法，不连接微信或调用模型。发送 WAV 语音和使用语音合成时，还需安装 `requirements-voice.txt`。

正常退出：网页先停止机器人，再在管理台窗口按 Ctrl+C；控制台入口输入 `quit`。关闭浏览器不会停止机器人。同一微信进程只能由一个项目副本连接。

## WeChat 4.1 V4 原生桥接网关 (`server_v4.py`)

本仓库内置了面向 **LangBot / WeChatFerry 协议 100% 兼容** 的微信 4.1 HTTP + WebSocket 原生桥接网关 [`server_v4.py`](server_v4.py)，无需更改上层机器人与插件协议即可直接接入微信 `4.1.15.13` / `4.1.15.56`，并支持微信 4.1 专属增强能力：

### 安装网关依赖与启动

```powershell
python -m pip install -r requirements-gateway.txt
python server_v4.py
```

（也可直接双击 `start_gateway_v4.bat` 启动，默认监听 `0.0.0.0:8080`，可通过环境变量 `WCF_PORT` 指定端口、`WXBOT_ACCOUNT` 指定目标微信号）。

### 核心接口一览

| 接口 | 方法 | 说明 |
| --- | --- | --- |
| `/api/status` | `GET` | 网关健康状态、当前登录 `self_wxid`、昵称与 WebSocket 客户端数 (`engine: "v4"`) |
| `/ws` | `WS` | 实时推送微信入站消息（文本、图片 `<img_b64>`、文件 `<file_b64>`、引用、合并转发 `<type>19</type>` 等） |
| `/api/send` | `POST` | 发送文本消息与真实群 `@` 提醒（超长文本自动按 UTF-8 字节安全分片） |
| `/api/send_image` | `POST` | 发送图片（检测到 `.gif` 时自动优先以 **Type 47 原生微信动画表情包** 发送） |
| `/api/send_voice` | `POST` | 发送 **Type 34 原生微信语音条 (SILK v3)**，支持 `.mp3` / `.wav` 自动转码为 24kHz 单声道 PCM |
| `/api/send_moment` | `POST` | 发布真实 **微信朋友圈 (Moments)**，支持纯文本或最多 9 张图文动态 |
| `/api/send_file` | `POST` | 发送任意文件（支持本地路径、URL、Base64 与自定义文件名） |
| `/api/contacts` | `GET` | 读取当前登录微信内存联系人列表 |
| `/api/chatroom_members` | `GET` | 读取指定群聊 (`roomid`) 成员 `{wxid: display_name}` 映射 |
| `/api/user_name` | `GET` | 查询指定 `wxid` 的群内昵称或好友显示名 |

## 配置与数据

| 文件 | 用途 |
| --- | --- |
| `bot_config.json` | 后端、会话范围、插件开关与选项 |
| `members_config.json` | 各群成员称呼与功能范围 |
| `llm_config.json` | 模型地址、密钥及模型名称 |
| `tts_config.json` | Fish Audio 密钥及语音选项 |

网页可编辑以上配置。多账号工作区位于 `.runtime/robots/`；聊天记录、媒体缓存和日志均为本地数据，已被 Git 忽略，请勿提交。具体字段见[配置参考](docs/guide/configuration.md)。

## 文档

| 需要做什么 | 文档 |
| --- | --- |
| 安装、选择账号、添加群 | [快速开始](docs/guide/quick-start.md) · [账号与群聊](docs/guide/account-and-groups.md) |
| 启停账号、监控消息、修改插件 | [网页管理台](docs/guide/web-panel.md) |
| 扫描好友、查看资料和标签 | [扫描联系人](docs/guide/web-panel.md#扫描联系人) |
| 图片、文件、表情和语音 | [图片](docs/messages/images.md) · [文件](docs/messages/files.md) · [表情](docs/messages/emoticons.md) · [语音](docs/messages/voice.md) |
| 合并转发与防撤回 | [合并转发](docs/messages/forwarded.md) · [防撤回](docs/messages/recall.md) |
| 好友申请、群成员与群管理 | [好友申请与群管理](docs/guide/friends-and-groups.md) |
| 开发和维护 | [插件开发](docs/development/plugins.md) · [代码结构与验证](docs/development/structure.md) |

## 目录

```text
server_v4.py          微信 4.1 HTTP + WebSocket 原生桥接网关入口
watchdog_v4.py        微信 4.1 与 V4 网关进程守护脚本
main.py / web.py      控制台 / 网页入口
wxbot_framework/      连接生命周期、消息、插件、模型和管理台
plugins/              独立功能与业务函数
native/               原生桥接、版本指纹与必要 DLL
webui/                管理台静态页面
docs/                 使用与开发文档
tests/                离线回归测试
tools/                配置初始化、测试与发布检查
```

## 交流

<details>
<summary>交流群二维码</summary>

<img width="630" height="952" alt="image" src="https://github.com/user-attachments/assets/cd2e28c8-e353-4c90-b425-5ddcf491ddc4" />


</details>

[linux.do 项目讨论](https://linux.do/t/topic/3029497/51)
