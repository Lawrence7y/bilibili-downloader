# CLAUDE.md — BillBill Downloader 开发指南

本文档为在 **BillBill Downloader** 代码库中进行开发、调试与重构的 AI 助手与开发者提供全局架构规范、设计决策及常用命令。

---

## 1. 系统架构与技术栈

项目采用 **前后端分离 + 多进程分层协作架构**：

```
[浏览器 / Vue3 前端] (:18080)
       │ HTTP REST API (任务管理/设置/解析) + WebSocket (实时进度推送)
       ▼
[Rust Axum Server] (crates/server)
       │ JSON-RPC 2.0 (NDJSON over stdin/stdout)
       ▼
[Python Sidecar] (sidecar/server.py)
       ├── douyin.py   — 抖音逆向解析与主页批量抓取 (基于 f2)
       ├── ytdlp.py    — Bilibili/YouTube 等多平台解析 (基于 yt-dlp)
       └── generic.py  — 通用网页嗅探 (HTML5/DPlayer/ArtPlayer/iframe/Playwright)
       │
       ▼
[Rust Core Engine] (crates/core-engine)
       ├── ChunkedDownloader — Range 分块多线程下载 (16~32线程 + BufWriter内存落盘)
       ├── FFmpegController  — 原生 M3U8 转储 / DASH 音画分离流合并 / 音频抽取
       ├── RateLimiter       — 令牌桶流量整形限速
       └── Database          — SQLite + FTS5 全文搜索历史记录归档
```

### 关键技术栈：
* **后端**：Rust (Edition 2021), Axum 0.7, Tokio (多线程异步运行时), Reqwest, Rusqlite, Which.
* **Sidecar**：Python 3.11+, f2 0.0.1.7+, yt-dlp 2024+, Playwright 1.40+.
* **前端**：Vue 3 (Composition API), Vite 5, Tailwind CSS 3, Lucide Vue Next.
* **多媒体工具**：FFmpeg (需要系统 PATH 或根目录下 `ffmpeg.exe`).

---

## 2. 常用开发与测试命令

### 编译与运行
```powershell
# 编译整个 Rust workspace
cargo build

# 仅编译服务端
cargo build -p server

# 生产模式编译
cargo build --release -p server

# 前端依赖安装与打包
cd frontend
npm install
npm run build
cd ..

# 一键启动（会自动检查并构建）
.\启动网页版.bat
```

### 自动化测试
```powershell
# 运行核心引擎单元测试与 Sidecar 真实进程 ping 集成测试
cargo test -p core-engine

# 运行服务器路由与路径测试
cargo test -p server

# 运行全部测试
cargo test
```

### Python Sidecar 单独调试
```powershell
# 使用专用虚拟环境测试 Sidecar Ping
bilibili-downloader/.venv/Scripts/python -c "
import asyncio, json
from sidecar.server import handle_request
asyncio.run(handle_request(json.dumps({'jsonrpc':'2.0','id':1,'method':'ping'})))
"

# 调试通用网页嗅探
bilibili-downloader/.venv/Scripts/python -c "
import asyncio
from sidecar.generic import resolve_generic_web
meta = asyncio.run(resolve_generic_web('https://www.91cg1.com/archives/122422/'))
print(meta.title, meta.streams[0].video_url)
"
```

---

## 3. 核心设计规范与关键实现细节

### 3.1 下载核心引擎（crates/core-engine）
1. **禁止单一使用 HEAD 请求探活**：
   * 国内大量 CDN（抖音火山引擎、B站、各类媒体 CDN）会拦截 HEAD 请求（返回 403/405）或不返回 `Accept-Ranges`。
   * `ChunkedDownloader::probe` 必须包含 `Range: bytes=0-0` 的轻量级 GET 探活；若返回 `206 Partial Content`，必须从 `Content-Range: bytes 0-0/总大小` 中精准提取文件总大小并激活多线程分块。
2. **磁盘写入必须使用 `BufWriter`**：
   * 在单流和并行分块下载中，必须将 `File` 包装在 `tokio::io::BufWriter::with_capacity(256 * 1024, file)` 中，严禁对每个几 KB 的网络 chunk 直接触发底层 Win32 I/O 写调用。
3. **并发分块策略（突破 CDN QoS 限速）**：
   * 基础切片数设定为 16 线程。根据视频大小自适应：<10MB (2~8线程)，10MB~50MB (16线程)，50MB~200MB (24线程)，>200MB (16~32线程)。
4. **M3U8 / HLS 协议下载**：
   * 当流格式为 `m3u8` 或 URL 包含 `.m3u8` 时，任务管理器必须路由给 `FFmpegController::download_m3u8` 处理，严禁直接交由 `ChunkedDownloader` 下载文本索引。
5. **DASH 音画分离流并行拉取**：
   * 视频流与音频流必须通过 `tokio::join!` 并发同时下载，下载完毕后再交给 FFmpeg 无损合并（`-c copy`）。

### 3.2 Python Sidecar 协议与并发调度（sidecar/）
1. **JSON-RPC 必须全并发**：
   * `sidecar/server.py` 的主循环必须通过 `asyncio.create_task` 处理每一行请求，保证多个解析任务或 ping 健康检查绝不因单个长请求发生串行阻塞。
   * 输出写回 stdout 必须加 `threading.Lock()`，保证多协程并发写入时单行 JSON 不交织穿插。
2. **环境自愈与防死代理**：
   * Sidecar 启动时必须执行 `_sanitize_proxy_env()`，探测本地环境变量中配置的代理端口。如果本地代理端口未监听（如残留的 8780），自动剔除该环境变量，避免抛出 `[WinError 10061]` 积极拒绝。
3. **抖音批量抓取频控与通知安全**：
   * `DouyinHandler` 实例化时必须显式设置 `enable_bark = False`，并在模块开头猴子补丁屏蔽 `f2.apps.bark`，防止向外网 `api.day.app` 发送无效推送造成数秒超时。
   * `kwargs["timeout"]` 会被 f2 用作分页休眠间隔，应控制在 `1.0` 秒以内，避免单页强制睡眠 8 秒。

### 3.3 通用网页嗅探器（sidecar/generic.py）
针对未知平台实施 **4 层递进式解析体系**：
* **Layer 1**：静态 HTML 正则与常见播放器语义解析（DPlayer `data-config`, ArtPlayer, MacCMS `player_aaaa`, HTML5 `<video src>` / `<source src>`, OpenGraph）。
* **Layer 2**：Iframe 递归穿透（提取 `<iframe src="...">` 并递归检查内部播放器）。
* **Layer 3**：yt-dlp 通用提取。
* **Layer 4**：Playwright 无头浏览器动态网络拦截（监听响应流量中的 `.m3u8` / `.mp4`，注入自动跳过 20 秒内前贴片广告脚本）。
* **过滤准则**：必须比对 `AD_TRACKING_DOMAINS`（过滤第三方广告流）与 `NON_VIDEO_MARKERS`（过滤缩略图 HLS/雪碧图流）。

---

## 4. 安全与开发红线

1. **严禁凭证入库**：
   * 严禁将真实 Cookie、`sessionid`、`SESSDATA` 等身份信息写入代码、测试用例或提交至 Git。
2. **路径逃逸防御**：
   * 任何通过 API（如 `/api/tasks`）传入的 `output_dir` 必须通过 `resolve_output_dir_checked` 验证，严格限制在项目根目录之内，防止任意路径写入攻击。
3. **Windows 编码与保留端口**：
   * 服务端默认绑定端口为 **`18080`**，严禁使用 8080（Windows/Hyper-V/NAT 动态保留端口段极易冲突）。
   * 所有跨进程标准输入输出（stdin/stdout）在 Windows 上必须显式配置为 UTF-8 编码。
