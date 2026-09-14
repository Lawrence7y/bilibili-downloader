# BillBill Downloader（Rust Web 版）

全平台多媒体与通用网页视频下载器：**抖音**（f2）+ **Bilibili / YouTube 等**（yt-dlp）+ **全网通用网页媒体嗅探（HTML5 / DPlayer / ArtPlayer / M3U8 / Playwright）**，结合 **Rust 高性能并发下载引擎** 与 **现代化 Vue3 Web UI**。

---

## ✨ 核心特性

### 1. 强大而统一的通用网页视频嗅探（打破平台限制）
* **4 层递进式通用解析**：对非主流/陌生网站（如各类 CMS、DPlayer、WordPress 博客、嵌套 iframe 播放器）提供统一支持。
  * **第 1 层（静态特征嗅探）**：毫秒级提取 HTML5 `<video>`、`<source>`、DPlayer `data-config`、ArtPlayer、MacCMS `player_aaaa` 等播放器配置与内联 `.m3u8` / `.mp4`。
  * **第 2 层（递归 Iframe 穿透）**：自动下钻 1~2 层播放器嵌入页，捕获跨域或嵌套流媒体。
  * **第 3 层（yt-dlp 通用增强）**：自动处理页面内嵌入的 YouTube、Vimeo、B 站第三方播放组件。
  * **第 4 层（Playwright 动态网络拦截）**：唤起 Chromium 内核监听底层网络流量，**自动快进跳过 5~20 秒前贴片广告**，拦截真实流媒体请求。
* **原生 M3U8 (HLS) 转储**：遇到 HLS 切片流自动调用底层 FFmpeg 进行无损快速封装与 AES-128 解密合并。

### 2. 深度优化的 Rust 高性能下载引擎
* **突破 CDN 单连接 QoS 限速**：支持 **8 ~ 16 ~ 32 线程** 动态激进并发分块切片，单视频下载轻松跑满百兆/千兆带宽。
* **智能 HTTP Range 探活**：使用轻量 `Range: bytes=0-0` 进行 206 探活，彻底解决国内 CDN 屏蔽 HEAD 请求导致退化单线程的行业痛点。
* **`BufWriter` 内存缓冲落盘**：每个分块配备 256KB 内存写入缓冲区，Win32 磁盘系统调用减少 **95% 以上**，彻底消除磁盘 I/O 阻塞。
* **DASH 音画分离双流并行下载**：B 站高清视频的音轨与视频轨通过 `tokio::join!` 同时并发拉取，下载耗时直接减半。
* **302 真实 CDN 节点预锁定**：预先捕获最终重定向地址，所有并发分块直连同台 CDN 边缘服务器，三次握手损耗归零。

### 3. 主流平台深度解析
* **抖音（f2 库驱动）**：
  * 支持单视频、图文笔记（多图一键打包下载）。
  * 支持用户主页、合集（Mix）批量快速抓取（单页步长优化至 35，耗时减少 50%）。
  * 自动拦截无用的 Bark 外网推送，过滤失效死代理，具备本地网络自愈能力。
* **Bilibili / YouTube 等（yt-dlp 驱动）**：
  * 支持高清 1080P / 4K DASH 音视频自动合并。
  * 独立音频抽取（导出为 MP3 / M4A / FLAC / WAV）。
  * 伴随下载封面图与字幕文件。

### 4. 现代化交互与任务调度
* **Vue3 + Tailwind 前端**：暗黑/明亮主题切换，任务实时速度、百分比与耗时显示。
* **实时 WebSocket 推送**：下载进度微秒级广播推送，告别页面轮询刷新。
* **原生文件夹选择器**：集成 Windows 原生目录挑选弹窗，无需手动手敲磁盘路径。
* **任务队列管控**：支持任务排队（Pending）、进行中、取消、失败一键重试、15 分钟终态垃圾回收（TTL）。
* **SQLite + FTS5 历史检索**：本地轻量数据库存储下载历史，支持标题全文检索。

---

## 🏗 系统架构

```
Vue3 前端 (frontend/)
    │  HTTP (REST API) + WebSocket (实时进度推送)
    ▼
Rust Web Server (crates/server, axum :18080)
    │  JSON-RPC 2.0 over stdin/stdout (全并发异步管道)
    ▼
Python Sidecar (sidecar/)
    ├── douyin.py   — f2 抖音高清解析 / 主页作品批量
    ├── ytdlp.py    — yt-dlp 多平台兜底
    └── generic.py  — 通用网页嗅探 (DOM特征/iframe穿透/Playwright动态拦截)
    │
    ▼
Rust Core Engine (crates/core-engine)
    ├── Range 分块并发下载 (16~32线程 + BufWriter内存缓冲)
    ├── FFmpeg 原生 M3U8 转储 / DASH音画双流合并 / 音频抽取
    └── SQLite + FTS5 历史索引库
```

---

## 🚀 快速启动

### 前置要求：
1. **Rust toolchain**（`cargo` 1.75+）
2. **Node.js 18+**（前端构建）
3. **Python 3.11+**（推荐 `bilibili-downloader/.venv`）
4. **FFmpeg**（加入系统 PATH，或放到项目根目录下 `ffmpeg.exe`）

### 一键启动（推荐）：
```powershell
.\启动网页版.bat
```
*脚本会自动按需编译 Rust 后端、构建前端，并在独立窗口启动服务，自动唤起浏览器打开 `http://127.0.0.1:18080`。*

### 手动构建与运行：
```powershell
# 1. 编译 Rust 后端
cargo build -p server

# 2. 构建前端页面
cd frontend
npm install
npm run build
cd ..

# 3. 运行服务端
.\target\debug\server.exe
# 浏览器访问 http://127.0.0.1:18080
```

---

## ⚙️ 配置说明

| 环境变量 / 配置项 | 说明 | 默认值 |
|---|---|---|
| `DDL_PORT` | HTTP 监听端口（默认避开 Hyper-V 保留的 8080） | `18080` |
| `DDL_PROJECT_ROOT` | 指定项目根目录（未配置则自动向上探测） | 自动解析 |
| Cookie | 可以在前端「系统设置」中粘贴保存（仅保存于本地浏览器 localStorage） | 无 |
| 输出目录 | 下载落盘目录，支持前端调用系统原生文件弹窗挑选 | `<项目根>/downloads` |
| 代理设置 | 可以在「系统设置」填入 HTTP/SOCKS5 代理（如 `http://127.0.0.1:8920`） | 直连 |

> ⚠️ **安全警告**：切勿将真实登录 Cookie 文件提交进公共 Git 仓库！参考 `bilibili-downloader/cookies.example.txt`。

---

## 📡 API 摘要

| 请求方式 | 路由 | 描述 |
|---|---|---|
| `GET` | `/api/health` | Sidecar 状态与 FFmpeg 可用性健康检查 |
| `GET`/`PUT`| `/api/settings` | 读取与保存下载配置（输出目录/限速/并发/代理等） |
| `POST` | `/api/dialog/pick-folder` | 调起操作系统原生文件夹选择弹窗 |
| `POST` | `/api/resolve` | 单链接解析（抖音 / yt-dlp / 通用网页嗅探） |
| `POST` | `/api/resolve/douyin/batch`| 抖音博主主页/合集批量视频抓取 |
| `POST` | `/api/cookie/check` | Cookie 有效性诊断 |
| `GET`/`POST`| `/api/tasks` | 获取当前任务列表 / 创建下载任务 |
| `POST` | `/api/tasks/:id/cancel` | 安全取消下载任务（即刻切断连接与落盘清理） |
| `POST` | `/api/tasks/:id/retry` | 重试失败或已取消的任务 |
| `GET` | `/api/history` | 分页获取历史下载记录 |
| `GET` | `/api/history/search?q=` | SQLite FTS5 全文搜索历史 |
| `WS` | `/api/ws` | WebSocket 实时进度、速度与百分比广播通道 |

---

## 🧪 测试与质量保证

```powershell
# 运行核心引擎与服务端测试（集成测试会自动探测并与真实 Sidecar 进行 ping 联调）
cargo test -p core-engine
cargo test -p server

# 检查前端构建
cd frontend && npm run build && cd ..
```

---

## 📂 项目结构

```text
├── crates/
│   ├── core-engine/    # Rust 核心引擎（Range分块、FFmpeg封装、SQLite、调度器）
│   └── server/         # Rust Axum Web 服务（REST API + WebSocket + 静态托管）
├── frontend/           # Vue3 + Vite + Tailwind CSS 现代化单页应用
├── sidecar/            # Python JSON-RPC 解析子进程
│   ├── server.py       # JSON-RPC 2.0 并发调度服务
│   ├── douyin.py       # 抖音逆向解析与主页批量抓取
│   ├── ytdlp.py        # yt-dlp 多平台流解析
│   ├── generic.py      # 通用网页媒体嗅探（DOM/iframe/Playwright网络拦截）
│   └── protocol.py     # 统一协议与数据结构
├── bilibili-downloader/# 早期 Python GUI 遗留仓库（共用 venv，已独立分离）
├── downloads/          # 默认下载与历史记录数据库存储路径
└── 启动网页版.bat       # Windows 桌面一键启动脚本
```

---

## 📄 开源许可证

本项目仅供个人学习、归档自己拥有合法权利的多媒体内容研究使用，请勿用于任何侵犯版权或违背相关平台服务协议的用途。
