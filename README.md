# BillBill Downloader（Rust Web 版）

多平台音视频下载器：**抖音**（f2）+ **Bilibili / YouTube 等**（yt-dlp），本地 Web UI。

## 架构

```
Vue3 前端 (frontend/)
    │  HTTP + WebSocket
    ▼
Rust server (crates/server, axum :18080)
    │  JSON-RPC over stdin/stdout
    ▼
Python sidecar (sidecar/)
    ├── douyin.py   — f2 抖音解析/批量
    └── ytdlp.py    — yt-dlp 多平台兜底
    │
    ▼
Rust core-engine (crates/core-engine)
    ├── Range 分块下载
    ├── FFmpeg 合并 / 抽音频
    └── SQLite + FTS5 历史
```

## 快速启动

前置：

1. Rust toolchain（`cargo`）
2. Node.js 18+（构建前端）
3. Python 3.11+，venv 位于 `bilibili-downloader/.venv`
4. `ffmpeg` 在 PATH 或项目根目录

```powershell
# 一键（会按需编译 / 构建）
.\启动网页版.bat
```

或手动：

```powershell
cargo build -p server
cd frontend; npm install; npm run build; cd ..
.\target\debug\server.exe
# 浏览器打开 http://127.0.0.1:18080
```

## 配置

| 项 | 说明 |
|---|---|
| `DDL_PORT` | 可选，默认 `18080`（避开 Windows/Hyper-V 常保留的 8080） |
| `DDL_PROJECT_ROOT` | 可选。指定项目根目录；不设则自动向上查找 `sidecar/server.py` |
| Cookie | 在「系统设置」粘贴，可保存到浏览器 localStorage（仅本机） |
| 输出目录 | 默认 `<项目根>/downloads`；API 传入的 `output_dir` **必须**在项目根内 |

**不要**把真实 Cookie 提交进仓库。参考 `bilibili-downloader/cookies.example.txt`。

## API 摘要

| Method | Path | 说明 |
|---|---|---|
| GET | `/api/health` | sidecar / ffmpeg 健康检查 |
| GET/PUT | `/api/settings` | 下载偏好（输出目录/限速/并发/代理/封面/字幕） |
| POST | `/api/resolve` | 解析单链接 |
| POST | `/api/resolve/douyin/batch` | 抖音主页/合集批量 |
| POST | `/api/cookie/check` | Cookie 诊断 |
| GET/POST | `/api/tasks` | 任务列表 / 创建 |
| POST | `/api/tasks/:id/cancel` | 取消任务 |
| POST | `/api/tasks/:id/retry` | 重试失败/取消任务 |
| GET | `/api/history` | 历史记录 |
| GET | `/api/history/search?q=` | FTS 搜索 |
| WS | `/api/ws` | 进度推送 |

## 测试

```powershell
cargo test -p core-engine
cargo test -p server
python scripts/smoke_api.py   # 需先启动 server
```

集成测试会真实拉起 Python sidecar（需要 venv 存在）。

## Release 打包

```powershell
.\build_release.bat
# 产物在 dist\BillBillDL\
```

## 目录说明

| 路径 | 用途 |
|---|---|
| `crates/` | Rust workspace（server + core-engine） |
| `frontend/` | Vue3 + Vite + Tailwind |
| `sidecar/` | Python JSON-RPC sidecar |
| `bilibili-downloader/` | 旧版 Python GUI（**独立 git 仓库**，本仓库已 ignore）+ 项目共用 venv |
| `downloads/` | 默认下载目录与 SQLite 历史（gitignored） |

## 遗留说明

`bilibili-downloader/` 下仍是早期 tkinter/ttkbootstrap 桌面版，与 Web 版共用 venv。  
`docs/UI_REDESIGN_PLAN.legacy.md` 面向旧 GUI，已过时，仅作历史参考。
