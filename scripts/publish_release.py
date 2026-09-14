import json
import os
import subprocess
import urllib.request

def get_git_token() -> str:
    p = subprocess.Popen(
        ["git", "credential", "fill"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, _ = p.communicate(input="protocol=https\nhost=github.com\n\n")
    for line in stdout.splitlines():
        if line.startswith("password="):
            return line.split("=", 1)[1]
    raise RuntimeError("GitHub token not found in git credential manager")

def publish():
    token = get_git_token()
    repo = "Lawrence7y/bilibili-downloader"
    api_url = f"https://api.github.com/repos/{repo}/releases"

    body_desc = """## 🚀 BillBill Downloader v1.0.0 (Rust Web 版)

全新一代高性能全平台多媒体与通用网页视频下载器！

### ✨ 核心亮点

- **全网通用网页嗅探 (Universal Web Media Resolver)**：
  - 支持 HTML5 视频、DPlayer、ArtPlayer、MacCMS、嵌入式 Iframe 递归解析。
  - 支持 Playwright 动态无头网络拦截，自动跳过前贴片广告。
  - 原生 M3U8 (HLS) 流无损合并转储与 AES-128 自动解密。
- **极限并发下载引擎 (Rust Core Engine)**：
  - 8~32 线程激进自适应分块切片，彻底突破国内各大 CDN 的单连接 QoS 限速。
  - 独创轻量 Range GET 206 探活机制，100% 激活分块并行下载。
  - 引入 256KB BufWriter 内存写入缓冲，消除 95% 以上磁盘 I/O 阻塞。
  - DASH 视频轨与音频轨并发双流同时下载，时间减半。
- **主流平台深度支持**：
  - 抖音单视频、图文多图、主页/合集作品批量高速抓取。
  - Bilibili / YouTube 高清流与独立音频提取 (MP3/M4A/FLAC/WAV)。
- **现代化体验**：
  - 本地原生文件夹选择器 (rfd)。
  - WebSocket 微秒级实时进度/速度推送。
  - SQLite + FTS5 本地历史归档与全文搜索。

### 📦 运行说明

1. 下载解压 **BillBillDownloader-v1.0.0-windows-x64.zip**；
2. 确保电脑已安装 Python 3.11+ 和 FFmpeg；
3. 双击 `启动程序.bat` 即可自动启动并在浏览器中打开使用！
"""

    # 1. Create release
    req_data = json.dumps({
        "tag_name": "v1.0.0",
        "target_commitish": "main",
        "name": "BillBill Downloader v1.0.0 (Rust Web 版)",
        "body": body_desc,
        "draft": False,
        "prerelease": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        api_url,
        data=req_data,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "Release-Publisher",
        },
    )

    with urllib.request.urlopen(req) as resp:
        rel_data = json.loads(resp.read().decode("utf-8"))
        release_id = rel_data["id"]
        html_url = rel_data["html_url"]
        upload_url_template = rel_data["upload_url"]
        print(f"Created Release: {html_url} (ID: {release_id})")

    # 2. Upload asset
    asset_file = "dist/BillBillDownloader-v1.0.0-windows-x64.zip"
    asset_name = "BillBillDownloader-v1.0.0-windows-x64.zip"
    upload_url = upload_url_template.split("{")[0] + f"?name={asset_name}"

    with open(asset_file, "rb") as f:
        asset_bytes = f.read()

    upload_req = urllib.request.Request(
        upload_url,
        data=asset_bytes,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/zip",
            "Content-Length": str(len(asset_bytes)),
            "User-Agent": "Release-Publisher",
        },
    )

    with urllib.request.urlopen(upload_req) as resp:
        asset_data = json.loads(resp.read().decode("utf-8"))
        print(f"Asset uploaded successfully! Download URL: {asset_data['browser_download_url']}")

if __name__ == "__main__":
    publish()
