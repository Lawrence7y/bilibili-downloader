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

    body_desc = """## 🚀 BillBill Downloader v1.0.2 (移动端适配与跨端升级版)

### ✨ 本次更新重点

- **全新移动端沉浸式 UI 适配**：
  - 手机端专属**底部毛玻璃导航栏 (Bottom Navigation Bar)**，支持单手舒适触控。
  - 手机端专属**顶部状态栏与安全区适配 (Safe Area Insets)**，完美贴合刘海屏与挖孔屏。
  - 单链接解析、批量抓取卡片自适应手机纵向流排版，触控按钮热区与动效全面提升。
  - 全新加入「后端服务地址配置」，手机 App / 局域网接入时一键直连电脑端下载核心。
- **全网通用网页视频嗅探与去广告增强**：
  - 支持 HTML5 视频、DPlayer、ArtPlayer、MacCMS 等任意非主流视频站点。
  - 彻底阻断片头视频广告，支持定制混淆 HLS (如 PNG roUd 封装) 极速无损转储。
- **极限并发下载引擎 (16~32 线程)** 与 Win32 内存缓冲 I/O。
"""

    # 1. Create release
    req_data = json.dumps({
        "tag_name": "v1.0.2",
        "target_commitish": "main",
        "name": "BillBill Downloader v1.0.2 (移动端 UI 适配版)",
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
    asset_file = "dist/BillBillDownloader-v1.0.2-windows-x64.zip"
    asset_name = "BillBillDownloader-v1.0.2-windows-x64.zip"
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
