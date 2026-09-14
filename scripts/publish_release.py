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

    body_desc = """## 🚀 BillBill Downloader v1.0.1 (Rust Web 版)

- 修复部分网站下载到片头贴片广告的问题（扩充广告 CDN 屏蔽列表，过滤 `bxcdn.net` / `bkcdn.net` 等广告视频流）。
- 增强针对 Next.js / SSR 动态站点的元数据直出与免广告解析（毫秒级提取正片）。
- 新增对伪装成 PNG 图片的自定义流媒体（如 `roUd` chunk 混淆 HLS）的高性能多线程解包与自动转储支持。
- 实测 25 分钟无广告完整视频直接秒级解析并无损落盘。
"""

    # 1. Create release
    req_data = json.dumps({
        "tag_name": "v1.0.1",
        "target_commitish": "main",
        "name": "BillBill Downloader v1.0.1 (Rust Web 版)",
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
    asset_file = "dist/BillBillDownloader-v1.0.1-windows-x64.zip"
    asset_name = "BillBillDownloader-v1.0.1-windows-x64.zip"
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
