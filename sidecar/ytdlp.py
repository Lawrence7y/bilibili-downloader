"""Multi-platform resolver wrapping yt-dlp (Bilibili, YouTube, Kuaishou, XHS, etc.)."""

from __future__ import annotations

from typing import Any
import yt_dlp

from sidecar.protocol import MediaMetadata, StreamInfo, SubItem


def check_bilibili_cookie(cookie: str | None) -> dict[str, Any]:
    """Check health of Bilibili cookie."""
    if not cookie or not cookie.strip():
        return {
            "valid": False,
            "missing": ["所有 Cookie"],
            "warnings": ["Cookie 为空，最高仅支持 360P/480P 标清下载"],
        }

    pairs: dict[str, str] = {}
    for item in cookie.split(";"):
        if "=" in item:
            k, v = item.strip().split("=", 1)
            pairs[k.strip()] = v.strip()

    required = {
        "SESSDATA": "登录会话（必需，用于获取 1080P/4K 等高清流）",
    }
    important = {
        "bili_jct": "CSRF 校验令牌",
        "DedeUserID": "用户 UID 标识",
    }

    missing = [f"{k} — {v}" for k, v in required.items() if not pairs.get(k)]
    warnings = [f"缺少 {k}（{v}）" for k, v in important.items() if not pairs.get(k)]

    return {
        "valid": len(missing) == 0,
        "missing": missing,
        "warnings": warnings,
    }


def resolve_with_ytdlp(
    url: str,
    cookie: str | None = None,
    proxy: str | None = None,
) -> MediaMetadata:
    """Resolve video metadata and stream URLs using yt-dlp."""
    ydl_opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": False,
        "skip_download": True,
    }

    if proxy:
        ydl_opts["proxy"] = proxy

    # Handle cookies (if string, pass as raw header or temp cookiejar)
    if cookie:
        ydl_opts["http_headers"] = {
            "Cookie": cookie,
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if not info:
            raise ValueError(f"无法使用 yt-dlp 解析链接: {url}")

    # Detect platform
    extractor_key = str(info.get("extractor_key", "")).lower()
    platform = "bilibili" if "bilibili" in extractor_key else extractor_key

    # Check if playlist / multi-P
    entries = info.get("entries")
    if entries is not None:
        # It's a playlist or Bilibili multi-P
        sub_items: list[SubItem] = []
        for entry in entries:
            if not entry:
                continue
            sub_items.append(
                SubItem(
                    item_id=str(entry.get("id", "")),
                    title=str(entry.get("title", "")),
                    url=str(entry.get("webpage_url") or entry.get("url") or ""),
                    duration=int(entry.get("duration", 0) or 0),
                    cover_url=str(entry.get("thumbnail", "")),
                    author=str(entry.get("uploader", "")),
                )
            )
        return MediaMetadata(
            platform=platform,
            content_type="batch_playlist",
            title=str(info.get("title", "分P列表")),
            author=str(info.get("uploader", "")),
            url=url,
            duration=int(info.get("duration", 0) or 0),
            cover_url=str(info.get("thumbnail", "")),
            sub_items=sub_items,
            extra={"entries_count": len(sub_items)},
        )

    # Single video: extract available streams
    streams: list[StreamInfo] = []
    formats = info.get("formats", [])
    headers = info.get("http_headers", {})

    # Best video stream + audio stream or muxed stream
    for fmt in formats:
        f_id = fmt.get("format_id", "")
        f_ext = fmt.get("ext", "mp4")
        f_url = fmt.get("url", "")
        vcodec = fmt.get("vcodec", "none")
        acodec = fmt.get("acodec", "none")
        proto = fmt.get("protocol", "http")
        resolution = fmt.get("format_note") or f"{fmt.get('width', 0)}x{fmt.get('height', 0)}"

        # Skip storyboard / mhtml
        if f_ext in ("mhtml", "jpg", "png") or not f_url:
            continue

        is_hls = "m3u8" in proto or f_url.endswith(".m3u8")
        protocol_str = "m3u8" if is_hls else "http"

        if vcodec != "none" and acodec != "none":
            # Direct muxed stream (video + audio)
            streams.append(
                StreamInfo(
                    format_id=f"muxed_{f_id}",
                    protocol=protocol_str,
                    video_url=f_url,
                    ext=f_ext,
                    resolution=resolution,
                    file_size_approx=int(fmt.get("filesize") or fmt.get("filesize_approx") or 0),
                    headers=headers,
                )
            )
        elif vcodec != "none" and acodec == "none":
            # Video only stream (typical for Bilibili DASH 1080P/4K)
            streams.append(
                StreamInfo(
                    format_id=f"video_{f_id}",
                    protocol=protocol_str,
                    video_url=f_url,
                    ext=f_ext,
                    resolution=resolution,
                    file_size_approx=int(fmt.get("filesize") or fmt.get("filesize_approx") or 0),
                    headers=headers,
                )
            )
        elif vcodec == "none" and acodec != "none":
            # Audio only stream
            streams.append(
                StreamInfo(
                    format_id=f"audio_{f_id}",
                    protocol=protocol_str,
                    audio_url=f_url,
                    ext=f_ext,
                    resolution="audio_only",
                    file_size_approx=int(fmt.get("filesize") or fmt.get("filesize_approx") or 0),
                    headers=headers,
                )
            )

    # If yt-dlp extracted direct top-level url
    if not streams and info.get("url"):
        streams.append(
            StreamInfo(
                format_id="default",
                protocol="http",
                video_url=info.get("url"),
                ext=info.get("ext", "mp4"),
                headers=headers,
            )
        )

    return MediaMetadata(
        platform=platform,
        content_type="single_video",
        title=str(info.get("title", "")),
        author=str(info.get("uploader", "") or info.get("creator", "")),
        author_id=str(info.get("uploader_id", "")),
        url=url,
        duration=int(info.get("duration", 0) or 0),
        cover_url=str(info.get("thumbnail", "")),
        streams=streams,
        extra={
            "id": info.get("id"),
            "view_count": info.get("view_count"),
            "like_count": info.get("like_count"),
        },
    )
