"""Douyin platform resolver using f2 library."""

from __future__ import annotations

import asyncio
import re
import urllib.request
from typing import Any
from urllib.parse import parse_qs, urlparse

from sidecar.protocol import AuthRequiredError, MediaMetadata, StreamInfo, SubItem


_USER_PROFILE_PATTERNS = [
    r"https?://(?:www\.)?douyin\.com/user/([a-zA-Z0-9_-]+)",
    r"https?://(?:www\.)?iesdouyin\.com/share/user/([a-zA-Z0-9_-]+)",
]

_MIX_PATTERNS = [
    r"https?://(?:www\.)?douyin\.com/collection/(\d+)",
    r"mix_id=(\d+)",
]

_LIVE_PATTERNS = [
    r"https?://live\.douyin\.com/(\d+)",
    r"https?://webcast\.amemv\.com/.*/(\d+)",
    r"rid=(\d+)",
]

_VIDEO_PATTERNS = [
    r"https?://(?:www\.)?douyin\.com/video/(\d+)",
    r"https?://(?:www\.)?iesdouyin\.com/share/video/(\d+)",
    r"modal_id=(\d+)",
    r"vid=(\d+)",
    r"aweme_id=(\d+)",
]


def expand_short_url(url: str, timeout: float = 10.0) -> str:
    """Expand v.douyin.com short URL to full target URL."""
    if "v.douyin.com" not in url:
        return url
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.geturl()
    except Exception:
        return url


def detect_douyin_content_type(url: str) -> tuple[str, str | None]:
    """Detect Douyin content type and extracted ID.

    Returns:
        tuple (type_str, id_str_or_none)
        type_str is one of: "video", "profile", "mix", "live", "unknown"
    """
    expanded = expand_short_url(url)

    # Check for modal_id or video ID first
    for pattern in _VIDEO_PATTERNS:
        m = re.search(pattern, expanded)
        if m:
            return "video", m.group(1)

    # Live room
    for pattern in _LIVE_PATTERNS:
        m = re.search(pattern, expanded)
        if m:
            return "live", m.group(1)

    # Mix / Collection
    for pattern in _MIX_PATTERNS:
        m = re.search(pattern, expanded)
        if m:
            return "mix", m.group(1)

    # User profile (only if not a video modal)
    query = parse_qs(urlparse(expanded).query)
    if "modal_id" not in query:
        for pattern in _USER_PROFILE_PATTERNS:
            m = re.search(pattern, expanded)
            if m:
                return "profile", m.group(1)

    return "unknown", None


def sanitize_cookie(cookie: str | None) -> str | None:
    """Clean cookie string, stripping out non-cookie headers and bad characters."""
    if not cookie:
        return cookie

    cookie = cookie.replace("\r\n", "; ").replace("\n", "; ").replace("\r", "; ")
    non_cookie_headers = {
        "referer", "referrer", "priority", "origin", "host",
        "accept", "accept-encoding", "accept-language",
        "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
        "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
        "user-agent", "connection", "cache-control",
    }

    parts = cookie.split(";")
    clean: list[str] = []
    for part in parts:
        part = part.strip()
        if "=" not in part:
            continue
        name, _, val = part.partition("=")
        name = name.strip()
        if not name or name.lower() in non_cookie_headers:
            continue
        clean.append(f"{name}={val.strip()}")

    return "; ".join(clean) if clean else None


def check_douyin_cookie(cookie: str | None) -> dict[str, Any]:
    """Check health of Douyin cookie."""
    if not cookie or not cookie.strip():
        return {
            "valid": False,
            "missing": ["所有 Cookie"],
            "warnings": ["Cookie 为空，受限视频及批量抓取可能失败"],
        }

    pairs: dict[str, str] = {}
    for item in cookie.split(";"):
        if "=" in item:
            k, v = item.strip().split("=", 1)
            pairs[k.strip()] = v.strip()

    required = {
        "sessionid": "登录会话（必需，否则无法抓取受限视频和高清接口）",
        "ttwid": "访问令牌（必需，用于 API 请求鉴权）",
    }
    important = {
        "msToken": "安全令牌（重要，缺少可能导致请求被反爬拦截）",
        "passport_csrf_token": "CSRF 令牌（登录态标识）",
    }

    missing = [f"{k} — {v}" for k, v in required.items() if not pairs.get(k)]
    warnings = [f"缺少 {k}（{v}）" for k, v in important.items() if not pairs.get(k)]

    return {
        "valid": len(missing) == 0,
        "missing": missing,
        "warnings": warnings,
    }


def _build_f2_kwargs(cookie: str | None = None, proxy: str | None = None) -> dict[str, Any]:
    """Build kwargs dict for f2 DouyinHandler."""
    clean_cookie = sanitize_cookie(cookie) or ""
    kwargs: dict[str, Any] = {
        "cookie": clean_cookie,
        "headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.douyin.com/",
        },
    }
    if proxy:
        kwargs["proxies"] = {
            "http://": proxy,
            "https://": proxy,
        }
    return kwargs


async def resolve_douyin_video(
    video_id: str,
    raw_url: str,
    cookie: str | None = None,
    proxy: str | None = None,
) -> MediaMetadata:
    """Resolve single Douyin video or note/image post."""
    from f2.apps.douyin.handler import DouyinHandler

    kwargs = _build_f2_kwargs(cookie, proxy)
    handler = DouyinHandler(kwargs)
    try:
        detail = await handler.fetch_one_video(aweme_id=video_id)
    except Exception as exc:
        text = str(exc)
        if any(h in text.lower() for h in ("cookie", "登录", "login", "401", "403", "signature")):
            raise AuthRequiredError("douyin", f"抖音接口拒绝请求: {text}") from exc
        raise

    if not detail:
        if not cookie:
            raise AuthRequiredError(
                "douyin",
                "未提供 Cookie 或视频不可见，可能需要登录后才能解析",
            )
        raise ValueError(f"无法获取抖音视频: {video_id}")

    video_play_addr = getattr(detail, "video_play_addr", None) or []
    video_url = video_play_addr[0] if video_play_addr else ""

    title = str(getattr(detail, "desc", "") or "")
    author = str(getattr(detail, "nickname", "") or "")
    author_id = str(getattr(detail, "sec_user_id", "") or "")
    duration_ms = int(getattr(detail, "duration", 0) or 0)
    duration = duration_ms // 1000 if duration_ms > 0 else 0
    cover_url = str(getattr(detail, "cover", "") or "")

    images = getattr(detail, "images", None) or []
    is_image_post = bool(images) and not video_url
    image_urls = []
    if is_image_post:
        for img in images:
            if isinstance(img, dict):
                ul = img.get("url_list", [])
                if ul:
                    image_urls.append(ul[0])

    streams: list[StreamInfo] = []
    if video_url:
        streams.append(
            StreamInfo(
                format_id="original",
                protocol="http",
                video_url=video_url,
                ext="mp4",
                resolution="1080p",
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Referer": "https://www.douyin.com/",
                },
            )
        )

    return MediaMetadata(
        platform="douyin",
        content_type="image_post" if is_image_post else "single_video",
        title=title or f"抖音作品_{video_id}",
        author=author,
        author_id=author_id,
        url=raw_url,
        duration=duration,
        cover_url=cover_url,
        streams=streams,
        extra={
            "aweme_id": video_id,
            "is_image_post": is_image_post,
            "image_urls": image_urls,
        },
    )


async def resolve_douyin_batch(
    target_type: str,  # "posts", "collects", "likes", "mix"
    target_id: str,    # sec_user_id or mix_id
    max_count: int = 50,
    cookie: str | None = None,
    proxy: str | None = None,
) -> MediaMetadata:
    """Resolve Douyin batch collections (user profile, collection, likes, etc)."""
    from f2.apps.douyin.handler import DouyinHandler

    kwargs = _build_f2_kwargs(cookie, proxy)
    handler = DouyinHandler(kwargs)

    sub_items: list[SubItem] = []
    title = f"抖音批量_{target_type}_{target_id}"
    author = ""

    if target_type == "posts":
        # First get author profile
        try:
            profile = await handler.fetch_user_profile(sec_user_id=target_id)
            author = getattr(profile, "nickname", "") or ""
            title = f"{author}的主页作品"
        except Exception:
            pass

        async for post_filter in handler.fetch_user_post_videos(
            sec_user_id=target_id,
            max_counts=max_count if max_count > 0 else None,
        ):
            if not post_filter.has_aweme:
                if not post_filter.has_more:
                    break
                continue

            # Process aweme list
            aweme_list = getattr(post_filter, "aweme_list", []) or []
            for aweme in aweme_list:
                item_id = str(aweme.get("aweme_id", ""))
                desc = aweme.get("desc", "")
                dur = (aweme.get("video", {}).get("duration", 0)) // 1000
                cov = ""
                cov_list = aweme.get("video", {}).get("cover", {}).get("url_list", [])
                if cov_list:
                    cov = cov_list[0]
                imgs = aweme.get("images", []) or []
                img_urls = [im["url_list"][0] for im in imgs if "url_list" in im and im["url_list"]]

                sub_items.append(
                    SubItem(
                        item_id=item_id,
                        title=desc or f"作品_{item_id}",
                        url=f"https://www.douyin.com/video/{item_id}",
                        duration=dur,
                        cover_url=cov,
                        author=author,
                        is_image_post=bool(imgs),
                        image_urls=img_urls,
                        create_time=aweme.get("create_time", 0),
                        like_count=aweme.get("statistics", {}).get("digg_count", 0),
                    )
                )
                if max_count > 0 and len(sub_items) >= max_count:
                    break
            if max_count > 0 and len(sub_items) >= max_count:
                break

    elif target_type == "mix":
        async for mix_filter in handler.fetch_user_mix_videos(
            mix_id=target_id,
            max_counts=max_count if max_count > 0 else None,
        ):
            if not mix_filter.has_aweme:
                break
            aweme_list = getattr(mix_filter, "aweme_list", []) or []
            for aweme in aweme_list:
                item_id = str(aweme.get("aweme_id", ""))
                desc = aweme.get("desc", "")
                dur = (aweme.get("video", {}).get("duration", 0)) // 1000
                cov_list = aweme.get("video", {}).get("cover", {}).get("url_list", [])
                cov = cov_list[0] if cov_list else ""

                sub_items.append(
                    SubItem(
                        item_id=item_id,
                        title=desc or f"作品_{item_id}",
                        url=f"https://www.douyin.com/video/{item_id}",
                        duration=dur,
                        cover_url=cov,
                        author=author,
                        create_time=aweme.get("create_time", 0),
                    )
                )
                if max_count > 0 and len(sub_items) >= max_count:
                    break
            if max_count > 0 and len(sub_items) >= max_count:
                break

    return MediaMetadata(
        platform="douyin",
        content_type="batch_playlist",
        title=title,
        author=author,
        url=f"https://www.douyin.com/user/{target_id}" if target_type == "posts" else f"https://www.douyin.com/collection/{target_id}",
        author_id=target_id,
        sub_items=sub_items,
        extra={"batch_type": target_type, "total_count": len(sub_items)},
    )
