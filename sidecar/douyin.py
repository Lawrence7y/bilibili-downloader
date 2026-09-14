"""Douyin platform resolver using f2 library."""

from __future__ import annotations

import asyncio
import re
import urllib.request
from typing import Any
from urllib.parse import parse_qs, urlparse

from sidecar.protocol import AuthRequiredError, MediaMetadata, StreamInfo, SubItem

# Patch f2 TokenManager to gracefully fallback to gen_false_msToken if real msToken endpoint fails
try:
    from f2.apps.douyin.utils import TokenManager
    _orig_gen_real_msToken = TokenManager.gen_real_msToken

    @classmethod
    def _safe_gen_real_msToken(cls):
        try:
            return _orig_gen_real_msToken()
        except Exception:
            return cls.gen_false_msToken()

    TokenManager.gen_real_msToken = _safe_gen_real_msToken
except Exception:
    pass

# Globally disable Bark notification to avoid useless network calls to api.day.app
try:
    from f2.apps.bark.utils import ClientConfManager as BarkConfManager
    BarkConfManager.enable_bark = classmethod(lambda cls: False)
except Exception:
    pass


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


def _build_f2_kwargs(
    cookie: str | None = None,
    proxy: str | None = None,
    *,
    max_retries: int = 5,
    timeout: float = 1.0,
) -> dict[str, Any]:
    """Build kwargs dict for f2 DouyinHandler."""
    clean_cookie = sanitize_cookie(cookie) or ""
    kwargs: dict[str, Any] = {
        "cookie": clean_cookie,
        "max_retries": max_retries,
        "timeout": timeout,
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
    handler.enable_bark = False
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

def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text or not text[0].isdigit():
        return default
    digits = ""
    for ch in text:
        if ch.isdigit():
            digits += ch
        elif digits:
            break
    try:
        return int(digits) if digits else default
    except ValueError:
        return default


def _items_from_filter(page_filter) -> tuple[list[dict], list[str]]:
    """Convert a f2 filter page into raw aweme dicts. Returns (items, errors)."""
    errors: list[str] = []

    to_list = getattr(page_filter, "_to_list", None)
    if callable(to_list):
        try:
            data = to_list()
            if isinstance(data, list) and data:
                for row in data:
                    if isinstance(row, dict) and "create_time" in row:
                        row["create_time"] = _safe_int(row.get("create_time"))
                return data, errors
        except Exception as exc:
            errors.append(f"_to_list: {exc}")

    try:
        ids = getattr(page_filter, "aweme_id", None) or []
        if not isinstance(ids, list):
            ids = [ids]
        descs = getattr(page_filter, "desc", None) or []
        if not isinstance(descs, list):
            descs = [descs]
        covers = getattr(page_filter, "cover", None) or []
        if not isinstance(covers, list):
            covers = [covers]
        durs = getattr(page_filter, "video_duration", None) or []
        if not isinstance(durs, list):
            durs = [durs]
        try:
            imgs_all = getattr(page_filter, "images", None) or []
        except Exception:
            imgs_all = []
        if not imgs_all:
            imgs_all = []

        out: list[dict] = []
        for i, aweme_id in enumerate(ids):
            if not aweme_id:
                continue
            img_urls: list[str] = []
            if i < len(imgs_all) and imgs_all[i]:
                raw_imgs = imgs_all[i]
                if isinstance(raw_imgs, list):
                    img_urls = [u for u in raw_imgs if isinstance(u, str) and u]
            dur = _safe_int(durs[i] if i < len(durs) else 0)
            if dur > 10_000:
                dur = dur // 1000
            out.append(
                {
                    "aweme_id": aweme_id,
                    "desc": descs[i] if i < len(descs) else "",
                    "cover": covers[i] if i < len(covers) else "",
                    "duration": dur,
                    "images": img_urls,
                    "create_time": 0,
                    "like_count": 0,
                }
            )
        return out, errors
    except Exception as exc:
        errors.append(f"property fallback: {exc}")
        return [], errors


def _append_aweme_items(
    raw_list: list[dict],
    sub_items: list[SubItem],
    author_state: dict[str, str],
) -> None:
    for aweme in raw_list:
        if not isinstance(aweme, dict):
            continue
        item_id = str(
            aweme.get("aweme_id")
            or aweme.get("awemeId")
            or aweme.get("id")
            or ""
        )
        if not item_id:
            continue
        desc = aweme.get("desc") or aweme.get("title") or ""
        video = aweme.get("video") or {}
        if not isinstance(video, dict):
            video = {}
        dur = _safe_int(aweme.get("duration") or video.get("duration") or 0)
        if dur > 10_000:
            dur = dur // 1000
        cov = aweme.get("cover") or ""
        if not cov:
            cover_obj = video.get("cover") or video.get("origin_cover") or {}
            if isinstance(cover_obj, dict):
                url_list = cover_obj.get("url_list") or []
                if url_list:
                    cov = url_list[0]
        imgs = aweme.get("images") or []
        img_urls: list[str] = []
        if isinstance(imgs, list):
            for im in imgs:
                if isinstance(im, dict):
                    ul = im.get("url_list") or []
                    if ul:
                        img_urls.append(ul[0])
                elif isinstance(im, str) and im:
                    img_urls.append(im)
        if not author_state.get("author"):
            nick = aweme.get("nickname")
            if not nick:
                author_obj = aweme.get("author")
                if isinstance(author_obj, dict):
                    nick = author_obj.get("nickname")
            if nick:
                author_state["author"] = str(nick)
        stats = aweme.get("statistics") or {}
        like_count = stats.get("digg_count", 0) if isinstance(stats, dict) else 0
        sub_items.append(
            SubItem(
                item_id=item_id,
                title=desc or f"作品_{item_id}",
                url=f"https://www.douyin.com/video/{item_id}",
                duration=dur,
                cover_url=str(cov or ""),
                author=author_state.get("author", ""),
                is_image_post=bool(img_urls),
                image_urls=img_urls,
                create_time=_safe_int(aweme.get("create_time")),
                like_count=_safe_int(like_count),
            )
        )


async def resolve_douyin_batch(
    target_type: str,  # "posts", "collects", "likes", "mix"
    target_id: str,  # sec_user_id or mix_id
    max_count: int = 50,
    cookie: str | None = None,
    proxy: str | None = None,
) -> MediaMetadata:
    """Resolve Douyin batch collections (user profile, collection, likes, etc)."""
    from f2.apps.douyin.handler import DouyinHandler

    clean = sanitize_cookie(cookie) or ""
    if "sessionid" not in clean and "ttwid" not in clean:
        raise AuthRequiredError(
            "douyin",
            "批量抓取需要有效 Cookie（至少包含 sessionid、ttwid）。请在系统设置粘贴后点「保存到本机」再重试",
        )

    kwargs = _build_f2_kwargs(cookie, proxy, max_retries=2, timeout=1.0)
    handler = DouyinHandler(kwargs)
    handler.enable_bark = False

    sub_items: list[SubItem] = []
    title = f"抖音批量_{target_type}_{target_id}"
    author_state: dict[str, str] = {"author": ""}
    errors: list[str] = []

    if target_type == "posts":
        # Launch profile fetch concurrently in the background so it doesn't block video pagination
        profile_task = asyncio.create_task(handler.fetch_user_profile(sec_user_id=target_id))

        try:
            # Douyin web API supports up to 35 items per page, reducing roundtrips significantly
            batch_page_size = 35 if (max_count <= 0 or max_count > 20) else max_count
            async for post_filter in handler.fetch_user_post_videos(
                sec_user_id=target_id,
                page_counts=batch_page_size,
                max_counts=max_count if max_count > 0 else None,
            ):
                has_aweme = bool(getattr(post_filter, "has_aweme", False))
                if not has_aweme:
                    if not getattr(post_filter, "has_more", False):
                        break
                    continue
                items, page_errors = _items_from_filter(post_filter)
                errors.extend(page_errors)
                _append_aweme_items(items, sub_items, author_state)
                if max_count > 0 and len(sub_items) >= max_count:
                    break
        except Exception as exc:
            errors.append(f"fetch_user_post_videos: {exc}")
            raise RuntimeError(
                "主页作品抓取失败: "
                + "; ".join(errors)
                + "。请确认 Cookie 有效且 sec_user_id 正确"
            ) from exc

        # Retrieve profile nickname if not already captured from aweme items
        if not author_state.get("author"):
            try:
                profile = await asyncio.wait_for(asyncio.shield(profile_task), timeout=2.0)
                nick = getattr(profile, "nickname", "") or ""
                if isinstance(nick, list):
                    nick = nick[0] if nick else ""
                if nick:
                    author_state["author"] = str(nick)
            except Exception as exc:
                errors.append(f"profile: {exc}")
        else:
            profile_task.cancel()

        title = f"{author_state['author'] or target_id}的主页作品"

    elif target_type == "mix":
        try:
            batch_page_size = 35 if (max_count <= 0 or max_count > 20) else max_count
            async for mix_filter in handler.fetch_user_mix_videos(
                mix_id=target_id,
                page_counts=batch_page_size,
                max_counts=max_count if max_count > 0 else None,
            ):
                if not getattr(mix_filter, "has_aweme", False):
                    if not getattr(mix_filter, "has_more", False):
                        break
                    continue
                items, page_errors = _items_from_filter(mix_filter)
                errors.extend(page_errors)
                _append_aweme_items(items, sub_items, author_state)
                if max_count > 0 and len(sub_items) >= max_count:
                    break
        except Exception as exc:
            errors.append(f"fetch_user_mix_videos: {exc}")
            raise RuntimeError("合集抓取失败: " + "; ".join(errors)) from exc
    else:
        raise ValueError(f"不支持的批量类型: {target_type}（当前仅支持 posts / mix）")

    if max_count > 0:
        sub_items = sub_items[:max_count]

    if not sub_items:
        detail = "; ".join(errors) if errors else "接口返回空列表"
        raise RuntimeError(
            f"未抓到任何作品（{detail}）。常见原因：未登录 Cookie、主页私密、sec_user_id 错误"
        )

    return MediaMetadata(
        platform="douyin",
        content_type="batch_playlist",
        title=title,
        author=author_state.get("author", ""),
        url=(
            f"https://www.douyin.com/user/{target_id}"
            if target_type == "posts"
            else f"https://www.douyin.com/collection/{target_id}"
        ),
        author_id=target_id,
        sub_items=sub_items,
        extra={
            "batch_type": target_type,
            "total_count": len(sub_items),
            "errors": errors,
        },
    )
