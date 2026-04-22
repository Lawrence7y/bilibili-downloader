#!/usr/bin/env python3
"""Video downloader core for Bilibili and Douyin (authorized content only)."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import json
import re
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from datetime import datetime
from http.cookiejar import LoadError, MozillaCookieJar
from pathlib import Path
import tempfile
from typing import Any
from urllib.error import URLError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

import yt_dlp

from runtime_env import configure_runtime_environment
from web_downloader import FFmpegDownloadCancelled, build_cookie_header, run_ffmpeg_download
from web_sniffer import sniff_media_sync

configure_runtime_environment()

PLATFORM_BILIBILI = "bilibili"
PLATFORM_DOUYIN = "douyin"
PLATFORM_WEB = "web"
PLATFORM_AUTO = "auto"
SUPPORTED_PLATFORMS = (PLATFORM_BILIBILI, PLATFORM_DOUYIN, PLATFORM_WEB, PLATFORM_AUTO)

RESOLUTION_BEST = "best"
SUPPORTED_RESOLUTIONS = ("best", "2160", "1440", "1080", "720", "480", "360")
SUPPORTED_AUDIO_FORMATS = ("mp3", "m4a", "wav", "flac")
SUPPORTED_BROWSERS = ("none", "edge", "chrome", "firefox")
DEFAULT_FILENAME_TEMPLATE = "%(title).120B [%(id)s]"
POST_ACTION_NONE = "none"
POST_ACTION_ARCHIVE = "archive"
POST_ACTION_TRANSCODE_H265 = "transcode_h265"
SUPPORTED_POST_ACTIONS = (
    POST_ACTION_NONE,
    POST_ACTION_ARCHIVE,
    POST_ACTION_TRANSCODE_H265,
)
DEFAULT_ADAPTIVE_RETRY_ATTEMPTS = 2
PROBE_PER_URL_TIMEOUT = 45
SHORT_LINK_EXPAND_TIMEOUT = 12
SHORT_LINK_FETCH_BYTES = 300000
DEFAULT_PROBE_WORKERS = 6

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
URL_RE = re.compile(r"https?://[^\s\"'<>]+")

_PLATFORM_NAMES_CN = {
    PLATFORM_BILIBILI: "\u0042\u7ad9",
    PLATFORM_DOUYIN: "\u6296\u97f3",
    PLATFORM_WEB: "\u7f51\u9875",
    PLATFORM_AUTO: "\u81ea\u52a8\u8bc6\u522b",
}

_PLATFORM_DOMAINS = {
    PLATFORM_BILIBILI: ("bilibili.com", "b23.tv"),
    PLATFORM_DOUYIN: ("douyin.com", "iesdouyin.com", "v.douyin.com", "amemv.com"),
}

LogFunc = Callable[[str], None]
ProgressHook = Callable[[dict[str, Any]], None]
StateHook = Callable[[dict[str, Any]], None]
CancelCheck = Callable[[], bool]


class DownloadCancelledError(RuntimeError):
    """Raised when caller requests cancellation."""


class DownloadFailure(RuntimeError):
    """Raised when a known failure category is detected."""

    def __init__(self, code: str, message: str):
        self.code = (code or "UNKNOWN").strip().upper() or "UNKNOWN"
        super().__init__(message)


PREVIEW_CACHE_TTL_SECONDS = 600
_PREVIEW_CACHE: dict[tuple[str, str, str, str, int], tuple[float, dict[str, Any]]] = {}
_PREVIEW_CACHE_LOCK = threading.Lock()


def _preview_cache_now() -> float:
    return time.monotonic()


def clear_preview_cache() -> None:
    with _PREVIEW_CACHE_LOCK:
        _PREVIEW_CACHE.clear()


def _cookiefile_token(cookiefile: str | None) -> str:
    if not cookiefile:
        return ""
    path = Path(cookiefile)
    try:
        stat = path.stat()
    except OSError:
        return str(path)
    return f"{path}|{int(stat.st_mtime)}|{stat.st_size}"


def _preview_cache_key(
    source_url: str,
    cookiefile: str | None,
    cookies_from_browser: str,
    proxy: str | None,
    retries: int,
) -> tuple[str, str, str, str, int]:
    return (
        source_url.strip(),
        _cookiefile_token(cookiefile),
        (cookies_from_browser or "").strip().lower(),
        (proxy or "").strip(),
        int(retries),
    )


def _get_cached_preview(cache_key: tuple[str, str, str, str, int]) -> dict[str, Any] | None:
    with _PREVIEW_CACHE_LOCK:
        cached = _PREVIEW_CACHE.get(cache_key)
        if cached is None:
            return None
        timestamp, payload = cached
        if _preview_cache_now() - timestamp > PREVIEW_CACHE_TTL_SECONDS:
            _PREVIEW_CACHE.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _put_cached_preview(cache_key: tuple[str, str, str, str, int], payload: dict[str, Any]) -> None:
    with _PREVIEW_CACHE_LOCK:
        _PREVIEW_CACHE[cache_key] = (_preview_cache_now(), copy.deepcopy(payload))


def classify_download_error(reason: str) -> str:
    text = (reason or "").strip().lower()
    if not text:
        return "UNKNOWN"
    if "no module named" in text and "playwright" in text:
        return "DEPENDENCY_PLAYWRIGHT_MISSING"
    if "playwright browser runtime is unavailable" in text:
        return "DEPENDENCY_PLAYWRIGHT_BROWSER_MISSING"
    if "playwright install chromium" in text:
        return "DEPENDENCY_PLAYWRIGHT_BROWSER_MISSING"
    if "ffmpeg" in text and ("not found" in text or "未检测到" in text):
        return "DEPENDENCY_FFMPEG_MISSING"
    if "timed out" in text or "timeout" in text or "network" in text:
        return "NETWORK_TRANSIENT"
    if "could not copy chrome cookie database" in text:
        return "COOKIE_DB_LOCKED"
    return "UNKNOWN"


def _friendly_preview_error_message(
    source_url: str,
    reason: str,
    cookiefile: str | None,
    cookies_from_browser: str,
) -> str:
    normalized_reason = strip_ansi(reason).strip()
    lowered = normalized_reason.lower()
    is_douyin = is_url_for_platform(source_url, PLATFORM_DOUYIN)
    if is_douyin and "fresh cookies" in lowered:
        browser_value = (cookies_from_browser or "").strip().lower()
        if cookiefile:
            return (
                "抖音需要新鲜 cookies。请重新导出最新 cookies.txt "
                "（确保包含 douyin.com 的 sessionid、msToken、ttwid 等）后重试。"
            )
        if browser_value == "none":
            return (
                "抖音需要新鲜 cookies。请在“cookies 来源”选择 Edge（推荐）或 Chrome 后重试；"
                "也可导入最新 cookies.txt。"
            )
        return (
            "抖音需要新鲜 cookies。请先关闭浏览器后重试，"
            "必要时在浏览器重新登录抖音后再试。"
        )
    return normalized_reason


def _summarize_no_allowed_links_reason(
    joined_blocked_messages: str,
    blocked_messages: list[str],
    cookiefile: str | None,
    cookies_from_browser: str,
) -> tuple[str, str]:
    joined = (joined_blocked_messages or "").strip()
    lower_joined = joined.lower()

    if "failed to decrypt with dpapi" in lower_joined or "unable to get key for cookie decryption" in lower_joined:
        return (
            "COOKIE_DECRYPT_FAILED",
            "无法解密浏览器 cookies（DPAPI）。请导入最新 cookies.txt，或将 cookies 来源切到 Firefox 后重试。",
        )
    if "could not copy chrome cookie database" in lower_joined:
        return (
            "COOKIE_DB_LOCKED",
            "浏览器 cookies 数据库被占用。请完全关闭 Edge/Chrome 后重试，或导入最新 cookies.txt。",
        )
    if "fresh cookies" in lower_joined:
        browser_value = (cookies_from_browser or "").strip().lower()
        if cookiefile:
            return (
                "FRESH_COOKIES_REQUIRED",
                "抖音需要新鲜 cookies。请重新导出最新 cookies.txt 后重试。",
            )
        if browser_value == "none":
            return (
                "FRESH_COOKIES_REQUIRED",
                "抖音需要新鲜 cookies。请在“cookies 来源”选择 Edge（推荐）或 Chrome，或导入最新 cookies.txt。",
            )
        return (
            "FRESH_COOKIES_REQUIRED",
            "抖音需要新鲜 cookies。请重新登录抖音网页后重试，必要时改用最新 cookies.txt。",
        )
    if "检查超时" in joined:
        return (
            "PROBE_TIMEOUT",
            "链接检查超时。请先在浏览器打开短链获取长链接后重试，或切换更稳定网络。",
        )

    if blocked_messages:
        return "NO_ALLOWED_LINKS", blocked_messages[-1]
    return "NO_ALLOWED_LINKS", "没有可下载的可访问视频链接，程序结束。"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def strip_ansi(text: str) -> str:
    return ANSI_RE.sub("", text or "")


def platform_name_cn(platform: str) -> str:
    return _PLATFORM_NAMES_CN.get(platform, platform)


def normalize_platform(platform: str) -> str:
    normalized = (platform or "").strip().lower()
    return normalized if normalized in SUPPORTED_PLATFORMS else ""


def normalize_resolution(resolution: str) -> str:
    normalized = (resolution or "").strip().lower()
    return normalized if normalized in SUPPORTED_RESOLUTIONS else ""


def normalize_audio_format(audio_format: str) -> str:
    normalized = (audio_format or "").strip().lower()
    return normalized if normalized in SUPPORTED_AUDIO_FORMATS else ""


def normalize_browser(browser: str) -> str:
    normalized = (browser or "").strip().lower()
    return normalized if normalized in SUPPORTED_BROWSERS else ""


def normalize_post_action(post_action: str) -> str:
    normalized = (post_action or "").strip().lower()
    return normalized if normalized in SUPPORTED_POST_ACTIONS else POST_ACTION_NONE


def normalize_filename_template(template: str | None) -> str:
    text = (template or "").strip().replace("\x00", "")
    if not text:
        text = DEFAULT_FILENAME_TEMPLATE
    if "%(ext)" not in text:
        text = f"{text}.%(ext)s"
    return text


def parse_rate_limit(rate_limit: str | None) -> tuple[int | None, str | None]:
    text = (rate_limit or "").strip()
    if not text:
        return None, None

    lowered = text.lower().strip()
    for suffix in ("/s", "ps"):
        if lowered.endswith(suffix):
            lowered = lowered[: -len(suffix)]
            break

    match = re.match(r"^(\d+(?:\.\d+)?)\s*([kmgt]?)\s*(?:i?b?)?$", lowered, flags=re.IGNORECASE)
    if not match:
        return None, "限速参数无效，请使用类似 800K、2M、1.5M 或 1048576 的格式。"

    value = float(match.group(1))
    unit = match.group(2).lower()
    unit_power = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4}
    multiplier = 1024 ** unit_power[unit]
    bytes_per_second = int(value * multiplier)
    if bytes_per_second <= 0:
        return None, "限速参数必须大于 0。"
    return bytes_per_second, None


def _version_key(version_text: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", version_text)
    if not parts:
        return (0,)
    return tuple(int(part) for part in parts)


def _is_transient_error(reason: str) -> bool:
    text = (reason or "").strip().lower()
    if not text:
        return False
    transient_patterns = (
        "timed out",
        "timeout",
        "temporarily unavailable",
        "try again",
        "connection reset",
        "connection aborted",
        "network is unreachable",
        "connection refused",
        "http error 429",
        "http error 500",
        "http error 502",
        "http error 503",
        "http error 504",
        "dns",
        "remote end closed",
        "server disconnected",
        "service unavailable",
    )
    return any(pattern in text for pattern in transient_patterns)


def _adaptive_backoff_seconds(attempt: int) -> float:
    # attempt is 1-based: attempt=1 means no wait (first run), attempt>=2 waits.
    if attempt <= 1:
        return 0.0
    return min(15.0, 1.2 + 1.8 * (attempt - 2))


def _snapshot_output_files(output_dir: Path) -> dict[Path, float]:
    if not output_dir.exists():
        return {}
    snapshot: dict[Path, float] = {}
    for path in output_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            snapshot[path] = path.stat().st_mtime
        except OSError:
            continue
    return snapshot


def _collect_new_or_updated_files(before: dict[Path, float], output_dir: Path) -> list[Path]:
    after = _snapshot_output_files(output_dir)
    changed: list[Path] = []
    for path, mtime in after.items():
        old_mtime = before.get(path)
        if old_mtime is None or mtime > old_mtime + 1e-6:
            changed.append(path)
    changed.sort(key=lambda p: str(p).lower())
    return changed


def _platform_from_url(url: str) -> str:
    if is_url_for_platform(url, PLATFORM_BILIBILI):
        return PLATFORM_BILIBILI
    if is_url_for_platform(url, PLATFORM_DOUYIN):
        return PLATFORM_DOUYIN
    return PLATFORM_WEB


def _unique_target_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    index = 1
    while True:
        candidate = path.with_name(f"{stem}_{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def _apply_archive_action(
    files: list[Path],
    output_dir: Path,
    source_url: str,
    log_func: LogFunc,
) -> None:
    if not files:
        return
    platform = _platform_from_url(source_url)
    day = datetime.now().strftime("%Y-%m-%d")
    archive_root = output_dir / "archive" / platform / day
    archive_root.mkdir(parents=True, exist_ok=True)
    moved = 0
    for src in files:
        if not src.exists():
            continue
        try:
            src.resolve().relative_to(archive_root.resolve())
            continue
        except ValueError:
            pass
        target = _unique_target_path(archive_root / src.name)
        try:
            src.parent.mkdir(parents=True, exist_ok=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(target))
            moved += 1
        except OSError as exc:
            log_func(f"[警告] 归档失败：{src.name} -> {strip_ansi(str(exc))}")
    if moved > 0:
        log_func(f"下载后归档完成：{moved} 个文件 -> {archive_root}")


def _apply_transcode_h265_action(
    files: list[Path],
    log_func: LogFunc,
) -> None:
    if not files:
        return
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        log_func("[警告] 下载后转码跳过：未检测到 ffmpeg（请先安装并加入 PATH）")
        return

    video_ext = {".mp4", ".mkv", ".flv", ".mov", ".webm"}
    candidates = [path for path in files if path.suffix.lower() in video_ext]
    if not candidates:
        return

    success_count = 0
    for src in candidates:
        if not src.exists():
            continue
        out_file = _unique_target_path(src.with_name(f"{src.stem}.h265.mp4"))
        cmd = [
            ffmpeg,
            "-y",
            "-i",
            str(src),
            "-c:v",
            "libx265",
            "-preset",
            "medium",
            "-crf",
            "27",
            "-c:a",
            "copy",
            str(out_file),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip().splitlines()
            detail = stderr[-1] if stderr else f"exit_code={proc.returncode}"
            log_func(f"[警告] H265 转码失败：{src.name} -> {detail}")
            continue
        success_count += 1
        log_func(f"H265 转码完成：{src.name} -> {out_file.name}")
    if success_count > 0:
        log_func(f"下载后转码完成：{success_count} 个文件")


def apply_post_action(
    post_action: str,
    files: list[Path],
    output_dir: Path,
    source_url: str,
    log_func: LogFunc,
) -> None:
    action = normalize_post_action(post_action)
    if action == POST_ACTION_NONE or not files:
        return
    if action == POST_ACTION_ARCHIVE:
        _apply_archive_action(files, output_dir, source_url, log_func)
        return
    if action == POST_ACTION_TRANSCODE_H265:
        _apply_transcode_h265_action(files, log_func)


def _extract_host(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower().strip()
    except (ValueError, AttributeError):
        return ""
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def is_url_for_platform(url: str, platform: str) -> bool:
    normalized = normalize_platform(platform)
    if not normalized or normalized not in _PLATFORM_DOMAINS:
        return False
    host = _extract_host(url)
    if not host:
        return False
    return any(host == d or host.endswith(f".{d}") for d in _PLATFORM_DOMAINS[normalized])


def detect_platform_from_urls(urls: list[str]) -> str:
    bili_count = sum(1 for url in urls if is_url_for_platform(url, PLATFORM_BILIBILI))
    douyin_count = sum(1 for url in urls if is_url_for_platform(url, PLATFORM_DOUYIN))
    if bili_count == 0 and douyin_count == 0:
        return ""
    return PLATFORM_BILIBILI if bili_count >= douyin_count else PLATFORM_DOUYIN


def split_urls_by_platform(urls: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {
        PLATFORM_BILIBILI: [],
        PLATFORM_DOUYIN: [],
        "unknown": [],
    }
    for url in _dedupe_keep_order(urls):
        if is_url_for_platform(url, PLATFORM_BILIBILI):
            grouped[PLATFORM_BILIBILI].append(url)
        elif is_url_for_platform(url, PLATFORM_DOUYIN):
            grouped[PLATFORM_DOUYIN].append(url)
        else:
            grouped["unknown"].append(url)
    return grouped


def build_auto_platform_batches(urls: list[str]) -> list[tuple[str, list[str]]]:
    grouped = split_urls_by_platform(urls)
    batches: list[tuple[str, list[str]]] = []
    for platform in (PLATFORM_BILIBILI, PLATFORM_DOUYIN):
        platform_urls = grouped[platform]
        if platform_urls:
            batches.append((platform, platform_urls))
    if grouped["unknown"]:
        batches.append((PLATFORM_WEB, grouped["unknown"]))
    return batches


def extract_urls_from_text(text: str) -> list[str]:
    if not text:
        return []
    found = URL_RE.findall(text)
    cleaned: list[str] = []
    for url in found:
        normalized = url.strip().rstrip(".,;:!?)\uff0c\u3002\uff1b\uff1a\uff01\uff1f\uff09\u3011\u300b")
        if normalized:
            cleaned.append(normalized)
    return cleaned


def extract_urls_from_inputs(items: list[str]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for item in items:
        for url in extract_urls_from_text(item.strip()):
            if url not in seen:
                seen.add(url)
                urls.append(url)
    return urls


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _load_history_records(history_file: str) -> list[dict[str, Any]]:
    path = Path(history_file)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return []
    if not isinstance(raw, dict):
        return []
    records = raw.get("records")
    if not isinstance(records, list):
        return []
    return [item for item in records if isinstance(item, dict)]


def _save_history_records(history_file: str, records: list[dict[str, Any]]) -> None:
    path = Path(history_file)
    data = {"records": records}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_input_url(url: str) -> str:
    normalized = (url or "").strip()
    normalized = normalized.replace("\\/", "/").replace("&amp;", "&")
    normalized = normalized.rstrip(".,;:!?)\uff0c\u3002\uff1b\uff1a\uff01\uff1f\uff09\u3011\u300b")
    return normalized


def _expand_short_url(url: str) -> str:
    request = Request(url, headers=HTTP_HEADERS, method="GET")
    with urlopen(request, timeout=SHORT_LINK_EXPAND_TIMEOUT) as response:
        final_url = response.geturl() or url
    return _normalize_input_url(final_url)


def _fetch_web_text(url: str) -> tuple[str, str]:
    request = Request(url, headers=HTTP_HEADERS, method="GET")
    with urlopen(request, timeout=SHORT_LINK_EXPAND_TIMEOUT) as response:
        final_url = _normalize_input_url(response.geturl() or url)
        content_type = (response.headers.get_content_charset() or "utf-8").strip() or "utf-8"
        body = response.read(SHORT_LINK_FETCH_BYTES).decode(content_type, errors="ignore")
    return final_url, body


def _extract_douyin_video_url_from_text(text: str) -> str | None:
    patterns = [
        r"https?://www\.douyin\.com/video/(\d{10,20})",
        r"\"aweme_id\"\s*:\s*\"(\d{10,20})\"",
        r"\"itemId\"\s*:\s*\"(\d{10,20})\"",
        r"\"group_id\"\s*:\s*\"(\d{10,20})\"",
        r"/video/(\d{10,20})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return f"https://www.douyin.com/video/{match.group(1)}"
    return None


def _extract_bilibili_video_url_from_text(text: str) -> str | None:
    bv_match = re.search(r"\bBV[0-9A-Za-z]{10}\b", text, flags=re.IGNORECASE)
    if bv_match:
        return f"https://www.bilibili.com/video/{bv_match.group(0)}"
    av_match = re.search(r"\bav\d+\b", text, flags=re.IGNORECASE)
    if av_match:
        return f"https://www.bilibili.com/video/{av_match.group(0)}"
    return None


def _build_probe_candidates(url: str, platform: str, log_func: LogFunc | None = None) -> list[str]:
    candidates: list[str] = []
    base_url = _normalize_input_url(unquote(url))
    if base_url:
        candidates.append(base_url)

    host = _extract_host(base_url)
    should_expand = host in {"v.douyin.com", "iesdouyin.com", "b23.tv", "bili2233.cn"}
    if should_expand:
        try:
            expanded = _expand_short_url(base_url)
            if expanded and expanded != base_url:
                if log_func:
                    log_func(f"短链已展开：{expanded}")
                candidates.append(expanded)
        except URLError:
            pass
        except (ValueError, RuntimeError):
            pass

    text_for_extract = " ".join(candidates)
    if platform == PLATFORM_DOUYIN:
        long_url = _extract_douyin_video_url_from_text(text_for_extract)
        if long_url:
            candidates.append(long_url)
        if not long_url and should_expand:
            try:
                final_url, body = _fetch_web_text(base_url)
                if final_url:
                    candidates.append(final_url)
                from_body = _extract_douyin_video_url_from_text(body)
                if from_body:
                    candidates.append(from_body)
            except (URLError, UnicodeDecodeError, RuntimeError):
                pass
    elif platform == PLATFORM_BILIBILI:
        canonical = _extract_bilibili_video_url_from_text(text_for_extract)
        if canonical:
            candidates.append(canonical)

    return _dedupe_keep_order(candidates)


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "paid", "member", "vip"}
    return False


def _detect_restricted(info: dict[str, Any], platform: str) -> tuple[bool, str]:
    direct_flags = {
        "is_upower_exclusive": "\u5145\u7535\u4e13\u5c5e\u5185\u5bb9",
        "is_charging_arc": "\u5145\u7535\u4e13\u5c5e\u5185\u5bb9",
        "is_paid": "\u4ed8\u8d39\u5185\u5bb9",
        "needs_purchase": "\u9700\u8981\u8d2d\u4e70\u540e\u89c2\u770b",
        "require_payment": "\u9700\u8981\u4ed8\u8d39",
        "need_vip": "\u4f1a\u5458\u4e13\u5c5e\u5185\u5bb9",
        "is_vip": "\u4f1a\u5458\u4e13\u5c5e\u5185\u5bb9",
        "payment_required": "\u9700\u8981\u4ed8\u8d39",
        "requires_subscription": "\u9700\u8981\u8ba2\u9605",
    }
    for field, reason in direct_flags.items():
        if _is_truthy(info.get(field)):
            return True, reason

    availability = str(info.get("availability", "")).lower()
    if any(token in availability for token in ("subscriber", "premium", "members", "paid")):
        return True, f"\u53d7\u9650\u5185\u5bb9\uff08availability={availability}\uff09"

    rights = info.get("rights")
    if isinstance(rights, dict):
        rights_flags = {
            "pay": "\u4ed8\u8d39\u5185\u5bb9",
            "is_upower_exclusive": "\u5145\u7535\u4e13\u5c5e\u5185\u5bb9",
            "need_vip": "\u4f1a\u5458\u4e13\u5c5e\u5185\u5bb9",
            "arc_pay": "\u4ed8\u8d39\u5185\u5bb9",
        }
        for field, reason in rights_flags.items():
            if _is_truthy(rights.get(field)):
                return True, reason

    if platform == PLATFORM_BILIBILI:
        badge = str(info.get("badge", "")).lower()
        if any(token in badge for token in ("charge", "paid", "vip", "member")):
            return True, f"\u53d7\u9650\u5185\u5bb9\uff08badge={badge}\uff09"

    return False, ""


def _iter_video_entries(info: dict[str, Any]) -> list[dict[str, Any]]:
    entries = info.get("entries")
    if isinstance(entries, list) and entries:
        return [entry for entry in entries if isinstance(entry, dict)]
    return [info]


def _apply_cookie_options(
    opts: dict[str, Any],
    cookiefile: str | None,
    cookies_from_browser: str,
) -> None:
    if cookiefile:
        opts["cookiefile"] = cookiefile
        return
    if cookies_from_browser != "none":
        opts["cookiesfrombrowser"] = (cookies_from_browser,)


def _normalize_cookie_lines(lines: list[str]) -> tuple[list[str], bool]:
    fixed: list[str] = []
    changed = False
    for raw in lines:
        line = raw.rstrip("\n")
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            fixed.append(line)
            continue

        parts = line.split("\t")
        if len(parts) < 7:
            fixed.append(line)
            continue

        domain = parts[0].strip()
        include_subdomains = parts[1].strip().upper()

        if domain.startswith(".") and include_subdomains == "FALSE":
            parts[1] = "TRUE"
            changed = True
        elif (not domain.startswith(".")) and include_subdomains == "TRUE":
            parts[1] = "FALSE"
            changed = True

        fixed.append("\t".join(parts))
    return fixed, changed


def _to_bool_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _cookie_expire_from_json(record: dict[str, Any]) -> int:
    for key in ("expirationDate", "expires", "expiry", "expiration"):
        value = record.get(key)
        if value in (None, ""):
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if numeric >= 1e12:
            numeric = numeric / 1000.0
        if numeric <= 0:
            return 0
        return int(numeric)

    if _to_bool_flag(record.get("session")):
        return 0
    return 0


def _json_cookie_record_to_netscape_line(record: dict[str, Any]) -> str | None:
    domain = str(record.get("domain") or record.get("host") or "").strip()
    name = str(record.get("name") or record.get("key") or "").strip()
    if (not domain) or (not name):
        return None

    host_only = _to_bool_flag(record.get("hostOnly"))
    include_subdomains = "FALSE" if host_only else "TRUE"
    path = str(record.get("path") or "/").strip() or "/"
    secure_flag = "TRUE" if _to_bool_flag(record.get("secure")) else "FALSE"
    expires = _cookie_expire_from_json(record)
    value = str(record.get("value") or "").strip()

    if _to_bool_flag(record.get("httpOnly")) and not domain.startswith("#HttpOnly_"):
        domain = f"#HttpOnly_{domain}"

    clean = lambda text: text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return (
        f"{clean(domain)}\t{include_subdomains}\t{clean(path)}\t{secure_flag}\t"
        f"{expires}\t{clean(name)}\t{clean(value)}"
    )


def _extract_json_cookie_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("cookies", "Cookies", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _parse_cookie_expire_text(text: str) -> int:
    value = (text or "").strip()
    if not value:
        return 0
    lower = value.lower()
    if lower in {"session", "session cookie", "none", "never"}:
        return 0

    patterns = (
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y-%m-%d",
    )
    for pattern in patterns:
        try:
            return int(datetime.strptime(value, pattern).timestamp())
        except ValueError:
            continue

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 0
    if numeric >= 1e12:
        numeric = numeric / 1000.0
    if numeric <= 0:
        return 0
    return int(numeric)


def _human_cookie_record_to_netscape_line(record: dict[str, str]) -> str | None:
    domain = (record.get("domain") or "").strip()
    name = (record.get("name") or "").strip()
    if (not domain) or (not name):
        return None

    include_subdomains = "TRUE" if domain.startswith(".") else "FALSE"
    path = (record.get("path") or "/").strip() or "/"
    secure = "TRUE" if _to_bool_flag(record.get("secure")) else "FALSE"
    expires = _parse_cookie_expire_text(record.get("expires", ""))
    value = (record.get("value") or "").strip()

    if _to_bool_flag(record.get("httponly")) and not domain.startswith("#HttpOnly_"):
        domain = f"#HttpOnly_{domain}"

    clean = lambda text: text.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return (
        f"{clean(domain)}\t{include_subdomains}\t{clean(path)}\t{secure}\t"
        f"{expires}\t{clean(name)}\t{clean(value)}"
    )


def _convert_human_cookie_dump_to_netscape_lines(raw_text: str) -> list[str] | None:
    text = (raw_text or "").lstrip("\ufeff")
    if "Cookie 1:" not in text:
        return None

    rows: list[dict[str, str]] = []
    current: dict[str, str] = {}

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith("cookie ") and line.endswith(":"):
            if current:
                rows.append(current)
            current = {}
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        current[key.strip().lower()] = value.strip()

    if current:
        rows.append(current)
    if not rows:
        return None

    lines = [
        "# Netscape HTTP Cookie File",
        "# Converted automatically from human-readable cookie dump.",
    ]
    converted_count = 0
    for row in rows:
        line = _human_cookie_record_to_netscape_line(row)
        if line:
            lines.append(line)
            converted_count += 1
    if converted_count == 0:
        return None
    return lines


def _convert_json_cookies_text_to_netscape_lines(raw_text: str) -> list[str] | None:
    text = (raw_text or "").lstrip("\ufeff").strip()
    if not text:
        return None
    if not (text.startswith("{") or text.startswith("[")):
        return None
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None

    items = _extract_json_cookie_items(payload)
    if not items:
        return None

    lines = [
        "# Netscape HTTP Cookie File",
        "# Converted automatically from JSON cookie export.",
    ]
    converted_count = 0
    for item in items:
        row = _json_cookie_record_to_netscape_line(item)
        if row:
            lines.append(row)
            converted_count += 1
    if converted_count == 0:
        return None
    return lines


def _prepare_cookiefile(cookiefile: str, log_func: LogFunc) -> tuple[str | None, str | None]:
    path = Path(cookiefile)
    if not path.exists():
        log_func("cookies.txt 路径不存在，请重新选择文件。")
        return None, None

    try:
        raw_text = path.read_text(encoding="utf-8", errors="ignore")
    except (OSError, IOError) as exc:
        log_func(f"读取 cookies.txt 失败：{strip_ansi(str(exc))}")
        return None, None

    source_lines = raw_text.splitlines()
    changed = False
    converted_lines = _convert_json_cookies_text_to_netscape_lines(raw_text)
    conversion_source = ""
    if converted_lines:
        source_lines = converted_lines
        changed = True
        conversion_source = "JSON cookies 文件"
    if not converted_lines:
        converted_lines = _convert_human_cookie_dump_to_netscape_lines(raw_text)
        if converted_lines:
            source_lines = converted_lines
            changed = True
            conversion_source = "文本 cookies 导出文件"

    if conversion_source:
        log_func(f"检测到 {conversion_source}，已自动转换为 Netscape 格式用于本次任务。")

    normalized_lines, normalized_changed = _normalize_cookie_lines(source_lines)
    changed = changed or normalized_changed
    use_path = str(path)
    temp_path: str | None = None

    if changed:
        tmp = tempfile.NamedTemporaryFile(prefix="fixed_cookies_", suffix=".txt", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        tmp_path.write_text("\n".join(normalized_lines) + "\n", encoding="utf-8")
        temp_path = str(tmp_path)
        use_path = temp_path
        log_func("检测到 cookies.txt 格式字段异常，已自动修复后用于本次任务。")

    try:
        jar = MozillaCookieJar()
        jar.load(use_path, ignore_discard=True, ignore_expires=True)
    except LoadError as exc:
        log_func(f"cookies.txt 格式无效：{strip_ansi(str(exc))}")
        log_func("请重新导出 Netscape 格式 cookies.txt，或更换导出工具。")
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)
        return None, None
    except (OSError, IOError) as exc:
        log_func(f"cookies.txt 校验失败：{strip_ansi(str(exc))}")
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)
        return None, None

    return use_path, temp_path


def _collect_cookie_meta(cookiefile: str) -> list[tuple[str, str]]:
    path = Path(cookiefile)
    if not path.exists():
        return []
    rows: list[tuple[str, str]] = []
    try:
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = raw.split("\t")
            if len(parts) < 7:
                continue
            domain = parts[0].strip().lower()
            name = parts[5].strip()
            if domain and name:
                rows.append((domain, name))
    except (OSError, IOError):
        return []
    return rows


def _warn_if_cookiefile_looks_incomplete(cookiefile: str, platform: str, log_func: LogFunc) -> None:
    if platform != PLATFORM_DOUYIN:
        return
    rows = _collect_cookie_meta(cookiefile)
    if not rows:
        return

    domains = {domain for domain, _name in rows}
    names = {name for _domain, name in rows}
    total = len(rows)
    log_func(f"cookies 统计：共 {total} 条，涉及域名 {len(domains)} 个。")

    has_session = any(n in names for n in ("sessionid", "sessionid_ss"))
    has_antibot = any(n in names for n in ("msToken", "ttwid", "odin_tt", "sid_guard"))
    has_douyin_domain = any("douyin.com" in d for d in domains)

    if total < 20 or (not has_session) or (not has_antibot) or (not has_douyin_domain):
        log_func("[提示] 当前 cookies.txt 可能不完整，抖音可能返回 Fresh cookies。")
        if total < 20:
            log_func(f"[提示] cookies 条目偏少（当前 {total} 条）。")
        if not has_session:
            log_func("[提示] 缺少 sessionid/sessionid_ss。")
        if not has_antibot:
            log_func("[提示] 缺少 msToken/ttwid/odin_tt/sid_guard 等关键字段。")
        if not has_douyin_domain:
            log_func("[提示] 未检测到 douyin.com 域名 cookies。")
        log_func("[提示] 请在已登录抖音网页版的浏览器中重新导出 Netscape cookies.txt 后重试。")


def _export_browser_cookies_with_browser_cookie3(
    browser: str,
    platform: str,
    log_func: LogFunc,
) -> str | None:
    if browser == "none":
        return None

    try:
        import browser_cookie3  # type: ignore
    except (ModuleNotFoundError, ImportError):
        return None

    loader_map = {
        "edge": browser_cookie3.edge,
        "chrome": browser_cookie3.chrome,
        "firefox": browser_cookie3.firefox,
    }
    loader = loader_map.get(browser)
    if not loader:
        return None

    domain_hint = "douyin.com" if platform == PLATFORM_DOUYIN else "bilibili.com"
    cookie_jar = None

    try:
        cookie_jar = loader(domain_name=domain_hint)
        if not any(True for _ in cookie_jar):
            cookie_jar = loader()
    except (PermissionError, OSError, Exception) as exc:
        reason = strip_ansi(str(exc))
        log_func(f"[警告] browser-cookie3 读取 {browser} cookies 失败：{reason}")
        if "requires admin" in reason.lower():
            log_func(
                "提示：当前操作需要管理员权限。请使用管理员身份运行本软件，"
                "并尽量关闭 Edge/Chrome 后重试。"
            )
        return None

    try:
        tmp = tempfile.NamedTemporaryFile(prefix=f"{browser}_cookies_", suffix=".txt", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()

        mozilla_jar = MozillaCookieJar(str(tmp_path))
        count = 0
        for cookie in cookie_jar:
            mozilla_jar.set_cookie(cookie)
            count += 1

        if count == 0:
            tmp_path.unlink(missing_ok=True)
            return None

        mozilla_jar.save(ignore_discard=True, ignore_expires=True)
        log_func(f"已通过 browser-cookie3 导出 cookies（{count} 条），用于本次任务。")
        return str(tmp_path)
    except (OSError, IOError) as exc:
        reason = strip_ansi(str(exc))
        log_func(f"[警告] browser-cookie3 导出 cookies 失败：{reason}")
        return None


def _probe_and_filter(
    urls: list[str],
    cookiefile: str | None,
    cookies_from_browser: str,
    platform: str,
    strict_platform: bool,
    log_func: LogFunc | None = None,
    probe_workers: int = DEFAULT_PROBE_WORKERS,
    cancel_check: CancelCheck | None = None,
    proxy: str | None = None,
    retries: int = 1,
) -> tuple[list[str], list[str]]:
    probe_opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "socket_timeout": 20,
        "retries": max(0, int(retries)),
        "extractor_retries": max(0, int(retries)),
        "http_headers": HTTP_HEADERS,
        "logger": _SilentProbeLogger(),
    }
    if proxy:
        probe_opts["proxy"] = proxy
    _apply_cookie_options(probe_opts, cookiefile, cookies_from_browser)

    allowed: list[str] = []
    blocked_messages: list[str] = []
    total = len(urls)

    def should_cancel() -> bool:
        return bool(cancel_check and cancel_check())

    def probe_one_url(url: str) -> dict[str, Any] | None:
        with yt_dlp.YoutubeDL(probe_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return info if isinstance(info, dict) else None

    def normalize_extract_error(reason: str) -> str:
        text = strip_ansi(reason or "")
        lower = text.lower()
        if (
            platform == PLATFORM_DOUYIN
            and "unsupported url" in lower
            and "https://www.douyin.com/" in lower
        ):
            return (
                "抖音短链解析后落到首页，通常是短链已失效或分享口令不完整。"
                "请在浏览器先打开短链，复制地址栏中的完整视频页链接后再下载。"
            )
        return text

    def probe_single_url(original_url: str) -> dict[str, Any]:
        local_logs: list[str] = []

        def push_log(text: str) -> None:
            if text:
                local_logs.append(text)

        if should_cancel():
            raise DownloadCancelledError("用户取消任务")

        candidates = _build_probe_candidates(original_url, platform, push_log)
        if not candidates:
            return {
                "allowed": None,
                "blocked": (
                    f"[\u65e0\u6cd5\u89e3\u6790] {original_url}\n  "
                    "\u539f\u56e0: \u672a\u83b7\u53d6\u5230\u6709\u6548\u94fe\u63a5"
                ),
                "logs": local_logs,
            }

        if len(candidates) > 1:
            push_log(f"已生成 {len(candidates)} 个候选链接，按顺序尝试解析。")

        if strict_platform and not any(is_url_for_platform(cand, platform) for cand in candidates):
            return {
                "allowed": None,
                "blocked": (
                    f"[\u5df2\u62e6\u622a] {original_url}\n  "
                    f"\u539f\u56e0: \u5f53\u524d\u6a21\u5f0f\u4e3a {platform_name_cn(platform)}\uff0c"
                    "\u5019\u9009\u94fe\u63a5\u4e0e\u5e73\u53f0\u4e0d\u5339\u914d"
                ),
                "logs": local_logs,
            }

        chosen_url: str | None = None
        chosen_info: dict[str, Any] | None = None
        candidate_errors: list[str] = []
        started = time.monotonic()

        for cand_idx, candidate_url in enumerate(candidates, start=1):
            if should_cancel():
                raise DownloadCancelledError("用户取消任务")
            if time.monotonic() - started > PROBE_PER_URL_TIMEOUT:
                candidate_errors.append(f"检查超时（>{PROBE_PER_URL_TIMEOUT} 秒）")
                break
            if len(candidates) > 1:
                push_log(f"尝试候选链接 {cand_idx}/{len(candidates)}：{candidate_url}")
            try:
                info = probe_one_url(candidate_url)
            except (RuntimeError, Exception) as exc:
                candidate_errors.append(normalize_extract_error(str(exc)))
                continue
            if not isinstance(info, dict):
                candidate_errors.append("无法读取视频信息")
                continue
            chosen_url = candidate_url
            chosen_info = info
            break

        if not chosen_url or not isinstance(chosen_info, dict):
            reason = candidate_errors[-1] if candidate_errors else "解析失败"
            if len(candidate_errors) > 1:
                reason = f"多次尝试仍失败。最后错误：{reason}"
            return {
                "allowed": None,
                "blocked": f"[\u65e0\u6cd5\u89e3\u6790] {original_url}\n  \u539f\u56e0: {reason}",
                "logs": local_logs,
            }

        for entry in _iter_video_entries(chosen_info):
            restricted, reason = _detect_restricted(entry, platform)
            if restricted:
                title = entry.get("title") or "\u672a\u77e5\u6807\u9898"
                return {
                    "allowed": None,
                    "blocked": (
                        f"[\u5df2\u62e6\u622a] {chosen_url}\n  "
                        f"\u6807\u9898: {title}\n  \u539f\u56e0: {reason}"
                    ),
                    "logs": local_logs,
                }

        return {"allowed": chosen_url, "blocked": None, "logs": local_logs}

    try:
        if total == 0:
            return allowed, blocked_messages

        workers = max(1, min(int(probe_workers), total))
        if log_func:
            log_func(f"链接检查并发数：{workers}")

        jobs: list[tuple[int, str, concurrent.futures.Future[dict[str, Any]]]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for idx, original_url in enumerate(urls, start=1):
                if should_cancel():
                    raise DownloadCancelledError("用户取消任务")
                if log_func:
                    log_func(f"正在检查第 {idx}/{total} 条链接：{original_url}")
                jobs.append((idx, original_url, executor.submit(probe_single_url, original_url)))

            wait_timeout = PROBE_PER_URL_TIMEOUT + SHORT_LINK_EXPAND_TIMEOUT + 8
            for _idx, original_url, future in jobs:
                if should_cancel():
                    raise DownloadCancelledError("用户取消任务")
                try:
                    result = future.result(timeout=wait_timeout)
                except concurrent.futures.TimeoutError:
                    blocked_messages.append(
                        f"[\u65e0\u6cd5\u89e3\u6790] {original_url}\n  "
                        f"\u539f\u56e0: 检查超时（>{PROBE_PER_URL_TIMEOUT} 秒）"
                    )
                    future.cancel()
                    continue
                except DownloadCancelledError:
                    raise
                except (RuntimeError, Exception) as exc:
                    blocked_messages.append(
                        f"[\u65e0\u6cd5\u89e3\u6790] {original_url}\n  "
                        f"\u539f\u56e0: {normalize_extract_error(str(exc))}"
                    )
                    continue

                logs = result.get("logs")
                if log_func and isinstance(logs, list):
                    for line in logs:
                        log_func(str(line))

                blocked = result.get("blocked")
                if blocked:
                    blocked_messages.append(str(blocked))
                    continue

                allowed_url = result.get("allowed")
                if isinstance(allowed_url, str) and allowed_url.strip():
                    allowed.append(allowed_url.strip())
    except (RuntimeError, ValueError) as exc:
        raise RuntimeError(strip_ansi(str(exc))) from exc
    except DownloadCancelledError:
        raise

    return _dedupe_keep_order(allowed), blocked_messages


def build_format_selector(resolution: str) -> str:
    if resolution == RESOLUTION_BEST:
        return "bv*+ba/b"
    height = int(resolution)
    return f"bv*[height<={height}]+ba/b[height<={height}]/bv*+ba/b"


class _YDLLogger:
    def __init__(self, log: LogFunc, state_hook: StateHook | None = None) -> None:
        self._log = log
        self._state_hook = state_hook

    def debug(self, msg: str) -> None:
        text = strip_ansi((msg or "").strip())
        lower = text.lower()
        if self._state_hook is not None and (
            "resuming download at byte" in lower
            or "resuming fragment" in lower
            or "[download] resuming" in lower
        ):
            try:
                self._state_hook({"event": "resume_detected", "message": text})
            except Exception:
                pass
        if text:
            self._log(text)

    def info(self, msg: str) -> None:
        self.debug(msg)

    def warning(self, msg: str) -> None:
        text = strip_ansi((msg or "").strip())
        if text:
            self._log(f"[\u8b66\u544a] {text}")

    def error(self, msg: str) -> None:
        text = strip_ansi((msg or "").strip())
        if text:
            self._log(f"[\u9519\u8bef] {text}")


class _SilentProbeLogger:
    def debug(self, _msg: str) -> None:
        return

    def info(self, _msg: str) -> None:
        return

    def warning(self, _msg: str) -> None:
        return

    def error(self, _msg: str) -> None:
        return


def _download_urls(
    urls: list[str],
    output_dir: Path,
    cookiefile: str | None,
    cookies_from_browser: str,
    resolution: str,
    extract_audio: bool,
    audio_format: str,
    write_subtitles: bool = False,
    write_thumbnail: bool = False,
    write_info_json: bool = False,
    proxy: str | None = None,
    rate_limit: int | None = None,
    retries: int = 1,
    filename_template: str | None = None,
    progress_hook: ProgressHook | None = None,
    log: LogFunc | None = None,
    state_hook: StateHook | None = None,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    outtmpl_value = normalize_filename_template(filename_template)
    ydl_opts: dict[str, Any] = {
        "outtmpl": str(output_dir / outtmpl_value),
        "format": build_format_selector(resolution),
        "merge_output_format": "mp4",
        "continuedl": True,
        "noplaylist": False,
        "ignoreerrors": False,
        "retries": max(0, int(retries)),
        "extractor_retries": max(0, int(retries)),
        "http_headers": HTTP_HEADERS,
    }
    if proxy:
        ydl_opts["proxy"] = proxy
    if rate_limit is not None:
        ydl_opts["ratelimit"] = int(rate_limit)
    _apply_cookie_options(ydl_opts, cookiefile, cookies_from_browser)

    if progress_hook:
        ydl_opts["progress_hooks"] = [progress_hook]
    if log:
        ydl_opts["logger"] = _YDLLogger(log, state_hook=state_hook)
    if write_subtitles:
        ydl_opts["writesubtitles"] = True
        ydl_opts["writeautomaticsub"] = True
        ydl_opts["subtitleslangs"] = ["all"]
    if write_thumbnail:
        ydl_opts["writethumbnail"] = True
    if write_info_json:
        ydl_opts["writeinfojson"] = True
    if extract_audio:
        ydl_opts["keepvideo"] = True
        ydl_opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": audio_format,
                "preferredquality": "192",
            }
        ]

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.download(urls)


def _sanitize_web_filename(name: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]+', "_", (name or "").strip())
    cleaned = cleaned.strip(" .")
    return cleaned or "web_video"


def _download_web_url(
    source_url: str,
    output_dir: Path,
    log_func: LogFunc,
    state_hook: StateHook | None = None,
    cancel_check: CancelCheck | None = None,
) -> int:
    if cancel_check and cancel_check():
        raise DownloadCancelledError("user cancelled task")

    try:
        sniff = sniff_media_sync(page_url=source_url, timeout_ms=45_000, wait_after_load_ms=6_000, headless=False)
    except ModuleNotFoundError as exc:
        reason = strip_ansi(str(exc))
        if "playwright" in reason.lower():
            raise DownloadFailure(
                "DEPENDENCY_PLAYWRIGHT_MISSING",
                "Missing Playwright dependency. Run: python -m pip install -r requirements.txt",
            ) from exc
        raise DownloadFailure(classify_download_error(reason), reason) from exc
    except Exception as exc:
        reason = strip_ansi(str(exc))
        raise DownloadFailure(classify_download_error(reason), f"Web sniff failed: {reason}") from exc

    media_url = str(sniff.get("best_url") or "").strip()
    if not media_url:
        raise DownloadFailure(
            "WEB_MEDIA_NOT_FOUND",
            "No downloadable media URL detected. Play the source page and retry.",
        )

    if state_hook is not None:
        try:
            state_hook({"event": "web_stream_resolved", "source_url": source_url, "media_url": media_url})
        except Exception:
            pass

    title = _sanitize_web_filename(str(sniff.get("title") or "web_video"))
    target = _unique_target_path(output_dir / f"{title}.mp4")
    cookie_header = build_cookie_header(sniff.get("cookies") or [])
    user_agent = str(sniff.get("user_agent") or "").strip() or None

    try:
        run_ffmpeg_download(
            media_url=media_url,
            output_path=str(target),
            user_agent=user_agent,
            referer=source_url,
            cookie_header=(cookie_header or None),
            cancel_check=cancel_check,
        )
    except FFmpegDownloadCancelled as exc:
        raise DownloadCancelledError("user cancelled task") from exc
    except FileNotFoundError as exc:
        raise DownloadFailure("DEPENDENCY_FFMPEG_MISSING", "ffmpeg was not found in PATH.") from exc
    except subprocess.CalledProcessError as exc:
        raise DownloadFailure("FFMPEG_DOWNLOAD_FAILED", f"ffmpeg exited with code {exc.returncode}.") from exc
    except Exception as exc:
        reason = strip_ansi(str(exc))
        raise DownloadFailure(classify_download_error(reason), f"ffmpeg download failed: {reason}") from exc

    log_func(f"[网页抓流] 下载完成：{target}")
    return 0


def run_download(
    urls: list[str],
    output_dir: Path,
    cookiefile: str | None = None,
    cookies_from_browser: str = "none",
    log: LogFunc | None = None,
    progress_hook: ProgressHook | None = None,
    state_hook: StateHook | None = None,
    cancel_check: CancelCheck | None = None,
    probe_workers: int = DEFAULT_PROBE_WORKERS,
    platform: str = PLATFORM_AUTO,
    strict_platform: bool = True,
    resolution: str = RESOLUTION_BEST,
    extract_audio: bool = False,
    audio_format: str = "mp3",
    write_subtitles: bool = False,
    write_thumbnail: bool = False,
    write_info_json: bool = False,
    proxy: str | None = None,
    rate_limit: str | None = None,
    retries: int = 1,
    adaptive_retry_attempts: int = DEFAULT_ADAPTIVE_RETRY_ATTEMPTS,
    filename_template: str | None = None,
    post_action: str = POST_ACTION_NONE,
    history_file: str | None = None,
    skip_history_success: bool = False,
) -> int:
    log_func = log if log else print
    temp_cookiefile_for_session: str | None = None
    temp_fixed_cookiefile: str | None = None
    history_records: list[dict[str, Any]] = []
    success_history_urls: set[str] = set()

    def should_cancel() -> bool:
        return bool(cancel_check and cancel_check())

    def emit_state(payload: dict[str, Any]) -> None:
        if state_hook is None:
            return
        try:
            state_hook(payload)
        except Exception:
            pass

    def guarded_progress_hook(payload: dict[str, Any]) -> None:
        if should_cancel():
            raise DownloadCancelledError("用户取消任务")
        if progress_hook is not None:
            progress_hook(payload)

    def append_history_record(url: str, status: str, detail: str = "") -> None:
        if not history_file:
            return
        history_records.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "url": url,
                "status": status,
                "detail": detail,
            }
        )

    def append_blocked_history(messages: list[str]) -> None:
        for message in messages:
            first_line = (message or "").strip().splitlines()[0] if message else ""
            if not first_line:
                continue
            match = re.match(r"\[[^\]]+\]\s+(\S+)", first_line)
            if not match:
                continue
            blocked_url = match.group(1).strip()
            if not blocked_url:
                continue
            append_history_record(blocked_url, "blocked", first_line)

    def emit_run_failed(reason: str, status_code: int = 1, error_code: str = "UNKNOWN") -> None:
        emit_state(
            {
                "event": "run_failed",
                "status_code": int(status_code),
                "error_code": (error_code or "UNKNOWN").strip().upper(),
                "reason": (reason or "").strip(),
            }
        )

    platform = normalize_platform(platform)
    if not platform:
        reason = (
            "\u5e73\u53f0\u53c2\u6570\u65e0\u6548\uff0c"
            f"\u53ef\u9009\uff1a{', '.join(SUPPORTED_PLATFORMS)}"
        )
        log_func(reason)
        emit_run_failed(reason, status_code=1, error_code="ARGUMENT_INVALID")
        return 1

    resolution = normalize_resolution(resolution)
    if not resolution:
        reason = f"\u5206\u8fa8\u7387\u53c2\u6570\u65e0\u6548\uff0c\u53ef\u9009\uff1a{', '.join(SUPPORTED_RESOLUTIONS)}"
        log_func(reason)
        emit_run_failed(reason, status_code=1, error_code="ARGUMENT_INVALID")
        return 1

    audio_format = normalize_audio_format(audio_format)
    if not audio_format:
        reason = f"\u97f3\u9891\u683c\u5f0f\u65e0\u6548\uff0c\u53ef\u9009\uff1a{', '.join(SUPPORTED_AUDIO_FORMATS)}"
        log_func(reason)
        emit_run_failed(reason, status_code=1, error_code="ARGUMENT_INVALID")
        return 1

    cookies_from_browser = normalize_browser(cookies_from_browser)
    if not cookies_from_browser:
        reason = f"cookies \u6765\u6e90\u65e0\u6548\uff0c\u53ef\u9009\uff1a{', '.join(SUPPORTED_BROWSERS)}"
        log_func(reason)
        emit_run_failed(reason, status_code=1, error_code="ARGUMENT_INVALID")
        return 1

    proxy = (proxy or "").strip() or None
    rate_limit_text = (rate_limit or "").strip() or None
    rate_limit_value, rate_limit_error = parse_rate_limit(rate_limit_text)
    if rate_limit_error:
        log_func(rate_limit_error)
        emit_run_failed(rate_limit_error, status_code=1, error_code="ARGUMENT_INVALID")
        return 1
    try:
        retries = max(0, int(retries))
    except (TypeError, ValueError):
        reason = "重试次数参数无效，应为非负整数。"
        log_func(reason)
        emit_run_failed(reason, status_code=1, error_code="ARGUMENT_INVALID")
        return 1
    try:
        adaptive_retry_attempts = max(1, int(adaptive_retry_attempts))
    except (TypeError, ValueError):
        adaptive_retry_attempts = DEFAULT_ADAPTIVE_RETRY_ATTEMPTS
    filename_template = normalize_filename_template(filename_template)
    post_action = normalize_post_action(post_action)

    cleaned_urls = extract_urls_from_inputs(urls)
    if not cleaned_urls:
        reason = (
            "\u672a\u63d0\u4f9b\u53ef\u7528\u94fe\u63a5\u3002\u53ef\u76f4\u63a5\u7c98\u8d34\u5206\u4eab\u6587\u6848\uff0c"
            "\u7a0b\u5e8f\u4f1a\u81ea\u52a8\u63d0\u53d6 http(s) \u94fe\u63a5\u3002"
        )
        log_func(reason)
        emit_run_failed(reason, status_code=1, error_code="INPUT_EMPTY")
        return 1

    if history_file:
        history_records = _load_history_records(history_file)
        success_history_urls = {
            str(item.get("url", "")).strip()
            for item in history_records
            if str(item.get("status", "")).lower() == "success"
        }

    pre_blocked_messages: list[str] = []
    if platform == PLATFORM_AUTO:
        platform_batches = build_auto_platform_batches(cleaned_urls)
    else:
        platform_batches = [(platform, cleaned_urls)]

    if not platform_batches:
        if pre_blocked_messages:
            log_func("")
            log_func("\u4ee5\u4e0b\u94fe\u63a5\u5df2\u62e6\u622a\uff1a")
            for msg in pre_blocked_messages:
                log_func(msg)
            append_blocked_history(pre_blocked_messages)
        log_func("\u6ca1\u6709\u53ef\u4e0b\u8f7d\u7684\u53ef\u8bbf\u95ee\u89c6\u9891\u94fe\u63a5\uff0c\u7a0b\u5e8f\u7ed3\u675f\u3002")
        if history_file:
            try:
                _save_history_records(history_file, history_records[-5000:])
            except OSError:
                pass
        emit_run_failed("没有可下载的可访问视频链接，程序结束。", status_code=1, error_code="NO_ALLOWED_LINKS")
        return 1

    log_func(f"\u5f53\u524d\u5e73\u53f0\uff1a{platform_name_cn(platform)}")
    if platform == PLATFORM_AUTO and len(platform_batches) > 1:
        log_func("\u5df2\u68c0\u6d4b\u5230\u6df7\u5408\u5e73\u53f0\u94fe\u63a5\uff0c\u5c06\u5206\u7ec4\u68c0\u67e5\u5e76\u4e0b\u8f7d\u3002")
    if resolution == RESOLUTION_BEST:
        log_func("\u5206\u8fa8\u7387\uff1a\u539f\u753b\uff08\u6700\u4f73\uff09")
    else:
        log_func(f"\u5206\u8fa8\u7387\uff1a\u4e0d\u9ad8\u4e8e {resolution}p")

    if extract_audio:
        log_func(f"\u5206\u79bb\u97f3\u9891\uff1a\u5f00\u542f\uff08{audio_format}\uff09")
        log_func("\u63d0\u793a\uff1a\u5206\u79bb\u97f3\u9891\u9700\u8981\u7cfb\u7edf\u5b89\u88c5 ffmpeg\u3002")
    else:
        log_func("\u5206\u79bb\u97f3\u9891\uff1a\u5173\u95ed")

    export_parts: list[str] = []
    if write_subtitles:
        export_parts.append("字幕")
    if write_thumbnail:
        export_parts.append("封面")
    if write_info_json:
        export_parts.append("元数据JSON")
    log_func(f"附加导出：{', '.join(export_parts) if export_parts else '关闭'}")
    log_func(f"网络设置：重试 {retries} 次，代理 {'已配置' if proxy else '未配置'}")
    if rate_limit_value is not None:
        log_func(f"限速：{rate_limit_text} ({rate_limit_value} B/s)")
    log_func(f"自适应重试：最多 {adaptive_retry_attempts} 轮")
    log_func(f"文件名模板：{filename_template}")
    if post_action != POST_ACTION_NONE:
        log_func(f"下载后动作：{post_action}")

    effective_cookiefile = cookiefile
    effective_cookies_from_browser = cookies_from_browser
    has_douyin_batch = any(p == PLATFORM_DOUYIN for p, _ in platform_batches)
    cookie_platform_hint = PLATFORM_DOUYIN if has_douyin_batch else PLATFORM_BILIBILI

    if cookiefile:
        fixed_cookiefile, temp_fixed_cookiefile = _prepare_cookiefile(cookiefile, log_func)
        if not fixed_cookiefile:
            return 1
        effective_cookiefile = fixed_cookiefile
        log_func(f"\u8eab\u4efd\u51ed\u636e\uff1acookies.txt -> {Path(cookiefile).resolve()}")
        if has_douyin_batch:
            _warn_if_cookiefile_looks_incomplete(effective_cookiefile, PLATFORM_DOUYIN, log_func)
    elif cookies_from_browser != "none":
        log_func(f"\u8eab\u4efd\u51ed\u636e\uff1a\u4ece {cookies_from_browser} \u6d4f\u89c8\u5668\u8bfb\u53d6 cookies")
        temp_cookiefile_for_session = _export_browser_cookies_with_browser_cookie3(
            cookies_from_browser,
            cookie_platform_hint,
            log_func,
        )
        if temp_cookiefile_for_session:
            effective_cookiefile = temp_cookiefile_for_session
            effective_cookies_from_browser = "none"
            log_func("已切换为临时 cookies 文件模式，可规避浏览器数据库复制失败问题。")
            if has_douyin_batch:
                _warn_if_cookiefile_looks_incomplete(effective_cookiefile, PLATFORM_DOUYIN, log_func)
    else:
        log_func("\u8eab\u4efd\u51ed\u636e\uff1a\u672a\u914d\u7f6e\uff08\u6296\u97f3\u53ef\u80fd\u9700\u8981\u65b0\u9c9c cookies\uff09")

    try:
        log_func("\u6b63\u5728\u68c0\u67e5\u94fe\u63a5\u6743\u9650\u72b6\u6001...")
        allowed: list[str] = []
        blocked_messages = list(pre_blocked_messages)

        for batch_platform, batch_urls in platform_batches:
            if should_cancel():
                raise DownloadCancelledError("用户取消任务")
            if platform == PLATFORM_AUTO:
                log_func(
                    f"\u68c0\u67e5 {platform_name_cn(batch_platform)} \u94fe\u63a5\uff1a"
                    f"{len(batch_urls)} \u6761"
                )
            if batch_platform == PLATFORM_WEB:
                allowed.extend(_dedupe_keep_order(batch_urls))
                continue
            allowed_batch, blocked_batch = _probe_and_filter(
                batch_urls,
                cookiefile=effective_cookiefile,
                cookies_from_browser=effective_cookies_from_browser,
                platform=batch_platform,
                strict_platform=(True if platform == PLATFORM_AUTO else strict_platform),
                log_func=log_func,
                probe_workers=probe_workers,
                cancel_check=cancel_check,
                proxy=proxy,
                retries=retries,
            )
            allowed.extend(allowed_batch)
            blocked_messages.extend(blocked_batch)

        allowed = _dedupe_keep_order(allowed)
        if skip_history_success and success_history_urls:
            filtered_allowed: list[str] = []
            for url in allowed:
                if url in success_history_urls:
                    blocked_messages.append(
                        f"[\u5df2\u8df3\u8fc7] {url}\n  \u539f\u56e0: \u4e0b\u8f7d\u5386\u53f2\u4e2d\u5df2\u6210\u529f"
                    )
                else:
                    filtered_allowed.append(url)
            allowed = filtered_allowed

        if blocked_messages:
            log_func("")
            log_func("\u4ee5\u4e0b\u94fe\u63a5\u5df2\u62e6\u622a\uff1a")
            for msg in blocked_messages:
                log_func(msg)
            append_blocked_history(blocked_messages)

        if not allowed:
            joined = "\n".join(blocked_messages)
            if "Could not copy Chrome cookie database" in joined:
                log_func("")
                log_func(
                    "提示：浏览器 cookies 数据库被占用。请完全关闭 Edge/Chrome 后重试，"
                    "或手动提供最新 cookies.txt。"
                )
                log_func(
                    "提示：若提示 requires admin，请用“管理员身份”启动本软件后再试。"
                )
            if "Failed to decrypt with DPAPI" in joined or "Unable to get key for cookie decryption" in joined:
                log_func("")
                log_func("提示：当前系统环境无法直接解密 Chromium（Edge/Chrome）cookies。")
                log_func("建议按以下顺序处理：")
                log_func("1. 在 Edge 打开抖音网页版并确认已登录，然后关闭所有 Edge 窗口。")
                log_func("2. 使用浏览器扩展导出 Netscape 格式 cookies.txt，并在软件中选择该文件。")
                log_func("3. 或改用 Firefox 登录抖音后，在软件里将 cookies 来源切到 Firefox 再试。")
            if "检查超时" in joined:
                log_func("")
                log_func("提示：部分链接检查超时，可能是短链重定向或网络波动导致。")
                log_func("建议：")
                log_func("1. 先在浏览器打开一次该短链，复制浏览器地址栏中的完整长链接再下载。")
                log_func("2. 使用稳定网络后重试。")
            if has_douyin_batch:
                log_func("")
                log_func(
                    "\u63d0\u793a\uff1a\u82e5\u51fa\u73b0 Fresh cookies \u62a5\u9519\uff0c\u8bf7\u5728\u8f6f\u4ef6\u4e2d\u542f\u7528"
                    "\u201c\u4ece\u6d4f\u89c8\u5668\u8bfb\u53d6 cookies\uff08\u63a8\u8350 Edge\uff09\u201d\uff0c"
                    "\u6216\u63d0\u4f9b\u6700\u65b0 cookies.txt\u3002"
                )
            final_reason = "\u6ca1\u6709\u53ef\u4e0b\u8f7d\u7684\u53ef\u8bbf\u95ee\u89c6\u9891\u94fe\u63a5\uff0c\u7a0b\u5e8f\u7ed3\u675f\u3002"
            log_func(final_reason)
            error_code, detailed_reason = _summarize_no_allowed_links_reason(
                joined_blocked_messages=joined,
                blocked_messages=blocked_messages,
                cookiefile=cookiefile,
                cookies_from_browser=cookies_from_browser,
            )
            emit_run_failed(detailed_reason, status_code=1, error_code=error_code)
            return 1

        log_func("")
        log_func(f"\u51c6\u5907\u4e0b\u8f7d {len(allowed)} \u4e2a\u94fe\u63a5\u5230\uff1a{output_dir.resolve()}")
        emit_state({"event": "plan", "total_links": len(allowed)})

        try:
            for idx, url in enumerate(allowed, start=1):
                if should_cancel():
                    raise DownloadCancelledError("用户取消任务")
                emit_state({"event": "link_start", "index": idx, "total_links": len(allowed), "url": url})
                log_func(f"[{idx}/{len(allowed)}] 开始下载：{url}")
                result = 2
                last_reason = ""
                error_code = "UNKNOWN"
                for attempt in range(1, adaptive_retry_attempts + 1):
                    if should_cancel():
                        raise DownloadCancelledError("用户取消任务")
                    snapshot_before = _snapshot_output_files(output_dir)
                    try:
                        task_platform = _platform_from_url(url)
                        if task_platform == PLATFORM_WEB:
                            result = _download_web_url(
                                source_url=url,
                                output_dir=output_dir,
                                log_func=log_func,
                                state_hook=emit_state,
                                cancel_check=should_cancel,
                            )
                        else:
                            result = _download_urls(
                                [url],
                                output_dir,
                                cookiefile=effective_cookiefile,
                                cookies_from_browser=effective_cookies_from_browser,
                                resolution=resolution,
                                extract_audio=extract_audio,
                                audio_format=audio_format,
                                write_subtitles=write_subtitles,
                                write_thumbnail=write_thumbnail,
                                write_info_json=write_info_json,
                                proxy=proxy,
                                rate_limit=rate_limit_value,
                                retries=retries,
                                filename_template=filename_template,
                                progress_hook=guarded_progress_hook,
                                log=log,
                                state_hook=emit_state,
                            )
                        last_reason = ""
                        error_code = ""
                    except DownloadCancelledError:
                        raise
                    except DownloadFailure as exc:
                        result = 2
                        error_code = exc.code
                        last_reason = f"[{exc.code}] {strip_ansi(str(exc))}"
                    except (RuntimeError, OSError, IOError) as exc:
                        result = 2
                        last_reason = strip_ansi(str(exc))
                        error_code = classify_download_error(last_reason)
                        if error_code != "UNKNOWN":
                            last_reason = f"[{error_code}] {last_reason}"

                    if result == 0:
                        new_files = _collect_new_or_updated_files(snapshot_before, output_dir)
                        apply_post_action(
                            post_action=post_action,
                            files=new_files,
                            output_dir=output_dir,
                            source_url=url,
                            log_func=log_func,
                        )
                        append_history_record(url, "success")
                        emit_state({"event": "link_done", "index": idx, "total_links": len(allowed), "url": url})
                        break

                    if not last_reason:
                        last_reason = f"status={result}"
                    has_next = attempt < adaptive_retry_attempts
                    should_retry = (
                        has_next
                        and (_is_transient_error(last_reason) or result != 0)
                        and not error_code.startswith("DEPENDENCY_")
                    )
                    if should_retry:
                        wait_seconds = _adaptive_backoff_seconds(attempt + 1)
                        log_func(
                            f"[重试] 第 {attempt}/{adaptive_retry_attempts} 次失败：{last_reason}，"
                            f"{wait_seconds:.1f} 秒后重试。"
                        )
                        emit_state(
                            {
                                "event": "link_retry",
                                "index": idx,
                                "total_links": len(allowed),
                                "url": url,
                                "attempt": attempt,
                                "max_attempts": adaptive_retry_attempts,
                                "wait_seconds": wait_seconds,
                                "reason": last_reason,
                            }
                        )
                        wait_until = time.monotonic() + wait_seconds
                        while time.monotonic() < wait_until:
                            if should_cancel():
                                raise DownloadCancelledError("用户取消任务")
                            time.sleep(0.2)
                        continue

                    append_history_record(url, "failed", last_reason)
                    emit_state(
                        {
                            "event": "link_failed",
                            "index": idx,
                            "total_links": len(allowed),
                            "url": url,
                            "status_code": result,
                            "error_code": error_code or classify_download_error(last_reason),
                            "reason": last_reason,
                        }
                    )
                    log_func(f"下载失败（最终）：{last_reason}")
                    return result
            log_func("\u4e0b\u8f7d\u5b8c\u6210\u3002")
            return 0
        except DownloadCancelledError:
            append_history_record("", "cancelled", "用户主动取消")
            log_func("\u4efb\u52a1\u5df2\u53d6\u6d88\u3002")
            return 130
        except (RuntimeError, OSError, IOError) as exc:
            reason = strip_ansi(str(exc))
            append_history_record("", "failed", reason)
            log_func(f"\u4e0b\u8f7d\u5931\u8d25\uff1a{reason}")
            if extract_audio:
                log_func(
                    "\u82e5\u62a5\u9519\u5305\u542b ffmpeg \u76f8\u5173\u4fe1\u606f\uff0c"
                    "\u8bf7\u5148\u5b89\u88c5 ffmpeg \u5e76\u52a0\u5165 PATH\u3002"
                )
            return 2
    except DownloadCancelledError:
        log_func("\u4efb\u52a1\u5df2\u53d6\u6d88\u3002")
        return 130
    except (RuntimeError, ValueError, OSError) as exc:
        reason = strip_ansi(str(exc))
        log_func(f"检查链接时发生异常：{reason}")
        if "invalid Netscape format cookies file" in reason:
            log_func("检测到 cookies.txt 不是有效 Netscape 格式，请重新导出后重试。")
        return 2
    finally:
        if temp_cookiefile_for_session:
            Path(temp_cookiefile_for_session).unlink(missing_ok=True)
        if temp_fixed_cookiefile:
            Path(temp_fixed_cookiefile).unlink(missing_ok=True)
        if history_file:
            try:
                _save_history_records(history_file, history_records[-5000:])
            except OSError:
                pass


def _duration_to_text(seconds: Any) -> str:
    if not isinstance(seconds, (int, float)) or seconds < 0:
        return "-"
    total = int(seconds)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def build_preview_items(
    urls: list[str],
    cookiefile: str | None = None,
    cookies_from_browser: str = "none",
    platform: str = PLATFORM_AUTO,
    proxy: str | None = None,
    retries: int = 1,
    log: LogFunc | None = None,
) -> list[dict[str, Any]]:
    log_func = log if log else (lambda _msg: None)
    cleaned_urls = extract_urls_from_inputs(urls)
    if not cleaned_urls:
        return []

    cookies_from_browser = normalize_browser(cookies_from_browser) or "none"
    proxy = (proxy or "").strip() or None
    retries = max(0, int(retries))

    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "socket_timeout": 20,
        "retries": retries,
        "extractor_retries": retries,
        "http_headers": HTTP_HEADERS,
        "noplaylist": False,
        "logger": _SilentProbeLogger(),
    }
    if proxy:
        opts["proxy"] = proxy
    _apply_cookie_options(opts, cookiefile, cookies_from_browser)

    selected_platform = normalize_platform(platform) or PLATFORM_AUTO
    if selected_platform == PLATFORM_AUTO:
        grouped = split_urls_by_platform(cleaned_urls)
        batches = [(p, grouped[p]) for p in (PLATFORM_BILIBILI, PLATFORM_DOUYIN) if grouped[p]]
    else:
        batches = [(selected_platform, cleaned_urls)]

    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    def push_item(
        item_url: str,
        item_platform: str,
        title: str,
        uploader: str,
        duration: Any,
        source_url: str,
        index_in_source: int | None,
    ) -> None:
        normalized_url = _normalize_input_url(item_url)
        if not normalized_url or normalized_url in seen_urls:
            return
        seen_urls.add(normalized_url)
        items.append(
            {
                "url": normalized_url,
                "platform": item_platform,
                "platform_name": platform_name_cn(item_platform),
                "title": title or "未命名视频",
                "uploader": uploader or "-",
                "duration": _duration_to_text(duration),
                "source_url": source_url,
                "index_in_source": index_in_source,
            }
        )

    with yt_dlp.YoutubeDL(opts) as ydl:
        for batch_platform, batch_urls in batches:
            for original_url in batch_urls:
                candidates = _build_probe_candidates(original_url, batch_platform, None)
                candidate = candidates[0] if candidates else original_url
                try:
                    info = ydl.extract_info(candidate, download=False)
                except Exception as exc:
                    log_func(f"预览失败：{original_url} -> {strip_ansi(str(exc))}")
                    continue
                if not isinstance(info, dict):
                    continue

                entries = info.get("entries")
                if isinstance(entries, list) and entries:
                    for idx, entry in enumerate(entries, start=1):
                        if not isinstance(entry, dict):
                            continue
                        entry_url = str(entry.get("webpage_url") or entry.get("url") or "").strip()
                        if entry_url and not entry_url.startswith("http"):
                            entry_url = ""
                        if not entry_url and batch_platform == PLATFORM_BILIBILI:
                            source_id = str(info.get("id") or "").strip()
                            if source_id:
                                entry_url = f"https://www.bilibili.com/video/{source_id}?p={idx}"
                        if not entry_url:
                            entry_url = candidate
                        push_item(
                            item_url=entry_url,
                            item_platform=batch_platform,
                            title=str(entry.get("title") or info.get("title") or ""),
                            uploader=str(
                                entry.get("uploader")
                                or entry.get("channel")
                                or info.get("uploader")
                                or info.get("channel")
                                or ""
                            ),
                            duration=entry.get("duration") or info.get("duration"),
                            source_url=original_url,
                            index_in_source=idx,
                        )
                else:
                    push_item(
                        item_url=str(info.get("webpage_url") or candidate),
                        item_platform=batch_platform,
                        title=str(info.get("title") or ""),
                        uploader=str(info.get("uploader") or info.get("channel") or ""),
                        duration=info.get("duration"),
                        source_url=original_url,
                        index_in_source=None,
                    )
    return items


def _pick_preview_stream_url(info: dict[str, Any]) -> str:
    direct_url = str(info.get("url") or "").strip()
    if direct_url.startswith("http"):
        return direct_url

    formats = info.get("formats")
    if not isinstance(formats, list):
        return ""

    usable = [fmt for fmt in formats if isinstance(fmt, dict) and str(fmt.get("url") or "").startswith("http")]
    if not usable:
        return ""

    def fmt_sort_key(fmt: dict[str, Any]) -> tuple[int, float]:
        height = int(fmt.get("height") or 0)
        tbr = float(fmt.get("tbr") or 0.0)
        return height, tbr

    av = [
        fmt
        for fmt in usable
        if str(fmt.get("vcodec") or "none") != "none" and str(fmt.get("acodec") or "none") != "none"
    ]
    if av:
        return str(sorted(av, key=fmt_sort_key, reverse=True)[0].get("url") or "")

    video_only = [fmt for fmt in usable if str(fmt.get("vcodec") or "none") != "none"]
    if video_only:
        return str(sorted(video_only, key=fmt_sort_key, reverse=True)[0].get("url") or "")

    return str(sorted(usable, key=fmt_sort_key, reverse=True)[0].get("url") or "")


def resolve_preview_stream(
    url: str,
    cookiefile: str | None = None,
    cookies_from_browser: str = "none",
    proxy: str | None = None,
    retries: int = 1,
) -> dict[str, Any]:
    source_url = _normalize_input_url(url)
    if not source_url:
        return {"ok": False, "message": "预览链接为空。"}

    cookies_from_browser = normalize_browser(cookies_from_browser) or "none"
    proxy = (proxy or "").strip() or None
    retries = max(0, int(retries))
    cache_key = _preview_cache_key(source_url, cookiefile, cookies_from_browser, proxy, retries)
    cached = _get_cached_preview(cache_key)
    if cached is not None:
        cached["cache_hit"] = True
        return cached

    preview_opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "socket_timeout": 20,
        "retries": retries,
        "extractor_retries": retries,
        "http_headers": HTTP_HEADERS,
        "noplaylist": True,
        "logger": _SilentProbeLogger(),
    }
    if proxy:
        preview_opts["proxy"] = proxy
    _apply_cookie_options(preview_opts, cookiefile, cookies_from_browser)

    platform_guess = ""
    if is_url_for_platform(source_url, PLATFORM_BILIBILI):
        platform_guess = PLATFORM_BILIBILI
    elif is_url_for_platform(source_url, PLATFORM_DOUYIN):
        platform_guess = PLATFORM_DOUYIN

    candidates = [source_url]
    if platform_guess:
        expanded = _build_probe_candidates(source_url, platform_guess, None)
        if expanded:
            candidates = expanded
        if source_url not in candidates:
            candidates.insert(0, source_url)
    candidates = _dedupe_keep_order(candidates)

    last_error = "无法解析可播放流地址。"
    with yt_dlp.YoutubeDL(preview_opts) as ydl:
        for candidate in candidates:
            try:
                info = ydl.extract_info(candidate, download=False)
            except Exception as exc:
                last_error = _friendly_preview_error_message(
                    source_url=source_url,
                    reason=str(exc),
                    cookiefile=cookiefile,
                    cookies_from_browser=cookies_from_browser,
                )
                continue
            if not isinstance(info, dict):
                continue

            first_info = info
            entries = info.get("entries")
            if isinstance(entries, list) and entries:
                first_dict = next((entry for entry in entries if isinstance(entry, dict)), None)
                if first_dict:
                    first_info = first_dict

            stream_url = _pick_preview_stream_url(first_info)
            if not stream_url:
                stream_url = _pick_preview_stream_url(info)
            if not stream_url:
                last_error = "已获取视频信息，但未找到可播放流地址。"
                continue

            headers: dict[str, str] = {}
            raw_headers = first_info.get("http_headers")
            if isinstance(raw_headers, dict):
                headers.update({str(k): str(v) for k, v in raw_headers.items() if v is not None})
            raw_headers_parent = info.get("http_headers")
            if isinstance(raw_headers_parent, dict):
                for key, value in raw_headers_parent.items():
                    if value is None:
                        continue
                    key_str = str(key)
                    if key_str not in headers:
                        headers[key_str] = str(value)

            webpage_url = str(first_info.get("webpage_url") or info.get("webpage_url") or candidate).strip()
            if webpage_url and "Referer" not in headers and "referer" not in headers:
                headers["Referer"] = webpage_url
            if "User-Agent" not in headers and "user-agent" not in headers:
                headers["User-Agent"] = HTTP_HEADERS["User-Agent"]

            payload = {
                "ok": True,
                "source_url": source_url,
                "resolved_url": candidate,
                "stream_url": stream_url,
                "title": str(first_info.get("title") or info.get("title") or "实时预览"),
                "webpage_url": webpage_url,
                "http_headers": headers,
                "message": "解析成功",
                "cache_hit": False,
            }
            _put_cached_preview(cache_key, payload)
            return payload

    if not platform_guess:
        try:
            sniff = sniff_media_sync(
                page_url=source_url,
                timeout_ms=45_000,
                wait_after_load_ms=12_000,
                headless=False,
            )
            stream_url = str(sniff.get("best_url") or "").strip()
            if stream_url:
                headers: dict[str, str] = {"Referer": source_url}
                user_agent = str(sniff.get("user_agent") or "").strip()
                if user_agent:
                    headers["User-Agent"] = user_agent
                cookie_header = build_cookie_header(sniff.get("cookies") or [])
                if cookie_header:
                    headers["Cookie"] = cookie_header
                payload = {
                    "ok": True,
                    "source_url": source_url,
                    "resolved_url": source_url,
                    "stream_url": stream_url,
                    "title": str(sniff.get("title") or "实时预览"),
                    "webpage_url": source_url,
                    "http_headers": headers,
                    "message": "解析成功（网页抓流）",
                    "cache_hit": False,
                }
                _put_cached_preview(cache_key, payload)
                return payload
            last_error = "未检测到可播放流地址。"
        except Exception as exc:
            reason = strip_ansi(str(exc))
            if reason:
                last_error = f"{last_error}；网页抓流失败：{_friendly_preview_error_message(source_url, reason, cookiefile, cookies_from_browser)}"

    return {"ok": False, "source_url": source_url, "message": last_error}


def check_yt_dlp_update(timeout: int = 5) -> dict[str, Any]:
    current_version = str(getattr(yt_dlp.version, "__version__", "unknown"))

    try:
        request = Request("https://pypi.org/pypi/yt-dlp/json", headers=HTTP_HEADERS, method="GET")
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
        latest_version = str(payload.get("info", {}).get("version", "")).strip() or "unknown"
    except Exception as exc:
        return {
            "ok": False,
            "current_version": current_version,
            "latest_version": "",
            "has_update": False,
            "message": f"更新检查失败：{strip_ansi(str(exc))}",
        }

    has_update = _version_key(latest_version) > _version_key(current_version)
    if has_update:
        message = f"检测到 yt-dlp 新版本：当前 {current_version}，最新 {latest_version}"
    else:
        message = f"yt-dlp 已是最新：{current_version}"
    return {
        "ok": True,
        "current_version": current_version,
        "latest_version": latest_version,
        "has_update": has_update,
        "message": message,
    }


def check_app_update(
    current_version: str,
    repo: str | None,
    timeout: int = 5,
) -> dict[str, Any]:
    normalized_repo = (repo or "").strip().strip("/")
    normalized_current = (current_version or "").strip() or "unknown"
    if not normalized_repo:
        return {
            "ok": False,
            "repo": "",
            "current_version": normalized_current,
            "latest_version": "",
            "has_update": False,
            "message": "未配置应用更新仓库（owner/repo），已跳过。",
        }

    api_url = f"https://api.github.com/repos/{normalized_repo}/releases/latest"
    request_headers = dict(HTTP_HEADERS)
    request_headers.update(
        {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    try:
        request = Request(api_url, headers=request_headers, method="GET")
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    except Exception as exc:
        return {
            "ok": False,
            "repo": normalized_repo,
            "current_version": normalized_current,
            "latest_version": "",
            "has_update": False,
            "message": f"应用更新检查失败：{strip_ansi(str(exc))}",
        }

    latest_version = str(payload.get("tag_name") or payload.get("name") or "").strip() or "unknown"
    release_url = str(payload.get("html_url") or "").strip()

    current_compare = normalized_current.lstrip("vV")
    latest_compare = latest_version.lstrip("vV")
    has_update = _version_key(latest_compare) > _version_key(current_compare)
    if has_update:
        message = f"检测到应用新版本：当前 {normalized_current}，最新 {latest_version}"
        if release_url:
            message = f"{message}，发布页：{release_url}"
    else:
        message = f"应用已是最新：{normalized_current}"

    return {
        "ok": True,
        "repo": normalized_repo,
        "current_version": normalized_current,
        "latest_version": latest_version,
        "has_update": has_update,
        "release_url": release_url,
        "message": message,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="\u4e0b\u8f7d B\u7ad9/\u6296\u97f3 \u53ef\u8bbf\u95ee\u89c6\u9891\uff08\u4e0d\u7ed5\u8fc7\u4ed8\u8d39\u6216\u6743\u9650\u9650\u5236\uff09\u3002"
    )
    parser.add_argument("urls", nargs="+", help="\u4e00\u4e2a\u6216\u591a\u4e2a\u89c6\u9891\u94fe\u63a5\uff0c\u652f\u6301\u7c98\u8d34\u5206\u4eab\u6587\u6848")
    parser.add_argument("--platform", default=PLATFORM_AUTO, choices=SUPPORTED_PLATFORMS)
    parser.add_argument("--resolution", default=RESOLUTION_BEST, choices=SUPPORTED_RESOLUTIONS)
    parser.add_argument("--extract-audio", action="store_true", help="\u4e0b\u8f7d\u540e\u989d\u5916\u5206\u79bb\u97f3\u9891")
    parser.add_argument("--audio-format", default="mp3", choices=SUPPORTED_AUDIO_FORMATS)
    parser.add_argument("--write-subs", action="store_true", help="\u5bfc\u51fa\u5b57\u5e55\uff08\u542b\u81ea\u52a8\u5b57\u5e55\uff09")
    parser.add_argument("--write-thumbnail", action="store_true", help="\u5bfc\u51fa\u5c01\u9762\u56fe")
    parser.add_argument("--write-info-json", action="store_true", help="\u5bfc\u51fa\u5143\u6570\u636e JSON")
    parser.add_argument("--cookies", default=None, help="cookies.txt \u8def\u5f84")
    parser.add_argument(
        "--cookies-from-browser",
        default="none",
        choices=SUPPORTED_BROWSERS,
        help="\u4ece\u6d4f\u89c8\u5668\u8bfb\u53d6 cookies",
    )
    parser.add_argument("--proxy", default=None, help="\u4ee3\u7406\uff08HTTP/SOCKS\uff09")
    parser.add_argument("--rate-limit", default=None, help="\u9650\u901f\uff0c\u4f8b\u5982 2M \u6216 800K")
    parser.add_argument("--retries", type=int, default=1, help="\u91cd\u8bd5\u6b21\u6570")
    parser.add_argument(
        "--adaptive-retry-attempts",
        type=int,
        default=DEFAULT_ADAPTIVE_RETRY_ATTEMPTS,
        help="\u5355\u94fe\u63a5\u81ea\u9002\u5e94\u91cd\u8bd5\u603b\u8f6e\u6b21\uff08\u542b\u9996\u6b21\uff09",
    )
    parser.add_argument(
        "--filename-template",
        default=DEFAULT_FILENAME_TEMPLATE,
        help="\u6587\u4ef6\u540d\u6a21\u677f\uff08yt-dlp outtmpl\uff09",
    )
    parser.add_argument(
        "--post-action",
        default=POST_ACTION_NONE,
        choices=SUPPORTED_POST_ACTIONS,
        help="\u4e0b\u8f7d\u540e\u52a8\u4f5c",
    )
    parser.add_argument("--history-file", default=None, help="\u4e0b\u8f7d\u5386\u53f2\u8bb0\u5f55\u6587\u4ef6\u8def\u5f84")
    parser.add_argument(
        "--skip-history-success",
        action="store_true",
        help="\u9047\u5230\u5386\u53f2\u4e2d\u5df2\u6210\u529f\u7684\u94fe\u63a5\u65f6\u81ea\u52a8\u8df3\u8fc7",
    )
    parser.add_argument("-o", "--output", default="downloads")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output).resolve()
    return run_download(
        args.urls,
        output_dir,
        cookiefile=args.cookies,
        cookies_from_browser=args.cookies_from_browser,
        platform=args.platform,
        resolution=args.resolution,
        extract_audio=args.extract_audio,
        audio_format=args.audio_format,
        write_subtitles=args.write_subs,
        write_thumbnail=args.write_thumbnail,
        write_info_json=args.write_info_json,
        proxy=args.proxy,
        rate_limit=args.rate_limit,
        retries=args.retries,
        adaptive_retry_attempts=args.adaptive_retry_attempts,
        filename_template=args.filename_template,
        post_action=args.post_action,
        history_file=args.history_file,
        skip_history_success=args.skip_history_success,
    )


if __name__ == "__main__":
    sys.exit(main())
