"""Universal Generic Web Media Resolver.

Resolves video/audio streams from arbitrary/unsupported websites using a multi-tiered pipeline:
1. Static HTML heuristics (HTML5 video/source, OpenGraph, Schema.org, DPlayer/ArtPlayer/MacCMS player config)
2. Iframe traversal (inspecting embedded player iframes)
3. Ad-filtered media URL discovery (clean .m3u8 / .mp4 streams)
4. Dynamic headless browser network interception via Playwright (when static extraction fails)
"""

from __future__ import annotations

import asyncio
import json
import re
import urllib.request
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse, urlsplit

from sidecar.protocol import MediaMetadata, StreamInfo

MEDIA_MARKERS = (".m3u8", ".mp4", ".m4s", ".ts", "mime=video")

NON_VIDEO_MARKERS = (
    "kind=thumb", "kind=preview", "kind=poster", "kind=cover", "kind=avatar",
    "/thumbs/", "/thumbnail", ".vtt", "poster.png", "preview", "storyboard",
)

AD_TRACKING_DOMAINS = (
    "tsyndicate.com", "playhubconnect.com", "clammyendearedkeg.com",
    "googlesyndication.com", "doubleclick.net", "googleadservices.com",
    "adservice.google.com", "imasdk.googleapis.com", "adnxs.com", "appnexus.com",
    "criteo.com", "criteo.net", "taboola.com", "outbrain.com", "moatads.com",
    "spotxchange.com", "springserve.com", "adform.net", "rubiconproject.com",
    "pubmatic.com", "openx.net", "casalemedia.com", "adsrvr.org",
    "amazon-adsystem.com", "adsafeprotected.com", "demdex.net",
    "scorecardresearch.com", "zeusadx.com", "adpushup.com", "adsterra.com",
    "propellerads.com", "popads.net", "exoclick.com", "hilltopads.net",
    "clickadu.com", "adskeeper.com", "adcash.com", "mgid.com", "revcontent.com",
    "mediavine.com", "ezoic.net", "pixfuture.com", "fuseplatform.net",
    "magsrv.com", "trafficjunky.com", "ptelastaxo.com",
)

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _hostname_of(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except Exception:
        return ""


def is_ad_or_tracking(url: str) -> bool:
    if not isinstance(url, str):
        return True
    low = url.lower()
    host = _hostname_of(low)
    return any(host == d or host.endswith("." + d) for d in AD_TRACKING_DOMAINS)


def is_non_video_media(url: str) -> bool:
    if not isinstance(url, str):
        return True
    low = url.lower()
    if any(m in low for m in NON_VIDEO_MARKERS):
        return True
    return low.rstrip("/").endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".vtt"))


def is_probable_media_url(url: str) -> bool:
    if not isinstance(url, str):
        return False
    low = url.lower()
    if is_ad_or_tracking(low) or is_non_video_media(low):
        return False
    try:
        parts = urlsplit(low)
        path_query = f"{parts.path}?{parts.query}"
        return any(m in path_query for m in (".m3u8", ".mp4", "mime=video", "/video/"))
    except Exception:
        return False


def _fetch_html(url: str, cookie: str | None = None, proxy: str | None = None, timeout: float = 8.0) -> str:
    """Fetch page HTML synchronously using urllib."""
    headers = {
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    if cookie:
        headers["Cookie"] = cookie

    handlers = []
    if proxy:
        handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
    opener = urllib.request.build_opener(*handlers)

    req = urllib.request.Request(url, headers=headers)
    with opener.open(req, timeout=timeout) as resp:
        content = resp.read()
        charset = resp.headers.get_content_charset() or "utf-8"
        try:
            return content.decode(charset, errors="ignore")
        except Exception:
            return content.decode("utf-8", errors="ignore")


def _extract_meta(html: str, base_url: str) -> tuple[str, str]:
    """Extract page title and cover/thumbnail from HTML."""
    title = ""
    og_title = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']', html, re.I)
    if not og_title:
        og_title = re.search(r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:title["\']', html, re.I)
    if og_title:
        title = unescape(og_title.group(1).strip())
    if not title:
        t_match = re.search(r'<title>(.*?)</title>', html, re.I | re.S)
        if t_match:
            raw_title = unescape(t_match.group(1).strip())
            # Clean common trailing site brandings
            title = re.split(r'[-_|｜_—]', raw_title)[0].strip() or raw_title

    if not title:
        h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.I | re.S)
        if h1:
            title = re.sub(r'<[^>]+>', '', h1.group(1)).strip()

    cover = ""
    og_img = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\'](.*?)["\']', html, re.I)
    if not og_img:
        og_img = re.search(r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:image["\']', html, re.I)
    if og_img:
        cover = urljoin(base_url, unescape(og_img.group(1).strip()))

    return title or "未知视频", cover


def _extract_media_candidates_static(html: str, base_url: str) -> list[str]:
    """Extract stream candidates from static HTML using known player configs and tags."""
    candidates: list[str] = []
    decoded_html = unescape(html.replace(r'\/', '/'))

    # 1. HTML5 <video src="..."> and <source src="...">
    for tag_match in re.finditer(r'<(?:video|source)[^>]+src=["\']([^"\']+)["\']', html, re.I):
        src = urljoin(base_url, tag_match.group(1).strip())
        if is_probable_media_url(src):
            candidates.append(src)

    # 2. OpenGraph / Twitter meta video
    for meta_match in re.finditer(r'<meta[^>]+(?:property|name)=["\'](?:og:video(?::url)?|twitter:player:stream)["\'][^>]+content=["\']([^"\']+)["\']', html, re.I):
        src = urljoin(base_url, meta_match.group(1).strip())
        if is_probable_media_url(src):
            candidates.append(src)

    # 3. DPlayer data-config attribute (e.g., 91cg1, many CMS sites)
    for dp_match in re.finditer(r'class=["\'][^"\']*dplayer[^"\']*["\'][^>]*data-config=(["\'])(.*?)\1', html, re.I | re.S):
        raw_cfg = dp_match.group(2)
        try:
            cfg = json.loads(unescape(raw_cfg))
            video_cfg = cfg.get("video") or {}
            v_url = video_cfg.get("url") if isinstance(video_cfg, dict) else None
            if v_url and isinstance(v_url, str):
                candidates.append(urljoin(base_url, v_url.strip()))
        except Exception:
            # Fallback regex search within config block
            cfg_m = re.search(r'https?://[^\s"\'<>]+\.(?:m3u8|mp4)(?:\?[^\s"\'<>]*)?', unescape(raw_cfg.replace(r'\/', '/')))
            if cfg_m:
                candidates.append(urljoin(base_url, cfg_m.group(0)))

    # 4. MacCMS / AppleCMS player_aaaa configuration
    maccms_match = re.search(r'var\s+player_aaaa\s*=\s*\{.*?"url"\s*:\s*"([^"]+)".*?\}', decoded_html, re.S)
    if maccms_match:
        url_part = maccms_match.group(1).strip()
        if is_probable_media_url(url_part):
            candidates.append(urljoin(base_url, url_part))

    # 5. ArtPlayer / dp config in JavaScript blocks
    for js_cfg in re.finditer(r'(?:url|video\s*:\s*\{[^}]*?url)\s*:\s*["\'](https?://[^"\']+\.(?:m3u8|mp4)[^"\']*)["\']', decoded_html, re.I):
        candidates.append(urljoin(base_url, js_cfg.group(1)))

    # 6. Global regex for direct .m3u8 / .mp4 links in raw page
    all_raw = re.findall(r'https?://[^\s"\'<>]+\.(?:m3u8|mp4)(?:\?[^\s"\'<>]*)?', decoded_html)
    for u in all_raw:
        if is_probable_media_url(u) and u not in candidates:
            candidates.append(u)

    return candidates


def _extract_iframes(html: str, base_url: str) -> list[str]:
    """Find valid embedded player iframe URLs."""
    iframes = []
    for m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', html, re.I):
        src = m.group(1).strip()
        if not src or src.startswith("javascript:") or is_ad_or_tracking(src):
            continue
        full = urljoin(base_url, src)
        if full not in iframes:
            iframes.append(full)
    return iframes


async def _sniff_with_playwright(
    url: str,
    cookie: str | None = None,
    proxy: str | None = None,
    timeout_secs: int = 15,
) -> tuple[list[str], str, str]:
    """Tier 4: Sniff media network requests dynamically using headless Chromium."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return [], "", ""

    captured_urls: list[str] = []
    page_title = ""
    page_cover = ""

    async with async_playwright() as p:
        launch_kwargs: dict[str, Any] = {"headless": True}
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}

        browser = None
        for channel in (None, "msedge", "chrome"):
            try:
                kw = dict(launch_kwargs)
                if channel:
                    kw["channel"] = channel
                browser = await p.chromium.launch(**kw)
                break
            except Exception:
                continue

        if not browser:
            return [], "", ""

        try:
            context = await browser.new_context(user_agent=DEFAULT_UA)
            page = await context.new_page()

            def on_response(resp):
                u = resp.url
                if is_probable_media_url(u) and u not in captured_urls:
                    captured_urls.append(u)

            page.on("response", on_response)

            try:
                await page.goto(url, timeout=timeout_secs * 1000, wait_until="domcontentloaded")
            except Exception:
                pass

            page_title = await page.title()

            # Attempt auto-play and ad skip in the page
            await page.evaluate('''() => {
                // Auto fast-forward pre-roll ads
                const videos = document.querySelectorAll('video');
                videos.forEach(v => {
                    v.muted = true;
                    if (v.duration && v.duration <= 20) {
                        v.currentTime = v.duration - 0.1;
                    }
                    v.play().catch(()=>{});
                });
                // Click common play buttons
                const clickSelectors = [
                    '.rv-player', '.dplayer-play-icon', '.artplay-icon',
                    '.vjs-big-play-button', 'video', 'button[aria-label*="play" i]'
                ];
                for (const sel of clickSelectors) {
                    const el = document.querySelector(sel);
                    if (el) { el.click(); break; }
                }
            }''')

            # Wait up to 5 seconds for video stream network requests
            for _ in range(10):
                if captured_urls:
                    break
                await asyncio.sleep(0.5)

        finally:
            await browser.close()

    return captured_urls, page_title, page_cover


async def resolve_generic_web(
    url: str,
    cookie: str | None = None,
    proxy: str | None = None,
) -> MediaMetadata:
    """Unified entry point to resolve video metadata from arbitrary websites."""
    loop = asyncio.get_running_loop()

    # Tier 1: Static HTML inspection
    try:
        html = await loop.run_in_executor(None, lambda: _fetch_html(url, cookie=cookie, proxy=proxy))
    except Exception as exc:
        html = ""

    title, cover = "", ""
    candidates: list[str] = []

    if html:
        title, cover = _extract_meta(html, url)
        candidates = _extract_media_candidates_static(html, url)

        # Tier 2: Iframe traversal if no direct stream found
        if not candidates:
            iframes = _extract_iframes(html, url)
            for ifr in iframes[:3]:  # inspect top 3 candidate player iframes
                try:
                    ifr_html = await loop.run_in_executor(
                        None, lambda: _fetch_html(ifr, cookie=cookie, proxy=proxy, timeout=5.0)
                    )
                    ifr_candidates = _extract_media_candidates_static(ifr_html, ifr)
                    if ifr_candidates:
                        candidates.extend(ifr_candidates)
                        break
                except Exception:
                    continue

    # Tier 4: Dynamic headless Playwright sniffing
    if not candidates:
        dyn_urls, dyn_title, dyn_cover = await _sniff_with_playwright(url, cookie=cookie, proxy=proxy)
        if dyn_urls:
            candidates.extend(dyn_urls)
        if not title and dyn_title:
            title = dyn_title
        if not cover and dyn_cover:
            cover = dyn_cover

    if not candidates:
        raise ValueError(f"未能从该网页提取到可用视频流: {url}")

    # Prioritize m3u8 playlists over direct mp4
    m3u8s = [c for c in candidates if ".m3u8" in c.lower()]
    best_url = m3u8s[0] if m3u8s else candidates[0]
    is_m3u8 = ".m3u8" in best_url.lower()

    host = _hostname_of(url) or "generic"
    stream = StreamInfo(
        format_id="original",
        protocol="m3u8" if is_m3u8 else "http",
        video_url=best_url,
        ext="mp4",
        resolution="原始画质",
        headers={
            "User-Agent": DEFAULT_UA,
            "Referer": url,
        },
    )

    return MediaMetadata(
        platform=host,
        content_type="single_video",
        title=title or f"网络视频_{host}",
        author=host,
        url=url,
        duration=0,
        cover_url=cover,
        streams=[stream],
        extra={
            "sniffed_media_url": best_url,
            "candidates_count": len(candidates),
        },
    )
