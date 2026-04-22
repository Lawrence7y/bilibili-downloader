from __future__ import annotations

import asyncio
from typing import Iterable

MEDIA_MARKERS = (".m3u8", ".mp4", ".m4s", ".ts")
MISSING_BROWSER_ERROR_HINTS = (
    "browsertype.launch: executable doesn't exist",
    "please run the following command to download new browsers",
    "playwright install",
)


def is_probable_media_url(url: str) -> bool:
    if not isinstance(url, str):
        return False
    lowered = url.lower()
    if any(marker in lowered for marker in MEDIA_MARKERS):
        return True
    return "mime=video" in lowered or "video/" in lowered


def choose_best_media_url(urls: Iterable[str]) -> str | None:
    candidates = [url for url in urls if isinstance(url, str) and url.strip()]
    if not candidates:
        return None
    m3u8 = [url for url in candidates if ".m3u8" in url.lower()]
    if m3u8:
        return m3u8[-1]
    mp4 = [url for url in candidates if ".mp4" in url.lower()]
    if mp4:
        return mp4[-1]
    return candidates[-1]


async def wait_for_media_event(media_event: asyncio.Event, timeout_ms: int) -> bool:
    if media_event.is_set():
        return True
    timeout_seconds = max(0, int(timeout_ms)) / 1000
    if timeout_seconds <= 0:
        return media_event.is_set()
    try:
        await asyncio.wait_for(media_event.wait(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return media_event.is_set()
    return True


def _looks_like_missing_playwright_browser(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in MISSING_BROWSER_ERROR_HINTS)


def _build_browser_launch_error(errors: list[tuple[str, Exception]]) -> RuntimeError:
    details = "; ".join(f"{name}: {str(err).strip()}" for name, err in errors if str(err).strip())
    summary = (
        "Playwright browser runtime is unavailable. "
        "Tried bundled Chromium, msedge, and chrome. "
        "Install Microsoft Edge/Google Chrome, or run: playwright install chromium."
    )
    if details:
        return RuntimeError(f"{summary} Details: {details}")
    return RuntimeError(summary)


async def launch_chromium_with_fallback(playwright, headless: bool):
    try:
        return await playwright.chromium.launch(headless=headless)
    except Exception as exc:
        if not _looks_like_missing_playwright_browser(exc):
            raise
        launch_errors: list[tuple[str, Exception]] = [("chromium", exc)]

    for channel in ("msedge", "chrome"):
        try:
            return await playwright.chromium.launch(headless=headless, channel=channel)
        except Exception as channel_exc:
            launch_errors.append((channel, channel_exc))

    raise _build_browser_launch_error(launch_errors)


async def sniff_media(
    page_url: str,
    timeout_ms: int = 45_000,
    wait_after_load_ms: int = 6_000,
    headless: bool = False,
) -> dict:
    from playwright.async_api import async_playwright

    observed: list[str] = []
    seen: set[str] = set()
    media_event = asyncio.Event()

    async with async_playwright() as p:
        browser = await launch_chromium_with_fallback(p, headless=headless)
        context = await browser.new_context()
        page = await context.new_page()

        def capture(url: str) -> None:
            if is_probable_media_url(url) and url not in seen:
                seen.add(url)
                observed.append(url)
                media_event.set()

        def on_request(request) -> None:
            capture(request.url)

        def on_response(response) -> None:
            capture(response.url)
            content_type = str(response.headers.get("content-type", "")).lower()
            if "application/vnd.apple.mpegurl" in content_type or "video/" in content_type:
                capture(response.url)

        page.on("request", on_request)
        page.on("response", on_response)

        await page.goto(page_url, timeout=timeout_ms, wait_until="domcontentloaded")
        media_hit = await wait_for_media_event(media_event, timeout_ms=wait_after_load_ms)
        if not media_hit:
            await page.mouse.click(20, 20)
            await wait_for_media_event(media_event, timeout_ms=wait_after_load_ms)

        title = await page.title()
        user_agent = await page.evaluate("() => navigator.userAgent")
        cookies = await context.cookies()
        await browser.close()

    best = choose_best_media_url(observed)
    return {
        "title": title,
        "user_agent": user_agent,
        "cookies": cookies,
        "candidates": observed,
        "best_url": best,
    }


def sniff_media_sync(
    page_url: str,
    timeout_ms: int = 45_000,
    wait_after_load_ms: int = 6_000,
    headless: bool = False,
) -> dict:
    return asyncio.run(
        sniff_media(
            page_url=page_url,
            timeout_ms=timeout_ms,
            wait_after_load_ms=wait_after_load_ms,
            headless=headless,
        )
    )
