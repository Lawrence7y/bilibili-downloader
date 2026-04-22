from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Callable, Iterable


class FFmpegDownloadCancelled(RuntimeError):
    """Raised when ffmpeg download is cancelled by caller."""


def build_cookie_header(cookies: Iterable[dict]) -> str:
    items: list[str] = []
    for cookie in cookies:
        name = str(cookie.get("name", "")).strip()
        if not name:
            continue
        value = str(cookie.get("value", ""))
        items.append(f"{name}={value}")
    return "; ".join(items)


def build_ffmpeg_command(
    media_url: str,
    output_path: str,
    user_agent: str | None = None,
    referer: str | None = None,
    cookie_header: str | None = None,
) -> list[str]:
    command = ["ffmpeg", "-y", "-loglevel", "error", "-stats"]
    if user_agent:
        command.extend(["-user_agent", user_agent])
    headers: list[str] = []
    if referer:
        headers.append(f"Referer: {referer}")
    if cookie_header:
        headers.append(f"Cookie: {cookie_header}")
    if headers:
        command.extend(["-headers", "\r\n".join(headers) + "\r\n"])
    command.extend(["-i", media_url, "-c", "copy", output_path])
    return command


def run_ffmpeg_download(
    media_url: str,
    output_path: str,
    user_agent: str | None = None,
    referer: str | None = None,
    cookie_header: str | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> None:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    command = build_ffmpeg_command(
        media_url=media_url,
        output_path=output_path,
        user_agent=user_agent,
        referer=referer,
        cookie_header=cookie_header,
    )
    process = subprocess.Popen(command)
    try:
        while True:
            if cancel_check is not None and cancel_check():
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
                raise FFmpegDownloadCancelled("ffmpeg download cancelled")

            return_code = process.poll()
            if return_code is not None:
                if return_code != 0:
                    raise subprocess.CalledProcessError(return_code, command)
                return
            time.sleep(0.2)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)
