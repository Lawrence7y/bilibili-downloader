"""Sidecar JSON-RPC 2.0 Server entry point reading NDJSON from stdin."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

# Add project root to sys.path so sidecar and existing modules can be imported
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

def _sanitize_proxy_env() -> None:
    """Detect and remove dead local proxy environment variables that block network calls."""
    import socket
    from urllib.parse import urlparse

    keys = ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]
    for k in keys:
        val = os.environ.get(k)
        if not val:
            continue
        try:
            parsed = urlparse(val if "://" in val else f"http://{val}")
            host = parsed.hostname or ""
            port = parsed.port
            if host in ("127.0.0.1", "localhost", "::1") and port:
                try:
                    with socket.create_connection((host, port), timeout=0.2):
                        pass
                except Exception:
                    sys.stderr.write(f"[sidecar] Dead local proxy detected in {k}={val}, removing to prevent WinError 10061\n")
                    sys.stderr.flush()
                    os.environ.pop(k, None)
        except Exception:
            pass

_sanitize_proxy_env()

import threading
import traceback
from typing import Any

from sidecar.douyin import (
    check_douyin_cookie,
    detect_douyin_content_type,
    resolve_douyin_batch,
    resolve_douyin_video,
)
from sidecar.protocol import (
    AuthRequiredError,
    make_jsonrpc_error,
    make_jsonrpc_response,
)
from sidecar.ytdlp import check_bilibili_cookie, resolve_with_ytdlp

VERSION = "1.0.0"
_stdout_lock = threading.Lock()

# Force UTF-8 on Windows stdout/stdin
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8")
except Exception:
    pass


_AUTH_HINTS = (
    "cookie",
    "登录",
    "login",
    "401",
    "403",
    "signature",
    "validate",
    "鉴权",
    "未登录",
    "expired",
    "session",
    "passport",
)


def _looks_like_auth_error(text: str) -> bool:
    low = text.lower()
    return any(h in low for h in _AUTH_HINTS)


def _safe_write_stdout(text: str) -> None:
    """Safely write UTF-8 text line to stdout."""
    with _stdout_lock:
        sys.stdout.write(text + "\n")
        sys.stdout.flush()


async def handle_request(raw_line: str) -> str:
    """Parse and dispatch a single JSON-RPC line."""
    line = raw_line.strip()
    if not line:
        return ""

    try:
        data = json.loads(line)
    except json.JSONDecodeError as exc:
        return make_jsonrpc_error(None, -32700, f"Parse error: {exc}")

    if not isinstance(data, dict) or data.get("jsonrpc") != "2.0":
        return make_jsonrpc_error(data.get("id"), -32600, "Invalid Request: expected jsonrpc 2.0")

    req_id = data.get("id")
    method = data.get("method")
    params = data.get("params", {})

    try:
        # 1. Ping
        if method == "ping":
            return make_jsonrpc_response(req_id, {"status": "ok", "version": VERSION})

        # 2. Check Cookie
        elif method == "check_cookie":
            platform = str(params.get("platform", "douyin")).lower()
            cookie = params.get("cookie", "")
            if "douyin" in platform:
                res = check_douyin_cookie(cookie)
            else:
                res = check_bilibili_cookie(cookie)
            return make_jsonrpc_response(req_id, res)

        # 3. Resolve single URL
        elif method == "resolve":
            url = str(params.get("url", "")).strip()
            cookie = params.get("cookie")
            proxy = params.get("proxy")

            if not url:
                return make_jsonrpc_error(req_id, -32602, "Missing 'url' parameter")

            # Check if Douyin URL
            is_douyin = any(k in url.lower() for k in ("douyin.com", "iesdouyin.com", "amemv.com"))
            if is_douyin:
                content_type, extracted_id = detect_douyin_content_type(url)
                if content_type == "video" and extracted_id:
                    meta = await resolve_douyin_video(extracted_id, url, cookie=cookie, proxy=proxy)
                    return make_jsonrpc_response(req_id, meta.to_dict())
                elif content_type in ("profile", "mix") and extracted_id:
                    # Resolve first batch of batch
                    batch_type = "posts" if content_type == "profile" else "mix"
                    meta = await resolve_douyin_batch(
                        batch_type, extracted_id, max_count=params.get("max_count", 30), cookie=cookie, proxy=proxy
                    )
                    return make_jsonrpc_response(req_id, meta.to_dict())

            # Fallback to yt-dlp resolver (Bilibili, YouTube, etc.)
            loop = asyncio.get_running_loop()
            try:
                meta = await loop.run_in_executor(
                    None, lambda: resolve_with_ytdlp(url, cookie=cookie, proxy=proxy)
                )
                if meta and (meta.streams or meta.sub_items):
                    return make_jsonrpc_response(req_id, meta.to_dict())
            except Exception:
                # yt-dlp does not support this site or failed, fall through to universal generic sniffer
                pass

            # Universal Generic Web Sniffer (for arbitrary CMS, DPlayer, HTML5 video, m3u8 sites)
            from sidecar.generic import resolve_generic_web
            meta = await resolve_generic_web(url, cookie=cookie, proxy=proxy)
            return make_jsonrpc_response(req_id, meta.to_dict())

        # 4. Resolve Douyin batch explicitly
        elif method == "resolve_douyin_batch":
            target_type = params.get("type", "posts")  # "posts", "mix", "likes", "collects"
            target_id = params.get("id")
            max_count = params.get("max_count", 50)
            cookie = params.get("cookie")
            proxy = params.get("proxy")

            if not target_id:
                return make_jsonrpc_error(req_id, -32602, "Missing 'id' (sec_user_id or mix_id)")

            meta = await resolve_douyin_batch(
                target_type, target_id, max_count=max_count, cookie=cookie, proxy=proxy
            )
            return make_jsonrpc_response(req_id, meta.to_dict())

        else:
            return make_jsonrpc_error(req_id, -32601, f"Method not found: {method}")

    except AuthRequiredError as exc:
        return make_jsonrpc_error(
            req_id,
            -32001,
            exc.message,
            data={"auth_required": True, "platform": exc.platform},
        )
    except Exception as exc:
        text = str(exc)
        platform = "douyin" if "douyin" in text.lower() or "f2" in text.lower() else "unknown"
        if _looks_like_auth_error(text):
            return make_jsonrpc_error(
                req_id,
                -32001,
                f"可能需要更新 Cookie/登录态: {text}",
                data={"auth_required": True, "platform": platform},
            )
        trace = traceback.format_exc()
        return make_jsonrpc_error(req_id, -32000, f"Execution failed: {exc}", data={"trace": trace})


def _stdin_reader_thread(queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    """Thread function to read lines from sys.stdin safely across all OS platforms."""
    # Force utf-8 encoding on Windows stdin if possible
    if sys.platform == "win32":
        try:
            import io
            sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass

    for line in sys.stdin:
        loop.call_soon_threadsafe(queue.put_nowait, line)
    # Signal EOF
    loop.call_soon_threadsafe(queue.put_nowait, None)


async def main():
    """Main async loop handling JSON-RPC requests concurrently."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[str | None] = asyncio.Queue()

    # Start reader thread
    t = threading.Thread(target=_stdin_reader_thread, args=(queue, loop), daemon=True)
    t.start()

    active_tasks: set[asyncio.Task] = set()

    async def _process_line(raw: str) -> None:
        try:
            response = await handle_request(raw)
            if response:
                _safe_write_stdout(response)
        except Exception as exc:
            try:
                err_resp = make_jsonrpc_error(None, -32000, f"Unhandled server exception: {exc}")
                _safe_write_stdout(err_resp)
            except Exception:
                pass

    while True:
        line = await queue.get()
        if line is None:  # EOF reached
            break
        task = asyncio.create_task(_process_line(line))
        active_tasks.add(task)
        task.add_done_callback(active_tasks.discard)

    if active_tasks:
        await asyncio.gather(*active_tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
