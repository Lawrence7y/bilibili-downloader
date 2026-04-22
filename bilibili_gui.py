#!/usr/bin/env python3
from __future__ import annotations

import os
import queue
import threading
import tkinter as tk
import json
import csv
import winsound
from pathlib import Path
from datetime import datetime
from tkinter import filedialog, messagebox, scrolledtext, ttk
from collections.abc import Callable
from typing import Any

from gui_utils import sanitize_window_geometry
from runtime_env import collect_runtime_health, resolve_state_paths, run_playwright_install
from bilibili_downloader import (
    DEFAULT_ADAPTIVE_RETRY_ATTEMPTS,
    DEFAULT_FILENAME_TEMPLATE,
    POST_ACTION_ARCHIVE,
    POST_ACTION_NONE,
    POST_ACTION_TRANSCODE_H265,
    PLATFORM_AUTO,
    PLATFORM_BILIBILI,
    PLATFORM_DOUYIN,
    PLATFORM_WEB,
    extract_urls_from_inputs,
    build_preview_items,
    check_app_update,
    check_yt_dlp_update,
    platform_name_cn,
    normalize_filename_template,
    resolve_preview_stream,
    run_download,
    split_urls_by_platform,
)
from version import APP_VERSION

try:
    import vlc  # type: ignore
except (ImportError, OSError):
    vlc = None

RESOLUTION_OPTIONS = [
    ("原画（最佳）", "best"),
    ("2160p", "2160"),
    ("1440p", "1440"),
    ("1080p", "1080"),
    ("720p", "720"),
    ("480p", "480"),
    ("360p", "360"),
]

AUDIO_FORMAT_OPTIONS = ["mp3", "m4a", "wav", "flac"]
COOKIES_BROWSER_OPTIONS = [
    ("不使用", "none"),
    ("Edge（推荐抖音）", "edge"),
    ("Chrome", "chrome"),
    ("Firefox", "firefox"),
]


POST_ACTION_OPTIONS = [
    ("None", POST_ACTION_NONE),
    ("Archive by platform/date", POST_ACTION_ARCHIVE),
    ("Transcode H265", POST_ACTION_TRANSCODE_H265),
]

FILENAME_TEMPLATE_PRESETS = [
    DEFAULT_FILENAME_TEMPLATE,
    "%(uploader).80B/%(title).120B [%(id)s]",
    "%(upload_date)s_%(title).120B [%(id)s]",
]

BATCH_PRESETS: dict[str, dict[str, Any]] = {
    "通用 1080p": {
        "resolution_value": "1080",
        "extract_audio": False,
        "audio_format": "mp3",
        "write_subtitles": False,
        "write_thumbnail": False,
        "write_info_json": False,
        "rate_limit": "",
        "retries": "1",
        "filename_template": DEFAULT_FILENAME_TEMPLATE,
        "post_action": POST_ACTION_NONE,
    },
    "音频优先": {
        "resolution_value": "best",
        "extract_audio": True,
        "audio_format": "mp3",
        "write_subtitles": False,
        "write_thumbnail": False,
        "write_info_json": False,
        "rate_limit": "",
        "retries": "1",
        "filename_template": "%(upload_date)s_%(title).120B [%(id)s]",
        "post_action": POST_ACTION_NONE,
    },
    "归档模式": {
        "resolution_value": "best",
        "extract_audio": False,
        "audio_format": "mp3",
        "write_subtitles": True,
        "write_thumbnail": True,
        "write_info_json": True,
        "rate_limit": "",
        "retries": "2",
        "filename_template": "%(uploader).80B/%(title).120B [%(id)s]",
        "post_action": POST_ACTION_ARCHIVE,
    },
    "弱网稳定": {
        "resolution_value": "720",
        "extract_audio": False,
        "audio_format": "mp3",
        "write_subtitles": False,
        "write_thumbnail": False,
        "write_info_json": False,
        "rate_limit": "2M",
        "retries": "3",
        "filename_template": DEFAULT_FILENAME_TEMPLATE,
        "post_action": POST_ACTION_NONE,
    },
}

APP_COLORS = {
    "bg": "#EEF2F8",
    "panel": "#FFFFFF",
    "panel_alt": "#F8FAFD",
    "text": "#1F2937",
    "subtext": "#5B677A",
    "accent": "#1D4ED8",
    "accent_soft": "#DBEAFE",
    "success_soft": "#DCFCE7",
    "danger": "#B91C1C",
    "danger_soft": "#FEE2E2",
    "border": "#D5DEEA",
}

_FRIENDLY_FAILURE_BY_CODE = {
    "COOKIE_DECRYPT_FAILED": "无法解密浏览器 cookies（DPAPI）。请导入最新 cookies.txt，或将 cookies 来源切到 Firefox 后重试。",
    "COOKIE_DB_LOCKED": "浏览器 cookies 数据库被占用。请完全关闭 Edge/Chrome 后重试，或导入最新 cookies.txt。",
    "FRESH_COOKIES_REQUIRED": "抖音需要新鲜 cookies。请先在浏览器重新登录抖音后重试，或导入最新 cookies.txt。",
    "PROBE_TIMEOUT": "链接检查超时。请先在浏览器打开短链获取长链接后重试，或切换更稳定网络。",
    "NO_ALLOWED_LINKS": "没有可下载的可访问视频链接，程序结束。",
}


def format_failure_reason_for_display(error_code: str, reason: str) -> str:
    code = (error_code or "").strip().upper()
    raw_reason = (reason or "").strip()
    lower_reason = raw_reason.lower()

    friendly_reason = _FRIENDLY_FAILURE_BY_CODE.get(code, "")
    if not friendly_reason and "failed to decrypt with dpapi" in lower_reason:
        code = code or "COOKIE_DECRYPT_FAILED"
        friendly_reason = _FRIENDLY_FAILURE_BY_CODE["COOKIE_DECRYPT_FAILED"]
    if not friendly_reason and "fresh cookies" in lower_reason:
        code = code or "FRESH_COOKIES_REQUIRED"
        friendly_reason = _FRIENDLY_FAILURE_BY_CODE["FRESH_COOKIES_REQUIRED"]
    if not friendly_reason and "could not copy chrome cookie database" in lower_reason:
        code = code or "COOKIE_DB_LOCKED"
        friendly_reason = _FRIENDLY_FAILURE_BY_CODE["COOKIE_DB_LOCKED"]

    message = friendly_reason or raw_reason or "任务失败，请查看日志。"
    if code and f"[{code}]" not in message:
        return f"[{code}] {message}"
    return message


class EmbeddedPreviewWindow:
    def __init__(self, parent: tk.Tk, logger: Callable[[str], None]) -> None:
        self._logger = logger
        self._window = tk.Toplevel(parent)
        self._window.title("内嵌实时预览（VLC）")
        self._window.geometry("980x600")
        self._window.minsize(760, 440)
        self._window.protocol("WM_DELETE_WINDOW", self.close)

        self._status_var = tk.StringVar(value="就绪")
        self._title_var = tk.StringVar(value="未开始播放")
        self._is_paused = False
        self._stream_url = ""

        container = ttk.Frame(self._window, padding=8)
        container.pack(fill=tk.BOTH, expand=True)

        self._video_frame = ttk.Frame(container)
        self._video_frame.pack(fill=tk.BOTH, expand=True)

        control_frame = ttk.Frame(container)
        control_frame.pack(fill=tk.X, pady=(8, 0))
        self._pause_button = ttk.Button(control_frame, text="暂停", command=self.toggle_pause, state=tk.DISABLED)
        self._pause_button.pack(side=tk.LEFT)
        self._stop_button = ttk.Button(control_frame, text="停止", command=self.stop, state=tk.DISABLED)
        self._stop_button.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Label(control_frame, textvariable=self._title_var).pack(side=tk.LEFT, padx=(12, 0))

        status_bar = ttk.Frame(container)
        status_bar.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(status_bar, textvariable=self._status_var).pack(side=tk.LEFT)

        self._instance = None
        self._player = None
        self._ready = False
        self._init_player()

    def _init_player(self) -> None:
        if vlc is None:
            self._status_var.set("未检测到 python-vlc，请安装 python-vlc 与 VLC 播放器。")
            return
        try:
            self._instance = vlc.Instance("--no-video-title-show", "--quiet")
            self._player = self._instance.media_player_new()
            self._ready = True
            self._window.after(120, self._bind_video_surface)
        except Exception as exc:
            self._status_var.set(f"VLC 初始化失败：{exc}")
            self._ready = False

    def _bind_video_surface(self) -> None:
        if not self._ready or self._player is None:
            return
        try:
            hwnd = int(self._video_frame.winfo_id())
            self._player.set_hwnd(hwnd)
        except Exception as exc:
            self._logger(f"[预览] 绑定播放窗口失败：{exc}")

    @property
    def ready(self) -> bool:
        return self._ready

    def exists(self) -> bool:
        try:
            return bool(self._window.winfo_exists())
        except tk.TclError:
            return False

    def show(self) -> None:
        if not self.exists():
            return
        self._window.deiconify()
        self._window.lift()
        self._window.focus_force()

    def play_stream(self, stream_url: str, title: str, headers: dict[str, str] | None = None) -> tuple[bool, str]:
        if not self._ready or self._instance is None or self._player is None:
            return False, "VLC 预览环境不可用，请安装 VLC 和 python-vlc。"
        self._bind_video_surface()
        try:
            media = self._instance.media_new(stream_url)
            use_headers = headers or {}
            ua = use_headers.get("User-Agent") or use_headers.get("user-agent")
            referer = use_headers.get("Referer") or use_headers.get("referer")
            if ua:
                media.add_option(f":http-user-agent={ua}")
            if referer:
                media.add_option(f":http-referrer={referer}")
            media.add_option(":network-caching=1000")
            self._player.set_media(media)
            self._player.play()
            self._title_var.set(title or "实时预览")
            self._status_var.set("播放中")
            self._is_paused = False
            self._pause_button.configure(text="暂停", state=tk.NORMAL)
            self._stop_button.configure(state=tk.NORMAL)
            self._stream_url = stream_url
            return True, "播放已启动"
        except Exception as exc:
            return False, f"启动播放失败：{exc}"

    def toggle_pause(self) -> None:
        if not self._ready or self._player is None:
            return
        try:
            self._player.pause()
            self._is_paused = not self._is_paused
            if self._is_paused:
                self._status_var.set("已暂停")
                self._pause_button.configure(text="继续")
            else:
                self._status_var.set("播放中")
                self._pause_button.configure(text="暂停")
        except Exception as exc:
            self._status_var.set(f"暂停/继续失败：{exc}")

    def stop(self) -> None:
        if not self._ready or self._player is None:
            return
        try:
            self._player.stop()
        except Exception:
            pass
        self._status_var.set("已停止")
        self._pause_button.configure(text="暂停", state=tk.DISABLED)
        self._stop_button.configure(state=tk.DISABLED)
        self._is_paused = False

    def close(self) -> None:
        try:
            self.stop()
        except Exception:
            pass
        try:
            if self._player is not None:
                self._player.release()
        except Exception:
            pass
        try:
            if self._instance is not None:
                self._instance.release()
        except Exception:
            pass
        try:
            self._window.destroy()
        except Exception:
            pass


class DownloaderGUI:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        state_paths = resolve_state_paths()
        self._state_dir = state_paths["state_dir"]
        self._settings_path = state_paths["settings_file"]
        self._history_path = state_paths["history_file"]
        self._settings = self._load_settings()

        safe_geometry = sanitize_window_geometry(str(self._settings.get("window_geometry", "980x780")))
        self.root.geometry(safe_geometry)
        self.root.minsize(860, 680)
        self._style = ttk.Style(self.root)
        self._init_theme()

        default_browser_label = COOKIES_BROWSER_OPTIONS[0][0]
        default_resolution_label = RESOLUTION_OPTIONS[0][0]
        browser_labels = {label for label, _value in COOKIES_BROWSER_OPTIONS}
        resolution_labels = {label for label, _value in RESOLUTION_OPTIONS}

        cookies_browser_label = str(self._settings.get("cookies_browser_label", default_browser_label))
        resolution_label = str(self._settings.get("resolution_label", default_resolution_label))
        audio_format = str(self._settings.get("audio_format", AUDIO_FORMAT_OPTIONS[0])).strip().lower()
        if audio_format not in AUDIO_FORMAT_OPTIONS:
            audio_format = AUDIO_FORMAT_OPTIONS[0]

        self.platform_var = tk.StringVar(value="")
        self.output_var = tk.StringVar(
            value=str(self._settings.get("output_dir", str(Path.cwd() / "downloads")))
        )
        self.cookies_var = tk.StringVar(value=str(self._settings.get("cookies_file", "")))
        self.cookies_browser_label_var = tk.StringVar(
            value=(cookies_browser_label if cookies_browser_label in browser_labels else default_browser_label)
        )
        self.resolution_label_var = tk.StringVar(
            value=(resolution_label if resolution_label in resolution_labels else default_resolution_label)
        )
        self.extract_audio_var = tk.BooleanVar(value=bool(self._settings.get("extract_audio", False)))
        self.audio_format_var = tk.StringVar(value=audio_format)
        self.write_subtitles_var = tk.BooleanVar(value=bool(self._settings.get("write_subtitles", False)))
        self.write_thumbnail_var = tk.BooleanVar(value=bool(self._settings.get("write_thumbnail", False)))
        self.write_info_json_var = tk.BooleanVar(value=bool(self._settings.get("write_info_json", False)))
        self.proxy_var = tk.StringVar(value=str(self._settings.get("proxy", "")))
        self.rate_limit_var = tk.StringVar(value=str(self._settings.get("rate_limit", "")))
        self.retries_var = tk.StringVar(value=str(self._settings.get("retries", "1")))
        self.adaptive_retry_attempts_var = tk.StringVar(
            value=str(self._settings.get("adaptive_retry_attempts", str(DEFAULT_ADAPTIVE_RETRY_ATTEMPTS)))
        )
        self.filename_template_var = tk.StringVar(
            value=normalize_filename_template(str(self._settings.get("filename_template", DEFAULT_FILENAME_TEMPLATE)))
        )
        self.post_action_var = tk.StringVar(
            value=str(self._settings.get("post_action", POST_ACTION_NONE))
        )
        self.batch_preset_var = tk.StringVar(value=str(self._settings.get("batch_preset", "通用 1080p")))
        self.schedule_time_var = tk.StringVar(value=str(self._settings.get("schedule_time", "")))
        self.keep_failed_in_queue_var = tk.BooleanVar(
            value=bool(self._settings.get("keep_failed_in_queue", True))
        )
        self.app_update_repo_var = tk.StringVar(value=str(self._settings.get("app_update_repo", "")))
        if self.post_action_var.get() not in {value for _label, value in POST_ACTION_OPTIONS}:
            self.post_action_var.set(POST_ACTION_NONE)
        if self.batch_preset_var.get() not in BATCH_PRESETS:
            self.batch_preset_var.set("通用 1080p")
        self.skip_history_success_var = tk.BooleanVar(
            value=bool(self._settings.get("skip_history_success", True))
        )
        self.notify_done_var = tk.BooleanVar(value=bool(self._settings.get("notify_done", True)))
        self.beep_on_done_var = tk.BooleanVar(value=bool(self._settings.get("beep_on_done", False)))
        self.open_output_on_done_var = tk.BooleanVar(
            value=bool(self._settings.get("open_output_on_done", False))
        )
        self.total_progress_var = tk.StringVar(value="总进度：等待开始")
        self.current_progress_var = tk.StringVar(value="当前文件：等待开始")

        self._queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._worker: threading.Thread | None = None
        self._cancel_event = threading.Event()
        self._total_links = 0
        self._completed_links = 0
        self._selected_urls_override: list[str] | None = None
        self._current_download_url: str = ""
        self._task_queue_urls = extract_urls_from_inputs(
            [str(item) for item in self._settings.get("task_queue", [])]
            if isinstance(self._settings.get("task_queue", []), list)
            else []
        )
        self._failed_urls_in_last_run: list[str] = []
        self._last_failure_reason: str = ""
        self._scheduled_download_at: datetime | None = None
        self._preview_window: EmbeddedPreviewWindow | None = None
        self._preview_resolving = False

        self._build_ui()
        self._refresh_queue_listbox()
        self._load_scheduled_time_from_settings()
        self._refresh_platform_ui()
        self._on_extract_audio_change()
        self._reset_progress()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._check_updates_async(silent=True)
        self._check_runtime_health_async(silent=True)
        self.root.after(100, self._drain_queue)

    def _init_theme(self) -> None:
        style = self._style
        try:
            if "clam" in style.theme_names():
                style.theme_use("clam")
        except tk.TclError:
            pass

        ui_font = ("Microsoft YaHei UI", 10)
        title_font = ("Microsoft YaHei UI", 14, "bold")
        subtitle_font = ("Microsoft YaHei UI", 10)

        self.root.configure(bg=APP_COLORS["bg"])
        self.root.option_add("*Font", ui_font)

        style.configure(".", background=APP_COLORS["bg"], foreground=APP_COLORS["text"], font=ui_font)
        style.configure("App.TFrame", background=APP_COLORS["bg"])
        style.configure("Header.TFrame", background=APP_COLORS["bg"])
        style.configure("Panel.TFrame", background=APP_COLORS["panel"])

        style.configure("Title.TLabel", background=APP_COLORS["bg"], foreground=APP_COLORS["text"], font=title_font)
        style.configure("SubTitle.TLabel", background=APP_COLORS["bg"], foreground=APP_COLORS["subtext"], font=subtitle_font)
        style.configure("Hint.TLabel", background=APP_COLORS["bg"], foreground=APP_COLORS["subtext"])
        style.configure(
            "StatusAuto.TLabel",
            background=APP_COLORS["accent_soft"],
            foreground=APP_COLORS["accent"],
            padding=(10, 4),
            borderwidth=0,
        )
        style.configure(
            "StatusBili.TLabel",
            background="#FDE68A",
            foreground="#7C2D12",
            padding=(10, 4),
            borderwidth=0,
        )
        style.configure(
            "StatusDouyin.TLabel",
            background="#FCE7F3",
            foreground="#9D174D",
            padding=(10, 4),
            borderwidth=0,
        )

        style.configure(
            "Card.TLabelframe",
            background=APP_COLORS["panel"],
            bordercolor=APP_COLORS["border"],
            borderwidth=1,
            relief=tk.SOLID,
            padding=6,
        )
        style.configure(
            "Card.TLabelframe.Label",
            background=APP_COLORS["panel"],
            foreground=APP_COLORS["text"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )

        style.configure("TLabel", background=APP_COLORS["bg"], foreground=APP_COLORS["text"])
        style.configure("TCheckbutton", background=APP_COLORS["bg"], foreground=APP_COLORS["text"])
        style.map("TCheckbutton", background=[("active", APP_COLORS["bg"])])

        style.configure(
            "TEntry",
            fieldbackground=APP_COLORS["panel"],
            background=APP_COLORS["panel"],
            foreground=APP_COLORS["text"],
            bordercolor=APP_COLORS["border"],
            lightcolor=APP_COLORS["border"],
            darkcolor=APP_COLORS["border"],
            padding=4,
        )
        style.configure(
            "TCombobox",
            fieldbackground=APP_COLORS["panel"],
            background=APP_COLORS["panel"],
            foreground=APP_COLORS["text"],
            bordercolor=APP_COLORS["border"],
            lightcolor=APP_COLORS["border"],
            darkcolor=APP_COLORS["border"],
            arrowsize=14,
            padding=3,
        )
        style.configure(
            "TSpinbox",
            fieldbackground=APP_COLORS["panel"],
            background=APP_COLORS["panel"],
            foreground=APP_COLORS["text"],
            arrowsize=12,
            padding=2,
        )

        style.configure("TButton", padding=(10, 6), borderwidth=0)
        style.configure("Primary.TButton", background=APP_COLORS["accent"], foreground="#FFFFFF", padding=(12, 7))
        style.map(
            "Primary.TButton",
            background=[("active", "#1E40AF"), ("disabled", "#93C5FD")],
            foreground=[("disabled", "#EFF6FF")],
        )
        style.configure("Danger.TButton", background=APP_COLORS["danger"], foreground="#FFFFFF", padding=(12, 7))
        style.map(
            "Danger.TButton",
            background=[("active", "#991B1B"), ("disabled", "#FCA5A5")],
            foreground=[("disabled", "#FEF2F2")],
        )
        style.configure("Soft.TButton", background=APP_COLORS["panel"], foreground=APP_COLORS["text"], padding=(10, 6))
        style.map("Soft.TButton", background=[("active", APP_COLORS["panel_alt"])])

        style.configure(
            "Accent.Horizontal.TProgressbar",
            troughcolor="#E5E7EB",
            background=APP_COLORS["accent"],
            bordercolor="#E5E7EB",
            lightcolor=APP_COLORS["accent"],
            darkcolor=APP_COLORS["accent"],
            thickness=12,
        )
        style.configure(
            "Info.Horizontal.TProgressbar",
            troughcolor="#E5E7EB",
            background="#0EA5E9",
            bordercolor="#E5E7EB",
            lightcolor="#0EA5E9",
            darkcolor="#0EA5E9",
            thickness=12,
        )

    def _build_ui(self) -> None:
        main_frame = ttk.Frame(self.root, style="App.TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True)

        self.main_scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL)
        self.main_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.main_canvas = tk.Canvas(
            main_frame,
            background=APP_COLORS["bg"],
            highlightthickness=0,
            borderwidth=0,
            yscrollcommand=self.main_scrollbar.set,
        )
        self.main_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.main_scrollbar.configure(command=self.main_canvas.yview)

        container = ttk.Frame(self.main_canvas, padding=(14, 12, 14, 12), style="App.TFrame")
        self._main_canvas_window = self.main_canvas.create_window((0, 0), window=container, anchor=tk.NW)
        container.bind("<Configure>", self._on_main_content_configure)
        self.main_canvas.bind("<Configure>", self._on_main_canvas_configure)
        self._bind_main_mousewheel()

        header_frame = ttk.Frame(container, style="Header.TFrame")
        header_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(header_frame, text=f"BillBill Downloader  {APP_VERSION}", style="Title.TLabel").pack(anchor=tk.W)
        ttk.Label(
            header_frame,
            text="B站/抖音视频批量下载、预览、队列、定时与历史去重",
            style="SubTitle.TLabel",
        ).pack(anchor=tk.W, pady=(2, 0))

        platform_frame = ttk.Frame(header_frame, style="Header.TFrame")
        platform_frame.pack(fill=tk.X, pady=(8, 0))
        self.platform_status_label = ttk.Label(platform_frame, text="", style="StatusAuto.TLabel")
        self.platform_status_label.pack(side=tk.LEFT)

        quick_action_frame = ttk.Frame(container)
        quick_action_frame.pack(fill=tk.X, pady=(0, 10))
        self.quick_start_button = ttk.Button(
            quick_action_frame,
            text="开始下载",
            command=self._start_download,
            style="Primary.TButton",
        )
        self.quick_start_button.pack(side=tk.LEFT)
        self.quick_health_button = ttk.Button(
            quick_action_frame,
            text="环境自检/修复",
            command=self._check_and_repair_runtime,
            style="Soft.TButton",
        )
        self.quick_health_button.pack(side=tk.LEFT, padx=(8, 0))
        self.quick_stop_button = ttk.Button(
            quick_action_frame,
            text="停止任务",
            command=self._request_stop,
            state=tk.DISABLED,
            style="Danger.TButton",
        )
        self.quick_stop_button.pack(side=tk.LEFT, padx=(8, 0))

        self.urls_label = ttk.Label(container, text="")
        self.urls_label.pack(anchor=tk.W)

        self.urls_text = scrolledtext.ScrolledText(container, height=9, wrap=tk.WORD)
        self.urls_text.pack(fill=tk.X, expand=False, pady=(6, 12))
        self.urls_text.configure(
            bg=APP_COLORS["panel"],
            fg=APP_COLORS["text"],
            insertbackground=APP_COLORS["text"],
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=APP_COLORS["border"],
            highlightcolor=APP_COLORS["accent"],
        )

        queue_frame = ttk.LabelFrame(container, text="任务队列", style="Card.TLabelframe")
        queue_frame.pack(fill=tk.X, pady=(0, 10))
        queue_top = ttk.Frame(queue_frame)
        queue_top.pack(fill=tk.X, padx=8, pady=(6, 4))
        self.queue_count_var = tk.StringVar(value="队列: 0")
        ttk.Label(queue_top, textvariable=self.queue_count_var).pack(side=tk.LEFT)
        self.queue_preview_button = ttk.Button(
            queue_top,
            text="预览并选择",
            command=self._preview_and_select,
            style="Soft.TButton",
        )
        self.queue_preview_button.pack(side=tk.LEFT, padx=(10, 0))
        self.keep_failed_check = ttk.Checkbutton(
            queue_top,
            text="任务结束后保留失败项",
            variable=self.keep_failed_in_queue_var,
        )
        self.keep_failed_check.pack(side=tk.RIGHT)

        queue_list_frame = ttk.Frame(queue_frame)
        queue_list_frame.pack(fill=tk.X, padx=8, pady=(0, 6))
        self.task_queue_listbox = tk.Listbox(queue_list_frame, height=5, selectmode=tk.EXTENDED)
        self.task_queue_listbox.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.task_queue_listbox.configure(
            bg=APP_COLORS["panel"],
            fg=APP_COLORS["text"],
            selectbackground=APP_COLORS["accent_soft"],
            selectforeground=APP_COLORS["text"],
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=APP_COLORS["border"],
            highlightcolor=APP_COLORS["accent"],
            activestyle="none",
        )
        queue_actions = ttk.Frame(queue_list_frame)
        queue_actions.pack(side=tk.LEFT, padx=(8, 0))
        self.queue_sync_button = ttk.Button(queue_actions, text="同步输入", command=self._queue_sync_from_input, style="Soft.TButton")
        self.queue_sync_button.pack(fill=tk.X)
        self.queue_up_button = ttk.Button(queue_actions, text="上移", command=self._queue_move_up, style="Soft.TButton")
        self.queue_up_button.pack(fill=tk.X, pady=(4, 0))
        self.queue_down_button = ttk.Button(queue_actions, text="下移", command=self._queue_move_down, style="Soft.TButton")
        self.queue_down_button.pack(fill=tk.X, pady=(4, 0))
        self.queue_remove_button = ttk.Button(queue_actions, text="移除", command=self._queue_remove_selected, style="Soft.TButton")
        self.queue_remove_button.pack(fill=tk.X, pady=(4, 0))
        self.queue_clear_button = ttk.Button(queue_actions, text="清空", command=self._queue_clear, style="Soft.TButton")
        self.queue_clear_button.pack(fill=tk.X, pady=(4, 0))

        output_frame = ttk.Frame(container)
        output_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(output_frame, text="保存目录：").pack(side=tk.LEFT)
        self.output_entry = ttk.Entry(output_frame, textvariable=self.output_var)
        self.output_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)
        ttk.Button(output_frame, text="浏览", command=self._choose_output_dir, style="Soft.TButton").pack(side=tk.LEFT)

        cookie_file_frame = ttk.Frame(container)
        cookie_file_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(cookie_file_frame, text="cookies.txt（可选）：").pack(side=tk.LEFT)
        self.cookies_entry = ttk.Entry(cookie_file_frame, textvariable=self.cookies_var)
        self.cookies_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)
        ttk.Button(cookie_file_frame, text="浏览", command=self._choose_cookies_file, style="Soft.TButton").pack(side=tk.LEFT)

        cookie_browser_frame = ttk.Frame(container)
        cookie_browser_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(cookie_browser_frame, text="cookies 来源：").pack(side=tk.LEFT)
        self.cookies_browser_combo = ttk.Combobox(
            cookie_browser_frame,
            textvariable=self.cookies_browser_label_var,
            values=[label for label, _value in COOKIES_BROWSER_OPTIONS],
            width=20,
            state="readonly",
        )
        self.cookies_browser_combo.pack(side=tk.LEFT, padx=(8, 0))

        options_frame = ttk.Frame(container)
        options_frame.pack(fill=tk.X, pady=(0, 12))

        ttk.Label(options_frame, text="分辨率：").pack(side=tk.LEFT)
        self.resolution_combo = ttk.Combobox(
            options_frame,
            textvariable=self.resolution_label_var,
            values=[label for label, _value in RESOLUTION_OPTIONS],
            width=14,
            state="readonly",
        )
        self.resolution_combo.pack(side=tk.LEFT, padx=(6, 14))

        self.extract_audio_check = ttk.Checkbutton(
            options_frame,
            text="分离音频",
            variable=self.extract_audio_var,
            command=self._on_extract_audio_change,
        )
        self.extract_audio_check.pack(side=tk.LEFT)

        ttk.Label(options_frame, text="音频格式：").pack(side=tk.LEFT, padx=(14, 0))
        self.audio_format_combo = ttk.Combobox(
            options_frame,
            textvariable=self.audio_format_var,
            values=AUDIO_FORMAT_OPTIONS,
            width=8,
            state="readonly",
        )
        self.audio_format_combo.pack(side=tk.LEFT, padx=(6, 0))

        export_frame = ttk.Frame(container)
        export_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(export_frame, text="附加导出：").pack(side=tk.LEFT)
        self.write_subtitles_check = ttk.Checkbutton(
            export_frame, text="字幕", variable=self.write_subtitles_var
        )
        self.write_subtitles_check.pack(side=tk.LEFT, padx=(6, 0))
        self.write_thumbnail_check = ttk.Checkbutton(
            export_frame, text="封面", variable=self.write_thumbnail_var
        )
        self.write_thumbnail_check.pack(side=tk.LEFT, padx=(6, 0))
        self.write_info_json_check = ttk.Checkbutton(
            export_frame, text="元数据JSON", variable=self.write_info_json_var
        )
        self.write_info_json_check.pack(side=tk.LEFT, padx=(6, 0))
        self.skip_history_check = ttk.Checkbutton(
            export_frame, text="跳过历史成功链接", variable=self.skip_history_success_var
        )
        self.skip_history_check.pack(side=tk.LEFT, padx=(12, 0))

        network_frame = ttk.Frame(container)
        network_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(network_frame, text="代理：").pack(side=tk.LEFT)
        self.proxy_entry = ttk.Entry(network_frame, textvariable=self.proxy_var, width=24)
        self.proxy_entry.pack(side=tk.LEFT, padx=(6, 12))
        ttk.Label(network_frame, text="限速：").pack(side=tk.LEFT)
        self.rate_limit_entry = ttk.Entry(network_frame, textvariable=self.rate_limit_var, width=10)
        self.rate_limit_entry.pack(side=tk.LEFT, padx=(6, 12))
        ttk.Label(network_frame, text="重试次数：").pack(side=tk.LEFT)
        self.retries_spin = ttk.Spinbox(network_frame, from_=0, to=10, textvariable=self.retries_var, width=6)
        self.retries_spin.pack(side=tk.LEFT, padx=(6, 0))

        advanced_frame = ttk.Frame(container)
        advanced_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(advanced_frame, text="自适应重试轮数:").pack(side=tk.LEFT)
        self.adaptive_retries_spin = ttk.Spinbox(
            advanced_frame,
            from_=1,
            to=6,
            textvariable=self.adaptive_retry_attempts_var,
            width=6,
        )
        self.adaptive_retries_spin.pack(side=tk.LEFT, padx=(6, 12))
        ttk.Label(advanced_frame, text="文件名模板:").pack(side=tk.LEFT)
        self.filename_template_combo = ttk.Combobox(
            advanced_frame,
            textvariable=self.filename_template_var,
            values=FILENAME_TEMPLATE_PRESETS,
            width=42,
        )
        self.filename_template_combo.pack(side=tk.LEFT, padx=(6, 12), fill=tk.X, expand=True)
        ttk.Label(advanced_frame, text="下载后动作:").pack(side=tk.LEFT)
        self.post_action_combo = ttk.Combobox(
            advanced_frame,
            textvariable=self.post_action_var,
            values=[value for _label, value in POST_ACTION_OPTIONS],
            width=18,
            state="readonly",
        )
        self.post_action_combo.pack(side=tk.LEFT, padx=(6, 0))

        preset_frame = ttk.Frame(container)
        preset_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(preset_frame, text="批量预设:").pack(side=tk.LEFT)
        self.batch_preset_combo = ttk.Combobox(
            preset_frame,
            textvariable=self.batch_preset_var,
            values=list(BATCH_PRESETS.keys()),
            width=20,
            state="readonly",
        )
        self.batch_preset_combo.pack(side=tk.LEFT, padx=(6, 0))
        self.apply_preset_button = ttk.Button(preset_frame, text="应用预设", command=self._apply_selected_preset, style="Soft.TButton")
        self.apply_preset_button.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Label(preset_frame, text="应用更新仓库(owner/repo):").pack(side=tk.LEFT, padx=(16, 0))
        self.app_update_repo_entry = ttk.Entry(preset_frame, textvariable=self.app_update_repo_var, width=28)
        self.app_update_repo_entry.pack(side=tk.LEFT, padx=(6, 0))

        schedule_frame = ttk.Frame(container)
        schedule_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(schedule_frame, text="定时下载(YYYY-MM-DD HH:MM:SS):").pack(side=tk.LEFT)
        self.schedule_entry = ttk.Entry(schedule_frame, textvariable=self.schedule_time_var, width=22)
        self.schedule_entry.pack(side=tk.LEFT, padx=(6, 8))
        self.schedule_button = ttk.Button(schedule_frame, text="设置定时", command=self._schedule_download, style="Soft.TButton")
        self.schedule_button.pack(side=tk.LEFT)
        self.cancel_schedule_button = ttk.Button(
            schedule_frame,
            text="取消定时",
            command=self._cancel_scheduled_download,
            style="Soft.TButton",
        )
        self.cancel_schedule_button.pack(side=tk.LEFT, padx=(8, 0))
        self.schedule_status_var = tk.StringVar(value="未设置定时")
        ttk.Label(schedule_frame, textvariable=self.schedule_status_var, foreground="#666666").pack(
            side=tk.LEFT,
            padx=(12, 0),
        )

        button_frame = ttk.Frame(container)
        button_frame.pack(fill=tk.X, pady=(0, 10))
        self.start_button = ttk.Button(button_frame, text="开始下载", command=self._start_download, style="Primary.TButton")
        self.start_button.pack(side=tk.LEFT)
        self.stop_button = ttk.Button(
            button_frame,
            text="停止任务",
            command=self._request_stop,
            state=tk.DISABLED,
            style="Danger.TButton",
        )
        self.stop_button.pack(side=tk.LEFT, padx=(8, 0))
        self.import_button = ttk.Button(button_frame, text="导入任务", command=self._import_tasks, style="Soft.TButton")
        self.import_button.pack(side=tk.LEFT, padx=(8, 0))
        self.export_button = ttk.Button(button_frame, text="导出任务", command=self._export_tasks, style="Soft.TButton")
        self.export_button.pack(side=tk.LEFT, padx=(8, 0))
        self.update_button = ttk.Button(button_frame, text="检查更新", command=self._check_updates_async, style="Soft.TButton")
        self.update_button.pack(side=tk.LEFT, padx=(8, 0))
        self.health_button = ttk.Button(button_frame, text="环境自检/修复", command=self._check_and_repair_runtime, style="Soft.TButton")
        self.health_button.pack(side=tk.LEFT, padx=(8, 0))
        self.clear_button = ttk.Button(button_frame, text="清空日志", command=self._clear_log, style="Soft.TButton")
        self.clear_button.pack(side=tk.LEFT, padx=(8, 0))
        self.open_output_button = ttk.Button(
            button_frame,
            text="打开保存目录",
            command=self._open_output_folder,
            style="Soft.TButton",
        )
        self.open_output_button.pack(side=tk.LEFT, padx=(8, 0))

        notify_frame = ttk.Frame(container)
        notify_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(notify_frame, text="完成通知：").pack(side=tk.LEFT)
        self.notify_done_check = ttk.Checkbutton(notify_frame, text="弹窗提示", variable=self.notify_done_var)
        self.notify_done_check.pack(side=tk.LEFT, padx=(6, 0))
        self.beep_done_check = ttk.Checkbutton(notify_frame, text="声音提醒", variable=self.beep_on_done_var)
        self.beep_done_check.pack(side=tk.LEFT, padx=(6, 0))
        self.open_done_check = ttk.Checkbutton(
            notify_frame, text="完成后打开目录", variable=self.open_output_on_done_var
        )
        self.open_done_check.pack(side=tk.LEFT, padx=(6, 0))

        progress_frame = ttk.LabelFrame(container, text="下载进度", style="Card.TLabelframe")
        progress_frame.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(progress_frame, textvariable=self.total_progress_var).pack(anchor=tk.W, padx=8, pady=(8, 4))
        self.total_progressbar = ttk.Progressbar(
            progress_frame,
            mode="determinate",
            maximum=100,
            style="Accent.Horizontal.TProgressbar",
        )
        self.total_progressbar.pack(fill=tk.X, padx=8)
        ttk.Label(progress_frame, textvariable=self.current_progress_var).pack(anchor=tk.W, padx=8, pady=(8, 4))
        self.current_progressbar = ttk.Progressbar(
            progress_frame,
            mode="determinate",
            maximum=100,
            style="Info.Horizontal.TProgressbar",
        )
        self.current_progressbar.pack(fill=tk.X, padx=8, pady=(0, 8))

        ttk.Label(container, text="运行日志：").pack(anchor=tk.W)
        self.log_text = scrolledtext.ScrolledText(container, height=18, wrap=tk.WORD, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
        self.log_text.configure(
            bg="#0F172A",
            fg="#E5E7EB",
            insertbackground="#E5E7EB",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=APP_COLORS["border"],
            highlightcolor=APP_COLORS["accent"],
        )

        self.note_label = ttk.Label(container, text="", style="Hint.TLabel")
        self.note_label.pack(anchor=tk.W, pady=(8, 0))

    def _on_main_content_configure(self, _event: tk.Event) -> None:
        if hasattr(self, "main_canvas"):
            self.main_canvas.configure(scrollregion=self.main_canvas.bbox("all"))

    def _on_main_canvas_configure(self, event: tk.Event) -> None:
        if hasattr(self, "main_canvas") and hasattr(self, "_main_canvas_window"):
            self.main_canvas.itemconfigure(self._main_canvas_window, width=event.width)

    def _bind_main_mousewheel(self) -> None:
        self.root.bind_all("<MouseWheel>", self._on_main_mousewheel, add="+")
        self.root.bind_all("<Button-4>", self._on_main_mousewheel_linux, add="+")
        self.root.bind_all("<Button-5>", self._on_main_mousewheel_linux, add="+")

    def _unbind_main_mousewheel(self) -> None:
        self.root.unbind_all("<MouseWheel>")
        self.root.unbind_all("<Button-4>")
        self.root.unbind_all("<Button-5>")

    @staticmethod
    def _is_descendant_widget(widget: object, parent: object) -> bool:
        current = widget
        while current is not None:
            if current == parent:
                return True
            current = getattr(current, "master", None)
        return False

    def _should_skip_main_mousewheel(self, event: tk.Event) -> bool:
        widget = getattr(event, "widget", None)
        skip_targets = (
            getattr(self, "urls_text", None),
            getattr(self, "log_text", None),
            getattr(self, "task_queue_listbox", None),
        )
        for target in skip_targets:
            if target is not None and self._is_descendant_widget(widget, target):
                return True
        return False

    def _on_main_mousewheel(self, event: tk.Event) -> str | None:
        if self._should_skip_main_mousewheel(event):
            return None
        delta = int(getattr(event, "delta", 0))
        if delta == 0:
            return None
        step = -1 if delta > 0 else 1
        self.main_canvas.yview_scroll(step, "units")
        return "break"

    def _on_main_mousewheel_linux(self, event: tk.Event) -> str | None:
        if self._should_skip_main_mousewheel(event):
            return None
        num = int(getattr(event, "num", 0))
        if num == 4:
            self.main_canvas.yview_scroll(-1, "units")
            return "break"
        if num == 5:
            self.main_canvas.yview_scroll(1, "units")
            return "break"
        return None

    def _refresh_platform_ui(self) -> None:
        platform = self.platform_var.get().strip()
        if platform:
            name = platform_name_cn(platform)
            self.platform_status_label.configure(text=f"当前平台：{name}")
            self.root.title(f"{name}视频下载器（自动识别，{APP_VERSION}）")
        else:
            self.platform_status_label.configure(text="当前平台：待自动识别")
            self.root.title(f"视频下载器（自动识别平台，{APP_VERSION}）")

        self.urls_label.configure(text="视频链接（支持 B站/抖音，每行一个）：")
        self.note_label.configure(
            text=(
                "说明：无需手动切换平台，点击“开始下载”后会自动识别 B站/抖音，"
                "并支持混合平台链接分组下载。"
            )
        )
        if platform == PLATFORM_BILIBILI:
            self.platform_status_label.configure(text="当前平台：B站", style="StatusBili.TLabel")
            self.root.title(f"B站视频下载器（{APP_VERSION}）")
        elif platform == PLATFORM_DOUYIN:
            self.platform_status_label.configure(text="当前平台：抖音", style="StatusDouyin.TLabel")
            self.root.title(f"抖音视频下载器（{APP_VERSION}）")
        else:
            self.platform_status_label.configure(text="当前平台：自动识别", style="StatusAuto.TLabel")
            self.root.title(f"BillBill Downloader（{APP_VERSION}）")

        self.urls_label.configure(text="视频链接（支持 B站 / 抖音，每行一个）：")
        self.note_label.configure(
            text="说明：可直接粘贴分享文案，程序会自动提取链接并按平台分组处理。支持队列、定时、去重与预览。"
        )

    def _load_settings(self) -> dict[str, Any]:
        if not self._settings_path.exists():
            return {}
        try:
            data = json.loads(self._settings_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _save_settings(self) -> None:
        data: dict[str, Any] = {
            "output_dir": self.output_var.get().strip(),
            "cookies_file": self.cookies_var.get().strip(),
            "cookies_browser_label": self.cookies_browser_label_var.get().strip(),
            "resolution_label": self.resolution_label_var.get().strip(),
            "extract_audio": bool(self.extract_audio_var.get()),
            "audio_format": (self.audio_format_var.get() or AUDIO_FORMAT_OPTIONS[0]).strip().lower(),
            "write_subtitles": bool(self.write_subtitles_var.get()),
            "write_thumbnail": bool(self.write_thumbnail_var.get()),
            "write_info_json": bool(self.write_info_json_var.get()),
            "proxy": self.proxy_var.get().strip(),
            "rate_limit": self.rate_limit_var.get().strip(),
            "retries": self.retries_var.get().strip(),
            "adaptive_retry_attempts": self.adaptive_retry_attempts_var.get().strip(),
            "filename_template": normalize_filename_template(self.filename_template_var.get().strip()),
            "post_action": self.post_action_var.get().strip(),
            "batch_preset": self.batch_preset_var.get().strip(),
            "schedule_time": self.schedule_time_var.get().strip(),
            "keep_failed_in_queue": bool(self.keep_failed_in_queue_var.get()),
            "task_queue": list(self._task_queue_urls),
            "app_update_repo": self.app_update_repo_var.get().strip(),
            "skip_history_success": bool(self.skip_history_success_var.get()),
            "notify_done": bool(self.notify_done_var.get()),
            "beep_on_done": bool(self.beep_on_done_var.get()),
            "open_output_on_done": bool(self.open_output_on_done_var.get()),
            "window_geometry": self.root.geometry(),
        }
        try:
            self._settings_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass

    def _on_close(self) -> None:
        if self._worker and self._worker.is_alive() and not self._cancel_event.is_set():
            self._cancel_event.set()
            self._append_log("窗口关闭：已请求停止任务。")
        self._cancel_scheduled_download(show_log=False, restart_loop=False)
        self._unbind_main_mousewheel()
        if self._preview_window is not None and self._preview_window.exists():
            self._preview_window.close()
            self._preview_window = None
        self._save_settings()
        self.root.destroy()

    def _read_input_lines(self) -> list[str]:
        raw = self.urls_text.get("1.0", tk.END)
        return [line for line in raw.splitlines() if line.strip()]

    def _parse_urls(self) -> list[str]:
        return extract_urls_from_inputs(self._read_input_lines())

    def _effective_urls_for_run(self) -> list[str]:
        if self._selected_urls_override:
            return list(self._selected_urls_override)
        if self._task_queue_urls:
            return list(self._task_queue_urls)
        return self._parse_urls()

    def _refresh_queue_listbox(self) -> None:
        if not hasattr(self, "task_queue_listbox"):
            return
        self.task_queue_listbox.delete(0, tk.END)
        for idx, url in enumerate(self._task_queue_urls, start=1):
            self.task_queue_listbox.insert(tk.END, f"{idx:03d}. {url}")
        if hasattr(self, "queue_count_var"):
            self.queue_count_var.set(f"队列: {len(self._task_queue_urls)}")

    def _queue_sync_from_input(self) -> None:
        urls = self._parse_urls()
        if not urls:
            messagebox.showwarning("无可同步内容", "请先在输入框填入至少一个链接。")
            return
        self._task_queue_urls = list(urls)
        self._selected_urls_override = None
        self._refresh_queue_listbox()
        self._save_settings()
        self._append_log(f"任务队列已同步：{len(urls)} 条")

    def _queue_remove_selected(self) -> None:
        indexes = sorted(self.task_queue_listbox.curselection(), reverse=True)
        if not indexes:
            return
        for idx in indexes:
            if 0 <= idx < len(self._task_queue_urls):
                self._task_queue_urls.pop(idx)
        self._refresh_queue_listbox()
        self._save_settings()

    def _queue_clear(self) -> None:
        if not self._task_queue_urls:
            return
        self._task_queue_urls.clear()
        self._refresh_queue_listbox()
        self._save_settings()

    def _queue_move_up(self) -> None:
        indexes = list(self.task_queue_listbox.curselection())
        if not indexes or indexes[0] <= 0:
            return
        for idx in indexes:
            self._task_queue_urls[idx - 1], self._task_queue_urls[idx] = (
                self._task_queue_urls[idx],
                self._task_queue_urls[idx - 1],
            )
        self._refresh_queue_listbox()
        for idx in indexes:
            self.task_queue_listbox.selection_set(idx - 1)
        self._save_settings()

    def _queue_move_down(self) -> None:
        indexes = list(self.task_queue_listbox.curselection())
        if not indexes or indexes[-1] >= len(self._task_queue_urls) - 1:
            return
        for idx in reversed(indexes):
            self._task_queue_urls[idx], self._task_queue_urls[idx + 1] = (
                self._task_queue_urls[idx + 1],
                self._task_queue_urls[idx],
            )
        self._refresh_queue_listbox()
        for idx in indexes:
            self.task_queue_listbox.selection_set(idx + 1)
        self._save_settings()

    def _apply_selected_preset(self) -> None:
        preset_name = self.batch_preset_var.get().strip()
        preset = BATCH_PRESETS.get(preset_name)
        if not preset:
            return
        resolution_value = str(preset.get("resolution_value", "best"))
        for label, value in RESOLUTION_OPTIONS:
            if value == resolution_value:
                self.resolution_label_var.set(label)
                break
        self.extract_audio_var.set(bool(preset.get("extract_audio", False)))
        self.audio_format_var.set(str(preset.get("audio_format", "mp3")))
        self.write_subtitles_var.set(bool(preset.get("write_subtitles", False)))
        self.write_thumbnail_var.set(bool(preset.get("write_thumbnail", False)))
        self.write_info_json_var.set(bool(preset.get("write_info_json", False)))
        self.rate_limit_var.set(str(preset.get("rate_limit", "")))
        self.retries_var.set(str(preset.get("retries", "1")))
        self.filename_template_var.set(normalize_filename_template(str(preset.get("filename_template", DEFAULT_FILENAME_TEMPLATE))))
        self.post_action_var.set(str(preset.get("post_action", POST_ACTION_NONE)))
        self._on_extract_audio_change()
        self._save_settings()
        self._append_log(f"已应用预设：{preset_name}")

    def _format_schedule_status(self) -> str:
        if self._scheduled_download_at is None:
            return "未设置定时"
        return f"已定时: {self._scheduled_download_at.strftime('%Y-%m-%d %H:%M:%S')}"

    def _load_scheduled_time_from_settings(self) -> None:
        raw = self.schedule_time_var.get().strip()
        if raw:
            try:
                dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                self._scheduled_download_at = None
            else:
                if dt > datetime.now():
                    self._scheduled_download_at = dt
                else:
                    self._scheduled_download_at = None
                    self.schedule_time_var.set("")
        if hasattr(self, "schedule_status_var"):
            self.schedule_status_var.set(self._format_schedule_status())
        self._schedule_after_id: str | None = None
        self._schedule_loop()

    def _schedule_loop(self) -> None:
        self._schedule_after_id = self.root.after(1000, self._schedule_loop)
        if self._scheduled_download_at is None:
            return
        if self._worker and self._worker.is_alive():
            return
        now = datetime.now()
        if now < self._scheduled_download_at:
            return
        self._append_log(f"触发定时任务：{self._scheduled_download_at.strftime('%Y-%m-%d %H:%M:%S')}")
        self._scheduled_download_at = None
        self.schedule_time_var.set("")
        if hasattr(self, "schedule_status_var"):
            self.schedule_status_var.set(self._format_schedule_status())
        self._save_settings()
        self._start_download()

    def _schedule_download(self) -> None:
        text = self.schedule_time_var.get().strip()
        if not text:
            messagebox.showwarning("时间为空", "请输入定时执行时间。")
            return
        try:
            target = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            messagebox.showwarning("时间格式错误", "请使用 YYYY-MM-DD HH:MM:SS 格式。")
            return
        if target <= datetime.now():
            messagebox.showwarning("时间无效", "定时时间必须晚于当前时间。")
            return
        self._scheduled_download_at = target
        if hasattr(self, "schedule_status_var"):
            self.schedule_status_var.set(self._format_schedule_status())
        self._save_settings()
        self._append_log(f"已设置定时任务：{target.strftime('%Y-%m-%d %H:%M:%S')}")

    def _cancel_scheduled_download(self, show_log: bool = True, restart_loop: bool = True) -> None:
        self._scheduled_download_at = None
        self.schedule_time_var.set("")
        if hasattr(self, "schedule_status_var"):
            self.schedule_status_var.set(self._format_schedule_status())
        if self._schedule_after_id:
            try:
                self.root.after_cancel(self._schedule_after_id)
            except Exception:
                pass
            self._schedule_after_id = None
        if show_log:
            self._append_log("已取消定时任务。")
        self._save_settings()
        if restart_loop:
            self._schedule_loop()

    def _on_extract_audio_change(self) -> None:
        self.audio_format_combo.configure(state=("readonly" if self.extract_audio_var.get() else "disabled"))

    def _append_log(self, text: str) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"{text}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _clear_log(self) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _choose_output_dir(self) -> None:
        selected = filedialog.askdirectory(initialdir=self.output_var.get() or str(Path.cwd()))
        if selected:
            self.output_var.set(selected)
            self._save_settings()

    def _choose_cookies_file(self) -> None:
        selected = filedialog.askopenfilename(
            title="选择 cookies.txt",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")],
        )
        if selected:
            self.cookies_var.set(selected)
            self._save_settings()

    def _open_output_folder(self) -> None:
        output = Path(self.output_var.get().strip() or "downloads").resolve()
        output.mkdir(parents=True, exist_ok=True)
        os.startfile(str(output))

    def _get_resolution_value(self) -> str:
        label = self.resolution_label_var.get()
        for option_label, value in RESOLUTION_OPTIONS:
            if option_label == label:
                return value
        return "best"

    def _get_browser_cookie_value(self) -> str:
        label = self.cookies_browser_label_var.get()
        for option_label, value in COOKIES_BROWSER_OPTIONS:
            if option_label == label:
                return value
        return "none"

    def _set_running_state(self, running: bool) -> None:
        state = tk.DISABLED if running else tk.NORMAL
        self.quick_start_button.configure(state=state)
        self.quick_health_button.configure(state=state)
        self.quick_stop_button.configure(state=(tk.NORMAL if running else tk.DISABLED))
        self.start_button.configure(state=state)
        self.stop_button.configure(state=(tk.NORMAL if running else tk.DISABLED))
        self.queue_preview_button.configure(state=state)
        self.import_button.configure(state=state)
        self.export_button.configure(state=state)
        self.update_button.configure(state=state)
        self.health_button.configure(state=state)
        self.output_entry.configure(state=state)
        self.cookies_entry.configure(state=state)
        self.urls_text.configure(state=state)
        self.proxy_entry.configure(state=state)
        self.rate_limit_entry.configure(state=state)
        self.retries_spin.configure(state=state)
        self.adaptive_retries_spin.configure(state=state)
        self.filename_template_combo.configure(state=state)
        self.post_action_combo.configure(state=("disabled" if running else "readonly"))
        self.batch_preset_combo.configure(state=("disabled" if running else "readonly"))
        self.apply_preset_button.configure(state=state)
        self.app_update_repo_entry.configure(state=state)
        self.schedule_entry.configure(state=state)
        self.schedule_button.configure(state=state)
        self.cancel_schedule_button.configure(state=state)
        self.cookies_browser_combo.configure(state=("disabled" if running else "readonly"))
        self.resolution_combo.configure(state=("disabled" if running else "readonly"))
        self.extract_audio_check.configure(state=state)
        self.write_subtitles_check.configure(state=state)
        self.write_thumbnail_check.configure(state=state)
        self.write_info_json_check.configure(state=state)
        self.skip_history_check.configure(state=state)
        self.notify_done_check.configure(state=state)
        self.beep_done_check.configure(state=state)
        self.open_done_check.configure(state=state)
        self.keep_failed_check.configure(state=state)
        self.queue_sync_button.configure(state=state)
        self.queue_up_button.configure(state=state)
        self.queue_down_button.configure(state=state)
        self.queue_remove_button.configure(state=state)
        self.queue_clear_button.configure(state=state)
        self.task_queue_listbox.configure(state=state)
        if running:
            self.audio_format_combo.configure(state="disabled")
        else:
            self._on_extract_audio_change()
        self.clear_button.configure(state=tk.NORMAL)
        self.open_output_button.configure(state=tk.NORMAL)

    def _reset_progress(self) -> None:
        self._total_links = 0
        self._completed_links = 0
        self.total_progressbar.configure(value=0)
        self.current_progressbar.configure(value=0)
        self.total_progress_var.set("总进度：等待开始")
        self.current_progress_var.set("当前文件：等待开始")

    def _update_total_progress(self) -> None:
        if self._total_links <= 0:
            self.total_progressbar.configure(value=0)
            self.total_progress_var.set("总进度：等待开始")
            return
        ratio = min(100.0, max(0.0, self._completed_links * 100.0 / self._total_links))
        self.total_progressbar.configure(value=ratio)
        self.total_progress_var.set(
            f"总进度：{self._completed_links}/{self._total_links}（{ratio:.1f}%）"
        )

    def _request_stop(self) -> None:
        if self._worker and self._worker.is_alive() and not self._cancel_event.is_set():
            self._cancel_event.set()
            self._append_log("已请求停止任务，正在等待当前下载安全中断...")
            self.quick_stop_button.configure(state=tk.DISABLED)
            self.stop_button.configure(state=tk.DISABLED)

    def _open_live_preview(self) -> None:
        if self._preview_resolving:
            messagebox.showinfo("请稍候", "正在解析预览流地址，请稍后重试。")
            return
        target_url = self._resolve_live_preview_target_url()
        if not target_url:
            messagebox.showwarning("无法预览", "请先输入链接，或先开始下载后再预览当前任务。")
            return
        if not self._ensure_preview_window():
            return
        self._preview_resolving = True
        self._append_log(f"正在解析内嵌预览流：{target_url}")
        threading.Thread(target=self._resolve_and_play_preview, args=(target_url,), daemon=True).start()

    def _resolve_live_preview_target_url(self) -> str:
        target_url = self._current_download_url.strip()
        if target_url:
            return target_url
        source_urls = self._effective_urls_for_run()
        if not source_urls:
            return ""
        if len(source_urls) > 1:
            self._append_log("检测到多条链接，内嵌预览默认打开第 1 条。")
        return source_urls[0]

    def _ensure_preview_window(self) -> bool:
        if vlc is None:
            messagebox.showwarning(
                "内嵌预览不可用",
                "未检测到 python-vlc。\n请执行：pip install python-vlc\n并确保系统已安装 VLC 播放器。",
            )
            return False
        if self._preview_window is None or not self._preview_window.exists():
            self._preview_window = EmbeddedPreviewWindow(self.root, self._append_log)
        self._preview_window.show()
        if not self._preview_window.ready:
            messagebox.showwarning(
                "内嵌预览不可用",
                "VLC 初始化失败，请确认已安装 VLC 播放器，并与当前系统位数匹配。",
            )
            return False
        return True

    def _resolve_and_play_preview(self, target_url: str) -> None:
        cookies_file = self.cookies_var.get().strip() or None
        cookies_from_browser = self._get_browser_cookie_value()
        proxy = self.proxy_var.get().strip() or None
        try:
            retries = max(0, int((self.retries_var.get() or "1").strip()))
        except ValueError:
            retries = 1
        result = resolve_preview_stream(
            target_url,
            cookiefile=cookies_file,
            cookies_from_browser=cookies_from_browser,
            proxy=proxy,
            retries=retries,
        )
        self._queue.put(("preview_ready", result))

    def _check_updates_async(self, silent: bool = False) -> None:
        if not silent:
            self._append_log("正在检查 yt-dlp 更新...")
        app_repo = self.app_update_repo_var.get().strip()

        def worker() -> None:
            yt_info = check_yt_dlp_update()
            yt_message = str(yt_info.get("message", "更新检查完成。"))

            app_info = check_app_update(APP_VERSION, app_repo) if app_repo else {
                "ok": False,
                "has_update": False,
                "message": "未配置应用更新仓库（owner/repo），已跳过。",
            }
            app_message = str(app_info.get("message", "应用更新检查完成。"))

            if silent:
                if bool(yt_info.get("has_update")):
                    self._queue.put(("log", yt_message))
                if bool(app_info.get("has_update")):
                    self._queue.put(("log", app_message))
                return

            self._queue.put(("log", yt_message))
            self._queue.put(("log", app_message))

        threading.Thread(target=worker, daemon=True).start()

    def _check_runtime_health_async(self, silent: bool = False) -> None:
        if not silent:
            self._append_log("[环境] 正在检查运行依赖...")

        def worker() -> None:
            health = collect_runtime_health()
            self._queue.put(("runtime_health", {"health": health, "silent": silent}))

        threading.Thread(target=worker, daemon=True).start()

    def _check_and_repair_runtime(self) -> None:
        self.quick_health_button.configure(state=tk.DISABLED)
        self.health_button.configure(state=tk.DISABLED)
        self._append_log("[环境] 检查中，准备执行自动修复...")

        def worker() -> None:
            health = collect_runtime_health()
            self._queue.put(("runtime_health", {"health": health, "silent": False}))

            if not bool(health.get("playwright_module_available", False)):
                self._queue.put(
                    (
                        "runtime_repair_done",
                        {
                            "ok": False,
                            "skipped": True,
                            "message": "Playwright module is missing. Install requirements first.",
                        },
                    )
                )
                return

            if bool(health.get("ready_for_web_sniff", False)):
                self._queue.put(
                    (
                        "runtime_repair_done",
                        {
                            "ok": True,
                            "skipped": True,
                            "message": "Runtime is already ready. No repair needed.",
                        },
                    )
                )
                return

            ok, message = run_playwright_install()
            self._queue.put(("runtime_repair_done", {"ok": ok, "skipped": False, "message": message}))

        threading.Thread(target=worker, daemon=True).start()

    def _handle_runtime_health(self, payload: dict[str, Any]) -> None:
        health = payload.get("health")
        if not isinstance(health, dict):
            return
        silent = bool(payload.get("silent"))
        issues = [str(item).strip() for item in health.get("issues", []) if str(item).strip()]
        if not issues:
            if not silent:
                self._append_log("[环境] 依赖检查通过。")
            return

        self._append_log("[环境] 发现依赖问题：")
        for issue in issues:
            self._append_log(f"  - {issue}")

        if not bool(health.get("playwright_module_available", False)):
            self._append_log("[环境] 请先执行：python -m pip install -r requirements.txt")
        elif not bool(health.get("ready_for_web_sniff", False)):
            self._append_log("[环境] 可点击“环境自检/修复”自动执行：playwright install chromium")

    def _handle_runtime_repair_result(self, payload: dict[str, Any]) -> None:
        running = bool(self._worker and self._worker.is_alive())
        state = tk.DISABLED if running else tk.NORMAL
        self.quick_health_button.configure(state=state)
        self.health_button.configure(state=state)

        message = str(payload.get("message", "")).strip()
        ok = bool(payload.get("ok"))
        skipped = bool(payload.get("skipped"))
        if message:
            self._append_log(f"[环境] {message}")
        if skipped:
            return
        if ok:
            messagebox.showinfo("依赖修复", "自动修复完成，请重试网页下载。")
        else:
            messagebox.showwarning("依赖修复失败", "自动修复未成功，请根据日志手动执行命令。")

    def _import_tasks(self) -> None:
        selected = filedialog.askopenfilename(
            title="导入任务",
            filetypes=[
                ("Supported", "*.txt;*.csv;*.json"),
                ("Text", "*.txt"),
                ("CSV", "*.csv"),
                ("JSON", "*.json"),
                ("All Files", "*.*"),
            ],
        )
        if not selected:
            return
        path = Path(selected)
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:
            messagebox.showerror("导入失败", f"读取文件失败：{exc}")
            return

        urls: list[str] = []
        suffix = path.suffix.lower()
        if suffix == ".csv":
            reader = csv.reader(content.splitlines())
            for row in reader:
                if not row:
                    continue
                urls.extend(extract_urls_from_inputs(row))
        elif suffix == ".json":
            try:
                obj = json.loads(content)
            except ValueError as exc:
                messagebox.showerror("导入失败", f"JSON 解析失败：{exc}")
                return
            if isinstance(obj, list):
                urls = extract_urls_from_inputs([str(x) for x in obj])
            elif isinstance(obj, dict):
                raw_urls = obj.get("urls", [])
                if isinstance(raw_urls, list):
                    urls = extract_urls_from_inputs([str(x) for x in raw_urls])
        else:
            urls = extract_urls_from_inputs(content.splitlines())

        if not urls:
            messagebox.showwarning("导入结果", "未从文件中解析到可用链接。")
            return

        self.urls_text.delete("1.0", tk.END)
        self.urls_text.insert(tk.END, "\n".join(urls))
        self._selected_urls_override = None
        self._task_queue_urls = list(urls)
        self._refresh_queue_listbox()
        self._save_settings()
        self._append_log(f"任务导入完成：{len(urls)} 条链接。")

    def _export_tasks(self) -> None:
        urls = self._parse_urls()
        if not urls:
            messagebox.showwarning("无可导出内容", "当前没有可导出的链接。")
            return
        selected = filedialog.asksaveasfilename(
            title="导出任务",
            defaultextension=".txt",
            filetypes=[
                ("Text", "*.txt"),
                ("CSV", "*.csv"),
                ("JSON", "*.json"),
            ],
        )
        if not selected:
            return
        path = Path(selected)
        suffix = path.suffix.lower()
        try:
            if suffix == ".csv":
                with path.open("w", encoding="utf-8", newline="") as fp:
                    writer = csv.writer(fp)
                    for url in urls:
                        writer.writerow([url])
            elif suffix == ".json":
                path.write_text(json.dumps({"urls": urls}, ensure_ascii=False, indent=2), encoding="utf-8")
            else:
                path.write_text("\n".join(urls) + "\n", encoding="utf-8")
        except OSError as exc:
            messagebox.showerror("导出失败", f"写入文件失败：{exc}")
            return
        self._append_log(f"任务已导出：{path}")

    def _preview_and_select(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("任务进行中", "请等待当前任务结束后再预览。")
            return

        urls = self._effective_urls_for_run()
        if not urls:
            messagebox.showwarning("未填写链接", "请先输入链接再进行预览。")
            return

        cookies_file = self.cookies_var.get().strip() or None
        cookies_from_browser = self._get_browser_cookie_value()
        proxy = self.proxy_var.get().strip() or None
        try:
            retries = max(0, int((self.retries_var.get() or "1").strip()))
        except ValueError:
            retries = 1

        self._append_log("正在预览链接并拉取元数据，请稍候...")
        preview_items = build_preview_items(
            urls,
            cookiefile=cookies_file,
            cookies_from_browser=cookies_from_browser,
            platform=PLATFORM_AUTO,
            proxy=proxy,
            retries=retries,
            log=lambda message: self._queue.put(("log", message)),
        )
        if not preview_items:
            messagebox.showwarning("预览失败", "未获取到可预览条目。")
            return

        selected = self._show_preview_dialog(preview_items)
        if selected is None:
            self._append_log("已取消预览选择。")
            return
        self._selected_urls_override = selected
        self._append_log(f"预览完成：共 {len(preview_items)} 条，已选择 {len(selected)} 条用于下载。")

    def _show_preview_dialog(self, items: list[dict[str, Any]]) -> list[str] | None:
        dialog = tk.Toplevel(self.root)
        dialog.title("下载前预览与选择")
        dialog.geometry("920x520")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="提示：默认全选，可取消不想下载的条目。").pack(anchor=tk.W, padx=10, pady=(10, 6))
        keyword_frame = ttk.Frame(dialog)
        keyword_frame.pack(fill=tk.X, padx=10, pady=(0, 6))
        ttk.Label(keyword_frame, text="关键词:").pack(side=tk.LEFT)
        keyword_var = tk.StringVar(value="")
        keyword_entry = ttk.Entry(keyword_frame, textvariable=keyword_var, width=24)
        keyword_entry.pack(side=tk.LEFT, padx=(6, 8))
        keyword_entry.focus_set()

        listbox = tk.Listbox(dialog, selectmode=tk.EXTENDED)
        listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        for idx, item in enumerate(items, start=1):
            text = (
                f"{idx:03d}. [{item.get('platform_name', '-')}] "
                f"{item.get('title', '-')}"
                f" | UP:{item.get('uploader', '-')}"
                f" | 时长:{item.get('duration', '-')}"
            )
            listbox.insert(tk.END, text)
            listbox.selection_set(idx - 1)

        result: dict[str, list[str] | None] = {"urls": None}

        def select_all() -> None:
            listbox.selection_set(0, tk.END)

        def invert_select() -> None:
            selected_set = set(listbox.curselection())
            listbox.selection_clear(0, tk.END)
            for i in range(listbox.size()):
                if i not in selected_set:
                    listbox.selection_set(i)

        def _match_keyword(item: dict[str, Any], keyword: str) -> bool:
            if not keyword:
                return False
            merged = " ".join(
                [
                    str(item.get("title", "")),
                    str(item.get("uploader", "")),
                    str(item.get("platform_name", "")),
                ]
            ).lower()
            return keyword.lower() in merged

        def select_by_keyword() -> None:
            keyword = keyword_var.get().strip()
            if not keyword:
                return
            for i, item in enumerate(items):
                if _match_keyword(item, keyword):
                    listbox.selection_set(i)

        def unselect_by_keyword() -> None:
            keyword = keyword_var.get().strip()
            if not keyword:
                return
            for i, item in enumerate(items):
                if _match_keyword(item, keyword):
                    listbox.selection_clear(i)

        ttk.Button(keyword_frame, text="关键词全选", command=select_by_keyword).pack(side=tk.LEFT, padx=(4, 0))
        ttk.Button(keyword_frame, text="关键词取消", command=unselect_by_keyword).pack(side=tk.LEFT, padx=(4, 0))

        def confirm() -> None:
            indexes = listbox.curselection()
            chosen = [str(items[i].get("url", "")).strip() for i in indexes]
            chosen = [u for u in chosen if u]
            result["urls"] = chosen
            dialog.destroy()

        def cancel() -> None:
            result["urls"] = None
            dialog.destroy()

        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Button(button_frame, text="全选", command=select_all).pack(side=tk.LEFT)
        ttk.Button(button_frame, text="反选", command=invert_select).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(button_frame, text="确定", command=confirm).pack(side=tk.RIGHT)
        ttk.Button(button_frame, text="取消", command=cancel).pack(side=tk.RIGHT, padx=(0, 8))

        dialog.wait_window()
        return result["urls"]

    @staticmethod
    def _parse_progress_percent(payload: dict[str, Any]) -> float | None:
        percent_text = str(payload.get("_percent_str", "")).strip().replace("%", "")
        if percent_text:
            try:
                return float(percent_text)
            except ValueError:
                pass
        downloaded = payload.get("downloaded_bytes")
        total = payload.get("total_bytes") or payload.get("total_bytes_estimate")
        if isinstance(downloaded, (int, float)) and isinstance(total, (int, float)) and total > 0:
            return float(downloaded) * 100.0 / float(total)
        return None

    @staticmethod
    def _text_or_dash(payload: dict[str, Any], key: str) -> str:
        value = payload.get(key)
        text = str(value).strip() if value is not None else ""
        return text if text else "-"

    def _handle_progress(self, payload: dict[str, Any]) -> None:
        status = str(payload.get("status", "")).lower()
        if status == "downloading":
            percent = self._parse_progress_percent(payload)
            speed = self._text_or_dash(payload, "_speed_str")
            eta = self._text_or_dash(payload, "_eta_str")
            if percent is not None:
                self.current_progressbar.configure(value=max(0.0, min(100.0, percent)))
                self.current_progress_var.set(f"当前文件：{percent:.1f}% | 速度：{speed} | ETA：{eta}")
            else:
                self.current_progress_var.set(f"当前文件：下载中 | 速度：{speed} | ETA：{eta}")
        elif status == "finished":
            self.current_progressbar.configure(value=100)
            self.current_progress_var.set("当前文件：已下载完成，正在处理后续步骤...")
        elif status == "error":
            self.current_progress_var.set("当前文件：下载出错，请查看日志")

    def _handle_state(self, payload: dict[str, Any]) -> None:
        event = str(payload.get("event", "")).lower()
        if event == "plan":
            total_links = int(payload.get("total_links", 0))
            self._total_links = max(0, total_links)
            self._completed_links = 0
            self._failed_urls_in_last_run = []
            self._last_failure_reason = ""
            self._update_total_progress()
            return
        if event == "link_start":
            total_links = int(payload.get("total_links", self._total_links or 0))
            if total_links > 0:
                self._total_links = total_links
            index = int(payload.get("index", self._completed_links + 1))
            self._current_download_url = str(payload.get("url", "")).strip()
            self._update_total_progress()
            self.total_progress_var.set(
                f"{self.total_progress_var.get()}，正在处理第 {index}/{max(1, self._total_links)} 条"
            )
            return
        if event == "link_done":
            index = int(payload.get("index", self._completed_links + 1))
            self._completed_links = max(self._completed_links, index)
            self._update_total_progress()
            return
        if event == "link_retry":
            attempt = int(payload.get("attempt", 0))
            max_attempts = int(payload.get("max_attempts", 0))
            reason = str(payload.get("reason", "")).strip()
            wait_seconds = float(payload.get("wait_seconds", 0.0))
            self.current_progress_var.set(
                f"当前文件：准备重试 {attempt + 1}/{max_attempts} | 等待 {wait_seconds:.1f}s"
            )
            if reason:
                self._append_log(f"[重试] {reason}")
            return
        if event == "resume_detected":
            msg = str(payload.get("message", "")).strip()
            self._append_log(f"[续传] {msg or '检测到断点续传。'}")
            self.current_progress_var.set("当前文件：检测到断点续传，继续下载中...")
            return
        if event == "link_failed":
            failed_url = str(payload.get("url", "")).strip()
            if failed_url:
                self._failed_urls_in_last_run.append(failed_url)
            reason = str(payload.get("reason", "")).strip()
            error_code = str(payload.get("error_code", "")).strip()
            if reason:
                self._last_failure_reason = format_failure_reason_for_display(error_code=error_code, reason=reason)
                self._append_log(f"[失败] {failed_url or '-'} -> {reason}")
            return
        if event == "run_failed":
            reason = str(payload.get("reason", "")).strip()
            error_code = str(payload.get("error_code", "")).strip()
            if reason:
                self._last_failure_reason = format_failure_reason_for_display(error_code=error_code, reason=reason)
                self._append_log(f"[失败] {self._last_failure_reason}")

    def _handle_preview_ready(self, payload: dict[str, Any]) -> None:
        ok = bool(payload.get("ok"))
        message = str(payload.get("message", "")).strip()
        if not ok:
            if message:
                self._append_log(f"[预览] 解析失败：{message}")
            messagebox.showwarning("预览失败", message or "未找到可播放流地址。")
            return
        if not self._ensure_preview_window():
            return
        if self._preview_window is None:
            return
        stream_url = str(payload.get("stream_url", "")).strip()
        title = str(payload.get("title", "实时预览")).strip()
        headers = payload.get("http_headers")
        use_headers = headers if isinstance(headers, dict) else {}
        started, play_msg = self._preview_window.play_stream(stream_url, title, use_headers)
        if started:
            self._append_log(f"[预览] 已开始内嵌播放：{title}")
        else:
            self._append_log(f"[预览] 播放失败：{play_msg}")
            messagebox.showwarning("播放失败", play_msg)

    def _start_download(self) -> None:
        if self._worker and self._worker.is_alive():
            messagebox.showinfo("任务进行中", "已有下载任务在运行，请稍候。")
            return

        urls = self._effective_urls_for_run()
        if not urls:
            messagebox.showwarning("未填写链接", "请至少粘贴一个有效链接或分享文案。")
            return

        grouped = split_urls_by_platform(urls)
        bili_count = len(grouped[PLATFORM_BILIBILI])
        douyin_count = len(grouped[PLATFORM_DOUYIN])
        unknown_count = len(grouped["unknown"])
        if bili_count + douyin_count + unknown_count == 0:
            messagebox.showwarning(
                "无法识别",
                "未识别到 B站/抖音 有效链接，请检查输入内容。",
            )
            return

        display_platform = PLATFORM_AUTO
        if bili_count > 0 and douyin_count == 0:
            display_platform = PLATFORM_BILIBILI
        elif douyin_count > 0 and bili_count == 0:
            display_platform = PLATFORM_DOUYIN
        elif unknown_count > 0 and bili_count == 0 and douyin_count == 0:
            display_platform = PLATFORM_WEB
        self.platform_var.set(display_platform)
        self._refresh_platform_ui()

        output_dir = Path(self.output_var.get().strip() or "downloads")
        cookies_file = self.cookies_var.get().strip() or None
        if cookies_file and not Path(cookies_file).exists():
            messagebox.showwarning("cookies.txt 无效", "你填写的 cookies.txt 路径不存在。")
            return

        cookies_from_browser = self._get_browser_cookie_value()
        resolution = self._get_resolution_value()
        extract_audio = self.extract_audio_var.get()
        audio_format = (self.audio_format_var.get() or AUDIO_FORMAT_OPTIONS[0]).strip().lower()
        write_subtitles = self.write_subtitles_var.get()
        write_thumbnail = self.write_thumbnail_var.get()
        write_info_json = self.write_info_json_var.get()
        proxy = self.proxy_var.get().strip() or None
        rate_limit = self.rate_limit_var.get().strip() or None
        try:
            retries = max(0, int((self.retries_var.get() or "1").strip()))
        except ValueError:
            retries = 1
            self.retries_var.set("1")
        try:
            adaptive_retry_attempts = max(1, int((self.adaptive_retry_attempts_var.get() or "2").strip()))
        except ValueError:
            adaptive_retry_attempts = DEFAULT_ADAPTIVE_RETRY_ATTEMPTS
            self.adaptive_retry_attempts_var.set(str(DEFAULT_ADAPTIVE_RETRY_ATTEMPTS))
        filename_template = normalize_filename_template(self.filename_template_var.get().strip())
        self.filename_template_var.set(filename_template)
        post_action = self.post_action_var.get().strip() or POST_ACTION_NONE
        skip_history_success = self.skip_history_success_var.get()
        history_file = str(self._history_path)

        self._append_log("-" * 72)
        self._append_log(f"程序版本：{APP_VERSION}")
        self._append_log(f"识别到链接数量：{len(urls)}")
        self._append_log(
            f"链接分布：B站 {bili_count} 条，抖音 {douyin_count} 条，未识别 {unknown_count} 条"
        )
        self._append_log("开始下载任务，平台模式：自动识别（支持混合分组下载）")
        if self._selected_urls_override is not None:
            self._append_log(f"已启用预览选择：{len(self._selected_urls_override)} 条。")
        if proxy:
            self._append_log(f"代理：{proxy}")
        if rate_limit:
            self._append_log(f"限速：{rate_limit}")
        self._append_log(f"重试次数：{retries}")
        self._append_log(f"自适应重试轮数：{adaptive_retry_attempts}")
        self._append_log(f"文件名模板：{filename_template}")
        self._append_log(f"下载后动作：{post_action}")
        if self._task_queue_urls:
            self._append_log(f"当前使用任务队列顺序：{len(self._task_queue_urls)} 条")

        self._cancel_event.clear()
        self._failed_urls_in_last_run = []
        self._last_failure_reason = ""
        self._reset_progress()
        self._save_settings()
        self._set_running_state(True)

        self._worker = threading.Thread(
            target=self._run_worker,
            args=(
                urls,
                output_dir,
                cookies_file,
                cookies_from_browser,
                resolution,
                extract_audio,
                audio_format,
                write_subtitles,
                write_thumbnail,
                write_info_json,
                proxy,
                rate_limit,
                retries,
                adaptive_retry_attempts,
                filename_template,
                post_action,
                history_file,
                skip_history_success,
            ),
            daemon=True,
        )
        self._worker.start()

    def _run_worker(
        self,
        urls: list[str],
        output_dir: Path,
        cookies_file: str | None,
        cookies_from_browser: str,
        resolution: str,
        extract_audio: bool,
        audio_format: str,
        write_subtitles: bool,
        write_thumbnail: bool,
        write_info_json: bool,
        proxy: str | None,
        rate_limit: str | None,
        retries: int,
        adaptive_retry_attempts: int,
        filename_template: str,
        post_action: str,
        history_file: str,
        skip_history_success: bool,
    ) -> None:
        def logger(message: str) -> None:
            self._queue.put(("log", message))

        def progress(data: dict[str, Any]) -> None:
            self._queue.put(("progress", data))

        def state(data: dict[str, Any]) -> None:
            self._queue.put(("state", data))

        try:
            result = run_download(
                urls,
                output_dir,
                cookiefile=cookies_file,
                cookies_from_browser=cookies_from_browser,
                log=logger,
                progress_hook=progress,
                state_hook=state,
                cancel_check=self._cancel_event.is_set,
                probe_workers=6,
                platform=PLATFORM_AUTO,
                strict_platform=True,
                resolution=resolution,
                extract_audio=extract_audio,
                audio_format=audio_format,
                write_subtitles=write_subtitles,
                write_thumbnail=write_thumbnail,
                write_info_json=write_info_json,
                proxy=proxy,
                rate_limit=rate_limit,
                retries=retries,
                adaptive_retry_attempts=adaptive_retry_attempts,
                filename_template=filename_template,
                post_action=post_action,
                history_file=history_file,
                skip_history_success=skip_history_success,
            )
        except (RuntimeError, ValueError, OSError) as exc:
            logger(f"后台任务异常：{exc}")
            result = 2
        self._queue.put(("done", result))

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, payload = self._queue.get_nowait()
                if kind == "log":
                    self._append_log(str(payload))
                elif kind == "progress" and isinstance(payload, dict):
                    self._handle_progress(payload)
                elif kind == "state" and isinstance(payload, dict):
                    self._handle_state(payload)
                elif kind == "preview_ready" and isinstance(payload, dict):
                    self._preview_resolving = False
                    self._handle_preview_ready(payload)
                elif kind == "runtime_health" and isinstance(payload, dict):
                    self._handle_runtime_health(payload)
                elif kind == "runtime_repair_done" and isinstance(payload, dict):
                    self._handle_runtime_repair_result(payload)
                elif kind == "done":
                    code = int(payload)
                    if self.beep_on_done_var.get():
                        try:
                            winsound.MessageBeep(winsound.MB_ICONASTERISK)
                        except RuntimeError:
                            pass
                    if code == 0:
                        self._append_log("任务执行成功。")
                        if self.open_output_on_done_var.get():
                            self._open_output_folder()
                        if self.notify_done_var.get():
                            messagebox.showinfo("完成", "下载完成。")
                    elif code == 130:
                        self._append_log("任务已取消。")
                        if self.notify_done_var.get():
                            messagebox.showinfo("已停止", "下载任务已取消。")
                    else:
                        self._append_log(f"任务结束，状态码：{code}")
                        if self.notify_done_var.get():
                            reason = self._last_failure_reason.strip()
                            if reason:
                                messagebox.showwarning("任务结束", f"任务结束，状态码：{code}\n{reason}")
                            else:
                                messagebox.showwarning("任务结束", f"任务结束，状态码：{code}")
                    if self.keep_failed_in_queue_var.get() and self._failed_urls_in_last_run:
                        self._task_queue_urls = extract_urls_from_inputs(self._failed_urls_in_last_run)
                        self._selected_urls_override = None
                        self._refresh_queue_listbox()
                        self.urls_text.delete("1.0", tk.END)
                        self.urls_text.insert(tk.END, "\n".join(self._task_queue_urls))
                        self._append_log(f"已保留失败项到任务队列：{len(self._task_queue_urls)} 条")
                    self._cancel_event.clear()
                    self._current_download_url = ""
                    self._set_running_state(False)
                    self._save_settings()
        except queue.Empty:
            pass
        self.root.after(100, self._drain_queue)


def main() -> None:
    root = tk.Tk()
    DownloaderGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
