from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

_ENV_CONFIGURED = False
DEFAULT_APP_STATE_DIRNAME = "BillBillDownloader_CN"


def _runtime_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _prepend_paths(paths: list[Path]) -> None:
    if not paths:
        return
    existing = [item for item in os.environ.get("PATH", "").split(os.pathsep) if item]
    normalized_existing = {item.lower() for item in existing}
    prepended: list[str] = []
    for path in paths:
        resolved = str(path.resolve())
        if resolved.lower() in normalized_existing:
            continue
        prepended.append(resolved)
        normalized_existing.add(resolved.lower())
    if prepended:
        os.environ["PATH"] = os.pathsep.join(prepended + existing)


def _find_existing_paths(candidates: list[Path]) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        try:
            resolved = path.resolve()
        except OSError:
            continue
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)
        if resolved.exists():
            result.append(resolved)
    return result


def configure_runtime_environment() -> None:
    global _ENV_CONFIGURED
    if _ENV_CONFIGURED:
        return

    base_dir = _runtime_base_dir()
    ffmpeg_bin_candidates = [
        base_dir / "third_party" / "ffmpeg" / "bin",
        base_dir / "ffmpeg" / "bin",
    ]
    vlc_dir_candidates = [
        base_dir / "third_party" / "vlc",
        base_dir / "vlc",
    ]

    program_files = os.environ.get("ProgramFiles")
    if program_files:
        vlc_dir_candidates.append(Path(program_files) / "VideoLAN" / "VLC")
    program_files_x86 = os.environ.get("ProgramFiles(x86)")
    if program_files_x86:
        vlc_dir_candidates.append(Path(program_files_x86) / "VideoLAN" / "VLC")

    ffmpeg_dirs = _find_existing_paths(ffmpeg_bin_candidates)
    vlc_dirs = _find_existing_paths(vlc_dir_candidates)
    _prepend_paths(ffmpeg_dirs + vlc_dirs)

    if vlc_dirs:
        vlc_dir = vlc_dirs[0]
        plugins_dir = vlc_dir / "plugins"
        if plugins_dir.exists():
            os.environ.setdefault("VLC_PLUGIN_PATH", str(plugins_dir))

        add_dll_dir = getattr(os, "add_dll_directory", None)
        if callable(add_dll_dir):
            for dll_dir in vlc_dirs:
                try:
                    add_dll_dir(str(dll_dir))
                except OSError:
                    continue

    _ENV_CONFIGURED = True


def resolve_state_paths(
    app_name: str = DEFAULT_APP_STATE_DIRNAME,
    ensure_dir: bool = True,
) -> dict[str, Path]:
    appdata = (os.environ.get("APPDATA") or "").strip()
    base_dir = Path(appdata) if appdata else _runtime_base_dir()
    state_dir = base_dir / app_name
    if ensure_dir:
        try:
            state_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            # Fall back to runtime dir if APPDATA is unavailable.
            state_dir = _runtime_base_dir() / app_name
            state_dir.mkdir(parents=True, exist_ok=True)
    return {
        "state_dir": state_dir,
        "settings_file": state_dir / "settings.json",
        "history_file": state_dir / "history.json",
    }


def _is_playwright_importable() -> bool:
    try:
        import playwright  # noqa: F401
    except Exception:
        return False
    return True


def _candidate_playwright_asset_dirs() -> list[Path]:
    candidates: list[Path] = []
    env_path = (os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "").strip()
    if env_path and env_path != "0":
        candidates.append(Path(env_path))
    candidates.append(Path.home() / "AppData" / "Local" / "ms-playwright")
    candidates.append(Path.home() / ".cache" / "ms-playwright")
    return candidates


def _has_playwright_browser_assets() -> bool:
    for root in _candidate_playwright_asset_dirs():
        try:
            if not root.exists():
                continue
            if any(root.glob("chromium-*")) or any(root.glob("chromium_headless_shell-*")):
                return True
        except OSError:
            continue
    return False


def _has_browser_channel(name: str) -> bool:
    for candidate in (name, f"{name}.exe"):
        if shutil.which(candidate):
            return True
    return False


def collect_runtime_health() -> dict[str, object]:
    ffmpeg_available = bool(shutil.which("ffmpeg"))
    playwright_module_available = _is_playwright_importable()
    playwright_browser_assets_available = _has_playwright_browser_assets()
    edge_available = _has_browser_channel("msedge")
    chrome_available = _has_browser_channel("chrome")
    browser_channel_available = edge_available or chrome_available

    ready_for_web_sniff = bool(
        playwright_module_available and (playwright_browser_assets_available or browser_channel_available)
    )

    issues: list[str] = []
    if not ffmpeg_available:
        issues.append("ffmpeg was not found in PATH.")
    if not playwright_module_available:
        issues.append("Playwright module is not installed.")
    elif not ready_for_web_sniff:
        issues.append("Playwright browser runtime is missing and no Edge/Chrome channel was found.")

    return {
        "ffmpeg_available": ffmpeg_available,
        "playwright_module_available": playwright_module_available,
        "playwright_browser_assets_available": playwright_browser_assets_available,
        "edge_available": edge_available,
        "chrome_available": chrome_available,
        "browser_channel_available": browser_channel_available,
        "ready_for_web_sniff": ready_for_web_sniff,
        "issues": issues,
        "repair_recommended": bool(not ready_for_web_sniff or not ffmpeg_available),
    }


def build_playwright_install_commands() -> list[list[str]]:
    commands: list[list[str]] = [[sys.executable, "-m", "playwright", "install", "chromium"]]
    alt_command = ["playwright", "install", "chromium"]
    if alt_command != commands[0]:
        commands.append(alt_command)
    return commands


def run_playwright_install(timeout_seconds: int = 600) -> tuple[bool, str]:
    errors: list[str] = []
    for command in build_playwright_install_commands():
        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except FileNotFoundError:
            errors.append(f"{' '.join(command)} -> command not found")
            continue
        except subprocess.TimeoutExpired:
            errors.append(f"{' '.join(command)} -> timed out")
            continue
        except OSError as exc:
            errors.append(f"{' '.join(command)} -> {exc}")
            continue

        combined = "\n".join(
            [text.strip() for text in (completed.stdout or "", completed.stderr or "") if text and text.strip()]
        ).strip()
        if completed.returncode == 0:
            message = "Playwright browser install succeeded."
            if combined:
                message = f"{message}\n{combined}"
            return True, message
        errors.append(f"{' '.join(command)} -> exit={completed.returncode}\n{combined}".strip())

    return False, "\n".join(errors) if errors else "Playwright browser install failed."
