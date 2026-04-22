from __future__ import annotations

import re

_GEOMETRY_RE = re.compile(r"^(\d{3,5})x(\d{3,5})([+-]\d+)?([+-]\d+)?$")
_DEFAULT_GEOMETRY = "980x780"
_MIN_WIDTH = 860
_MIN_HEIGHT = 680


def sanitize_window_geometry(raw: str | None, fallback: str = _DEFAULT_GEOMETRY) -> str:
    text = str(raw or "").strip()
    matched = _GEOMETRY_RE.match(text)
    if not matched:
        return fallback

    width = int(matched.group(1))
    height = int(matched.group(2))
    if width < _MIN_WIDTH or height < _MIN_HEIGHT:
        return fallback

    x_token = matched.group(3)
    y_token = matched.group(4)
    if x_token is not None and int(x_token) < 0:
        return fallback
    if y_token is not None and int(y_token) < 0:
        return fallback
    return text

