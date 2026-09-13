"""JSON-RPC 2.0 protocol specifications and serialization models for Sidecar."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any


class AuthRequiredError(Exception):
    """Raised when the platform rejects the request due to missing/expired login."""

    def __init__(self, platform: str, message: str):
        super().__init__(message)
        self.platform = platform
        self.message = message


@dataclass
class StreamInfo:
    """Represents a downloadable stream (video, audio, or muxed)."""

    format_id: str
    protocol: str  # "http", "m3u8", "dash"
    video_url: str | None = None
    audio_url: str | None = None
    resolution: str | None = None  # e.g., "1080p", "720p", "audio_only"
    ext: str = "mp4"  # "mp4", "m4a", "ts", etc.
    file_size_approx: int = 0
    headers: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SubItem:
    """Represents an item within a batch (e.g. video in user profile or multi-P part)."""

    item_id: str
    title: str
    url: str
    duration: int = 0
    cover_url: str = ""
    author: str = ""
    is_image_post: bool = False
    image_urls: list[str] = field(default_factory=list)
    create_time: int = 0
    like_count: int = 0
    comment_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MediaMetadata:
    """Unified metadata returned after URL resolution."""

    platform: str
    content_type: str  # "single_video", "image_post", "batch_playlist", "live_stream"
    title: str
    author: str
    url: str
    author_id: str = ""
    duration: int = 0
    cover_url: str = ""
    streams: list[StreamInfo] = field(default_factory=list)
    sub_items: list[SubItem] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "content_type": self.content_type,
            "title": self.title,
            "author": self.author,
            "url": self.url,
            "author_id": self.author_id,
            "duration": self.duration,
            "cover_url": self.cover_url,
            "streams": [s.to_dict() for s in self.streams],
            "sub_items": [i.to_dict() for i in self.sub_items],
            "extra": self.extra,
        }


def make_jsonrpc_response(req_id: Any, result: Any) -> str:
    """Encode success JSON-RPC 2.0 response."""
    return json.dumps({
        "jsonrpc": "2.0",
        "id": req_id,
        "result": result,
    }, ensure_ascii=False)


def make_jsonrpc_error(req_id: Any, code: int, message: str, data: Any = None) -> str:
    """Encode error JSON-RPC 2.0 response."""
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return json.dumps({
        "jsonrpc": "2.0",
        "id": req_id,
        "error": err,
    }, ensure_ascii=False)
