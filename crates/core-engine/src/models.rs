use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub format_id: String,
    pub protocol: String, // "http", "m3u8", "dash"
    #[serde(default)]
    pub video_url: Option<String>,
    #[serde(default)]
    pub audio_url: Option<String>,
    #[serde(default)]
    pub resolution: Option<String>,
    #[serde(default = "default_ext")]
    pub ext: String,
    #[serde(default)]
    pub file_size_approx: u64,
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_ext() -> String {
    "mp4".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubItem {
    pub item_id: String,
    pub title: String,
    pub url: String,
    #[serde(default)]
    pub duration: u64,
    #[serde(default)]
    pub cover_url: String,
    #[serde(default)]
    pub author: String,
    #[serde(default)]
    pub is_image_post: bool,
    #[serde(default)]
    pub image_urls: Vec<String>,
    #[serde(default)]
    pub create_time: u64,
    #[serde(default)]
    pub like_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaMetadata {
    pub platform: String,
    pub content_type: String, // "single_video", "image_post", "batch_playlist", "live_stream"
    pub title: String,
    pub author: String,
    pub url: String,
    #[serde(default)]
    pub author_id: String,
    #[serde(default)]
    pub duration: u64,
    #[serde(default)]
    pub cover_url: String,
    #[serde(default)]
    pub streams: Vec<StreamInfo>,
    #[serde(default)]
    pub sub_items: Vec<SubItem>,
    #[serde(default)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Resolving,
    Downloading,
    Merging,
    Completed,
    Failed,
    Paused,
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "pending"),
            TaskStatus::Resolving => write!(f, "resolving"),
            TaskStatus::Downloading => write!(f, "downloading"),
            TaskStatus::Merging => write!(f, "merging"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Failed => write!(f, "failed"),
            TaskStatus::Paused => write!(f, "paused"),
            TaskStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskProgress {
    pub task_id: String,
    pub status: TaskStatus,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub speed_bps: u64,
    pub percentage: f32,
    pub eta_seconds: u64,
    pub error_msg: Option<String>,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub platform: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub output_path: String,
    #[serde(default)]
    pub can_retry: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub id: Option<i64>,
    pub url: String,
    pub status: String,
    pub reason: String,
    pub title: String,
    pub platform: String,
    pub author: String,
    pub duration: i64,
    pub file_size: i64,
    pub file_format: String,
    pub output_path: String,
    pub timestamp: String,
}

/// User-facing app settings, persisted in SQLite `settings` table as JSON values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettings {
    /// Default output directory (relative to project root or absolute under root).
    #[serde(default)]
    pub output_dir: String,
    /// Download rate limit in KB/s. 0 = unlimited.
    #[serde(default)]
    pub rate_limit_kbps: u64,
    /// Max concurrent download tasks.
    #[serde(default = "default_concurrent")]
    pub max_concurrent: usize,
    /// Default HTTP/SOCKS proxy for resolve+download.
    #[serde(default)]
    pub proxy: String,
    /// Also save cover image next to video.
    #[serde(default)]
    pub download_cover: bool,
    /// Also save subtitles when available.
    #[serde(default)]
    pub download_subs: bool,
}

fn default_concurrent() -> usize {
    5
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            output_dir: "downloads".to_string(),
            rate_limit_kbps: 0,
            max_concurrent: 5,
            proxy: String::new(),
            download_cover: false,
            download_subs: false,
        }
    }
}
