use crate::db::Database;
use crate::downloader::{ChunkedDownloader, DownloadProgress, RateLimiter};
use crate::ffmpeg::FFmpegController;
use crate::models::{AppSettings, DownloadRecord, TaskProgress, TaskStatus};
use crate::sidecar::SidecarClient;
use anyhow::{bail, Context, Result};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{error, info, warn};

/// Finished tasks are kept in memory this long so late-joining UIs can still show them.
const FINISHED_TASK_TTL: Duration = Duration::from_secs(15 * 60);
const SETTINGS_KEY: &str = "app_settings";
const MAX_CONCURRENT_HARD_CAP: usize = 32;

/// Decrements the active-task counter when dropped.
struct ActiveGuard(Arc<AtomicUsize>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateTaskRequest {
    pub url: String,
    pub output_dir: Option<String>,
    pub extract_audio: Option<String>, // e.g. "mp3", "m4a", None
    pub cookie: Option<String>,
    pub proxy: Option<String>,
    /// Preferred stream format_id from a prior /resolve call.
    #[serde(default)]
    pub format_id: Option<String>,
    #[serde(default)]
    pub download_cover: Option<bool>,
    #[serde(default)]
    pub download_subs: Option<bool>,
}

#[derive(Clone)]
pub struct TaskItem {
    pub id: String,
    pub url: String,
    pub title: Arc<Mutex<String>>,
    pub platform: Arc<Mutex<String>>,
    pub status: Arc<Mutex<TaskStatus>>,
    pub cancel_token: Arc<AtomicBool>,
    pub progress: Arc<Mutex<TaskProgress>>,
    pub output_path: Arc<Mutex<String>>,
    pub finished_at: Arc<Mutex<Option<Instant>>>,
    /// Original request so failed/cancelled tasks can be retried.
    pub original_request: Arc<Mutex<CreateTaskRequest>>,
}

pub struct TaskManager {
    tasks: Arc<Mutex<HashMap<String, TaskItem>>>,
    sidecar: Arc<SidecarClient>,
    db: Arc<Database>,
    ffmpeg: Arc<FFmpegController>,
    limiter: RateLimiter,
    /// Hard cap semaphore; actual concurrency limited by `max_concurrent`.
    concurrency_semaphore: Arc<Semaphore>,
    active_tasks: Arc<AtomicUsize>,
    max_concurrent: Arc<AtomicUsize>,
    progress_broadcast: broadcast::Sender<TaskProgress>,
    project_root: PathBuf,
    settings: Arc<Mutex<AppSettings>>,
}

impl TaskManager {
    pub fn new(
        sidecar: Arc<SidecarClient>,
        db: Arc<Database>,
        _max_concurrent_tasks: usize,
        project_root: PathBuf,
    ) -> Result<Self> {
        let (tx, _) = broadcast::channel(500);
        let ffmpeg = Arc::new(FFmpegController::new()?);
        let loaded = Self::load_settings_from_db(&db);
        let limiter = RateLimiter::new(loaded.rate_limit_kbps.saturating_mul(1024));
        let max_c = loaded.max_concurrent.clamp(1, MAX_CONCURRENT_HARD_CAP);

        Ok(Self {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            sidecar,
            db,
            ffmpeg,
            limiter,
            concurrency_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_HARD_CAP)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            max_concurrent: Arc::new(AtomicUsize::new(max_c)),
            progress_broadcast: tx,
            project_root,
            settings: Arc::new(Mutex::new(loaded)),
        })
    }

    fn load_settings_from_db(db: &Database) -> AppSettings {
        match db.get_setting(SETTINGS_KEY) {
            Ok(Some(raw)) => serde_json::from_str(&raw).unwrap_or_default(),
            _ => AppSettings::default(),
        }
    }

    pub async fn get_settings(&self) -> AppSettings {
        self.settings.lock().await.clone()
    }

    pub async fn update_settings(&self, next: AppSettings) -> Result<AppSettings> {
        let mut next = next;
        next.max_concurrent = next.max_concurrent.clamp(1, MAX_CONCURRENT_HARD_CAP);
        // Rate limit: KB/s → bytes/s; 0 = unlimited
        self.limiter.set_rate(next.rate_limit_kbps.saturating_mul(1024));
        self.max_concurrent.store(next.max_concurrent, Ordering::Relaxed);

        let json = serde_json::to_string(&next)?;
        self.db.set_setting(SETTINGS_KEY, &json)?;

        let mut guard = self.settings.lock().await;
        *guard = next.clone();
        Ok(next)
    }

    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn subscribe_progress(&self) -> broadcast::Receiver<TaskProgress> {
        self.progress_broadcast.subscribe()
    }

    pub async fn check_ffmpeg(&self) -> bool {
        self.ffmpeg.check_available().await
    }

    pub async fn check_sidecar(&self) -> bool {
        self.sidecar.ping().await.is_ok()
    }

    async fn acquire_concurrency_slot(
        &self,
    ) -> Result<(tokio::sync::OwnedSemaphorePermit, ActiveGuard)> {
        loop {
            let permit = Arc::clone(&self.concurrency_semaphore).acquire_owned().await?;
            let active = self.active_tasks.load(Ordering::Relaxed);
            let max = self.max_concurrent.load(Ordering::Relaxed).max(1);
            if active < max {
                self.active_tasks.fetch_add(1, Ordering::Relaxed);
                let guard = ActiveGuard(Arc::clone(&self.active_tasks));
                return Ok((permit, guard));
            }
            drop(permit);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn prune_finished(&self) {
        let mut tasks = self.tasks.lock().await;
        let mut doomed = Vec::new();
        for (id, item) in tasks.iter() {
            if let Some(t) = *item.finished_at.lock().await {
                if t.elapsed() > FINISHED_TASK_TTL {
                    doomed.push(id.clone());
                }
            }
        }
        for id in doomed {
            tasks.remove(&id);
        }
    }

    pub async fn get_all_tasks(&self) -> Vec<TaskProgress> {
        self.prune_finished().await;
        let tasks = self.tasks.lock().await;
        let mut list = Vec::new();
        for item in tasks.values() {
            let p = item.progress.lock().await.clone();
            list.push(p);
        }
        list
    }

    pub async fn cancel_task(&self, task_id: &str) -> bool {
        let tasks = self.tasks.lock().await;
        if let Some(task) = tasks.get(task_id) {
            task.cancel_token.store(true, Ordering::Relaxed);
            let mut s = task.status.lock().await;
            *s = TaskStatus::Cancelled;
            let mut p = task.progress.lock().await;
            p.status = TaskStatus::Cancelled;
            p.can_retry = true;
            let _ = self.progress_broadcast.send(p.clone());
            let mut fa = task.finished_at.lock().await;
            *fa = Some(Instant::now());
            true
        } else {
            false
        }
    }

    /// Re-queue a failed/cancelled task with its original request.
    pub async fn retry_task(self: &Arc<Self>, task_id: &str) -> Result<String> {
        let req = {
            let tasks = self.tasks.lock().await;
            let Some(task) = tasks.get(task_id) else {
                bail!("任务不存在: {task_id}");
            };
            let status = *task.status.lock().await;
            if !matches!(
                status,
                TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Completed
            ) {
                bail!("仅失败/取消/已完成的任务可重试");
            }
            let req = task.original_request.lock().await.clone();
            req
        };
        self.create_task(req).await
    }

    pub async fn create_task(self: &Arc<Self>, req: CreateTaskRequest) -> Result<String> {
        self.prune_finished().await;

        if req.url.trim().is_empty() {
            bail!("URL 不能为空");
        }

        // Apply settings defaults for empty optional fields.
        let mut req = req;
        {
            let settings = self.settings.lock().await;
            if req.output_dir.as_deref().map(str::trim).unwrap_or("").is_empty()
                && !settings.output_dir.trim().is_empty()
            {
                req.output_dir = Some(settings.output_dir.clone());
            }
            if req.proxy.as_deref().map(str::trim).unwrap_or("").is_empty()
                && !settings.proxy.trim().is_empty()
            {
                req.proxy = Some(settings.proxy.clone());
            }
            if req.download_cover.is_none() {
                req.download_cover = Some(settings.download_cover);
            }
            if req.download_subs.is_none() {
                req.download_subs = Some(settings.download_subs);
            }
        }

        // Validate output dir early so the API returns a clear error.
        let out_dir = resolve_output_dir_checked(&req.output_dir, &self.project_root)?;
        tokio::fs::create_dir_all(&out_dir).await?;

        let task_id = format!(
            "task_{}_{}",
            Local::now().timestamp_millis(),
            fastrand::u32(..1000)
        );
        let cancel_token = Arc::new(AtomicBool::new(false));

        let display_title = req
            .url
            .trim()
            .to_string();

        let item = TaskItem {
            id: task_id.clone(),
            url: req.url.clone(),
            title: Arc::new(Mutex::new(display_title.clone())),
            platform: Arc::new(Mutex::new("auto".to_string())),
            status: Arc::new(Mutex::new(TaskStatus::Resolving)),
            cancel_token: Arc::clone(&cancel_token),
            progress: Arc::new(Mutex::new(TaskProgress {
                task_id: task_id.clone(),
                status: TaskStatus::Resolving,
                total_bytes: 0,
                downloaded_bytes: 0,
                speed_bps: 0,
                percentage: 0.0,
                eta_seconds: 0,
                error_msg: None,
                title: display_title,
                platform: "auto".to_string(),
                url: req.url.clone(),
                output_path: String::new(),
                can_retry: false,
            })),
            output_path: Arc::new(Mutex::new(String::new())),
            finished_at: Arc::new(Mutex::new(None)),
            original_request: Arc::new(Mutex::new(req.clone())),
        };

        {
            let mut tasks = self.tasks.lock().await;
            tasks.insert(task_id.clone(), item.clone());
        }

        let this = Arc::clone(self);
        let task_id_clone = task_id.clone();

        tokio::spawn(async move {
            if let Err(err) = this.run_task_pipeline(&task_id_clone, req, item, out_dir).await {
                error!("Task {} failed: {:?}", task_id_clone, err);
            }
        });

        Ok(task_id)
    }

    /// Download a small side file (cover / image / subtitle) using a fresh client.
    async fn download_side_file(
        &self,
        url: &str,
        path: &Path,
        req: &CreateTaskRequest,
    ) -> Result<()> {
        let headers = HashMap::from([(
            "User-Agent".to_string(),
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36".to_string(),
        )]);
        let dl = ChunkedDownloader::new(req.proxy.as_deref(), Some(self.limiter.clone()))?;
        let cancel = Arc::new(AtomicBool::new(false));
        dl.download(url, path, &headers, 1, cancel, None).await
    }

    async fn update_progress(
        &self,
        item: &TaskItem,
        status: TaskStatus,
        total: u64,
        dl: u64,
        speed: u64,
        pct: f32,
        err: Option<String>,
    ) {
        {
            let t = item.title.lock().await;
            let plat = item.platform.lock().await;
            let op = item.output_path.lock().await;
            {
                let mut s = item.status.lock().await;
                *s = status;
            }
            let mut p = item.progress.lock().await;
            p.status = status;
            p.total_bytes = total;
            p.downloaded_bytes = dl;
            p.speed_bps = speed;
            p.percentage = pct;
            p.error_msg = err;
            p.title = t.clone();
            p.platform = plat.clone();
            p.url = item.url.clone();
            p.output_path = op.clone();
            p.can_retry = matches!(
                status,
                TaskStatus::Failed | TaskStatus::Cancelled | TaskStatus::Completed
            );
            let _ = self.progress_broadcast.send(p.clone());
        }

        if matches!(
            status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            let mut fa = item.finished_at.lock().await;
            *fa = Some(Instant::now());
        }
    }

    async fn run_task_pipeline(
        &self,
        task_id: &str,
        req: CreateTaskRequest,
        item: TaskItem,
        out_dir: PathBuf,
    ) -> Result<()> {
        let (_permit, _active_guard) = self.acquire_concurrency_slot().await?;

        if item.cancel_token.load(Ordering::Relaxed) {
            self.update_progress(&item, TaskStatus::Cancelled, 0, 0, 0, 0.0, None)
                .await;
            return Ok(());
        }

        // 1. Resolve URL metadata
        self.update_progress(&item, TaskStatus::Resolving, 0, 0, 0, 0.0, None)
            .await;

        let meta = match self
            .sidecar
            .resolve_url(&req.url, req.cookie.as_deref(), req.proxy.as_deref())
            .await
        {
            Ok(m) => m,
            Err(e) => {
                let err_str = format!("解析失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }
        };

        {
            let mut t = item.title.lock().await;
            *t = meta.title.clone();
            let mut p = item.platform.lock().await;
            *p = meta.platform.clone();
        }
        // Broadcast resolved title immediately so the UI can show it.
        {
            let mut prog = item.progress.lock().await;
            prog.title = meta.title.clone();
            prog.platform = meta.platform.clone();
            let _ = self.progress_broadcast.send(prog.clone());
        }

        let safe_title: String = meta
            .title
            .chars()
            .map(|c| if "\\/:*?\"<>|".contains(c) { '_' } else { c })
            .take(60)
            .collect();

        let final_filename = format!("{}.mp4", safe_title);
        let final_output = out_dir.join(&final_filename);
        {
            let mut op = item.output_path.lock().await;
            *op = final_output.to_string_lossy().to_string();
        }

        // Optional cover image
        if req.download_cover.unwrap_or(false) && !meta.cover_url.is_empty() {
            let cover_path = out_dir.join(format!("{}.jpg", safe_title));
            if let Err(e) = self.download_side_file(&meta.cover_url, &cover_path, &req).await {
                warn!("Cover download failed: {:#}", e);
            }
        }

        // Image post: download all images and finish (no video stream).
        let image_urls: Vec<String> = meta
            .extra
            .get("image_urls")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let is_image_post = meta.content_type == "image_post" || (!image_urls.is_empty() && meta.streams.is_empty());
        if is_image_post {
            if !image_urls.is_empty() {
                self.update_progress(&item, TaskStatus::Downloading, image_urls.len() as u64, 0, 0, 0.0, None)
                    .await;
                let img_dir = out_dir.join(format!("{}_images", safe_title));
                tokio::fs::create_dir_all(&img_dir).await?;
                let mut ok_count = 0u64;
                for (i, url) in image_urls.iter().enumerate() {
                    if item.cancel_token.load(Ordering::Relaxed) {
                        self.update_progress(&item, TaskStatus::Cancelled, image_urls.len() as u64, ok_count, 0, 0.0, None)
                            .await;
                        return Ok(());
                    }
                    let ext = if url.contains(".png") { "png" } else { "jpg" };
                    let path = img_dir.join(format!("{:03}.{}", i + 1, ext));
                    match self.download_side_file(url, &path, &req).await {
                        Ok(()) => ok_count += 1,
                        Err(e) => warn!("Image {} failed: {:#}", i + 1, e),
                    }
                    let pct = (ok_count as f32 / image_urls.len() as f32) * 100.0;
                    self.update_progress(&item, TaskStatus::Downloading, image_urls.len() as u64, ok_count, 0, pct, None)
                        .await;
                }
                if ok_count == 0 {
                    let err_str = "图文作品图片下载全部失败".to_string();
                    self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                        .await;
                    bail!(err_str);
                }
                let rec = DownloadRecord {
                    id: None,
                    url: req.url.clone(),
                    status: "success".to_string(),
                    reason: String::new(),
                    title: meta.title.clone(),
                    platform: meta.platform.clone(),
                    author: meta.author.clone(),
                    duration: meta.duration as i64,
                    file_size: 0,
                    file_format: "images".to_string(),
                    output_path: img_dir.to_string_lossy().to_string(),
                    timestamp: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                };
                if let Err(e) = self.db.add_record(&rec) {
                    warn!("Failed to persist image-post history: {:#}", e);
                }
                {
                    let mut op = item.output_path.lock().await;
                    *op = img_dir.to_string_lossy().to_string();
                }
                self.update_progress(&item, TaskStatus::Completed, ok_count, ok_count, 0, 100.0, None)
                    .await;
                info!("Image post task {} finished: {:?}", task_id, img_dir);
                return Ok(());
            }
        }

        // Optional subtitles from yt-dlp extra
        if req.download_subs.unwrap_or(false) {
            if let Some(subs) = meta.extra.get("subtitles").and_then(|v| v.as_array()) {
                for sub in subs {
                    let Some(url) = sub.get("url").and_then(|u| u.as_str()) else {
                        continue;
                    };
                    let lang = sub.get("lang").and_then(|l| l.as_str()).unwrap_or("sub");
                    let ext = sub.get("ext").and_then(|e| e.as_str()).unwrap_or("vtt");
                    let path = out_dir.join(format!("{}.{}.{}", safe_title, lang, ext));
                    if let Err(e) = self.download_side_file(url, &path, &req).await {
                        warn!("Subtitle {} download failed: {:#}", lang, e);
                    }
                }
            }
        }

        // 2. Choose Streams
        // Parentheses matter: `&&` binds tighter than `||`.
        let downloader = ChunkedDownloader::new(req.proxy.as_deref(), Some(self.limiter.clone()))?;

        // Prefer user-selected format_id from /resolve when present.
        let preferred = req.format_id.as_deref().map(str::trim).filter(|s| !s.is_empty());
        let preferred_stream = preferred.and_then(|fid| meta.streams.iter().find(|s| s.format_id == fid));

        let muxed_stream = preferred_stream
            .filter(|s| {
                s.video_url.is_some()
                    && s.audio_url.is_none()
                    && (s.format_id.starts_with("muxed_")
                        || s.format_id == "original"
                        || s.format_id == "default")
            })
            .or_else(|| {
                meta.streams.iter().find(|s| {
                    s.video_url.is_some()
                        && s.audio_url.is_none()
                        && (s.format_id.starts_with("muxed_")
                            || s.format_id == "original"
                            || s.format_id == "default")
                })
            });
        let video_only = preferred_stream
            .filter(|s| s.video_url.is_some() && s.audio_url.is_none() && s.format_id.starts_with("video_"))
            .or_else(|| {
                meta.streams
                    .iter()
                    .find(|s| s.video_url.is_some() && s.audio_url.is_none() && s.format_id.starts_with("video_"))
            });
        let audio_only = preferred
            .and_then(|fid| {
                // If user picked a video_* stream, pair with best audio_*; if audio_*, use that.
                if fid.starts_with("audio_") {
                    meta.streams.iter().find(|s| s.format_id == fid)
                } else {
                    meta.streams.iter().find(|s| {
                        s.audio_url.is_some() && s.video_url.is_none() && s.format_id.starts_with("audio_")
                    })
                }
            })
            .or_else(|| meta.streams.iter().find(|s| s.audio_url.is_some()));

        if item.cancel_token.load(Ordering::Relaxed) {
            self.update_progress(&item, TaskStatus::Cancelled, 0, 0, 0, 0.0, None)
                .await;
            return Ok(());
        }

        if let Some(stream) = muxed_stream {
            // Single stream download
            let v_url = stream.video_url.as_ref().unwrap();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<DownloadProgress>(100);

            let item_clone = item.clone();
            let p_task = tokio::spawn(async move {
                while let Some(prog) = rx.recv().await {
                    let mut p = item_clone.progress.lock().await;
                    p.status = TaskStatus::Downloading;
                    p.total_bytes = prog.total_bytes;
                    p.downloaded_bytes = prog.downloaded_bytes;
                    p.speed_bps = prog.speed_bps;
                    p.percentage = prog.percentage;
                }
            });

            let dl_result = downloader
                .download(
                    v_url,
                    &final_output,
                    &stream.headers,
                    4,
                    Arc::clone(&item.cancel_token),
                    Some(tx),
                )
                .await;
            let _ = p_task.await;

            if let Err(e) = dl_result {
                if item.cancel_token.load(Ordering::Relaxed) {
                    self.update_progress(&item, TaskStatus::Cancelled, 0, 0, 0, 0.0, None)
                        .await;
                    return Ok(());
                }
                let err_str = format!("下载失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }
        } else if let (Some(v_stream), Some(a_stream)) = (video_only, audio_only) {
            // DASH separate streams: download video and audio, then merge
            let tmp_v = out_dir.join(format!("{}.v.mp4", task_id));
            let tmp_a = out_dir.join(format!("{}.a.m4a", task_id));

            info!("Downloading separate video stream: {:?}", tmp_v);
            if let Err(e) = downloader
                .download(
                    v_stream.video_url.as_ref().unwrap(),
                    &tmp_v,
                    &v_stream.headers,
                    4,
                    Arc::clone(&item.cancel_token),
                    None,
                )
                .await
            {
                let _ = tokio::fs::remove_file(&tmp_v).await;
                if item.cancel_token.load(Ordering::Relaxed) {
                    self.update_progress(&item, TaskStatus::Cancelled, 0, 0, 0, 0.0, None)
                        .await;
                    return Ok(());
                }
                let err_str = format!("视频流下载失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }

            info!("Downloading separate audio stream: {:?}", tmp_a);
            if let Err(e) = downloader
                .download(
                    a_stream.audio_url.as_ref().unwrap(),
                    &tmp_a,
                    &a_stream.headers,
                    4,
                    Arc::clone(&item.cancel_token),
                    None,
                )
                .await
            {
                let _ = tokio::fs::remove_file(&tmp_v).await;
                let _ = tokio::fs::remove_file(&tmp_a).await;
                if item.cancel_token.load(Ordering::Relaxed) {
                    self.update_progress(&item, TaskStatus::Cancelled, 0, 0, 0, 0.0, None)
                        .await;
                    return Ok(());
                }
                let err_str = format!("音频流下载失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }

            // Merge with FFmpeg
            self.update_progress(&item, TaskStatus::Merging, 0, 0, 0, 100.0, None)
                .await;
            if let Err(e) = self.ffmpeg.merge_video_audio(&tmp_v, &tmp_a, &final_output).await {
                let _ = tokio::fs::remove_file(&tmp_v).await;
                let _ = tokio::fs::remove_file(&tmp_a).await;
                let err_str = format!("音视频合并失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }

            // Cleanup temps
            let _ = tokio::fs::remove_file(&tmp_v).await;
            let _ = tokio::fs::remove_file(&tmp_a).await;
        } else if let Some(stream) = meta.streams.first() {
            // Fallback first available stream
            let v_url = stream
                .video_url
                .as_ref()
                .or(stream.audio_url.as_ref())
                .context("流缺少可下载 URL")?;
            if let Err(e) = downloader
                .download(
                    v_url,
                    &final_output,
                    &stream.headers,
                    4,
                    Arc::clone(&item.cancel_token),
                    None,
                )
                .await
            {
                if item.cancel_token.load(Ordering::Relaxed) {
                    self.update_progress(&item, TaskStatus::Cancelled, 0, 0, 0, 0.0, None)
                        .await;
                    return Ok(());
                }
                let err_str = format!("下载失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }
        } else {
            let err_str = "未找到可用的下载音视频流".to_string();
            self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                .await;
            bail!(err_str);
        }

        // 3. Audio post-action if requested
        if let Some(ref fmt) = req.extract_audio {
            let audio_output = out_dir.join(format!("{}.{}", safe_title, fmt));
            if let Err(e) = self.ffmpeg.extract_audio(&final_output, &audio_output, fmt).await {
                let err_str = format!("音频提取失败: {}", e);
                self.update_progress(&item, TaskStatus::Failed, 0, 0, 0, 0.0, Some(err_str.clone()))
                    .await;
                bail!(err_str);
            }
        }

        // 4. Save to Database
        let file_size = tokio::fs::metadata(&final_output)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let rec = DownloadRecord {
            id: None,
            url: req.url.clone(),
            status: "success".to_string(),
            reason: String::new(),
            title: meta.title.clone(),
            platform: meta.platform.clone(),
            author: meta.author.clone(),
            duration: meta.duration as i64,
            file_size: file_size as i64,
            file_format: "mp4".to_string(),
            output_path: final_output.to_string_lossy().to_string(),
            timestamp: Local::now().format("%Y-%m-%d %H:%M:%S").to_string(),
        };
        if let Err(e) = self.db.add_record(&rec) {
            warn!("Failed to persist download history for task {}: {:#}", task_id, e);
        }

        self.update_progress(&item, TaskStatus::Completed, file_size, file_size, 0, 100.0, None)
            .await;
        info!("Task {} fully finished: {:?}", task_id, final_output);

        Ok(())
    }
}

/// Mirror of server::paths::resolve_output_dir — kept here so core-engine can validate
/// independently of the HTTP layer. Project root is provided by the caller.
fn resolve_output_dir_checked(requested: &Option<String>, root: &Path) -> Result<PathBuf> {
    let raw = requested.as_deref().map(str::trim).unwrap_or("");
    if raw.is_empty() {
        return Ok(root.join("downloads"));
    }

    let candidate = {
        let p = PathBuf::from(raw);
        if p.is_absolute() {
            p
        } else {
            root.join(p)
        }
    };

    let normalized = normalize_path(&candidate);
    let normalized_root = normalize_path(root);

    if !normalized.starts_with(&normalized_root) {
        bail!(
            "输出目录必须位于项目根目录内: {}",
            normalized.display()
        );
    }

    Ok(normalized)
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}
