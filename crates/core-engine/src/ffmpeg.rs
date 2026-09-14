use anyhow::{bail, Context, Result};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tracing::{info, warn};

pub struct FFmpegController {
    ffmpeg_bin: PathBuf,
}

impl FFmpegController {
    pub fn new() -> Result<Self> {
        let bin = Self::find_ffmpeg()?;
        Ok(Self { ffmpeg_bin: bin })
    }

    fn find_ffmpeg() -> Result<PathBuf> {
        // 1. Check local third_party / bin directory
        for candidate in &["ffmpeg.exe", "third_party/ffmpeg.exe", "bin/ffmpeg.exe", "ffmpeg"] {
            let path = PathBuf::from(candidate);
            if path.exists() {
                return Ok(path);
            }
        }

        // 2. Try PATH
        if let Ok(path) = which::which("ffmpeg") {
            return Ok(path);
        }

        // Last resort: bare name so Command can still resolve via PATH at spawn time.
        // merge/extract will surface a clear error if it is missing.
        warn!("ffmpeg not found in common paths or PATH; will fail at merge/extract if unavailable");
        Ok(PathBuf::from("ffmpeg"))
    }

    pub async fn check_available(&self) -> bool {
        Command::new(&self.ffmpeg_bin)
            .arg("-version")
            .output()
            .await
            .map(|out| out.status.success())
            .unwrap_or(false)
    }

    pub async fn merge_video_audio(
        &self,
        video_path: &Path,
        audio_path: &Path,
        output_path: &Path,
    ) -> Result<()> {
        info!(
            "FFmpeg merging: {:?} + {:?} -> {:?}",
            video_path, audio_path, output_path
        );

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let status = Command::new(&self.ffmpeg_bin)
            .args([
                "-y",
                "-i",
                video_path.to_str().unwrap(),
                "-i",
                audio_path.to_str().unwrap(),
                "-c:v",
                "copy",
                "-c:a",
                "copy",
                output_path.to_str().unwrap(),
            ])
            .status()
            .await
            .with_context(|| {
                format!(
                    "无法启动 FFmpeg（{:?}）。请安装 ffmpeg 并加入 PATH，或放到项目根目录 ffmpeg.exe",
                    self.ffmpeg_bin
                )
            })?;

        if !status.success() {
            bail!(
                "FFmpeg 合并失败（exit {:?}）。请确认 ffmpeg 可用且输入流完整",
                status.code()
            );
        }

        Ok(())
    }

    pub async fn extract_audio(
        &self,
        input_path: &Path,
        output_path: &Path,
        format: &str, // "mp3", "m4a", "flac", "wav"
    ) -> Result<()> {
        info!("FFmpeg extracting audio to {}: {:?}", format, output_path);

        let mut cmd = Command::new(&self.ffmpeg_bin);
        cmd.arg("-y").arg("-i").arg(input_path.to_str().unwrap());

        match format {
            "mp3" => cmd.args(["-vn", "-c:a", "libmp3lame", "-q:a", "2"]),
            "m4a" => cmd.args(["-vn", "-c:a", "aac", "-b:a", "192k"]),
            "flac" => cmd.args(["-vn", "-c:a", "flac"]),
            "wav" => cmd.args(["-vn", "-c:a", "pcm_s16le"]),
            _ => cmd.args(["-vn", "-c:a", "copy"]),
        };

        cmd.arg(output_path.to_str().unwrap());

        let status = cmd.status().await.with_context(|| {
            format!(
                "无法启动 FFmpeg（{:?}）。请安装 ffmpeg 并加入 PATH",
                self.ffmpeg_bin
            )
        })?;
        if !status.success() {
            bail!("FFmpeg 音频提取失败（exit {:?}）", status.code());
        }

        Ok(())
    }

    pub async fn download_m3u8(
        &self,
        m3u8_url: &str,
        output_path: &Path,
        headers: &std::collections::HashMap<String, String>,
        cancel_token: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<()> {
        info!("FFmpeg downloading M3U8: {} -> {:?}", m3u8_url, output_path);

        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut header_str = String::new();
        for (k, v) in headers {
            header_str.push_str(&format!("{}: {}\r\n", k, v));
        }

        let mut cmd = Command::new(&self.ffmpeg_bin);
        cmd.arg("-y");

        if !header_str.is_empty() {
            cmd.arg("-headers").arg(&header_str);
        }

        cmd.args([
            "-i",
            m3u8_url,
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            output_path.to_str().unwrap(),
        ]);

        let mut child = cmd.spawn().with_context(|| {
            format!(
                "无法启动 FFmpeg（{:?}）下载 M3U8。请确认 ffmpeg 可用",
                self.ffmpeg_bin
            )
        })?;

        loop {
            if cancel_token.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = child.kill().await;
                bail!("Download cancelled by user");
            }

            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        bail!("FFmpeg M3U8 下载失败（exit {:?}）", status.code());
                    }
                    break;
                }
                Ok(None) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
                Err(e) => {
                    bail!("FFmpeg 等待失败: {}", e);
                }
            }
        }

        info!("FFmpeg M3U8 download completed: {:?}", output_path);
        Ok(())
    }
}
