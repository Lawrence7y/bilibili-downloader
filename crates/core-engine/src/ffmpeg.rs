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
}
