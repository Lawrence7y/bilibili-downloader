use super::limiter::RateLimiter;
use anyhow::{bail, Context, Result};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT_RANGES, CONTENT_LENGTH, RANGE};
use reqwest::Client;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::info;

const MIN_PARALLEL_SIZE: u64 = 5 * 1024 * 1024; // 5 MB

#[derive(Clone, Debug)]
pub struct DownloadProgress {
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub speed_bps: u64,
    pub percentage: f32,
}

fn parse_total_from_content_range(val: &str) -> Option<u64> {
    // Standard format: "bytes 0-0/1234567"
    let parts: Vec<&str> = val.split('/').collect();
    if parts.len() == 2 {
        parts[1].trim().parse::<u64>().ok()
    } else {
        None
    }
}

pub struct ChunkedDownloader {
    client: Client,
    limiter: Option<RateLimiter>,
}

impl ChunkedDownloader {
    pub fn new(proxy: Option<&str>, limiter: Option<RateLimiter>) -> Result<Self> {
        let mut builder = Client::builder()
            .tcp_nodelay(true)
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .timeout(std::time::Duration::from_secs(120))
            .connect_timeout(std::time::Duration::from_secs(15));

        if let Some(p) = proxy {
            builder = builder.proxy(reqwest::Proxy::all(p)?);
        }

        let client = builder.build()?;
        Ok(Self { client, limiter })
    }

    fn build_headers(custom_headers: &HashMap<String, String>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        );
        for (k, v) in custom_headers {
            if let (Ok(name), Ok(val)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(v)) {
                headers.insert(name, val);
            }
        }
        headers
    }

    pub async fn probe(&self, url: &str, headers: &HashMap<String, String>) -> (u64, bool, String) {
        let req_headers = Self::build_headers(headers);

        // 1. Try quick HEAD request first
        if let Ok(resp) = self.client.head(url).headers(req_headers.clone()).send().await {
            if resp.status().is_success() {
                let final_url = resp.url().to_string();
                let content_length = resp
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);

                let accept_ranges = resp
                    .headers()
                    .get(ACCEPT_RANGES)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.eq_ignore_ascii_case("bytes"))
                    .unwrap_or(false);

                // If HEAD confirmed Range support and returned content-length, return immediately
                if accept_ranges && content_length > 0 {
                    return (content_length, true, final_url);
                }
            }
        }

        // 2. Fallback to smart GET probe with Range: bytes=0-0
        // Many Chinese CDNs (Douyin, Bilibili CDN, etc.) forbid HEAD or omit Accept-Ranges headers,
        // but fully honor HTTP Range requests with 206 Partial Content.
        let mut range_headers = req_headers;
        range_headers.insert(RANGE, HeaderValue::from_static("bytes=0-0"));

        if let Ok(resp) = self.client.get(url).headers(range_headers).send().await {
            let final_url = resp.url().to_string();
            let status = resp.status();
            if status == reqwest::StatusCode::PARTIAL_CONTENT {
                // 206 Partial Content -> Definitively supports Range!
                let total_size = resp
                    .headers()
                    .get(reqwest::header::CONTENT_RANGE)
                    .and_then(|v| v.to_str().ok())
                    .and_then(parse_total_from_content_range)
                    .unwrap_or(0);
                return (total_size, true, final_url);
            } else if status.is_success() {
                // 200 OK -> Range ignored, single-stream download only
                let total_size = resp
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(0);
                return (total_size, false, final_url);
            }
        }

        (0, false, url.to_string())
    }

    pub async fn download(
        &self,
        url: &str,
        output_path: &Path,
        headers: &HashMap<String, String>,
        concurrency: usize,
        cancel_token: Arc<AtomicBool>,
        progress_tx: Option<mpsc::Sender<DownloadProgress>>,
    ) -> Result<()> {
        if let Some(parent) = output_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let part_path = PathBuf::from(format!("{}.part", output_path.to_string_lossy()));
        let (total_size, accept_ranges, final_url) = self.probe(url, headers).await;

        // Resume: if a .part file already exists, continue from its length (single-stream).
        let existing = tokio::fs::metadata(&part_path).await.map(|m| m.len()).unwrap_or(0);
        let resume_from = if existing > 0 && (total_size == 0 || existing < total_size) {
            existing
        } else if total_size > 0 && existing == total_size {
            // Complete part from a previous run — just rename.
            tokio::fs::rename(&part_path, output_path).await.ok();
            if output_path.exists() {
                info!("Resumed complete part file for {:?}", output_path);
                return Ok(());
            }
            0
        } else {
            0
        };

        info!(
            "Download starting: {} -> {} (size: {} bytes, range: {}, resume_from: {})",
            url, final_url, total_size, accept_ranges, resume_from
        );

        let downloaded_bytes = Arc::new(AtomicU64::new(resume_from));

        // Separate flag so completing one file does not poison the caller's cancel token.
        let progress_stop = Arc::new(AtomicBool::new(false));
        let progress_cancel = Arc::clone(&progress_stop);
        let downloaded_clone = Arc::clone(&downloaded_bytes);
        let progress_task = tokio::spawn(async move {
            let mut last_bytes = 0u64;
            let mut last_time = Instant::now();

            while !progress_cancel.load(Ordering::Relaxed) {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                let current_bytes = downloaded_clone.load(Ordering::Relaxed);
                let elapsed = last_time.elapsed().as_secs_f64();

                let speed = if elapsed > 0.0 {
                    ((current_bytes.saturating_sub(last_bytes)) as f64 / elapsed) as u64
                } else {
                    0
                };

                let percentage = if total_size > 0 {
                    (current_bytes as f32 / total_size as f32) * 100.0
                } else {
                    0.0
                };

                last_bytes = current_bytes;
                last_time = Instant::now();

                if let Some(ref tx) = progress_tx {
                    let _ = tx
                        .send(DownloadProgress {
                            total_bytes: total_size,
                            downloaded_bytes: current_bytes,
                            speed_bps: speed,
                            percentage,
                        })
                        .await;
                }

                if total_size > 0 && current_bytes >= total_size {
                    break;
                }
            }
        });

        // Parallel only when starting fresh (no resume) and file is large.
        let result = if accept_ranges
            && total_size > MIN_PARALLEL_SIZE
            && concurrency > 1
            && resume_from == 0
        {
            // Aggressive parts allocation to break CDN per-connection QoS limits:
            // CDN limits single stream to ~1-2 MB/s. 
            // 8-32 connections allow saturating 100M-1000M broadband.
            let parts = if total_size >= 200 * 1024 * 1024 {
                concurrency.clamp(16, 32)
            } else if total_size >= 50 * 1024 * 1024 {
                concurrency.clamp(8, 24)
            } else if total_size >= 10 * 1024 * 1024 {
                concurrency.clamp(4, 16)
            } else {
                concurrency.clamp(2, 8)
            };
            self.download_parallel_range(
                &final_url,
                &part_path,
                headers,
                total_size,
                parts,
                Arc::clone(&downloaded_bytes),
                Arc::clone(&cancel_token),
            )
            .await
        } else {
            self.download_single_stream(
                &final_url,
                &part_path,
                headers,
                resume_from,
                Arc::clone(&downloaded_bytes),
                Arc::clone(&cancel_token),
            )
            .await
        };

        progress_stop.store(true, Ordering::Relaxed);
        let _ = progress_task.await;

        if let Err(e) = result {
            // Keep .part for resume unless cancelled mid-write with no progress need.
            // Only delete when file is empty/corrupt-sized zero.
            let sz = tokio::fs::metadata(&part_path).await.map(|m| m.len()).unwrap_or(0);
            if sz == 0 {
                let _ = tokio::fs::remove_file(&part_path).await;
            }
            return Err(e);
        }

        tokio::fs::rename(&part_path, output_path)
            .await
            .with_context(|| format!("Failed to rename {:?} to {:?}", part_path, output_path))?;

        info!("Download completed: {:?}", output_path);
        Ok(())
    }

    async fn download_single_stream(
        &self,
        url: &str,
        part_path: &Path,
        headers: &HashMap<String, String>,
        resume_from: u64,
        downloaded: Arc<AtomicU64>,
        cancel_token: Arc<AtomicBool>,
    ) -> Result<()> {
        let mut req_headers = Self::build_headers(headers);
        if resume_from > 0 {
            req_headers.insert(RANGE, HeaderValue::from_str(&format!("bytes={}-", resume_from))?);
        }

        let resp = self
            .client
            .get(url)
            .headers(req_headers)
            .send()
            .await?
            .error_for_status()?;

        let file = if resume_from > 0 {
            OpenOptions::new().write(true).create(true).open(part_path).await?
        } else {
            File::create(part_path).await?
        };
        let mut writer = tokio::io::BufWriter::with_capacity(256 * 1024, file);
        if resume_from > 0 {
            writer.seek(SeekFrom::End(0)).await?;
        }

        let mut stream = resp.bytes_stream();

        while let Some(chunk_res) = stream.next().await {
            if cancel_token.load(Ordering::Relaxed) {
                writer.flush().await.ok();
                bail!("Download cancelled by user");
            }

            let chunk = chunk_res?;
            if let Some(ref lim) = self.limiter {
                lim.consume(chunk.len() as u64).await;
            }

            writer.write_all(&chunk).await?;
            downloaded.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }

        writer.flush().await?;
        Ok(())
    }

    async fn download_parallel_range(
        &self,
        url: &str,
        part_path: &Path,
        headers: &HashMap<String, String>,
        total_size: u64,
        parts: usize,
        downloaded: Arc<AtomicU64>,
        cancel_token: Arc<AtomicBool>,
    ) -> Result<()> {
        {
            let file = File::create(part_path).await?;
            file.set_len(total_size).await?;
        }

        let chunk_size = total_size / (parts as u64);
        let mut tasks = Vec::new();

        for i in 0..parts {
            let start = i as u64 * chunk_size;
            let end = if i == parts - 1 {
                total_size - 1
            } else {
                (i as u64 + 1) * chunk_size - 1
            };

            let client = self.client.clone();
            let url_str = url.to_string();
            let path_buf = part_path.to_path_buf();
            let headers_map = headers.clone();
            let lim = self.limiter.clone();
            let dl_counter = Arc::clone(&downloaded);
            let cancel = Arc::clone(&cancel_token);

            let task = tokio::spawn(async move {
                let mut req_headers = Self::build_headers(&headers_map);
                let range_val = format!("bytes={}-{}", start, end);
                req_headers.insert(RANGE, HeaderValue::from_str(&range_val).unwrap());

                let resp = client
                    .get(&url_str)
                    .headers(req_headers)
                    .send()
                    .await?
                    .error_for_status()?;

                let file = OpenOptions::new().write(true).open(&path_buf).await?;
                let mut writer = tokio::io::BufWriter::with_capacity(256 * 1024, file);
                writer.seek(SeekFrom::Start(start)).await?;

                let mut stream = resp.bytes_stream();
                while let Some(chunk_res) = stream.next().await {
                    if cancel.load(Ordering::Relaxed) {
                        writer.flush().await.ok();
                        bail!("Download cancelled");
                    }
                    let chunk = chunk_res?;
                    if let Some(ref l) = lim {
                        l.consume(chunk.len() as u64).await;
                    }

                    writer.write_all(&chunk).await?;
                    dl_counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                }

                writer.flush().await?;
                Ok::<(), anyhow::Error>(())
            });

            tasks.push(task);
        }

        for task in tasks {
            task.await??;
        }

        Ok(())
    }
}
