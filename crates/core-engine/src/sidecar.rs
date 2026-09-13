use crate::models::MediaMetadata;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{oneshot, Mutex};
use tracing::{error, info, warn};

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Value,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Option<u64>,
    result: Option<Value>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize, Debug)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

pub struct SidecarClient {
    #[allow(dead_code)]
    python_bin: PathBuf,
    #[allow(dead_code)]
    script_path: PathBuf,
    stdin: Arc<Mutex<BufWriter<ChildStdin>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>,
    next_id: AtomicU64,
    _child: Arc<Mutex<Child>>,
}

impl SidecarClient {
    pub async fn new<P1: AsRef<Path>, P2: AsRef<Path>>(
        python_bin: P1,
        script_path: P2,
    ) -> Result<Arc<Self>> {
        let python_bin = python_bin.as_ref().to_path_buf();
        let script_path = script_path.as_ref().to_path_buf();

        info!(
            "Starting Python Sidecar: {:?} {:?}",
            python_bin, script_path
        );

        let mut child = Command::new(&python_bin)
            .arg(&script_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .with_context(|| format!("Failed to spawn sidecar at {:?}", script_path))?;

        let child_stdin = child.stdin.take().context("Failed to take stdin")?;
        let child_stdout = child.stdout.take().context("Failed to take stdout")?;

        let stdin = Arc::new(Mutex::new(BufWriter::new(child_stdin)));
        let pending: Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let child_ref = Arc::new(Mutex::new(child));

        let client = Arc::new(Self {
            python_bin,
            script_path,
            stdin,
            pending: Arc::clone(&pending),
            next_id: AtomicU64::new(1),
            _child: child_ref,
        });

        // Background loop to read stdout
        tokio::spawn(async move {
            let mut reader = BufReader::new(child_stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                if let Ok(resp) = serde_json::from_str::<JsonRpcResponse>(line) {
                    if let Some(id) = resp.id {
                        let mut map = pending.lock().await;
                        if let Some(tx) = map.remove(&id) {
                            if let Some(res) = resp.result {
                                let _ = tx.send(Ok(res));
                            } else if let Some(err) = resp.error {
                                let mut msg = format!("Error {}: {}", err.code, err.message);
                                if let Some(data) = err.data {
                                    if let Some(auth) = data.get("auth_required").and_then(|v| v.as_bool()) {
                                        if auth {
                                            let platform = data
                                                .get("platform")
                                                .and_then(|v| v.as_str())
                                                .unwrap_or("unknown");
                                            msg = format!(
                                                "登录态失效或 Cookie 无效（{}），请到「系统设置」更新 Cookie。{}",
                                                platform, err.message
                                            );
                                        }
                                    }
                                }
                                let _ = tx.send(Err(msg));
                            }
                        }
                    }
                } else {
                    warn!("Unrecognized sidecar output: {}", line);
                }
            }
            error!("Sidecar stdout closed");
        });

        // Perform initial ping
        client.ping().await.context("Sidecar ping failed")?;
        info!("Sidecar ping successful!");

        Ok(client)
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        self.call_with_timeout(method, params, std::time::Duration::from_secs(90))
            .await
    }

    pub async fn call_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: std::time::Duration,
    ) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let req = JsonRpcRequest {
            jsonrpc: "2.0",
            id,
            method,
            params,
        };

        let req_json = serde_json::to_string(&req)? + "\n";
        let (tx, rx) = oneshot::channel();

        {
            let mut map = self.pending.lock().await;
            map.insert(id, tx);
        }

        {
            let mut stdin = self.stdin.lock().await;
            stdin.write_all(req_json.as_bytes()).await?;
            stdin.flush().await?;
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(val))) => Ok(val),
            Ok(Ok(Err(err_msg))) => bail!("{}", err_msg),
            Ok(Err(_)) => {
                let mut map = self.pending.lock().await;
                map.remove(&id);
                bail!("Sidecar response channel dropped")
            }
            Err(_) => {
                let mut map = self.pending.lock().await;
                map.remove(&id);
                bail!(
                    "Sidecar request timed out after {}s",
                    timeout.as_secs()
                )
            }
        }
    }

    pub async fn ping(&self) -> Result<()> {
        let res = self.call("ping", serde_json::json!({})).await?;
        if res.get("status").and_then(|s| s.as_str()) == Some("ok") {
            Ok(())
        } else {
            bail!("Unexpected ping response: {:?}", res)
        }
    }

    pub async fn check_cookie(&self, platform: &str, cookie: &str) -> Result<Value> {
        self.call(
            "check_cookie",
            serde_json::json!({
                "platform": platform,
                "cookie": cookie
            }),
        )
        .await
    }

    pub async fn resolve_url(
        &self,
        url: &str,
        cookie: Option<&str>,
        proxy: Option<&str>,
    ) -> Result<MediaMetadata> {
        let val = self
            .call(
                "resolve",
                serde_json::json!({
                    "url": url,
                    "cookie": cookie,
                    "proxy": proxy
                }),
            )
            .await?;

        let meta: MediaMetadata = serde_json::from_value(val)?;
        Ok(meta)
    }

    pub async fn resolve_douyin_batch(
        &self,
        batch_type: &str,
        target_id: &str,
        max_count: u32,
        cookie: Option<&str>,
        proxy: Option<&str>,
    ) -> Result<MediaMetadata> {
        let val = self
            .call_with_timeout(
                "resolve_douyin_batch",
                serde_json::json!({
                    "type": batch_type,
                    "id": target_id,
                    "max_count": max_count,
                    "cookie": cookie,
                    "proxy": proxy
                }),
                // Batch crawls many pages; allow up to 3 minutes.
                std::time::Duration::from_secs(180),
            )
            .await?;

        let meta: MediaMetadata = serde_json::from_value(val)?;
        Ok(meta)
    }
}
