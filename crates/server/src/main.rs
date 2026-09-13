mod paths;

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use core_engine::{
    db::Database,
    models::{DownloadRecord, MediaMetadata, TaskProgress},
    sidecar::SidecarClient,
    task_manager::{CreateTaskRequest, TaskManager},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    sidecar: Arc<SidecarClient>,
    task_manager: Arc<TaskManager>,
    db: Arc<Database>,
    project_root: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Video Downloader Backend Server...");

    let project_root = paths::project_root();
    info!("Project root: {:?}", project_root);

    // 1. Initialize Sidecar
    let python_bin = project_root.join("bilibili-downloader/.venv/Scripts/python.exe");
    let script_path = project_root.join("sidecar/server.py");

    if !python_bin.exists() {
        anyhow::bail!(
            "Python sidecar interpreter not found: {:?}\n\
             Create a venv at bilibili-downloader/.venv or set DDL_PROJECT_ROOT.",
            python_bin
        );
    }
    if !script_path.exists() {
        anyhow::bail!("Sidecar script not found: {:?}", script_path);
    }

    let sidecar = SidecarClient::new(python_bin, script_path).await?;

    // 2. Initialize Database
    let db_path = project_root.join("downloads/history.db");
    let db = Arc::new(Database::new(&db_path)?);

    // 3. Initialize Task Manager (concurrency = 5)
    let task_manager = Arc::new(TaskManager::new(
        Arc::clone(&sidecar),
        Arc::clone(&db),
        5,
        project_root.clone(),
    )?);

    let state = AppState {
        sidecar,
        task_manager,
        db,
        project_root,
    };

    // 4. Build Axum Router
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api_router = Router::new()
        .route("/health", get(health_check))
        .route("/resolve", post(resolve_url))
        .route("/resolve/douyin/batch", post(resolve_douyin_batch))
        .route("/cookie/check", post(check_cookie))
        .route("/tasks", get(list_tasks).post(create_task))
        .route("/tasks/:id/cancel", post(cancel_task))
        .route("/tasks/:id/retry", post(retry_task))
        .route("/history", get(list_history))
        .route("/history/search", get(search_history))
        .route("/ws", get(ws_handler));

    let dist_dir = state.project_root.join("frontend/dist");
    let app = Router::new()
        .nest("/api", api_router)
        .nest_service("/", ServeDir::new(&dist_dir).fallback(ServeDir::new(&dist_dir)))
        .layer(cors)
        .with_state(state);

    // Default 18080: 8080 is often reserved by Hyper-V/Windows NAT excludedportrange.
    let port: u16 = std::env::var("DDL_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18080);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to bind {addr}: {e}. Try set DDL_PORT to a free port."))?;
    info!("Server listening on http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check(State(state): State<AppState>) -> Json<serde_json::Value> {
    let sidecar_ok = state.task_manager.check_sidecar().await;
    let ffmpeg_ok = state.task_manager.check_ffmpeg().await;
    Json(serde_json::json!({
        "status": if sidecar_ok { "ok" } else { "degraded" },
        "sidecar": sidecar_ok,
        "ffmpeg": ffmpeg_ok,
        "project_root": state.project_root.display().to_string(),
    }))
}

#[derive(Deserialize)]
struct ResolveRequest {
    url: String,
    cookie: Option<String>,
    proxy: Option<String>,
}

async fn resolve_url(
    State(state): State<AppState>,
    Json(payload): Json<ResolveRequest>,
) -> Result<Json<MediaMetadata>, (StatusCode, String)> {
    match state
        .sidecar
        .resolve_url(&payload.url, payload.cookie.as_deref(), payload.proxy.as_deref())
        .await
    {
        Ok(meta) => Ok(Json(meta)),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

#[derive(Deserialize)]
struct ResolveDouyinBatchRequest {
    batch_type: String,
    target_id: String,
    max_count: Option<u32>,
    cookie: Option<String>,
    proxy: Option<String>,
}

async fn resolve_douyin_batch(
    State(state): State<AppState>,
    Json(payload): Json<ResolveDouyinBatchRequest>,
) -> Result<Json<MediaMetadata>, (StatusCode, String)> {
    match state
        .sidecar
        .resolve_douyin_batch(
            &payload.batch_type,
            &payload.target_id,
            payload.max_count.unwrap_or(30),
            payload.cookie.as_deref(),
            payload.proxy.as_deref(),
        )
        .await
    {
        Ok(meta) => Ok(Json(meta)),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

#[derive(Deserialize)]
struct CheckCookieRequest {
    platform: String,
    cookie: String,
}

async fn check_cookie(
    State(state): State<AppState>,
    Json(payload): Json<CheckCookieRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match state
        .sidecar
        .check_cookie(&payload.platform, &payload.cookie)
        .await
    {
        Ok(val) => Ok(Json(val)),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<TaskProgress>> {
    let tasks = state.task_manager.get_all_tasks().await;
    Json(tasks)
}

async fn create_task(
    State(state): State<AppState>,
    Json(payload): Json<CreateTaskRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match state.task_manager.create_task(payload).await {
        Ok(id) => Ok(Json(serde_json::json!({ "task_id": id }))),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

async fn cancel_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    let ok = state.task_manager.cancel_task(&id).await;
    Json(serde_json::json!({ "success": ok }))
}

async fn retry_task(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    match state.task_manager.retry_task(&id).await {
        Ok(new_id) => Ok(Json(serde_json::json!({ "task_id": new_id }))),
        Err(err) => Err((StatusCode::BAD_REQUEST, err.to_string())),
    }
}

#[derive(Deserialize)]
struct PaginationQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn list_history(
    State(state): State<AppState>,
    Query(q): Query<PaginationQuery>,
) -> Result<Json<Vec<DownloadRecord>>, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(50);
    let offset = q.offset.unwrap_or(0);
    match state.db.get_recent_records(limit, offset) {
        Ok(recs) => Ok(Json(recs)),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}

#[derive(Deserialize)]
struct SearchQuery {
    q: String,
    limit: Option<usize>,
}

async fn search_history(
    State(state): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<Vec<DownloadRecord>>, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(50);
    match state.db.search_records(&q.q, limit) {
        Ok(recs) => Ok(Json(recs)),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut tx, mut rx_client) = socket.split();
    let mut rx_progress = state.task_manager.subscribe_progress();

    loop {
        tokio::select! {
            // Client disconnect / close / ping-pong
            client_msg = rx_client.next() => {
                match client_msg {
                    None => break,
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
            // Broadcast progress
            prog = rx_progress.recv() => {
                match prog {
                    Ok(prog) => {
                        if let Ok(msg_text) = serde_json::to_string(&prog) {
                            if tx.send(Message::Text(msg_text)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("WebSocket progress subscriber lagged by {} messages", n);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}
