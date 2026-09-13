use core_engine::db::Database;
use core_engine::downloader::RateLimiter;
use core_engine::models::DownloadRecord;
use core_engine::sidecar::SidecarClient;
use std::path::{Path, PathBuf};

/// Workspace root = parent of the `crates/` directory containing this package.
fn workspace_root() -> PathBuf {
    // Prefer env override for CI / relocated checkouts.
    if let Ok(p) = std::env::var("DDL_PROJECT_ROOT") {
        return PathBuf::from(p);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // crates/core-engine → project root
    manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or(manifest)
}

fn python_bin(root: &Path) -> PathBuf {
    root.join("bilibili-downloader/.venv/Scripts/python.exe")
}

fn sidecar_script(root: &Path) -> PathBuf {
    root.join("sidecar/server.py")
}

#[tokio::test]
async fn test_database_and_fts() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let db_path = tmp_dir.path().join("test.db");
    let db = Database::new(&db_path).unwrap();

    let rec1 = DownloadRecord {
        id: None,
        url: "https://www.bilibili.com/video/BV1xx411c7mD".to_string(),
        status: "success".to_string(),
        reason: "".to_string(),
        title: "Rust 现代化重构实战视频".to_string(),
        platform: "bilibili".to_string(),
        author: "TechDev".to_string(),
        duration: 360,
        file_size: 1048576,
        file_format: "mp4".to_string(),
        output_path: "downloads/rust.mp4".to_string(),
        timestamp: "2026-03-08 12:00:00".to_string(),
    };

    let id = db.add_record(&rec1).unwrap();
    assert!(id > 0);

    let recent = db.get_recent_records(10, 0).unwrap();
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].title, "Rust 现代化重构实战视频");

    // Full text & substring search
    let search_results = db.search_records("重构", 10).unwrap();
    assert_eq!(search_results.len(), 1);
    assert_eq!(search_results[0].id, Some(id));
}

#[tokio::test]
async fn test_rate_limiter() {
    let limiter = RateLimiter::new(1000); // 1000 bytes per second
    limiter.consume(500).await;
}

#[tokio::test]
async fn test_sidecar_ping() {
    let root = workspace_root();
    let python = python_bin(&root);
    let script = sidecar_script(&root);

    assert!(
        python.exists(),
        "python venv missing at {:?}; create it or set DDL_PROJECT_ROOT",
        python
    );
    assert!(
        script.exists(),
        "sidecar script missing at {:?}; set DDL_PROJECT_ROOT",
        script
    );

    let sidecar = SidecarClient::new(&python, &script).await.unwrap();
    sidecar.ping().await.unwrap();

    let res = sidecar
        .check_cookie("douyin", "sessionid=test_dummy")
        .await
        .unwrap();
    assert!(res.get("valid").is_some());
}
