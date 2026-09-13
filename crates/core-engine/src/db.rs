use crate::models::DownloadRecord;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Database {
    conn: Arc<Mutex<Connection>>,
    #[allow(dead_code)]
    db_path: PathBuf,
}

impl Database {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(&db_path)
            .with_context(|| format!("Failed to open sqlite database at {:?}", db_path))?;

        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path,
        };

        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.conn.lock();

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS records (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                url         TEXT    NOT NULL,
                status      TEXT    NOT NULL DEFAULT '',
                reason      TEXT    NOT NULL DEFAULT '',
                title       TEXT    NOT NULL DEFAULT '',
                platform    TEXT    NOT NULL DEFAULT '',
                author      TEXT    NOT NULL DEFAULT '',
                duration    INTEGER NOT NULL DEFAULT 0,
                file_size   INTEGER NOT NULL DEFAULT 0,
                file_format TEXT    NOT NULL DEFAULT '',
                output_path TEXT    NOT NULL DEFAULT '',
                timestamp   TEXT    NOT NULL DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_records_url ON records(url);
            CREATE INDEX IF NOT EXISTS idx_records_status ON records(status);
            CREATE INDEX IF NOT EXISTS idx_records_platform ON records(platform);
            CREATE INDEX IF NOT EXISTS idx_records_timestamp ON records(timestamp);

            CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
                title,
                url,
                content='records',
                content_rowid='id',
                tokenize='unicode61'
            );

            CREATE TRIGGER IF NOT EXISTS records_ai AFTER INSERT ON records BEGIN
                INSERT INTO records_fts(rowid, title, url)
                VALUES (new.id, new.title, new.url);
            END;

            CREATE TRIGGER IF NOT EXISTS records_ad AFTER DELETE ON records BEGIN
                INSERT INTO records_fts(records_fts, rowid, title, url)
                VALUES ('delete', old.id, old.title, old.url);
            END;

            CREATE TRIGGER IF NOT EXISTS records_au AFTER UPDATE ON records BEGIN
                INSERT INTO records_fts(records_fts, rowid, title, url)
                VALUES ('delete', old.id, old.title, old.url);
                INSERT INTO records_fts(rowid, title, url)
                VALUES (new.id, new.title, new.url);
            END;

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )?;

        Ok(())
    }

    pub fn add_record(&self, rec: &DownloadRecord) -> Result<i64> {
        let conn = self.conn.lock();
        conn.execute(
            r#"
            INSERT INTO records (
                url, status, reason, title, platform, author,
                duration, file_size, file_format, output_path, timestamp
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                rec.url,
                rec.status,
                rec.reason,
                rec.title,
                rec.platform,
                rec.author,
                rec.duration,
                rec.file_size,
                rec.file_format,
                rec.output_path,
                rec.timestamp
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    pub fn get_recent_records(&self, limit: usize, offset: usize) -> Result<Vec<DownloadRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare(
            r#"
            SELECT id, url, status, reason, title, platform, author,
                   duration, file_size, file_format, output_path, timestamp
            FROM records
            ORDER BY id DESC
            LIMIT ?1 OFFSET ?2
            "#,
        )?;

        let rows = stmt.query_map(params![limit as i64, offset as i64], |row| {
            Ok(DownloadRecord {
                id: Some(row.get(0)?),
                url: row.get(1)?,
                status: row.get(2)?,
                reason: row.get(3)?,
                title: row.get(4)?,
                platform: row.get(5)?,
                author: row.get(6)?,
                duration: row.get(7)?,
                file_size: row.get(8)?,
                file_format: row.get(9)?,
                output_path: row.get(10)?,
                timestamp: row.get(11)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn search_records(&self, keyword: &str, limit: usize) -> Result<Vec<DownloadRecord>> {
        let conn = self.conn.lock();
        let like_query = format!("%{}%", keyword);
        let mut stmt = conn.prepare(
            r#"
            SELECT id, url, status, reason, title, platform, author,
                   duration, file_size, file_format, output_path, timestamp
            FROM records
            WHERE title LIKE ?1 OR url LIKE ?1 OR author LIKE ?1
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )?;

        let rows = stmt.query_map(params![like_query, limit as i64], |row| {
            Ok(DownloadRecord {
                id: Some(row.get(0)?),
                url: row.get(1)?,
                status: row.get(2)?,
                reason: row.get(3)?,
                title: row.get(4)?,
                platform: row.get(5)?,
                author: row.get(6)?,
                duration: row.get(7)?,
                file_size: row.get(8)?,
                file_format: row.get(9)?,
                output_path: row.get(10)?,
                timestamp: row.get(11)?,
            })
        })?;

        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT value FROM settings WHERE key = ?1")?;
        let mut rows = stmt.query(params![key])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn get_all_settings(&self) -> Result<Vec<(String, String)>> {
        let conn = self.conn.lock();
        let mut stmt = conn.prepare("SELECT key, value FROM settings ORDER BY key")?;
        let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
        let mut list = Vec::new();
        for r in rows {
            list.push(r?);
        }
        Ok(list)
    }

    pub fn delete_setting(&self, key: &str) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute("DELETE FROM settings WHERE key = ?1", params![key])?;
        Ok(())
    }
}
