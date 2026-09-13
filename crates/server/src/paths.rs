use std::path::{Path, PathBuf};

/// Locate the project root (directory containing sidecar/server.py + Cargo workspace).
///
/// Resolution order:
/// 1. `DDL_PROJECT_ROOT` env var
/// 2. Walk up from current directory
/// 3. Walk up from current executable (`target/debug/server.exe` → project root)
/// 4. Fall back to current directory
pub fn project_root() -> PathBuf {
    if let Ok(p) = std::env::var("DDL_PROJECT_ROOT") {
        let p = PathBuf::from(p);
        if is_project_root(&p) {
            return p;
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        if let Some(root) = walk_up_for_root(&cwd) {
            return root;
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            if let Some(root) = walk_up_for_root(dir) {
                return root;
            }
        }
    }

    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn is_project_root(dir: &Path) -> bool {
    dir.join("sidecar").join("server.py").is_file() && dir.join("Cargo.toml").is_file()
}

fn walk_up_for_root(start: &Path) -> Option<PathBuf> {
    let mut dir = Some(start);
    while let Some(d) = dir {
        if is_project_root(d) {
            return Some(d.to_path_buf());
        }
        dir = d.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_root_from_nested_dir() {
        let root = project_root();
        assert!(
            root.join("sidecar/server.py").is_file(),
            "project_root should resolve to a directory containing sidecar/server.py, got {:?}",
            root
        );
    }
}
