use std::path::PathBuf;

/// A temporary database directory that cleans up after itself
pub struct TestDb {
    pub dir: PathBuf,
    pub storage: abcsql::Storage,
}

impl TestDb {
    pub fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("abcsql_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let storage = abcsql::Storage::new(&dir).expect("failed to create test storage");
        TestDb { dir, storage }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
