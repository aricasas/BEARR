use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct TestPath {
    inner: PathBuf,
}

fn get_path(base: &str, name: &str) -> PathBuf {
    Path::new("test_files").join(base).join(name)
}

fn delete_path(path: &Path) {
    if path.is_dir() {
        _ = fs::remove_dir_all(path);
    } else {
        _ = fs::remove_file(path);
    }
}

impl TestPath {
    pub fn new(base: &str, name: &str) -> Self {
        let path = get_path(base, name);
        delete_path(&path);
        Self { inner: path }
    }
}

impl AsRef<Path> for TestPath {
    fn as_ref(&self) -> &Path {
        &self.inner
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        if cfg!(feature = "delete_test_files") {
            delete_path(&self.inner);
        }
    }
}
