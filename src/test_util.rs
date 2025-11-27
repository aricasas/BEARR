use std::{
    fs,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use crate::file_system::FileSystem;

pub struct TestPath {
    inner: PathBuf,
}

fn get_path_base(base: &str) -> PathBuf {
    Path::new("test_files").join(base)
}

fn get_path(base: &str, name: &str) -> PathBuf {
    get_path_base(base).join(name)
}

fn delete_path(path: &Path) {
    if path.is_dir() {
        _ = fs::remove_dir_all(path);
    } else {
        _ = fs::remove_file(path);
    }
}

impl TestPath {
    pub fn create(base: &str, name: &str) -> Self {
        let path = get_path(base, name);
        delete_path(&path);
        fs::create_dir_all(get_path_base(base)).unwrap();
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
        if !cfg!(feature = "keep_test_files") {
            delete_path(&self.inner);
        }
    }
}

pub fn assert_panics(mut f: impl FnMut()) {
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f();
        }))
        .is_err()
    );
}

pub struct TestFs {
    // Used for dropping
    _path: TestPath,
    fs: FileSystem,
}
impl TestFs {
    pub fn create(base: &str, name: &str) -> Self {
        let path = TestPath::create(base, name);
        fs::create_dir_all(&path).unwrap();
        let fs = FileSystem::new(&path, 16, 1).unwrap();
        Self { _path: path, fs }
    }
}

impl Deref for TestFs {
    type Target = FileSystem;

    fn deref(&self) -> &Self::Target {
        &self.fs
    }
}

impl DerefMut for TestFs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fs
    }
}
