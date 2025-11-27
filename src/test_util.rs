use std::{
    fs,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use crate::file_system::FileSystem;

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

pub fn assert_panics(mut f: impl FnMut()) {
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f();
        }))
        .is_err()
    );
}

pub struct TestFs {
    prefix: PathBuf,
    fs: FileSystem,
}

/*
 * Should create directories before starting each test
 * */
impl TestFs {
    pub fn new(prefix: impl AsRef<Path>) -> Self {
        let _ = fs::create_dir_all(&prefix);
        Self {
            prefix: prefix.as_ref().to_owned(),
            fs: FileSystem::new(prefix, 16, 1).unwrap(),
        }
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

/*
 * Everything should be deleted after the tests are done
 * */
impl Drop for TestFs {
    fn drop(&mut self) {
        if cfg!(feature = "delete_test_files") {
            delete_path(&self.prefix);
        }
    }
}
