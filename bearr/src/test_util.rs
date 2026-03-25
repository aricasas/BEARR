use std::{
    fs,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use crate::file_system::FileSystem;

/// A path used in testing, automatically creating and deleting files as needed.
pub struct TestPath {
    inner: PathBuf,
}

fn get_path_base(base: &str) -> PathBuf {
    Path::new("test_files").join(base)
}

pub fn get_path(base: &str, name: &str) -> PathBuf {
    get_path_base(base).join(name)
}

fn delete_path(path: &Path) {
    _ = fs::remove_dir_all(path);
}

impl TestPath {
    /// Creates a test path located at `<current folder>/test_files/{base}/{name}`.
    /// Any existing files at the path are deleted,
    /// and a folder is created at `<current folder>/test_files/{base}` if it doesn't already exist.
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

/// Removes all files at the path, unless the `keep_test_files` feature is active.
impl Drop for TestPath {
    fn drop(&mut self) {
        if !cfg!(feature = "keep_test_files") {
            delete_path(&self.inner);
        }
    }
}

/// Asserts that the given operation panics when executed.
pub fn assert_panics(mut f: impl FnMut()) {
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            f();
        }))
        .is_err()
    );
}

/// A file system used in testing, automatically creating and deleting files as needed.
pub struct TestFs {
    // Used for dropping
    _path: TestPath,
    fs: FileSystem,
}

impl TestFs {
    /// Creates a test file system located at `<current folder>/test_files/{base}/{name}`.
    /// Any existing files at the path are deleted,
    /// and a folder is created at the path if it doesn't already exist.
    pub fn create(base: &str, name: &str) -> Self {
        let path = TestPath::create(base, name);
        fs::create_dir_all(&path).unwrap();
        let fs = FileSystem::new(&path, 16, 1, 1).unwrap();
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
