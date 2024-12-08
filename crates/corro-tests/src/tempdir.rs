use std::ops::Deref;
use std::{env, mem};

pub struct TempDir(Option<tempfile::TempDir>);

impl TempDir {
    pub fn new(inner: tempfile::TempDir) -> Self {
        Self(Some(inner))
    }
}

impl Deref for TempDir {
    type Target = tempfile::TempDir;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if env::var_os("NO_TEMPDIR_CLEANUP").is_some() {
            if let Some(dir) = &self.0 {
                println!("Not cleaning up temp dir {}", dir.path().display());
            }
            mem::forget(self.0.take())
        }
    }
}
