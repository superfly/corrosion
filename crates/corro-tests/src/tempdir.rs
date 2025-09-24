use std::ops::Deref;
use std::env;

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
            if let Some(dir) = &mut self.0 {
                dir.disable_cleanup(true);
                dbg!(dir.path().display());
                println!("Not cleaning up temp dir {}", dir.path().display());
            }
        }
    }
}
