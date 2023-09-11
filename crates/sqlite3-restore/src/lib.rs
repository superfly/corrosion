use std::{
    fs::File,
    io::{Read, Seek},
    os::{fd::AsRawFd, unix::prelude::FileExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use nix::{
    fcntl::{fcntl, FcntlArg},
    libc::{flock, SEEK_SET},
};
use tracing::info;

// Database file lock bytes.
const PENDING: i64 = 0x40000000;
const RESERVED: i64 = 0x40000001;
const SHARED: i64 = 0x40000002;

// SHM file lock bytes.
const WRITE: i64 = 120;
const CKPT: i64 = 121;
const RECOVER: i64 = 122;
const READ0: i64 = 123;
const READ1: i64 = 124;
const READ2: i64 = 125;
const READ3: i64 = 126;
const READ4: i64 = 127;
const DMS: i64 = 128;

const MIN_DB_HDR_READ_LEN: usize = 20;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("acquiring lock timed out")]
    LockTimedOut,

    #[error("inconsistent restore copy, expected {expected} bytes, but copied {actual}")]
    InconsistentCopy { expected: u64, actual: u64 },

    #[error("header read was too short ({0} bytes), need at least {MIN_DB_HDR_READ_LEN}")]
    HeaderShortRead(usize),

    #[error("database read and write format mismatched: {read} != {write}")]
    ReadWriteFormatMismatch { read: u8, write: u8 },
}

pub struct Restored {
    pub old_len: u64,
    pub new_len: u64,
    pub is_wal: bool,
}

pub fn restore<P1: AsRef<Path>, P2: AsRef<Path>>(
    src: P1,
    dst: P2,
    timeout: Duration,
) -> Result<Restored, Error> {
    let mut src_db_file = std::fs::OpenOptions::new().read(true).open(src.as_ref())?;
    let mut dst_db_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(dst.as_ref())?;

    let mut dst_locked = lock_all(&mut dst_db_file, dst.as_ref(), timeout)?;

    let dst_journal_path = format!("{}-journal", dst.as_ref().display());
    info!("removing the journal file at: '{dst_journal_path}'");
    if let Err(e) = std::fs::remove_file(PathBuf::from(dst_journal_path)) {
        if e.kind() != std::io::ErrorKind::NotFound {
            return Err(e.into());
        }
    }

    if dst_locked.is_wal() {
        let dst_wal_path = format!("{}-wal", dst.as_ref().display());
        info!("truncating WAL file '{dst_wal_path}'");
        let wal_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(PathBuf::from(dst_wal_path))?;
        wal_file.set_len(0)?;
    }

    let src_meta = src_db_file.metadata()?;
    let dst_meta = dst_db_file.metadata()?;

    dst_db_file.seek(std::io::SeekFrom::Start(0))?;
    src_db_file.seek(std::io::SeekFrom::Start(0))?;

    info!(
        "copying from source to destination: {}",
        src.as_ref().display()
    );

    let n = std::io::copy(&mut src_db_file, &mut dst_db_file)?;
    if n != src_meta.len() {
        return Err(Error::InconsistentCopy {
            expected: src_meta.len(),
            actual: n,
        });
    }

    dst_db_file.set_len(src_meta.len())?;
    dst_db_file.sync_all()?;

    if let Locked::Wal(ref mut dst_shm_file) = dst_locked {
        dst_shm_file.write_at(&[136], 0)?;
    }

    info!("done");

    Ok(Restored {
        old_len: dst_meta.len(),
        new_len: src_meta.len(),
        is_wal: dst_locked.is_wal(),
    })
}

enum Locked {
    Wal(File),
    Other,
}

impl Locked {
    fn is_wal(&self) -> bool {
        matches!(self, Locked::Wal(_))
    }
}

fn lock_all<P: AsRef<Path>>(
    db_file: &mut File,
    db_path: P,
    timeout: Duration,
) -> Result<Locked, Error> {
    info!("locking to determine journal mode");

    lock(db_file, LockType::Read, PENDING, timeout)?;
    lock(db_file, LockType::Read, SHARED, timeout)?;
    lock(db_file, LockType::Unlock, PENDING, timeout)?;

    info!("reading database mode");

    let is_wal = is_wal_mode(db_file)?;

    if !is_wal {
        info!("destination database is in a non-WAL journal mode");

        lock(db_file, LockType::Write, RESERVED, timeout)?;
        lock(db_file, LockType::Write, PENDING, timeout)?;
        lock(db_file, LockType::Write, SHARED, timeout)?;
        return Ok(Locked::Other);
    }

    info!("destination database is in WAL journal mode");

    let shm_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(PathBuf::from(format!("{}-shm", db_path.as_ref().display())))?;

    lock(&shm_file, LockType::Read, DMS, timeout)?;
    lock(&shm_file, LockType::Write, WRITE, timeout)?;
    lock(&shm_file, LockType::Write, CKPT, timeout)?;
    lock(&shm_file, LockType::Write, RECOVER, timeout)?;
    lock(&shm_file, LockType::Write, READ0, timeout)?;
    lock(&shm_file, LockType::Write, READ1, timeout)?;
    lock(&shm_file, LockType::Write, READ2, timeout)?;
    lock(&shm_file, LockType::Write, READ3, timeout)?;
    lock(&shm_file, LockType::Write, READ4, timeout)?;

    Ok(Locked::Wal(shm_file))
}

#[allow(clippy::unnecessary_cast)] // required for other platforms
#[derive(Debug, Clone, Copy)]
#[repr(i16)]
enum LockType {
    Read = nix::libc::F_RDLCK as i16,
    Write = nix::libc::F_WRLCK as i16,
    Unlock = nix::libc::F_UNLCK as i16,
}

fn lock(f: &File, l_type: LockType, l_start: i64, timeout: Duration) -> Result<(), Error> {
    let mut l_len = 1i64;

    if l_start == SHARED {
        l_len = 510;
    }

    info!(
        "acquiring lock ({l_type:?} ({}),{l_start},{l_len}) on fd: {}",
        l_type as i16,
        f.as_raw_fd()
    );

    let started_at = Instant::now();
    loop {
        let flock = flock {
            l_start,
            l_len,
            l_pid: 0,
            l_type: l_type as i16,
            l_whence: SEEK_SET as i16,
        };
        match fcntl(f.as_raw_fd(), FcntlArg::F_SETLK(&flock)) {
            Ok(_) => {
                info!("lock acquired");
                return Ok(());
            }
            Err(_e) => {
                // still waiting on lock...
            }
        }
        if started_at.elapsed() > timeout {
            return Err(Error::LockTimedOut);
        }
    }
}

fn is_wal_mode(f: &mut File) -> Result<bool, Error> {
    let mut hdr = [0u8; 100];

    match f.read(&mut hdr) {
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
        Err(e) => return Err(e.into()),
        Ok(n) => {
            if n < MIN_DB_HDR_READ_LEN {
                return Err(Error::HeaderShortRead(n));
            }
        }
    }

    if hdr[18] != hdr[19] {
        return Err(Error::ReadWriteFormatMismatch {
            read: hdr[18],
            write: hdr[19],
        });
    }

    Ok(hdr[18] == 2)
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::*;

    #[test]
    fn test_basic_non_wal() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::TempDir::new()?;

        let src = tmpdir.path().join("src.db");
        let dst = tmpdir.path().join("dst.db");

        // seed the dst db
        {
            let conn = Connection::open(&dst)?;

            conn.execute_batch(
                "CREATE TABLE foo (a INT PRIMARY KEY, b INT); INSERT INTO foo (a,b) VALUES (1,1);",
            )?;
        }

        {
            let conn = Connection::open(&src)?;
            conn.execute_batch(
                "CREATE TABLE foo (a INT PRIMARY KEY, b INT); INSERT INTO foo (a,b) VALUES (1,2);",
            )?;
        }

        let restored = restore(src, &dst, Duration::from_secs(2))?;
        assert!(!restored.is_wal);

        {
            let conn = Connection::open(&dst)?;

            let journal_mode: String =
                conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;

            assert_eq!(journal_mode, "delete");

            let (a, b): (i64, i64) =
                conn.query_row("SELECT a,b FROM foo WHERE a = ?;", [1], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;

            assert_eq!(a, 1);
            assert_eq!(b, 2);
        }

        Ok(())
    }

    #[test]
    fn test_basic_wal() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::TempDir::new()?;

        let src = tmpdir.path().join("src.db");
        let dst = tmpdir.path().join("dst.db");

        // seed dst
        {
            let conn = Connection::open(&dst)?;
            conn.execute_batch("PRAGMA journal_mode = WAL;")?;

            conn.execute_batch(
                "CREATE TABLE foo (a INT PRIMARY KEY, b INT); INSERT INTO foo (a,b) VALUES (1,1);",
            )?;

            let journal_mode: String =
                conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;

            assert_eq!(journal_mode, "wal");
        }

        {
            let conn = Connection::open(&src)?;
            conn.execute_batch("PRAGMA journal_mode = WAL;")?;

            conn.execute_batch(
                "CREATE TABLE foo (a INT PRIMARY KEY, b INT); INSERT INTO foo (a,b) VALUES (1,2);",
            )?;

            let journal_mode: String =
                conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;

            assert_eq!(journal_mode, "wal");
        }

        let restored = restore(src, &dst, Duration::from_secs(2))?;
        assert!(restored.is_wal);

        {
            let conn = Connection::open(&dst)?;

            let journal_mode: String =
                conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;

            assert_eq!(journal_mode, "wal");

            let (a, b): (i64, i64) =
                conn.query_row("SELECT a,b FROM foo WHERE a = ?;", [1], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;

            assert_eq!(a, 1);
            assert_eq!(b, 2);
        }

        Ok(())
    }
}
