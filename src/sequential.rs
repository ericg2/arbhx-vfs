use std::io;
use std::io::{ErrorKind, Read};
use std::path::PathBuf;
use arbhx_core::{DataReadSeek, DataWrite};
use crate::file::DataFile;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum WriteMode {
    Append,
    Overwrite, // truncate
}

#[derive(Debug)]
pub struct SeqLockHandle {
    path: PathBuf,
    file: DataFile,
    read: Option<Box<dyn DataReadSeek>>,
    write: Option<Box<dyn DataWrite>>,
    mode: WriteMode,
    dirty: bool,
}

impl SeqLockHandle {
    pub fn new(path: PathBuf, file: DataFile, truncate: bool) -> Self {
        Self {
            path,
            file,
            read: None,
            write: None,
            mode: if truncate {
                WriteMode::Overwrite
            } else {
                WriteMode::Append
            },
            dirty: truncate, // truncate must commit even without writes
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_owned()
    }

    pub async fn lock_read(&mut self) -> io::Result<&mut dyn DataReadSeek> {
        // If writing, close (commit) first
        if let Some(mut w) = self.write.take() {
            w.close().await?;
            // after first commit, all future writes must be appended
            self.mode = WriteMode::Append;
        }

        if self.read.is_none() {
            // If truncate happened but nothing written, treat as empty file
            if self.dirty && self.write.is_none() {
                // backend-specific: either return empty reader or just open normally
                // assuming backend now reflects empty file after commit
            }
            self.read = Some(self.file.open_read_full().await?);
        }

        self.read
            .as_deref_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "failed to acquire read handle"))
    }

    pub async fn lock_write(&mut self) -> io::Result<&mut dyn DataWrite> {
        // drop read handle (no commit needed)
        self.read = None;

        if self.write.is_none() {
            let append = matches!(self.mode, WriteMode::Append);
            self.write = Some(self.file.open_append(append).await?);
        }

        self.dirty = true;

        self.write
            .as_deref_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::Other, "failed to acquire write handle"))
    }
}