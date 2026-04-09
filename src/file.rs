use arbhx_core::{
    DataFull, DataRead, DataReadSeek, DataWrite, DataWriteSeek, Metadata, VfsBackend,
};
use std::io;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct DataFile {
    pub path: PathBuf,
    pub meta: Metadata,
    pub be: Arc<dyn VfsBackend>,
}

impl DataFile {
    pub fn new(path: &Path, meta: Metadata, be: Arc<dyn VfsBackend>) -> Self {
        Self {
            path: path.to_path_buf(),
            meta,
            be,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> Metadata {
        self.meta.clone()
    }

    pub async fn open_read(&self) -> io::Result<Box<dyn DataRead>> {
        self.be
            .clone()
            .reader()
            .ok_or(ErrorKind::Unsupported)?
            .open_read_start(&self.path)
            .await
    }

    pub async fn open_read_full(&self) -> io::Result<Box<dyn DataReadSeek>> {
        self.be
            .clone()
            .reader()
            .ok_or(ErrorKind::Unsupported)?
            .open_read_random(&self.path)
            .await?
            .ok_or(ErrorKind::Unsupported.into())
    }

    pub async fn open_append(&self, overwrite: bool) -> io::Result<Box<dyn DataWrite>> {
        self.be
            .clone()
            .writer()
            .ok_or(ErrorKind::Unsupported)?
            .open_write_append(&self.path, overwrite)
            .await
    }

    pub async fn open_write_full(&self) -> io::Result<Box<dyn DataWriteSeek>> {
        self.be
            .clone()
            .writer()
            .ok_or(ErrorKind::Unsupported)?
            .open_write_random(&self.path)
            .await?
            .ok_or(ErrorKind::Unsupported.into())
    }

    pub async fn open_full(&self) -> io::Result<Box<dyn DataFull>> {
        self.be
            .clone()
            .full()
            .ok_or(ErrorKind::Unsupported)?
            .open_full_random()
            .await?
            .ok_or(ErrorKind::Unsupported.into())
    }
}
