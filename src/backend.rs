use crate::handle::{DirFlags, DirHandle, FileHandle, VfsFlags, VfsHandle};
use crate::sequential::SeqLockHandle;
use crate::{VfsPoint, VfsUser};
use arbhx::DataUsage;
use arbhx::fs::Metadata;
use async_trait::async_trait;
use bytes::Bytes;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::ffi::OsStr;
use std::fmt::Debug;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

#[async_trait]
pub trait UserVfs: Send + Sync + Debug + Unpin + 'static {
    fn get_user(&self) -> VfsUser;
    fn get_handles(&self) -> Vec<VfsHandle>;
    async fn get_info(&mut self, path: &Path) -> io::Result<VfsInfo>;
    async fn get_infos(&mut self) -> io::Result<Vec<VfsInfo>>;
    async fn open_dir(&mut self, path: &Path, flags: DirFlags) -> io::Result<DirHandle>;
    async fn open_file(&mut self, path: &Path, flags: VfsFlags) -> io::Result<FileHandle>;
    async fn open_seq(&mut self, path: &Path) -> io::Result<Mutex<SeqLockHandle>>;
    async fn close(&mut self, handle: Uuid) -> io::Result<()>;
    async fn read(&mut self, handle: Uuid, offset: u64, length: u64) -> io::Result<Bytes>;
    async fn write(&mut self, handle: Uuid, offset: u64, data: Bytes) -> io::Result<usize>;
    //async fn remove_file(&mut self, path: &Path) -> io::Result<()>;
    //async fn remove_dir(&mut self, path: &Path) -> io::Result<()>;
    async fn remove(&mut self, path: &Path) -> io::Result<()>;
    async fn create_dir(&mut self, path: &Path) -> io::Result<()>;
    async fn stat_f(&mut self, path: &Path) -> io::Result<VfsMetadata>;
    async fn stat_h(&mut self, handle: Uuid) -> io::Result<VfsMetadata>;
    async fn list_f(&mut self, path: &Path) -> io::Result<Vec<VfsMetadata>>;
    async fn list_h(&mut self, handle: Uuid) -> io::Result<Vec<VfsMetadata>>;
    async fn copy(&mut self, src: &Path, dest: &Path, overwrite: bool) -> io::Result<()>;
    async fn rename(&mut self, src: &Path, dest: &Path, overwrite: bool) -> io::Result<()>;
}

#[derive(Error, Debug)]
pub enum UserAuthError {
    #[error("Invalid login!")]
    InvalidLogin,

    #[error("Not supported method")]
    NotSupported,

    #[error(transparent)]
    IoError(#[from] io::Error),
}

pub type AuthResult<T> = Result<T, UserAuthError>;

#[async_trait]
pub trait VfsAuth: Send + Sync {
    async fn auth_pass(&self, username: &str, password: &str) -> AuthResult<Box<dyn UserVfs>>;
    async fn auth_key(&self, username: &str, key: &str) -> AuthResult<Box<dyn UserVfs>>;
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct VfsMetadata {
    pub(crate) path: PathBuf,
    pub(crate) is_dir: bool,
    pub(crate) vfs: Option<VfsPoint>,
    pub(crate) meta: Option<Metadata>,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct VfsInfo {
    pub(crate) path: PathBuf,
    pub(crate) usage: Option<DataUsage>,
    pub(crate) vfs: Option<VfsPoint>,
}

impl VfsInfo {
    pub(crate) fn new(path: PathBuf, usage: Option<DataUsage>, vfs: Option<VfsPoint>) -> Self {
        Self { path, usage, vfs }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn usage(&self) -> Option<DataUsage> {
        self.usage.clone()
    }
    pub fn vfs(&self) -> Option<VfsPoint> {
        self.vfs.clone()
    }
}

impl VfsMetadata {
    pub(crate) fn new(
        path: PathBuf,
        is_dir: bool,
        vfs: Option<VfsPoint>,
        meta: Option<Metadata>,
    ) -> Self {
        Self {
            path,
            vfs,
            is_dir,
            meta,
        }
    }

    /// # Returns
    /// The [`VfsPoint`] represented by this path.
    pub fn vfs(&self) -> Option<VfsPoint> {
        self.vfs.clone()
    }

    /// # Returns
    /// The [`Path::file_name`] of this file.
    pub fn name(&self) -> &OsStr {
        self.path.file_name().unwrap_or_default()
    }

    /// # Returns
    /// The full, absolute [`Path`] of the node.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// # Returns
    /// If the [`Path`] is a directory.
    pub fn is_dir(&self) -> bool {
        self.is_dir
    }

    /// # Returns
    /// If the [`Path`] is a file (not a directory).
    ///
    /// # Important
    /// This backend does <b>NOT support symbolic links</b>. As a result, this
    /// function is simply the inverse of [`is_dir`] - with no special processing.
    pub fn is_file(&self) -> bool {
        !self.is_dir
    }

    /// # Returns
    /// The last modified [`DateTime`] if supported.
    pub fn mtime(&self) -> Option<DateTime<Utc>> {
        self.meta.clone().map(|x| x.mtime()).flatten()
    }

    /// # Returns
    /// The last accessed [`DateTime`] if supported.
    pub fn atime(&self) -> Option<DateTime<Utc>> {
        self.meta.clone().map(|x| x.atime()).flatten()
    }

    /// # Returns
    /// The [`ByteSize`] of this node.
    pub fn size(&self) -> ByteSize {
        self.meta.clone().map(|x| x.size()).unwrap_or_default()
    }

    /// # Returns
    /// If the [`VfsPoint`] is writable at this [`Path`].
    pub fn writable(&self) -> bool {
        self.vfs.clone().map(|x| x.can_write).unwrap_or(false)
    }
}
