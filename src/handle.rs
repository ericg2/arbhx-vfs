use std::path::PathBuf;
use std::sync::Arc;
use arbhx_core::{DataFull, DataRead, DataReadSeek, DataWrite, Metadata, VfsBackend};
use bitflags::bitflags;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::sequential::SeqLockHandle;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct VfsFlags(u32);

bitflags! {
    impl VfsFlags: u32 {
        const READ = 0x00000001;
        const WRITE = 0x00000002;
        const APPEND = 0x00000004;
        const CREATE = 0x00000008;
        const TRUNCATE = 0x00000010;
        const EXCLUDE = 0x00000020;
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct DirFlags(u32);

bitflags! {
    impl DirFlags: u32 {
        const READ = 0x00000001;
        const CREATE = 0x00000002;
        const EXCLUDE = 0x00000004;
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DirHandle {
    pub id: Uuid,
    pub path: PathBuf,
    pub flags: DirFlags,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileHandle {
    pub id: Uuid,
    pub path: PathBuf,
    pub flags: VfsFlags,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum VfsHandle {
    Directory(DirHandle),
    File(FileHandle),
}

impl From<&VirtualHandle> for VfsHandle {
    fn from(value: &VirtualHandle) -> Self {
        match value.mode {
            HandleMode::Read(_) => VfsHandle::File(FileHandle {
                id: value.id,
                path: value.path.clone(),
                flags: VfsFlags::READ,
            }),
            HandleMode::FullRW(_) => VfsHandle::File(FileHandle {
                id: value.id,
                path: value.path.clone(),
                flags: VfsFlags::READ | VfsFlags::WRITE,
            }),
            HandleMode::AppendRW(_) => VfsHandle::File(FileHandle {
                id: value.id,
                path: value.path.clone(),
                flags: VfsFlags::READ | VfsFlags::APPEND,
            }),
            HandleMode::Append(_) => VfsHandle::File(FileHandle {
                id: value.id,
                path: value.path.clone(),
                flags: VfsFlags::APPEND,
            }),
            HandleMode::Directory(flags) => VfsHandle::Directory(DirHandle {
                id: value.id,
                path: value.path.clone(),
                flags,
            }),
        }
    }
}

#[derive(Debug)]
pub enum HandleMode {
    Read(Mutex<Box<dyn DataReadSeek>>),
    FullRW(Mutex<Box<dyn DataFull>>),
    AppendRW(Mutex<SeqLockHandle>),
    Append(Mutex<Box<dyn DataWrite>>),
    Directory(DirFlags),
}

#[derive(Debug)]
pub struct VirtualHandle {
    pub id: Uuid,
    pub mode: HandleMode,
    pub path: PathBuf,
    pub meta: Option<Metadata>,
    pub src: Option<Arc<dyn VfsBackend>>,
}

