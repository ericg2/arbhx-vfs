use crate::backend::VfsInfo;
use crate::config::{VfsPoint, VfsUser};
use crate::file::DataFile;
use crate::handle::{
    DirFlags, DirHandle, FileHandle, HandleMode, VfsFlags, VfsHandle, VirtualHandle,
};
use crate::sequential::SeqLockHandle;
use crate::{UserVfs, VfsMetadata};
use arbhx_core::{DataFull, DataReadSeek, DataWrite, VfsBackend};
use async_trait::async_trait;
use bitflags::bitflags;
use bytes::Bytes;
use chrono::Duration;
use futures_lite::{FutureExt, Stream, StreamExt};
use log::{debug, trace, warn};
use std::collections::{HashMap, HashSet};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::slice::SliceIndex;
use std::str::FromStr;
use std::sync::Arc;
use std::{io, thread};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncSeek};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Clone)]
struct OneMatch {
    prefix: PathBuf,
    rel_path: PathBuf,
    src: Arc<dyn VfsBackend>,
    vfs: VfsPoint,
}

impl OneMatch {
    async fn get_meta(&self) -> io::Result<VfsMetadata> {
        let meta = self
            .src
            .clone()
            .reader()
            .ok_or(ErrorKind::Unsupported)?
            .get_metadata(&self.rel_path)
            .await?
            .ok_or(io::Error::from(ErrorKind::NotFound))?;
        let vfs = self.vfs.clone();
        let path = self.rel_path.clone();
        let ret = VfsMetadata::from_be(
            self.prefix.clone(),
            path,
            meta.is_dir(),
            Some(vfs),
            Some(meta),
        );
        Ok(ret)
    }
}

#[derive(Clone)]
struct ListMatch {
    prefix: PathBuf,
    rel_path: PathBuf,
    names: Vec<String>, // 4-3-26: We don't use Operator to avoid init.
}

#[derive(Clone)]
enum VirtualPath {
    ExactMatch(OneMatch),
    Multiple(ListMatch),
    Root(PathBuf),
}

impl VirtualPath {
    pub fn exact(self, err: ErrorKind) -> io::Result<OneMatch> {
        if let VirtualPath::ExactMatch(x) = self {
            Ok(x)
        } else {
            Err(io::Error::from(err))
        }
    }
    pub async fn info(self) -> io::Result<VfsInfo> {
        Ok(match self {
            VirtualPath::ExactMatch(x) => VfsInfo {
                path: x.rel_path,
                usage: x.src.get_usage().await?,
                vfs: Some(x.vfs),
            },
            VirtualPath::Multiple(x) => VfsInfo {
                path: x.rel_path,
                usage: None,
                vfs: None,
            },
            VirtualPath::Root(path) => VfsInfo {
                path,
                usage: None,
                vfs: None,
            },
        })
    }
}

#[derive(Debug)]
pub struct VirtualFS {
    user: VfsUser,
    handles: HashMap<Uuid, VirtualHandle>,
    offsets: HashMap<Uuid, u64>, // 4-4-26: Optimization to help with SEEK.
    buffer: Arc<Semaphore>,
}

impl VirtualFS {
    pub fn new(user: VfsUser) -> Self {
        Self {
            user,
            handles: HashMap::new(),
            offsets: HashMap::new(),
            buffer: Arc::new(Semaphore::new(64)),
        }
    }

    fn normalize_virtual_path(path: &Path) -> PathBuf {
        let mut stack = Vec::new();

        for comp in path.components() {
            match comp {
                std::path::Component::RootDir => {
                    // Start from root
                    stack.clear();
                }
                std::path::Component::ParentDir => {
                    // Pop last component if possible, but never pop above root
                    stack.pop();
                }
                std::path::Component::CurDir => {
                    // Skip "."
                }
                std::path::Component::Normal(s) => {
                    stack.push(s.to_str().unwrap());
                }
                _ => {}
            }
        }

        // Build normalized path
        let mut normalized = PathBuf::from("/");
        for s in stack {
            normalized.push(s.replace("\\", "/"));
        }

        normalized
    }

    async fn resolve_path(&mut self, path: impl AsRef<Path>) -> io::Result<VirtualPath> {
        let f_path = Self::normalize_virtual_path(path.as_ref())
            .to_str()
            .ok_or(io::Error::from(ErrorKind::InvalidInput))?
            .replace("\\", "/");
        let spl: Vec<_> = crate::strip_all(&f_path, "/")
            .splitn(3, "/") // base / point / rest
            .map(|x| x.to_string())
            .collect();
        debug!("Resolved path {f_path}");

        // Base component
        let start_path = spl.get(0).map(|s| s.as_str()).unwrap_or("");
        if f_path.is_empty() || f_path == "/" {
            return Ok(VirtualPath::Root(PathBuf::from("/")));
        }
        match spl.get(1) {
            Some(point_name) => {
                if let Some(src) = self
                    .user
                    .points
                    .iter()
                    .find(|x| x.name == *point_name && x.root == start_path)
                {
                    let prefix = PathBuf::from(format!("/{start_path}/{point_name}"));
                    let backend = src.point.clone();
                    let rel_path = spl
                        .get(2)
                        .map(|s| format!("/{}", s))
                        .unwrap_or_else(|| "/".to_string());
                    let mut vfs = src.clone();
                    let w = backend.clone().writer();
                    if w.is_none() {
                        vfs.can_write = false;
                    }
                    Ok(VirtualPath::ExactMatch(OneMatch {
                        rel_path: PathBuf::from(rel_path),
                        src: backend,
                        prefix,
                        vfs,
                    }))
                } else {
                    Err(io::Error::from(ErrorKind::NotFound))
                }
            }
            None => {
                // Only the base is specified: return all points under that base
                let ret = ListMatch {
                    prefix: PathBuf::from(format!("/{start_path}")),
                    rel_path: PathBuf::from(f_path),
                    names: self
                        .user
                        .points
                        .iter()
                        .filter(|x| x.root == start_path)
                        .map(|x| x.name.to_owned())
                        .collect::<Vec<_>>(),
                };
                Ok(VirtualPath::Multiple(ret))
            }
        }
    }

    async fn raw_open_handle(
        &self,
        file: DataFile,
        flags: VfsFlags,
        did_create: bool,
    ) -> io::Result<HandleMode> {
        if file.metadata().is_dir() {
            return Err(ErrorKind::IsADirectory.into());
        }
        match (
            flags.contains(VfsFlags::READ),
            flags.contains(VfsFlags::WRITE),
            flags.contains(VfsFlags::APPEND),
            flags.contains(VfsFlags::TRUNCATE),
        ) {
            // Read-only
            (true, false, _, _) => {
                let reader = file.open_read_full().await?;
                Ok(HandleMode::Read(Mutex::new(reader)))
            }

            // Write only, append mode
            (false, true, true, truncate) => {
                // 3-24-26: If the file does NOT exist, we don't need append mode!
                let handle = if did_create {
                    file.open_append(true).await?
                } else {
                    file.open_append(truncate).await?
                };
                Ok(HandleMode::Append(Mutex::new(handle)))
            }

            // Read + write, append mode. TODO: make random-supported BE not go through this too!
            (true, true, true, truncate) => {
                let handle = SeqLockHandle::new(file.path().to_path_buf(), file, truncate);
                Ok(HandleMode::AppendRW(Mutex::new(handle)))
            }

            // Full read/write (no append)
            (true, true, false, _) | (false, true, false, _) => {
                let handle = file.open_full().await?;
                Ok(HandleMode::FullRW(Mutex::new(handle)))
            }

            // Should not happen: read & no write & not append
            (false, false, _, _) => {
                let reader = file.open_read_full().await?;
                Ok(HandleMode::Read(Mutex::new(reader)))
            }
        }
    }

    async fn raw_transfer(
        &mut self,
        src: impl AsRef<Path>,
        dest: impl AsRef<Path>,
        do_move: bool, // optional: move instead of copy
        do_overwrite: bool,
    ) -> io::Result<()> {
        let src = src.as_ref();
        let dest = dest.as_ref();
        let sv_path = self
            .resolve_path(src)
            .await?
            .exact(ErrorKind::Unsupported)?;

        let dv_path = self
            .resolve_path(dest)
            .await?
            .exact(ErrorKind::ReadOnlyFilesystem)?;
        if !dv_path.vfs.can_write {
            return Err(ErrorKind::ReadOnlyFilesystem.into());
        }

        // 4-3-26: All moves have to be in the same backend!
        if sv_path.src.id() == dv_path.src.id() {
            if let Some(vfs) = dv_path.src.clone().full() {
                if !do_overwrite && vfs.get_metadata(dest).await?.is_some() {
                    return Err(ErrorKind::AlreadyExists.into());
                }
                if do_move {
                    vfs.move_to(&sv_path.rel_path, &dv_path.rel_path).await
                } else {
                    if let Some(usage) = dv_path.src.get_usage().await? {
                        let size = sv_path.get_meta().await?.size();
                        if size >= usage.free_bytes {
                            return Err(ErrorKind::QuotaExceeded.into());
                        }
                    }
                    vfs.copy_to(&sv_path.rel_path, &dv_path.rel_path).await
                }
            } else {
                Err(ErrorKind::ReadOnlyFilesystem.into()) // *** not allowed to write!
            }
        } else {
            Err(ErrorKind::CrossesDevices.into()) // *** cannot cross FS!
        }
    }
}

#[async_trait]
impl UserVfs for VirtualFS {
    fn get_user(&self) -> VfsUser {
        self.user.clone()
    }

    fn get_handles(&self) -> Vec<VfsHandle> {
        self.handles.iter().map(|x| x.1.into()).collect::<Vec<_>>()
    }

    async fn get_info(&mut self, path: &Path) -> io::Result<VfsInfo> {
        self.resolve_path(path).await?.info().await
    }

    async fn get_infos(&mut self) -> io::Result<Vec<VfsInfo>> {
        let mut ret = Vec::new();
        for x in self.user.points.clone() {
            let path = format!("{}/{}", x.root, x.name);
            ret.push(self.get_info(&PathBuf::from(path)).await?);
        }
        Ok(ret)
    }

    async fn open_dir(&mut self, path: &Path, flags: DirFlags) -> io::Result<DirHandle> {
        let v_path = self.resolve_path(path).await?;
        let mut do_handle = |rel_path: PathBuf| {
            let id = Uuid::new_v4();
            let handle = VirtualHandle {
                id,
                mode: HandleMode::Directory(DirFlags::READ),
                path: rel_path.clone(),
                meta: None,
                src: None,
            };
            self.handles.insert(id, handle);
            DirHandle {
                id,
                path: rel_path,
                flags: DirFlags::READ,
            }
        };
        match v_path {
            // =========================
            // Exact match (can be RW)
            // =========================
            VirtualPath::ExactMatch(v) => {
                // Enforce read-only if backend is not writable
                let reader = v.src.clone().reader().ok_or(ErrorKind::Unsupported)?;
                if !v.vfs.can_write && flags.bits() != DirFlags::READ.bits() {
                    return Err(ErrorKind::ReadOnlyFilesystem.into());
                }
                match reader.get_metadata(&v.rel_path).await? {
                    Some(meta) => {
                        if !meta.is_dir() {
                            return Err(ErrorKind::NotADirectory.into());
                        }
                        if flags.contains(DirFlags::CREATE) && flags.contains(DirFlags::EXCLUDE) {
                            return Err(ErrorKind::AlreadyExists.into());
                        }
                    }
                    None => {
                        if flags.contains(DirFlags::CREATE) {
                            if !v.vfs.can_write {
                                return Err(ErrorKind::ReadOnlyFilesystem.into());
                            }
                            v.src
                                .clone()
                                .writer()
                                .ok_or(ErrorKind::Unsupported)?
                                .create_dir(&v.rel_path)
                                .await?;
                        } else {
                            return Err(ErrorKind::NotFound.into());
                        }
                    }
                }

                // At this point it's guaranteed to exist and be a directory
                if !reader
                    .get_metadata(&v.rel_path)
                    .await?
                    .ok_or(ErrorKind::NotFound)?
                    .is_dir()
                {
                    return Err(ErrorKind::NotADirectory.into());
                }
            }

            // =========================
            // Non-exact (always R/O dirs)
            // =========================
            VirtualPath::Multiple(_) => {
                if flags.bits() != DirFlags::READ.bits() {
                    return Err(ErrorKind::ReadOnlyFilesystem.into());
                }
            }

            VirtualPath::Root(_) => {
                if flags.bits() != DirFlags::READ.bits() {
                    return Err(ErrorKind::ReadOnlyFilesystem.into());
                }
            }
        }
        Ok(do_handle(path.to_path_buf()))
    }

    async fn open_file(&mut self, path: &Path, flags: VfsFlags) -> io::Result<FileHandle> {
        let v_path = self
            .resolve_path(path)
            .await?
            .exact(ErrorKind::IsADirectory)?;
        if !v_path.vfs.can_write {
            if flags.contains(VfsFlags::WRITE)
                || flags.contains(VfsFlags::APPEND)
                || flags.contains(VfsFlags::CREATE)
                || flags.contains(VfsFlags::TRUNCATE)
            {
                return Err(ErrorKind::ReadOnlyFilesystem.into());
            }
        }

        // Handle all rules to fix the file backend.
        let reader = v_path.src.clone().reader().ok_or(ErrorKind::Unsupported)?;
        let existing_file = reader.get_metadata(&v_path.rel_path).await?;
        let did_create: bool = existing_file.is_none();
        if existing_file.is_some()
            && flags.contains(VfsFlags::CREATE)
            && flags.contains(VfsFlags::EXCLUDE)
        {
            return Err(ErrorKind::AlreadyExists.into());
        }
        if existing_file.is_none() {
            if flags.contains(VfsFlags::CREATE) {
                // 4-8-26: Make sure the file actually exists!
                if reader.get_metadata(&v_path.rel_path).await?.is_none() {
                    v_path
                        .src
                        .clone()
                        .writer()
                        .ok_or(ErrorKind::ReadOnlyFilesystem)?
                        .set_length(&v_path.rel_path, 0)
                        .await?;
                }
            } else {
                return Err(ErrorKind::NotFound.into());
            }
        }

        // Get or create the actual file object
        let meta = reader
            .get_metadata(&v_path.rel_path)
            .await?
            .ok_or(ErrorKind::NotFound)?;
        if meta.is_dir() {
            return Err(ErrorKind::IsADirectory.into());
        }

        let id = Uuid::new_v4();
        let path = meta.path().to_path_buf();
        let meta = meta.clone();
        let file = DataFile::new(&v_path.rel_path, meta.clone(), v_path.src.clone());
        let mode = self.raw_open_handle(file, flags, did_create).await?;
        self.handles.insert(
            id,
            VirtualHandle {
                id,
                mode,
                path: path.clone(),
                meta: Some(meta),
                src: Some(v_path.src),
            },
        );
        Ok(FileHandle { id, path, flags })
    }

    async fn open_read(&mut self, path: &Path) -> io::Result<Box<dyn DataReadSeek>> {
        let f = self.open_file(path, VfsFlags::READ).await?;
        let handle = self.handles.remove(&f.id).ok_or(ErrorKind::NotFound)?;
        if let HandleMode::Read(x) = handle.mode {
            Ok(x.into_inner())
        } else {
            Err(ErrorKind::Unsupported.into())
        }
    }

    async fn open_seq(&mut self, path: &Path) -> io::Result<SeqLockHandle> {
        let ret = self
            .open_file(
                path,
                VfsFlags::READ | VfsFlags::WRITE | VfsFlags::APPEND | VfsFlags::TRUNCATE,
            )
            .await?;
        if let Some(handle) = self.handles.remove(&ret.id) {
            if let HandleMode::AppendRW(x) = handle.mode {
                return Ok(x.into_inner());
            }
        }
        unreachable!("Expected to open handle!")
    }

    async fn open_append(
        &mut self,
        path: &Path,
        overwrite: bool,
    ) -> io::Result<Box<dyn DataWrite>> {
        let mut flags = VfsFlags::WRITE | VfsFlags::APPEND | VfsFlags::CREATE;
        if overwrite {
            flags |= VfsFlags::TRUNCATE;
        }
        let f = self.open_file(path, flags).await?;
        let handle = self.handles.remove(&f.id).ok_or(ErrorKind::NotFound)?;
        if let HandleMode::Append(x) = handle.mode {
            Ok(x.into_inner())
        } else {
            Err(ErrorKind::Unsupported.into())
        }
    }

    async fn open_full(&mut self, path: &Path) -> io::Result<Box<dyn DataFull>> {
        let f = self
            .open_file(path, VfsFlags::READ | VfsFlags::WRITE | VfsFlags::CREATE)
            .await?;
        let handle = self.handles.remove(&f.id).ok_or(ErrorKind::NotFound)?;
        if let HandleMode::FullRW(x) = handle.mode {
            Ok(x.into_inner())
        } else {
            Err(ErrorKind::Unsupported.into())
        }
    }

    async fn close(&mut self, handle: Uuid) -> io::Result<()> {
        trace!("close - {handle:?}");
        let o_len = self.handles.len();
        self.handles.remove(&handle);
        self.offsets.remove(&handle);
        if self.handles.len() != o_len {
            Ok(())
        } else {
            Err(ErrorKind::NotFound.into())
        }
    }

    async fn read(&mut self, handle: Uuid, offset: u64, length: u64) -> io::Result<Bytes> {
        warn!("read - {handle:?}; offset: {offset}; length: {length}");
        let vfs = self.handles.get_mut(&handle).ok_or(ErrorKind::NotFound)?;
        let length = if let Some(ref meta) = vfs.meta {
            if offset >= meta.size() {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            (meta.size() - offset).min(length)
        } else {
            length
        } as usize;

        async fn read_exact_from<T: AsyncRead + AsyncSeek + Unpin + ?Sized>(
            file: &mut T,
            offset: u64,
            length: usize,
            do_seek: bool,
        ) -> io::Result<(Bytes, usize)> {
            if do_seek {
                file.seek(SeekFrom::Start(offset)).await?;
            }
            let mut buf = vec![0u8; length];
            let n = file.read(&mut buf).await?;
            buf.truncate(n);
            Ok((Bytes::from(buf), n))
        }

        let do_seek = match self.offsets.get(&handle) {
            Some(&expected) if expected == offset => false,
            _ => true,
        };

        warn!("ENTER READ");
        let result = match vfs.mode {
            HandleMode::Append(_) => Err(ErrorKind::Unsupported.into()),
            HandleMode::Directory(_) => Err(ErrorKind::IsADirectory.into()),
            HandleMode::Read(ref mut x) => {
                let _permit = self.buffer.acquire().await.map_err(|_| ErrorKind::Other)?;
                let lck = &mut *x.lock().await;
                read_exact_from(lck, offset, length, do_seek)
                    .await
                    .map(|(b, n)| (b, n))
            }
            HandleMode::FullRW(ref mut x) => {
                let _permit = self.buffer.acquire().await.map_err(|_| ErrorKind::Other)?;
                let lck = &mut *x.lock().await;
                read_exact_from(lck, offset, length, do_seek)
                    .await
                    .map(|(b, n)| (b, n))
            }
            HandleMode::AppendRW(ref mut x) => {
                let _permit = self.buffer.acquire().await.map_err(|_| ErrorKind::Other)?;
                let lck = &mut *x.lock().await;
                let lck2 = lck.lock_read().await?;
                read_exact_from(lck2, offset, length, do_seek)
                    .await
                    .map(|(b, n)| (b, n))
            }
        };

        let (bytes, n) = result?;
        if n > 0 {
            // IMPORTANT: update using actual bytes read, not requested length
            self.offsets.insert(handle, offset + n as u64);
        }
        Ok(bytes)
    }

    async fn write(&mut self, handle: Uuid, offset: u64, data: Bytes) -> io::Result<usize> {
        warn!("write - {handle:?}");
        let vfs = self.handles.get_mut(&handle).ok_or(ErrorKind::NotFound)?;
        let check_space = async || {
            if let Some(ref src) = vfs.src {
                if let Some(x) = src.get_usage().await? {
                    if data.len() as u64 > x.free_bytes {
                        return Err(io::Error::from(ErrorKind::QuotaExceeded));
                    }
                }
            }

            Ok(())
        };
        match vfs.mode {
            HandleMode::FullRW(ref mut x) => {
                let _permit = self.buffer.acquire().await.map_err(|_| ErrorKind::Other)?;
                check_space().await?;
                let lck = &mut *x.lock().await;
                lck.seek(SeekFrom::Start(offset)).await?;
                lck.write(&*data).await
            }
            HandleMode::AppendRW(ref mut x) => {
                let _permit = self.buffer.acquire().await.map_err(|_| ErrorKind::Other)?;
                check_space().await?; // TODO: add offset checking here!
                let lck = &mut *x.lock().await;
                lck.lock_write().await?.write(&*data).await
            }
            HandleMode::Append(ref mut x) => {
                let _permit = self.buffer.acquire().await.map_err(|_| ErrorKind::Other)?;
                check_space().await?;
                let lck = &mut *x.lock().await;
                lck.write(&*data).await
            }
            HandleMode::Directory(_) => Err(ErrorKind::IsADirectory.into()),
            HandleMode::Read(_) => Err(ErrorKind::ReadOnlyFilesystem.into()),
        }
    }

    async fn remove(&mut self, path: &Path) -> io::Result<()> {
        trace!("remove - {path:?}");
        let v_path = self
            .resolve_path(path)
            .await?
            .exact(ErrorKind::IsADirectory)?;
        if !v_path.vfs.can_write {
            return Err(ErrorKind::ReadOnlyFilesystem.into());
        }
        let meta = v_path.get_meta().await?;
        let writer = v_path.src.writer().ok_or(ErrorKind::ReadOnlyFilesystem)?;
        if meta.is_dir() {
            writer.remove_dir(&v_path.rel_path).await?;
        } else {
            writer.remove_file(&v_path.rel_path).await?;
        }
        self.handles.retain(|_, x| !x.path.starts_with(path));
        Ok(())
    }

    async fn create_dir(&mut self, path: &Path) -> io::Result<()> {
        trace!("create_dir - {path:?}");
        let v_path = self
            .resolve_path(path)
            .await?
            .exact(ErrorKind::IsADirectory)?;
        if !v_path.vfs.can_write {
            return Err(ErrorKind::ReadOnlyFilesystem.into());
        }
        v_path
            .src
            .writer()
            .ok_or(ErrorKind::ReadOnlyFilesystem)?
            .create_dir(&v_path.rel_path)
            .await?;
        Ok(())
    }

    async fn stat_f(&mut self, path: &Path) -> io::Result<VfsMetadata> {
        trace!("stat_f - {path:?}");
        match self.resolve_path(path).await? {
            VirtualPath::ExactMatch(x) => Ok(x.get_meta().await?),
            VirtualPath::Multiple(x) => {
                Ok(VfsMetadata::from_be(x.prefix, x.rel_path, true, None, None))
            }
            VirtualPath::Root(root) => {
                Ok(VfsMetadata::from_be(root, PathBuf::new(), true, None, None))
            }
        }
    }

    async fn stat_h(&mut self, handle: Uuid) -> io::Result<VfsMetadata> {
        trace!("stat_h - {handle:?}");
        let path = self
            .handles
            .get_mut(&handle)
            .map(|x| x.path.to_owned())
            .ok_or(ErrorKind::NotFound)?;
        self.stat_f(&path).await
    }

    async fn list_f(&mut self, path: &Path) -> io::Result<Vec<VfsMetadata>> {
        trace!("list_f - {path:?}");
        match self.resolve_path(path).await? {
            VirtualPath::ExactMatch(x) => {
                let li = x
                    .src
                    .reader()
                    .ok_or(ErrorKind::Unsupported)?
                    .read_dir(&x.rel_path, None, false, false)
                    .await?;
                let it = li.stream().await?;
                let ret = it
                    .filter_map(|x| x.ok())
                    .map(|meta| {
                        VfsMetadata::from_be(
                            x.prefix.clone(),
                            meta.path().to_path_buf(),
                            meta.is_dir(),
                            Some(x.vfs.clone()),
                            Some(meta),
                        )
                    })
                    .filter(|y| y.vfs_path() != x.rel_path)
                    .collect::<Vec<_>>()
                    .await;
                println!("{:?}", ret);
                Ok(ret)
            }
            VirtualPath::Multiple(m) => Ok(m
                .names
                .iter()
                .map(|x| {
                    Ok(VfsMetadata::from_be(
                        m.prefix.clone(),
                        PathBuf::from_str(x).unwrap(),
                        true,
                        None,
                        None,
                    ))
                })
                .collect::<io::Result<Vec<_>>>()?),
            VirtualPath::Root(_) => {
                let roots = self
                    .user
                    .points
                    .iter()
                    .map(|x| x.root.clone())
                    .filter(|x| !x.is_empty())
                    .collect::<HashSet<_>>();
                Ok(roots
                    .into_iter()
                    .map(|x| {
                        VfsMetadata::from_be(PathBuf::from(x), PathBuf::new(), true, None, None)
                    })
                    .collect())
            }
        }
    }

    async fn list_h(&mut self, handle: Uuid) -> io::Result<Vec<VfsMetadata>> {
        let path = self
            .handles
            .get_mut(&handle)
            .map(|x| x.path.to_owned())
            .ok_or(ErrorKind::NotFound)?;
        self.list_f(&path).await
    }

    async fn copy(&mut self, src: &Path, dest: &Path, overwrite: bool) -> io::Result<()> {
        self.raw_transfer(src, dest, false, overwrite).await
    }

    async fn rename(&mut self, src: &Path, dest: &Path, overwrite: bool) -> io::Result<()> {
        self.raw_transfer(src, dest, true, overwrite).await
    }

    async fn realpath(&mut self, path: &Path) -> io::Result<PathBuf> {
        Ok(Self::normalize_virtual_path(path))
    }
}
