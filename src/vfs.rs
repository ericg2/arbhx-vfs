use crate::cache::DataCache;
use crate::handle::{
    DirFlags, DirHandle, FileHandle, HandleMode, VfsFlags, VfsHandle, VirtualHandle,
};
use crate::sequential::SeqLockHandle;
use crate::{UserVfs, VfsMetadata, VfsPoint, VfsUser};
use arbhx::fs::{DataFile, Metadata};
use arbhx::{DataMode, DataRead, Operator};
use async_trait::async_trait;
use bitflags::bitflags;
use bytes::Bytes;
use futures_lite::{Stream, StreamExt};
use log::{trace, warn};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::slice::SliceIndex;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncSeek};
use uuid::Uuid;

#[derive(Clone)]
struct OneMatch {
    rel_path: PathBuf,
    src: Operator,
    vfs: VfsPoint,
}

impl OneMatch {
    async fn get_meta(&self) -> io::Result<VfsMetadata> {
        let meta = self
            .src
            .stat(&self.rel_path)
            .await?
            .ok_or(io::Error::from(ErrorKind::NotFound))?;
        let vfs = self.vfs.clone();
        let path = self.rel_path.clone();
        let is_dir = meta.is_dir();
        Ok(VfsMetadata {
            path,
            vfs: Some(vfs),
            meta: Some(meta),
            is_dir,
        })
    }
}

#[derive(Clone)]
struct ListMatch {
    rel_path: PathBuf,
    names: Vec<String>, // 4-3-26: We don't use Operator to avoid init.
}

#[derive(Clone)]
enum VirtualPath {
    ExactMatch(OneMatch),
    Multiple(ListMatch),
    Root,
}

impl VirtualPath {
    pub fn exact(self, err: ErrorKind) -> io::Result<OneMatch> {
        if let VirtualPath::ExactMatch(x) = self {
            Ok(x)
        } else {
            Err(io::Error::from(err))
        }
    }
}

#[derive(Debug)]
pub struct VirtualFS {
    user: VfsUser,
    handles: HashMap<Uuid, VirtualHandle>,
    offsets: HashMap<Uuid, u64>, // 4-4-26: Optimization to help with SEEK.
    cache: DataCache,
}

impl VirtualFS {
    pub fn new(user: VfsUser) -> Self {
        Self {
            user,
            handles: HashMap::new(),
            cache: DataCache::new(),
            offsets: HashMap::new(),
        }
    }

    async fn resolve_path(&self, path: impl AsRef<Path>) -> io::Result<VirtualPath> {
        let f_path = path
            .as_ref()
            .to_str()
            .ok_or(io::Error::from(ErrorKind::InvalidInput))?
            .replace("\\", "/");
        let spl: Vec<_> = crate::strip_all(&f_path, "/")
            .splitn(3, "/") // base / point / rest
            .map(|x| x.to_string())
            .collect();

        // Base component
        let start_path = spl.get(0).map(|s| s.as_str()).unwrap_or("");
        if f_path.is_empty() || f_path == "/" {
            return Ok(VirtualPath::Root);
        }
        match spl.get(1) {
            Some(point_name) => {
                if let Some(src) = self
                    .user
                    .points
                    .iter()
                    .find(|x| x.name == *point_name && x.root == start_path)
                {
                    let backend = self.cache.get_data(src.point.clone()).await?;
                    let rel_path = spl
                        .get(2)
                        .map(|s| format!("/{}", s))
                        .unwrap_or_else(|| "/".to_string());
                    let mut vfs = src.clone();
                    if vfs.can_write && !backend.info().can_append {
                        vfs.can_write = false; // *** set it to false!
                    }
                    Ok(VirtualPath::ExactMatch(OneMatch {
                        rel_path: PathBuf::from(rel_path),
                        src: backend,
                        vfs,
                    }))
                } else {
                    Err(io::Error::from(ErrorKind::NotFound))
                }
            }
            None => {
                // Only the base is specified: return all points under that base
                let ret = ListMatch {
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
                let reader = file.open_read().await?;
                Ok(HandleMode::Read(reader))
            }

            // Write only, append mode
            (false, true, true, truncate) => {
                // 3-24-26: If the file does NOT exist, we don't need append mode!
                let handle = if did_create {
                    file.open_append(true).await?
                } else {
                    file.open_append(truncate).await?
                };
                Ok(HandleMode::Append(handle))
            }

            // Read + write, append mode. TODO: make random-supported BE not go through this too!
            (true, true, true, truncate) => {
                let handle = SeqLockHandle::new(file.path().to_path_buf(), file, truncate);
                Ok(HandleMode::AppendRW(handle))
            }

            // Full read/write (no append)
            (true, true, false, _) | (false, true, false, _) => {
                let handle = file.open_full().await?;
                Ok(HandleMode::FullRW(handle))
            }

            // Should not happen: read & no write & not append
            (false, false, _, _) => {
                let reader = file.open_read().await?;
                Ok(HandleMode::Read(reader))
            }
        }
    }

    async fn raw_transfer(
        &self,
        src: impl AsRef<Path>,
        dest: impl AsRef<Path>,
        do_move: bool, // optional: move instead of copy
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
        if dv_path.src.info().can_append {
            if sv_path.src.id() == dv_path.src.id() {
                if do_move {
                    sv_path
                        .src
                        .move_to(&sv_path.rel_path, &dv_path.rel_path)
                        .await
                } else {
                    if let Some(usage) = dv_path.src.usage().await? {
                        let size = sv_path.get_meta().await?.size();
                        if size >= usage.free_bytes {
                            return Err(ErrorKind::QuotaExceeded.into());
                        }
                    }
                    sv_path
                        .src
                        .copy_to(&sv_path.rel_path, &dv_path.rel_path)
                        .await
                }
            } else {
                Err(ErrorKind::CrossesDevices.into()) // *** cannot cross FS!
            }
        } else {
            Err(ErrorKind::ReadOnlyFilesystem.into()) // *** not allowed to write!
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
                if !v.vfs.can_write && flags.bits() != DirFlags::READ.bits() {
                    return Err(ErrorKind::ReadOnlyFilesystem.into());
                }
                match v.src.stat(&v.rel_path).await? {
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
                            v.src.create_dir(&v.rel_path).await?;
                        } else {
                            return Err(ErrorKind::NotFound.into());
                        }
                    }
                }

                // At this point it's guaranteed to exist and be a directory
                if !v.src.get_existing(&v.rel_path).await?.metadata().is_dir() {
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

            VirtualPath::Root => {
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
        let existing_file = v_path.src.stat(&v_path.rel_path).await?;
        let did_create: bool = existing_file.is_none();
        if existing_file.is_some()
            && flags.contains(VfsFlags::CREATE)
            && flags.contains(VfsFlags::EXCLUDE)
        {
            return Err(ErrorKind::AlreadyExists.into());
        }
        if existing_file.is_none() {
            if flags.contains(VfsFlags::CREATE) {
                v_path.src.ensure_file(&v_path.rel_path).await?;
            } else {
                return Err(ErrorKind::NotFound.into());
            }
        }

        // Get or create the actual file object
        let file = v_path.src.get_existing(&v_path.rel_path).await?;
        if file.metadata().is_dir() {
            return Err(ErrorKind::IsADirectory.into());
        }

        let id = Uuid::new_v4();
        let path = file.path().to_path_buf();
        let meta = file.metadata().clone();
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

    async fn open_seq(&mut self, path: &Path) -> io::Result<SeqLockHandle> {
        let ret = self
            .open_file(
                path,
                VfsFlags::READ | VfsFlags::WRITE | VfsFlags::APPEND | VfsFlags::TRUNCATE,
            )
            .await?;
        if let Some(handle) = self.handles.remove(&ret.id) {
            if let HandleMode::AppendRW(x) = handle.mode {
                return Ok(x);
            }
        }
        unreachable!("Expected to open handle!")
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
            if offset >= meta.size().0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            (meta.size().0 - offset).min(length)
        } else {
            length
        } as usize;

        async fn read_exact_from<T: AsyncRead + AsyncSeek + Unpin + ?Sized>(
            file: &mut T,
            offset: u64,
            length: usize,
            do_seek: bool,
        ) -> io::Result<(Bytes, usize)> {
            //if do_seek {
                file.seek(SeekFrom::Start(offset)).await?;
            //}

            let mut buf = vec![0u8; length];
            let n = file.read(&mut buf).await?;
            buf.truncate(n);
            Ok((Bytes::from(buf), n))
        }

        let do_seek = match self.offsets.get(&handle) {
            Some(&expected) if expected == offset => false,
            _ => true,
        };

        let result = match &mut *vfs.mode.lock().await {
            HandleMode::Append(_) => Err(ErrorKind::Unsupported.into()),
            HandleMode::Directory(_) => Err(ErrorKind::IsADirectory.into()),
            HandleMode::Read( x) => read_exact_from(x, offset, length, do_seek)
                .await
                .map(|(b, n)| (b, n)),
            HandleMode::FullRW(x) => read_exact_from(x, offset, length, do_seek)
                .await
                .map(|(b, n)| (b, n)),
            HandleMode::AppendRW(x) => {
                let mut lck = x.lock_read().await?;
                read_exact_from(&mut lck, offset, length, do_seek)
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
                if let Some(x) = src.usage().await? {
                    if data.len() as u64 > x.free_bytes.as_u64() {
                        return Err(io::Error::from(ErrorKind::QuotaExceeded));
                    }
                }
            }

            Ok(())
        };
        match  &mut *vfs.mode.lock().await {
            HandleMode::FullRW(x) => {
                check_space().await?;
                x.seek(SeekFrom::Start(offset)).await?;
                x.write(&*data).await
            }
            HandleMode::AppendRW(x) => {
                check_space().await?; // TODO: add offset checking here!
                let lck = x.lock_write().await?;
                lck.write(&*data).await
            }
            HandleMode::Append(x) => {
                check_space().await?;
                x.write(&*data).await
            }
            HandleMode::Directory(_) => Err(ErrorKind::IsADirectory.into()),
            HandleMode::Read(_) => Err(ErrorKind::ReadOnlyFilesystem.into()),
        }
    }

    async fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        trace!("remove_file - {path:?}");
        let v_path = self
            .resolve_path(path)
            .await?
            .exact(ErrorKind::IsADirectory)?;
        if !v_path.vfs.can_write {
            return Err(ErrorKind::ReadOnlyFilesystem.into());
        }
        v_path.src.remove_file(&v_path.rel_path).await?;
        self.handles.retain(|_, x| !x.path.starts_with(path));
        Ok(())
    }

    async fn remove_dir(&mut self, path: &Path) -> io::Result<()> {
        trace!("remove_dir - {path:?}");
        let v_path = self
            .resolve_path(path)
            .await?
            .exact(ErrorKind::IsADirectory)?;
        if !v_path.vfs.can_write {
            return Err(ErrorKind::ReadOnlyFilesystem.into());
        }
        v_path.src.remove_dir(&v_path.rel_path).await?;
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
        v_path.src.create_dir(&v_path.rel_path).await?;
        Ok(())
    }

    async fn stat_f(&mut self, path: &Path) -> io::Result<VfsMetadata> {
        trace!("stat_f - {path:?}");
        match self.resolve_path(path).await? {
            VirtualPath::ExactMatch(x) => Ok(x.get_meta().await?),
            VirtualPath::Multiple(x) => {
                let meta = VfsMetadata {
                    path: x.rel_path,
                    is_dir: true,
                    vfs: None,
                    meta: None,
                };
                Ok(meta)
            }
            VirtualPath::Root => {
                let meta = VfsMetadata {
                    path: PathBuf::from_str("/").expect("Failed to parse '/'"),
                    is_dir: true,
                    vfs: None,
                    meta: None,
                };
                Ok(meta)
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
                let li = x.src.list(&x.rel_path, None, false, false).await?;
                let it = li.stream().await?;
                let ret = it
                    .filter_map(|x| x.ok())
                    .map(|mut z| {
                        let meta = z.metadata();
                        VfsMetadata {
                            path: z.path().to_path_buf(),
                            is_dir: meta.is_dir(),
                            vfs: Some(x.vfs.clone()),
                            meta: Some(meta),
                        }
                    })
                    .filter(|y| y.path != x.rel_path)
                    .collect::<Vec<_>>()
                    .await;
                println!("{:?}", ret);
                Ok(ret)
            }
            VirtualPath::Multiple(x) => Ok(x
                .names
                .iter()
                .map(|x| {
                    Ok(VfsMetadata {
                        path: PathBuf::from(x),
                        is_dir: true,
                        vfs: None,
                        meta: None,
                    })
                })
                .collect::<io::Result<Vec<_>>>()?),
            VirtualPath::Root => {
                let roots = self
                    .user
                    .points
                    .iter()
                    .map(|x| x.root.clone())
                    .filter(|x| !x.is_empty())
                    .collect::<HashSet<_>>();
                Ok(roots
                    .into_iter()
                    .map(|x| VfsMetadata {
                        path: PathBuf::from(x),
                        is_dir: true,
                        vfs: None,
                        meta: None,
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

    async fn copy(&mut self, src: &Path, dest: &Path) -> io::Result<()> {
        self.raw_transfer(src, dest, false).await
    }

    async fn rename(&mut self, src: &Path, dest: &Path) -> io::Result<()> {
        self.raw_transfer(src, dest, true).await
    }
}
