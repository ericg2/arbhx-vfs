use crate::handle::{DirFlags, VfsFlags};
use crate::{UserVfs, VfsMetadata};
use bytes::Bytes;
use log::{error, info, trace, warn};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Packet, Status, StatusCode, Version,
};
use std::collections::HashMap;
use std::io;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use uuid::Uuid;

fn get_attrs(meta: VfsMetadata) -> FileAttributes {
    let mut perms = if meta.is_dir() {
        0o040000 // directory
    } else {
        0o100000 // regular file
    };
    if meta.writable() {
        perms = perms | 0o666;
    } else {
        perms = perms | 0o444;
    }
    FileAttributes {
        size: Some(meta.size().0),
        uid: None,
        user: None,
        gid: None,
        group: None,
        permissions: Some(perms), // ← REQUIRED
        atime: meta.atime().map(|t| t.timestamp() as u32),
        mtime: meta.mtime().map(|t| t.timestamp() as u32),
    }
}

fn get_file(meta: VfsMetadata, abs: bool) -> File {
    let name = if meta.path.as_os_str() == "/" {
        "/".to_string() // root itself
    } else if !abs {
        meta.path
            .file_name()
            .unwrap_or_default()
            .to_str()
            .unwrap()
            .to_string()
    } else {
        meta.path.to_str().unwrap().to_string()
    };
    File::new(name.replace("\\", "/").clone(), get_attrs(meta))
}

fn get_flags(flags: OpenFlags) -> VfsFlags {
    VfsFlags::from_bits_truncate(flags.bits())
}

fn str_to_path(st: &str) -> Result<PathBuf, StatusCode> {
    let mut st = st.to_string();
    if st.trim() == "." {
        st = "/".into()
    }
    st = st.replace("\\", "/");
    PathBuf::from_str(&st).map_err(|_| StatusCode::BadMessage)
}

fn str_to_id(st: &str) -> Result<Uuid, StatusCode> {
    Uuid::from_str(st).map_err(|_| StatusCode::BadMessage)
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

fn map_io_error(err: io::Error) -> StatusCode {
    match err.kind() {
        ErrorKind::NotFound => StatusCode::NoSuchFile,
        ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        ErrorKind::ConnectionRefused => StatusCode::BadMessage,
        ErrorKind::AlreadyExists => StatusCode::Failure,
        ErrorKind::ReadOnlyFilesystem => StatusCode::PermissionDenied,
        ErrorKind::Unsupported => StatusCode::OpUnsupported,
        ErrorKind::UnexpectedEof => StatusCode::Eof,
        _ => StatusCode::Failure,
    }
}

pub struct SFtpSession {
    pub version: Option<u32>,
    pub fs: Box<dyn UserVfs>,
    pub reads: Vec<String>,
}

impl SFtpSession {
    pub fn new(fs: Box<dyn UserVfs>) -> Self {
        Self {
            fs,
            version: None,
            reads: vec![],
        }
    }
}

impl russh_sftp::server::Handler for SFtpSession {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    async fn init(
        &mut self,
        version: u32,
        extensions: HashMap<String, String>,
    ) -> Result<Version, Self::Error> {
        if self.version.is_some() {
            error!("duplicate SSH_FXP_VERSION packet");
            return Err(StatusCode::ConnectionLost);
        }

        self.version = Some(version);
        info!("version: {:?}, extensions: {:?}", self.version, extensions);
        Ok(Version::new())
    }

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        flags: OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        trace!("OPEN {filename:?}");
        let path = str_to_path(&filename)?;
        let handle = self
            .fs
            .open_file(&path, get_flags(flags))
            .await
            .map_err(map_io_error)?;
        Ok(Handle {
            id,
            handle: handle.id.to_string(),
        })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, Self::Error> {
        let h_id = str_to_id(&handle)?;
        self.fs.close(h_id).await.map_err(map_io_error)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, Self::Error> {
        let h_id = str_to_id(&handle)?;
        let ret = self
            .fs
            .read(h_id, offset, len as u64)
            .await
            .map_err(map_io_error)?;
        Ok(Data {
            id,
            data: ret.to_vec(),
        })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, Self::Error> {
        let h_id = str_to_id(&handle)?;
        self.fs
            .write(h_id, offset, Bytes::from(data))
            .await
            .map_err(map_io_error)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let path = str_to_path(&path)?;
        let attrs = self
            .fs
            .stat_f(&path)
            .await
            .map(get_attrs)
            .map_err(map_io_error)?;
        Ok(Attrs { id, attrs })
    }

    async fn fstat(&mut self, id: u32, handle: String) -> Result<Attrs, Self::Error> {
        let h_id = str_to_id(&handle)?;
        let attrs = self
            .fs
            .stat_h(h_id)
            .await
            .map(get_attrs)
            .map_err(map_io_error)?;
        Ok(Attrs { id, attrs })
    }

    async fn setstat(
        &mut self,
        _id: u32,
        _path: String,
        _attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        Err(StatusCode::OpUnsupported.into())
    }

    async fn fsetstat(
        &mut self,
        _id: u32,
        _handle: String,
        _attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        Err(StatusCode::OpUnsupported.into())
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, Self::Error> {
        let path = str_to_path(&path)?;
        let handle = self
            .fs
            .open_dir(&path, DirFlags::READ)
            .await
            .map_err(map_io_error)?;
        Ok(Handle {
            id,
            handle: handle.id.to_string(),
        })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, Self::Error> {
        if self.reads.contains(&handle) {
            Err(StatusCode::Eof)
        } else {
            let h_id = str_to_id(&handle)?;
            let files = self
                .fs
                .list_h(h_id)
                .await
                .map_err(map_io_error)?
                .into_iter()
                .map(|x| get_file(x, false))
                .filter(|x| x.filename != "/")
                .collect::<Vec<_>>();
            self.reads.push(handle.clone());
            Ok(Name { id, files })
        }
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, Self::Error> {
        let path = str_to_path(&filename)?;
        self.fs.remove_file(&path).await.map_err(map_io_error)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        let path = str_to_path(&path)?;
        self.fs.create_dir(&path).await.map_err(map_io_error)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn rmdir(&mut self, id: u32, path: String) -> Result<Status, Self::Error> {
        let path = str_to_path(&path)?;
        self.fs.remove_dir(&path).await.map_err(map_io_error)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn realpath(&mut self, id: u32, path: String) -> Result<Name, Self::Error> {
        let path_raw = str_to_path(&path)?;
        let path = normalize_virtual_path(&path_raw);
        let path2 = path.clone();
        warn!("REALPATH {path_raw:?} -> {path:?}");
        let mut meta = self.fs.stat_f(&path2).await.map_err(map_io_error)?;
        meta.path = path.to_owned(); // *** fix all issues now!
        let file = get_file(meta, true);
        Ok(Name {
            id,
            files: vec![file],
        })
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        let path = str_to_path(&path)?;
        let attrs = self
            .fs
            .stat_f(&path)
            .await
            .map(get_attrs)
            .map_err(map_io_error)?;
        Ok(Attrs { id, attrs })
    }

    async fn rename(
        &mut self,
        id: u32,
        old_path: String,
        new_path: String,
    ) -> Result<Status, Self::Error> {
        let old_path = str_to_path(&old_path)?;
        let new_path = str_to_path(&new_path)?;
        self.fs
            .rename(&old_path, &new_path)
            .await
            .map_err(map_io_error)?;
        Ok(Status {
            id,
            status_code: StatusCode::Ok,
            error_message: "".to_string(),
            language_tag: "".to_string(),
        })
    }

    async fn readlink(&mut self, _id: u32, _path: String) -> Result<Name, Self::Error> {
        Err(StatusCode::OpUnsupported)
    }

    async fn symlink(
        &mut self,
        _id: u32,
        _link_path: String,
        _target_path: String,
    ) -> Result<Status, Self::Error> {
        Err(StatusCode::OpUnsupported)
    }

    async fn extended(
        &mut self,
        _id: u32,
        _request: String,
        _data: Vec<u8>,
    ) -> Result<Packet, Self::Error> {
        Err(StatusCode::OpUnsupported)
    }
}
