use crate::{UserAuthError, UserVfs, VfsAuth, VfsUser};
use async_trait::async_trait;
use dashmap::DashMap;
use std::fmt::Debug;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncSeekExt};
use unftp_core::auth::{AuthenticationError, Authenticator, Credentials, Principal};
use unftp_core::storage;
use unftp_core::storage::{ErrorKind, FEATURE_RESTART, Fileinfo, Metadata, StorageBackend};

#[derive(Debug)]
struct UserSession {
    handle: Box<dyn UserVfs>,
    cwd: PathBuf,
}

impl UserSession {
    pub fn new(vfs: Box<dyn UserVfs>) -> Self {
        Self {
            handle: vfs,
            cwd: PathBuf::from("/"),
        }
    }
    pub fn set_cwd(&mut self, path: impl AsRef<Path>) {
        self.cwd = path.as_ref().to_path_buf();
    }
    pub fn resolve(&self, path: impl AsRef<Path>) -> PathBuf {
        self.cwd.join(path.as_ref())
    }
}

#[derive(Debug)]
pub struct FtpBackend {
    auth: Arc<dyn VfsAuth>,
    vfs: DashMap<String, UserSession>,
}

impl FtpBackend {
    pub fn new(auth: Arc<dyn VfsAuth>) -> Self {
        Self {
            auth,
            vfs: DashMap::new(),
        }
    }
}

impl From<UserAuthError> for AuthenticationError {
    fn from(value: UserAuthError) -> Self {
        match value {
            UserAuthError::InvalidLogin => AuthenticationError::BadPassword,
            UserAuthError::NotSupported => AuthenticationError::IpDisallowed,
            UserAuthError::IoError(_) => AuthenticationError::ImplPropagated(
                "Failed to login. Please contact sysadmin".into(),
                None,
            ),
        }
    }
}

#[async_trait]
impl Authenticator for FtpBackend {
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> Result<Principal, AuthenticationError> {
        let pass = creds.password.clone().unwrap_or_default();
        let vfs = self.auth.auth_pass(username, &pass).await?;
        self.vfs.insert(username.to_string(), UserSession::new(vfs));
        Ok(Principal {
            username: username.to_string(),
        })
    }
}

#[async_trait]
impl StorageBackend<VfsUser> for FtpBackend {
    type Metadata = crate::VfsMetadata;

    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> storage::Result<Self::Metadata> {
        Ok(self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?
            .handle
            .stat_f(path.as_ref())
            .await?)
    }

    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> storage::Result<Vec<Fileinfo<PathBuf, Self::Metadata>>>
    where
        <Self as StorageBackend<VfsUser>>::Metadata: Metadata,
    {
        let ret = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?
            .handle
            .list_f(path.as_ref())
            .await?
            .into_iter()
            .map(|x| Fileinfo {
                path: x.path.clone(),
                metadata: x,
            })
            .collect::<Vec<_>>();
        Ok(ret)
    }

    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
        start_pos: u64,
    ) -> storage::Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        let mut vfs = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?;
        let path = vfs.resolve(path);
        let mut handle = vfs.handle.open_read(&path).await?;
        handle.seek(SeekFrom::Start(start_pos)).await?;
        Ok(handle)
    }

    async fn put<P: AsRef<Path> + Send + Debug, R: AsyncRead + Send + Sync + Unpin + 'static>(
        &self,
        user: &VfsUser,
        mut input: R,
        path: P,
        start_pos: u64,
    ) -> storage::Result<u64> {
        let mut vfs = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?;
        let path = vfs.resolve(path);
        let mut handle = vfs.handle.open_full(path.as_ref()).await?;
        handle.seek(SeekFrom::Start(start_pos)).await?;
        Ok(tokio::io::copy(&mut input, &mut handle).await?)
    }

    async fn del<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> storage::Result<()> {
        let mut vfs = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?;
        let path = vfs.resolve(path);
        vfs.handle.remove(path.as_ref()).await?;
        Ok(())
    }

    async fn mkd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> storage::Result<()> {
        let mut vfs = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?;
        let path = vfs.resolve(path);
        vfs.handle.create_dir(path.as_ref()).await?;
        Ok(())
    }

    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        from: P,
        to: P,
    ) -> storage::Result<()> {
        let mut vfs = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?;
        let from = vfs.resolve(from);
        let to = vfs.resolve(to);
        vfs.handle.rename(&from, &to, true).await?;
        Ok(())
    }

    async fn rmd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> storage::Result<()> {
        let mut vfs = self
            .vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?;
        let path = vfs.resolve(path);
        vfs.handle.remove(path.as_ref()).await?;
        Ok(())
    }

    async fn cwd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &VfsUser,
        path: P,
    ) -> storage::Result<()> {
        self.vfs
            .get_mut(&user.user_name)
            .ok_or(ErrorKind::LocalError)?
            .set_cwd(path);
        Ok(())
    }
}
