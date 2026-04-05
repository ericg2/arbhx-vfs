mod backend;
mod cache;
mod config;
mod handle;
mod sequential;
mod vfs;
mod service;

pub mod sftp;

use sha2::{Digest, Sha256};
pub use backend::{AuthResult, UserAuthError, UserVfs, VfsAuth, VfsMetadata};
pub use config::{VfsPoint, VfsUser};
pub use vfs::VirtualFS;
pub use service::VfsManager;

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;
    use arbhx::DataMode;
    use arbhx::local::LocalConfig;
    use log::LevelFilter;
    use simplelog::{Config, SimpleLogger};
    use crate::service::VfsManager;
    use crate::sftp::SFTPServer;
    use crate::{VfsPoint, VfsUser};

    #[tokio::test]
    async fn thing() {
        let _ = SimpleLogger::init(LevelFilter::Warn, Config::default());
        let point = VfsPoint {
            name: "test".into(),
            root: "data".to_string(),
            can_write: true,
            max_bytes: 1000000,
            point: DataMode::Local(LocalConfig {
                path: PathBuf::from_str("C:\\Users\\Eric\\test4426").unwrap()
            }),
        };
        let user = VfsUser::new("eric", "Rugratse124!", vec![], vec![point]);
        let vfs = VfsManager::new(vec![user]);
        let sftp = SFTPServer::start_file(Arc::new(vfs), "test-key");
        sftp.await;
        loop {}
    }
}

pub(crate) fn strip_all<'a>(st: &'a str, p: &'a str) -> &'a str {
    let x = st.strip_prefix(p).unwrap_or(st);
    x.strip_suffix(p).unwrap_or(x)
}

pub(crate) fn sha256_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    hex::encode(result.as_slice())
}
