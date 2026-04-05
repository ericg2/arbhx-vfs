mod backend;
mod cache;
mod config;
mod handle;
mod sequential;
mod service;
mod vfs;

pub mod sftp;

pub use backend::{AuthResult, UserAuthError, UserVfs, VfsAuth, VfsMetadata};
pub use config::{VfsPoint, VfsUser};
pub use service::VfsManager;
use sha2::{Digest, Sha256};
pub use vfs::VirtualFS;

// Re-export to allow systems to use it.
pub use arbhx::DataMode;
pub use arbhx::local::LocalConfig;
pub use arbhx::remote::RemoteConfig;
pub use arbhx::remote::services::*;

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
