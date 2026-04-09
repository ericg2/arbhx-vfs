mod backend;
mod sequential;
mod service;
mod vfs;
mod file;

use sha2::{Digest, Sha256};

pub use backend::{AuthResult, UserAuthError, UserVfs, VfsAuth, VfsMetadata, VfsInfo};
pub use service::VfsManager;
pub use vfs::VirtualFS;

pub mod ftp;
pub mod config;
pub mod handle;

/// Re-export of core stuff
pub use arbhx_core::{DataUsage, Metadata};

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

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn do_test() {
        
    }
}