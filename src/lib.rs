mod backend;
mod file;
mod sequential;
mod service;
mod vfs;

use sha2::{Digest, Sha256};
use std::path::{Component, Path, PathBuf};

pub use backend::{AuthResult, UserAuthError, UserVfs, VfsAuth, VfsInfo, VfsMetadata};
pub use service::VfsManager;
pub use vfs::VirtualFS;

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

pub(crate) fn join_force(base: impl AsRef<Path>, p: impl AsRef<Path>) -> PathBuf {
    let mut out = PathBuf::from(base.as_ref());
    for comp in p.as_ref().components() {
        match comp {
            Component::Prefix(_) => {} // skip drive letters / UNC prefix
            Component::RootDir => {}   // skip leading /
            other => out.push(other.as_os_str()),
        }
    }
    out
}

pub(crate) fn fix_path(path: impl AsRef<Path>, blank: bool) -> PathBuf {
    let mut s = path.as_ref().to_string_lossy().replace('\\', "/");

    if s.is_empty() && blank {
        return PathBuf::from(s);
    }

    // ensure leading slash
    if !s.starts_with('/') {
        s.insert(0, '/');
    }

    // remove trailing slash (except root "/")
    if s.len() > 1 && s.ends_with('/') {
        s.pop();
    }

    PathBuf::from(s)
}

#[cfg(test)]
mod tests {
    use crate::config::{VfsPoint, VfsUser};
    use crate::{VfsAuth, VfsManager, VirtualFS};
    use arbhx_core::{VfsBackend, VfsReader};
    use arbhx_local::LocalVfs;
    use std::sync::Arc;

    #[tokio::test]
    async fn do_test() {
        let point = VfsPoint {
            name: "test".to_string(),
            root: "abcd".to_string(),
            can_write: true,
            max_bytes: 1000000,
            point: Arc::new(LocalVfs::new("hello", "C:\\Users\\Eric\\test4426")),
        };

        let user = VfsUser::new("eric", "h!", vec![], vec![point]);
        let vfs = Arc::new(VfsManager::new(vec![user]));
        let mut x = vfs
            .auth_pass("eric", "h!")
            .await
            .expect("Failed to auth!");
        let ret = x.list_f("/abcd/test".as_ref()).await.unwrap();
        for item in ret {
            println!("{:?}", item.path());
        }
        let h = x.open_read("/abcd/test/rider.exe".as_ref()).await;
        println!("DID it!")
        //   x.create_dir("/abcd".as_ref()).await.unwrap();
    }
}
