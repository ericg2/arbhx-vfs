use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::Arc;
use arbhx_core::VfsBackend;
use serde::{Deserialize, Serialize};
use unftp_core::auth::UserDetail;
use crate::sha256_hash;

#[derive(Clone, Debug)]
pub struct VfsUser {
    pub user_name: String,
    pub sha256_hash: String,
    pub points: Vec<VfsPoint>,
    pub keys: Vec<String>,
}

impl Display for VfsUser {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl UserDetail for VfsUser {
    fn account_enabled(&self) -> bool {
        true
    }
    fn home(&self) -> Option<&Path> {
        Some("/".as_ref())
    }
}

impl VfsUser {
    pub fn new(name: &str, password: &str, keys: Vec<String>, points: Vec<VfsPoint>) -> Self {
        Self {
            user_name: name.to_string(),
            sha256_hash: sha256_hash(password),
            keys,
            points
        }
    }
    pub fn with_hash(name: &str, hash: &str, keys: Vec<String>, points: Vec<VfsPoint>) -> Self {
        Self {
            user_name: name.to_string(),
            sha256_hash: hash.to_string(),
            keys,
            points
        }
    }
}

#[derive(Clone, Debug)]
pub struct VfsPoint {
    pub name: String,
    pub root: String,
    pub can_write: bool,
    pub max_bytes: u64,
    pub point: Arc<dyn VfsBackend>,
}



