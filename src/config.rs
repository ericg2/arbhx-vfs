use arbhx::{DataMode};
use serde::{Deserialize, Serialize};
use crate::sha256_hash;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct VfsUser {
    pub user_name: String,
    pub sha256_hash: String,
    pub points: Vec<VfsPoint>,
    pub keys: Vec<String>,
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

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct VfsPoint {
    pub name: String,
    pub root: String,
    pub can_write: bool,
    pub max_bytes: u64,
    pub point: DataMode,
}



