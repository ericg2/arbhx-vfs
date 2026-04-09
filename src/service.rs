use std::sync::RwLock;
use async_trait::async_trait;
use crate::{sha256_hash, AuthResult, UserAuthError, UserVfs, VfsAuth, VirtualFS};
use crate::config::VfsUser;

#[derive(Debug)]
pub struct VfsManager {
    users: RwLock<Vec<VfsUser>>,
}

impl VfsManager {
    pub fn new(users: Vec<VfsUser>) -> Self {
        Self {
            users: RwLock::new(users)
        }
    }
    pub fn set_users(&self, users: Vec<VfsUser>) {
        let lck = &mut *self.users.write().unwrap();
        *lck = users;
    }
}


#[async_trait]
impl VfsAuth for VfsManager {
    // TODO: add some security and rate limiting etc.
    async fn auth_pass(&self, username: &str, password: &str) -> AuthResult<Box<dyn UserVfs>> {
        let hash = sha256_hash(password);
        if let Some(user) = self
            .users
            .read()
            .unwrap()
            .iter()
            .find(|x| x.user_name == username && x.sha256_hash == hash)
            .cloned()
        {
            let vfs = VirtualFS::new(user);
            Ok(Box::new(vfs))
        } else {
            Err(UserAuthError::InvalidLogin)
        }
    }

    async fn auth_key(&self, username: &str, key: &str) -> AuthResult<Box<dyn UserVfs>> {
        if let Some(user) = self
            .users
            .read()
            .unwrap()
            .iter()
            .find(|x| x.user_name == username && x.keys.iter().any(|x| x == key))
            .cloned()
        {
            let vfs = VirtualFS::new(user);
            Ok(Box::new(vfs))
        } else {
            Err(UserAuthError::InvalidLogin)
        }
    }

    async fn get_user(&self, username: &str) -> Option<VfsUser> {
        self.users.read().unwrap().iter().find(|x|x.user_name == username).cloned()
    }
}