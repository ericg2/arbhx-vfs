use log::info;
use russh::keys::PublicKey;
use russh::server::{Auth, Msg, Session};
use russh::{Channel, ChannelId};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::backend::{AuthResult, UserAuthError};
use crate::sftp::service::SFtpSession;
use crate::{UserVfs, VfsAuth};

pub struct SshSession {
    pub auth: Arc<dyn VfsAuth>,
    pub clients: Arc<Mutex<HashMap<ChannelId, Channel<Msg>>>>,
    pub session: Option<Box<dyn UserVfs>>,
    pub ip: Option<SocketAddr>,
}

impl SshSession {
    pub fn new(ip: Option<SocketAddr>, auth: Arc<dyn VfsAuth>) -> Self {
        Self {
            ip,
            auth,
            clients: Arc::new(Mutex::new(HashMap::new())),
            session: None,
        }
    }
    pub async fn get_channel(&mut self, channel_id: ChannelId) -> Channel<Msg> {
        let mut clients = self.clients.lock().await;
        clients.remove(&channel_id).unwrap()
    }
    fn handle_auth(
        &mut self,
        res: AuthResult<Box<dyn UserVfs>>,
    ) -> Result<Auth, Box<dyn Error + Send + Sync>> {
        match res {
            Ok(x) => {
                self.session = Some(x);
                Ok(Auth::Accept)
            }
            Err(e) => {
                self.session = None;
                match e {
                    UserAuthError::InvalidLogin => Ok(Auth::Reject {
                        proceed_with_methods: None,
                        partial_success: false,
                    }),
                    UserAuthError::NotSupported => Ok(Auth::UnsupportedMethod),
                    UserAuthError::IoError(x) => Err(x.into()),
                }
            }
        }
    }
}

impl russh::server::Handler for SshSession {
    type Error = Box<dyn Error + Send + Sync>;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        self.handle_auth(self.auth.auth_pass(user, password).await)
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        self.handle_auth(self.auth.auth_key(user, &public_key.to_openssh()?).await)
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        // After a client has sent an EOF, indicating that they don't want
        // to send more data in this session, the channel can be closed.
        session.close(channel)?;
        Ok(())
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        {
            let mut clients = self.clients.lock().await;
            clients.insert(channel.id(), channel);
        }
        Ok(true)
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        info!("subsystem: {}", name);
        if name == "sftp" {
            if let Some(x) = self.session.take() {
                let channel = self.get_channel(channel_id).await;
                session.channel_success(channel_id)?;
                russh_sftp::server::run(channel.into_stream(), SFtpSession::new(x)).await;
            }
        } else {
            session.channel_failure(channel_id)?;
        }
        Ok(())
    }
}
