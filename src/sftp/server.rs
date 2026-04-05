use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::VfsAuth;
use crate::sftp::ssh::SshSession;
use russh::keys::PrivateKey;
use russh::keys::ssh_key::LineEnding;
use russh::keys::ssh_key::rand_core::OsRng;
use russh::server::Server;

struct SFTPHandler {
    pub auth: Arc<dyn VfsAuth>,
}

impl russh::server::Server for SFTPHandler {
    type Handler = SshSession;

    fn new_client(&mut self, peer_addr: Option<SocketAddr>) -> Self::Handler {
        SshSession::new(peer_addr, self.auth.clone())
    }
}

#[cfg(windows)]
const LINE_ENDING: LineEnding = LineEnding::CRLF;

#[cfg(not(windows))]
const LINE_ENDING: LineEnding = LineEnding::LF;

pub struct SFTPServer {
    auth: Arc<dyn VfsAuth>,
}

impl SFTPServer {
    pub async fn start_file(auth: Arc<dyn VfsAuth>, key_file: impl AsRef<Path>) {
        let key_file = key_file.as_ref();
        if !std::fs::exists(key_file).unwrap() {
            let mut rng = OsRng;
            let key = PrivateKey::random(&mut OsRng, russh::keys::Algorithm::Ed25519)
                .expect("Failed to generate private key!");

            let key_text = key.to_openssh(LINE_ENDING).unwrap().to_string();
            std::fs::write(key_file, key_text).unwrap();
        }

        let key = std::fs::read_to_string(key_file).unwrap();
        Self::start(auth, &key).await;
    }

    pub async fn start(auth: Arc<dyn VfsAuth>, key: &str) {
        let key = PrivateKey::from_openssh(key).expect("Key is invalid!");
        let config = russh::server::Config {
            auth_rejection_time: Duration::from_secs(3),
            auth_rejection_time_initial: Some(Duration::from_secs(0)),
            keys: vec![key],
            ..Default::default()
        };

        let mut server = SFTPHandler { auth };
        if let Err(e) = server
            .run_on_address(
                Arc::new(config),
                (
                    "0.0.0.0",
                    std::env::var("PORT")
                        .unwrap_or_else(|_| "22".to_string())
                        .parse()
                        .expect("Invalid port"),
                ),
            )
            .await
        {
            panic!("Failed to start SFTP server: {e}");
        }
    }
}
