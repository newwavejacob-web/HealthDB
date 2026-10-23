// TLS for both ports, turned on with --tls <dir>. the dir holds (see scripts/gen-certs.sh):
//   ca.pem                 the cluster CA
//   node-<id>.pem          this node's cert, signed by the CA
//   node-<id>-key.pem      its private key
// raft rpcs use mutual TLS: each side must present a cert signed by the cluster CA, so
// nothing outside the cluster can vote or append entries. the client port is server-auth
// only, so clients verify they're talking to the cluster (e.g. openssl s_client -CAfile ca.pem).
use std::io;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::server::WebPkiClientVerifier;

pub struct Tls {
    // outgoing raft rpcs, presents this node's cert
    pub connector: TlsConnector,
    // incoming raft rpcs, rejects peers without a CA-signed cert
    pub peer_acceptor: TlsAcceptor,
    // client port
    pub client_acceptor: TlsAcceptor,
}

fn err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

impl Tls {
    pub fn load(dir: &Path, node_id: u64) -> io::Result<Tls> {
        let read = |name: String| std::fs::read(dir.join(&name))
            .map_err(|e| io::Error::new(e.kind(), format!("{}: {}", dir.join(&name).display(), e)));
        Tls::from_pem(
            &read("ca.pem".to_string())?,
            &read(format!("node-{}.pem", node_id))?,
            &read(format!("node-{}-key.pem", node_id))?,
        )
    }

    pub fn from_pem(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> io::Result<Tls> {
        let provider: Arc<CryptoProvider> = Arc::new(rustls::crypto::ring::default_provider());

        let mut roots = RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(ca_pem) {
            roots.add(ca.map_err(err)?).map_err(err)?;
        }
        let roots = Arc::new(roots);
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<_, _>>()
            .map_err(err)?;
        let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(err)?;

        let client_config = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions().map_err(err)?
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(certs.clone(), key.clone_key()).map_err(err)?;

        let verifier = WebPkiClientVerifier::builder_with_provider(roots, provider.clone())
            .build().map_err(err)?;
        let peer_config = ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions().map_err(err)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs.clone(), key.clone_key()).map_err(err)?;

        let client_port_config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions().map_err(err)?
            .with_no_client_auth()
            .with_single_cert(certs, key).map_err(err)?;

        Ok(Tls {
            connector: TlsConnector::from(Arc::new(client_config)),
            peer_acceptor: TlsAcceptor::from(Arc::new(peer_config)),
            client_acceptor: TlsAcceptor::from(Arc::new(client_port_config)),
        })
    }
}

// the name a peer's cert must match: the host part of "host:port" (an IP or a DNS name)
pub fn server_name(addr: &str) -> io::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
    ServerName::try_from(host.to_string()).map_err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{RaftMsg, RequestVoteResponse, send_rpc, read_rpc, write_rpc};
    use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    use tokio::net::TcpListener;

    // a CA, plus a 127.0.0.1 cert it signed for one node: (ca pem, node cert pem, node key pem)
    fn cluster_certs() -> (String, String, String) {
        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).unwrap();

        let node_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
        let cert = params.signed_by(&node_key, &ca, &ca_key).unwrap();
        (ca.pem(), cert.pem(), node_key.serialize_pem())
    }

    fn tls_from((ca, cert, key): &(String, String, String)) -> Tls {
        Tls::from_pem(ca.as_bytes(), cert.as_bytes(), key.as_bytes()).unwrap()
    }

    fn reply() -> RaftMsg {
        RaftMsg::RequestVoteResponse(RequestVoteResponse { current_term: 9, voted: true })
    }

    // a raft listener that requires mTLS and answers one rpc; returns its address and
    // a handle that resolves to whether the handshake succeeded
    async fn mtls_server(tls: Tls) -> (String, tokio::task::JoinHandle<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            match tls.peer_acceptor.accept(stream).await {
                Ok(mut stream) => {
                    read_rpc(&mut stream).await.unwrap();
                    write_rpc(&mut stream, reply()).await.unwrap();
                    true
                }
                Err(_) => false,
            }
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn nodes_with_cluster_certs_talk_over_mutual_tls() {
        let certs = cluster_certs();
        let (addr, server) = mtls_server(tls_from(&certs)).await;

        let response = send_rpc(&addr, reply(), Some(&tls_from(&certs))).await;
        assert!(matches!(response, Ok(RaftMsg::RequestVoteResponse(r)) if r.current_term == 9));
        assert!(server.await.unwrap());
    }

    #[tokio::test]
    async fn peer_with_cert_from_another_ca_is_rejected() {
        let ours = cluster_certs();
        let theirs = cluster_certs();
        let (addr, server) = mtls_server(tls_from(&ours)).await;

        assert!(send_rpc(&addr, reply(), Some(&tls_from(&theirs))).await.is_err());
        assert!(!server.await.unwrap(), "server must refuse the handshake");
    }

    #[tokio::test]
    async fn plaintext_rpc_to_a_tls_node_is_rejected() {
        let (addr, server) = mtls_server(tls_from(&cluster_certs())).await;

        assert!(send_rpc(&addr, reply(), None).await.is_err());
        assert!(!server.await.unwrap());
    }
}
