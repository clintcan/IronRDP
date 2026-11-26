//! Regression coverage for the server's two pre-finalize budgets:
//! `PRE_HANDSHAKE_TIMEOUT`, which bounds the X.224 leg (how long a peer may
//! take to say anything at all), and `NEGOTIATION_TIMEOUT`, which bounds the
//! rest of negotiation and authentication.

use core::time::Duration;
use std::sync::Arc;

use ironrdp_server::{RdpServer, TransportTls};
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;

/// The server's private `PRE_HANDSHAKE_TIMEOUT`; change both together. The
/// tests assert that the connection is dropped within one second after this, so
/// any change to either value fails them instead of passing silently.
const PRE_HANDSHAKE_BUDGET: Duration = Duration::from_secs(5);

/// The server's private `NEGOTIATION_TIMEOUT`; change both together. Only
/// reached once a peer has sent its X.224 Connection Request.
const NEGOTIATION_BUDGET: Duration = Duration::from_secs(60);

/// Comfortably longer than either budget. Under paused time this costs no
/// wall-clock time; it only turns "the server never times out" into a clean
/// failure instead of a hung test.
const TEST_DEADLINE: Duration = Duration::from_secs(300);

/// A 19-byte X.224 Connection Request offering `PROTOCOL_HYBRID`, the same
/// bytes a client sends before starting TLS. Enough to clear the pre-handshake
/// leg and park on the TLS handshake instead.
const X224_HYBRID_CONNECTION_REQUEST: [u8; 19] = [
    0x03, 0x00, 0x00, 0x13, 0x0E, 0xE0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x08, 0x00, 0x02, 0x00, 0x00, 0x00,
];

/// A `TlsAcceptor` that can be constructed without any certificate material.
/// It would fail the first handshake, but nothing here handshakes: the client
/// stops before its ClientHello, so only a value of the `Hybrid` variant is
/// needed.
#[derive(Debug)]
struct NoCert;

impl ResolvesServerCert for NoCert {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        None
    }
}

fn tls_acceptor() -> TlsAcceptor {
    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(NoCert));
    TlsAcceptor::from(Arc::new(config))
}

fn server() -> RdpServer {
    RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_no_security()
        .with_no_input()
        .with_no_display()
        .build()
}

fn hybrid_server() -> RdpServer {
    RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_hybrid(tls_acceptor(), Vec::new())
        .with_no_input()
        .with_no_display()
        .build()
}

fn assert_dropped_at_budget(elapsed: Duration, budget: Duration) {
    assert!(
        elapsed >= budget && elapsed < budget + Duration::from_secs(1),
        "expected the connection to be dropped once the {budget:?} budget ran out, it took {elapsed:?}"
    );
}

/// A client that connects and sends nothing must be dropped once the
/// pre-handshake budget runs out, and not before. This is the slot a silent
/// peer would otherwise hold for the whole negotiation budget, keeping every
/// other client out of the accept loop.
// oracle: mutant removing the timeout around accept_begin (the server parks on the read, and the test deadline fires)
// oracle: mutant shortening or lengthening the budget (the drop lands outside the asserted window)
#[tokio::test(start_paused = true)]
async fn silent_client_is_dropped_after_the_pre_handshake_budget() {
    let mut server = server();
    // The client half stays open, and silent, for the whole test.
    let (_client, server_side) = tokio::io::duplex(4096);

    // `RdpServer`'s future is !Send, hence the LocalSet rather than a plain spawn.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let start = Instant::now();
            let serving = tokio::task::spawn_local(async move {
                server.run_connection_with(server_side, TransportTls::AlreadyDone).await
            });

            let outcome = tokio::time::timeout(TEST_DEADLINE, serving).await;
            let joined = outcome
                .expect("server never gave up on a client that sent nothing; the connection is held indefinitely");
            let result = joined.expect("serving task panicked");
            result.expect_err("a silent client must surface as an error, not a clean disconnect");
            assert_dropped_at_budget(start.elapsed(), PRE_HANDSHAKE_BUDGET);
        })
        .await;
}

/// The same bound applies to the split `run_connection_handshake` API.
// oracle: mutant removing the timeout around accept_begin (the server parks on the read, and the test deadline fires)
// oracle: mutant shortening or lengthening the budget (the drop lands outside the asserted window)
#[tokio::test(start_paused = true)]
async fn silent_client_is_dropped_by_run_connection_handshake() {
    let mut server = server();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    // The client stays connected, and silent, for the whole test.
    let _client = TcpStream::connect(addr).await.expect("connect");
    let (server_side, _) = listener.accept().await.expect("accept");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let start = Instant::now();
            let handshake =
                tokio::task::spawn_local(async move { server.run_connection_handshake(server_side).await.map(|_| ()) });

            let outcome = tokio::time::timeout(TEST_DEADLINE, handshake).await;
            let joined = outcome.expect("run_connection_handshake never gave up on a client that sent nothing");
            let result = joined.expect("handshake task panicked");
            result.expect_err("a silent client must surface as an error");
            assert_dropped_at_budget(start.elapsed(), PRE_HANDSHAKE_BUDGET);
        })
        .await;
}

/// Once the peer has sent its X.224, the shorter pre-handshake budget no longer
/// applies: a client that parks before its TLS ClientHello is bounded by
/// `NEGOTIATION_TIMEOUT` instead. Without this, the new bound could be read as
/// covering TLS/CredSSP, where a person may be typing a password.
// oracle: mutant shortening NEGOTIATION_TIMEOUT (the drop lands outside the asserted window)
#[tokio::test(start_paused = true)]
async fn client_that_sends_x224_then_parks_is_bounded_by_the_negotiation_budget() {
    let mut server = hybrid_server();
    let (mut client, server_side) = tokio::io::duplex(4096);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let start = Instant::now();
            let serving = tokio::task::spawn_local(async move {
                server.run_connection_with(server_side, TransportTls::Managed).await
            });

            client
                .write_all(&X224_HYBRID_CONNECTION_REQUEST)
                .await
                .expect("write X.224");
            // Keep the client half open and silent: no TLS ClientHello.
            let outcome = tokio::time::timeout(TEST_DEADLINE, serving).await;
            let joined = outcome.expect("server never gave up on a client that parked before TLS");
            let result = joined.expect("serving task panicked");
            result.expect_err("a peer that parks before TLS must surface as an error");
            assert_dropped_at_budget(start.elapsed(), NEGOTIATION_BUDGET);
            drop(client);
        })
        .await;
}
