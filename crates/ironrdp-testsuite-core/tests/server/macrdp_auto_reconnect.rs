//! (macrdp 30) An auto-reconnect cookie that fails verification falls back to a
//! normal logon instead of denying the connection.
//!
//! Plays the client by hand over plain RDP security, where no cookie ever
//! verifies, so the Client Info PDU's auto-reconnect packet is always unknown.

use core::time::Duration;
use std::sync::Arc;

use async_trait::async_trait;
use ironrdp_async::FramedWrite as _;
use ironrdp_core::{decode, encode_vec};
use ironrdp_pdu::mcs;
use ironrdp_pdu::nego::{self, SecurityProtocol};
use ironrdp_pdu::rdp::ClientInfoPdu;
use ironrdp_pdu::rdp::capability_sets::CapabilitySet;
use ironrdp_pdu::rdp::client_info::{ClientAutoReconnect, ExtendedClientOptionalInfo, PerformanceFlags};
use ironrdp_pdu::rdp::headers::{ShareControlHeader, ShareControlPdu};
use ironrdp_pdu::rdp::session_info::ServerAutoReconnect;
use ironrdp_pdu::x224::{X224, X224Data};
use ironrdp_server::{
    BoundConnection, ConnectionBinder, ConnectionHandler, ConnectionInfo, CredentialDecision,
    CredentialValidationError, CredentialValidator, Credentials, DesktopSize, DisplayContext, DisplayUpdate, RdpServer,
    RdpServerDisplay, RdpServerDisplayUpdates, ServerResult,
};
use ironrdp_testsuite_core::client_info::CLIENT_INFO_UNICODE;
use ironrdp_testsuite_core::gcc::CLIENT_GCC_WITHOUT_OPTIONAL_FIELDS;
use ironrdp_testsuite_core::rdp::{
    CLIENT_DEMAND_ACTIVE_PDU_BUFFER, CLIENT_FONT_LIST_BUFFER, CLIENT_INFO_PDU, CLIENT_SYNCHRONIZE_BUFFER,
    CONTROL_COOPERATE_BUFFER, CONTROL_REQUEST_CONTROL_BUFFER,
};
use ironrdp_tokio::TokioFramed;
use tokio::io::DuplexStream;
use tokio::sync::mpsc;

use super::acceptor::encode_send_data_request;

const SIZE: DesktopSize = DesktopSize {
    width: 1024,
    height: 768,
};

struct Display;

struct NoUpdates;

#[async_trait]
impl RdpServerDisplay for Display {
    async fn size(&mut self) -> DesktopSize {
        SIZE
    }

    async fn updates(&mut self, _: DisplayContext) -> ServerResult<Box<dyn RdpServerDisplayUpdates>> {
        Ok(Box::new(NoUpdates))
    }

    async fn offers_display_control(&mut self) -> bool {
        false
    }
}

#[async_trait]
impl RdpServerDisplayUpdates for NoUpdates {
    async fn next_update(&mut self) -> ServerResult<Option<DisplayUpdate>> {
        core::future::pending().await
    }
}

/// Reports `ConnectionInfo::auto_reconnect` of each logon that got past the checks.
struct Recorder(mpsc::UnboundedSender<bool>);

impl ConnectionHandler for Recorder {
    fn on_connection_info(&mut self, info: &ConnectionInfo) {
        let _ = self.0.send(info.auto_reconnect);
    }
}

struct AlwaysAccept;

#[async_trait]
impl CredentialValidator for AlwaysAccept {
    async fn validate(&self, _: &Credentials) -> Result<CredentialDecision, CredentialValidationError> {
        Ok(CredentialDecision::Accept)
    }
}

struct NeverBinds;

#[async_trait]
impl ConnectionBinder for NeverBinds {
    async fn bind_connection(&self, _: &Credentials, _: DesktopSize) -> ServerResult<BoundConnection> {
        panic!("an auto-reconnect attempt never reaches the binder");
    }
}

fn server(
    logons: mpsc::UnboundedSender<bool>,
    validator: Option<Arc<dyn CredentialValidator>>,
    binder: Option<Arc<dyn ConnectionBinder>>,
) -> RdpServer {
    RdpServer::builder()
        .with_addr(([127, 0, 0, 1], 0))
        .with_no_security()
        .with_no_input()
        .with_display_handler(Display)
        .with_auto_reconnect_cookie(Some(ServerAutoReconnect {
            logon_id: 1,
            random_bits: [0x11; 16],
        }))
        .with_credential_validator(validator)
        .with_connection_binder(binder)
        .with_connection_handler(Some(Box::new(Recorder(logons))))
        .build()
}

/// A Client Info PDU that carries an auto-reconnect packet for a cookie the
/// server never issued.
fn client_info_with_a_stale_cookie() -> Vec<u8> {
    let stale = ClientAutoReconnect::from_server_cookie(&ServerAutoReconnect {
        logon_id: 1,
        random_bits: [0x22; 16],
    });
    let cookie: [u8; 28] = encode_vec(&stale).unwrap().try_into().unwrap();
    let mut client_info = CLIENT_INFO_UNICODE.clone();
    let timezone = client_info.extra_info.optional_data.timezone().cloned().unwrap();
    client_info.extra_info.optional_data = ExtendedClientOptionalInfo::builder()
        .timezone(timezone)
        .session_id(0)
        .performance_flags(PerformanceFlags::DISABLE_WALLPAPER)
        .reconnect_cookie(cookie)
        .build();
    let pdu = ClientInfoPdu {
        security_header: CLIENT_INFO_PDU.security_header.clone(),
        client_info,
    };
    encode_vec(&pdu).unwrap()
}

async fn read_pdu(framed: &mut TokioFramed<DuplexStream>) -> Option<Vec<u8>> {
    framed.read_pdu().await.ok().map(|(_, bytes)| bytes.to_vec())
}

fn x224<'a, T: ironrdp_pdu::x224::X224Pdu<'a>>(pdu: T) -> Vec<u8> {
    encode_vec(&X224(pdu)).unwrap()
}

/// Plays the client through finalization. Stops early when the server closes.
async fn play_client(mut framed: TokioFramed<DuplexStream>) -> TokioFramed<DuplexStream> {
    let request = nego::ConnectionRequest {
        nego_data: None,
        flags: nego::RequestFlags::empty(),
        protocol: SecurityProtocol::empty(),
        correlation_info: None,
    };
    framed.write_all(&x224(request)).await.unwrap();
    read_pdu(&mut framed).await.unwrap();

    let mut blocks = CLIENT_GCC_WITHOUT_OPTIONAL_FIELDS.clone();
    blocks.network = None;
    blocks.core.desktop_width = SIZE.width;
    blocks.core.desktop_height = SIZE.height;
    let connect_initial = mcs::ConnectInitial::with_gcc_blocks(blocks).unwrap();
    let mut buf = ironrdp_core::WriteBuf::new();
    ironrdp_connector::encode_x224_packet(&connect_initial, &mut buf).unwrap();
    framed.write_all(buf.filled()).await.unwrap();
    let response = read_pdu(&mut framed).await.unwrap();
    let payload = decode::<X224<X224Data<'_>>>(&response).unwrap().0;
    let response = decode::<mcs::ConnectResponse>(payload.data.as_ref()).unwrap();
    let blocks = response.conference_create_response.gcc_blocks();
    let io_channel_id = blocks.network.io_channel;

    framed
        .write_all(&x224(mcs::ErectDomainPdu {
            sub_height: 0,
            sub_interval: 0,
        }))
        .await
        .unwrap();
    framed.write_all(&x224(mcs::AttachUserRequest)).await.unwrap();
    let confirm = read_pdu(&mut framed).await.unwrap();
    let user_channel_id = decode::<X224<mcs::AttachUserConfirm>>(&confirm).unwrap().0.initiator_id;
    for channel_id in [user_channel_id, io_channel_id] {
        framed
            .write_all(&x224(mcs::ChannelJoinRequest {
                initiator_id: user_channel_id,
                channel_id,
            }))
            .await
            .unwrap();
        read_pdu(&mut framed).await.unwrap();
    }

    framed
        .write_all(&encode_send_data_request(
            user_channel_id,
            io_channel_id,
            &client_info_with_a_stale_cookie(),
        ))
        .await
        .unwrap();
    // The license PDU, then Demand Active.
    for _ in 0..2 {
        if read_pdu(&mut framed).await.is_none() {
            return framed;
        }
    }

    let mut confirm = decode::<ShareControlHeader>(&CLIENT_DEMAND_ACTIVE_PDU_BUFFER).unwrap();
    let ShareControlPdu::ClientConfirmActive(active) = &mut confirm.share_control_pdu else {
        panic!("expected the Confirm Active fixture");
    };
    for capability in &mut active.pdu.capability_sets {
        if let CapabilitySet::Bitmap(bitmap) = capability {
            bitmap.desktop_width = SIZE.width;
            bitmap.desktop_height = SIZE.height;
        }
    }
    let confirm = encode_vec(&confirm).unwrap();
    for pdu in [
        confirm.as_slice(),
        &CLIENT_SYNCHRONIZE_BUFFER[..],
        &CONTROL_COOPERATE_BUFFER[..],
        &CONTROL_REQUEST_CONTROL_BUFFER[..],
        &CLIENT_FONT_LIST_BUFFER[..],
    ] {
        if framed
            .write_all(&encode_send_data_request(user_channel_id, io_channel_id, pdu))
            .await
            .is_err()
        {
            return framed;
        }
    }
    // The four finalization responses; the server checks the cookie after them.
    for _ in 0..4 {
        if read_pdu(&mut framed).await.is_none() {
            break;
        }
    }
    framed
}

enum Outcome {
    /// The logon got past the checks; `ConnectionInfo::auto_reconnect` is attached.
    LoggedOn(bool),
    /// The server ended the connection first.
    Ended(ServerResult<()>),
}

/// Runs one connection until the server either logs the client on or ends it.
async fn run(validator: Option<Arc<dyn CredentialValidator>>, binder: Option<Arc<dyn ConnectionBinder>>) -> Outcome {
    let (logons, mut logged_on) = mpsc::unbounded_channel();
    let mut server = server(logons, validator, binder);
    let (client, server_stream) = tokio::io::duplex(64 * 1024);
    let serving = server.run_connection(server_stream);
    let client = async {
        let _client = play_client(TokioFramed::new(client)).await;
        core::future::pending::<()>().await;
    };
    let outcome = async {
        tokio::select! {
            result = serving => Outcome::Ended(result),
            auto_reconnect = logged_on.recv() => Outcome::LoggedOn(auto_reconnect.unwrap()),
            () = client => unreachable!(),
        }
    };
    tokio::time::timeout(Duration::from_secs(10), outcome)
        .await
        .expect("the connection neither logged on nor ended")
}

#[tokio::test]
async fn a_stale_auto_reconnect_cookie_falls_back_to_a_normal_logon() {
    match run(None, None).await {
        Outcome::LoggedOn(auto_reconnect) => assert!(!auto_reconnect, "a normal logon, not an auto-reconnect"),
        Outcome::Ended(result) => {
            panic!("the server ended the connection instead of falling back to a normal logon: {result:?}")
        }
    }
}

/// With a credential validator, the acceptor skipped it because the client sent
/// a cookie, so the fallback is denied: it grants nothing.
#[tokio::test]
async fn with_a_validator_the_fallback_is_denied() {
    match run(Some(Arc::new(AlwaysAccept)), None).await {
        Outcome::LoggedOn(_) => panic!("the fallback logged on without validated credentials"),
        Outcome::Ended(result) => {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("credentials were not prepared"), "{error}");
        }
    }
}

/// A connection binder never runs for an auto-reconnect attempt, so with a
/// binder the unknown cookie is still denied, as upstream does.
#[tokio::test]
async fn with_a_connection_binder_the_cookie_is_still_denied() {
    match run(None, Some(Arc::new(NeverBinds))).await {
        Outcome::LoggedOn(_) => panic!("the fallback logged on without a connection binding"),
        Outcome::Ended(result) => {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("cookie rejected"), "{error}");
        }
    }
}
