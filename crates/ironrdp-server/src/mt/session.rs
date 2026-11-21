//! (macrdp divergence 12) The server side of macrdp's UDP multitransport.
//!
//! Declared from `macrdp.rs` (`#[path]`), so it sits under the `server` module and
//! can extend [`RdpServer`] and [`ConnectionState`] with their private fields;
//! `server.rs` itself only carries one-line hook calls. Upstream's own
//! multitransport (`with_udp_transport`) is a separate implementation that stays
//! inert: macrdp never sets `udp_bind_addr`, so upstream never offers UDP, never
//! gets a transport handle, and never spends the connection's one Soft-Sync.
//!
//! Configuration lives on the server ([`MtState`]); each connection gets an
//! [`MtConnection`] in its [`ConnectionState`].
//!
//! Lifecycle per connection: [`RdpServer::mt_offer`] (before negotiation) →
//! [`RdpServer::mt_begin_connection`] (the connection's state exists) →
//! [`ConnectionState::mt_on_finalized`] (after the acceptor sent the Initiate
//! Multitransport Request) → Soft-Sync and routing while the session runs → the
//! [`MtConnection`] drop once the connection's state is dropped, on every path out
//! of it. See the divergence log's (12) for the history behind each rule.

use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use ironrdp_acceptor::{Acceptor, MultitransportSecurityRng};
use ironrdp_dvc as dvc;
use ironrdp_dvc::pdu::SoftSyncTunnelType;
use ironrdp_pdu::gcc::MultiTransportFlags;
use ironrdp_pdu::rdp::multitransport::{MultitransportResponsePdu, RequestedProtocol};
use ironrdp_rdpsnd::pdu::AudioFormat;
use ironrdp_svc::{ChannelFlags, SvcMessage, server_encode_svc_messages};
#[cfg(feature = "egfx")]
use ironrdp_tokio::FramedWrite;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

#[cfg(feature = "egfx")]
use crate::GfxDvcBridge;
use crate::error::{ServerError, ServerErrorExt as _, ServerResult};
use crate::mt::audio_dvc::{AudioLossyDvc, AudioReliableDvc, NegotiatedAudioFormat};
use crate::mt::{CookieRegistry, MigrationState, MultitransportProvider, TunnelSender, env_truthy};
use crate::server::{ConnectionOutput, ConnectionState, DynamicChannelFactory, RdpServer};

/// Configuration, set once at startup by macrdp, plus the offer of the connection
/// being negotiated.
#[derive(Default)]
pub(in crate::server) struct MtState {
    provider: Option<Box<dyn MultitransportProvider>>,
    cookies: Option<CookieRegistry>,
    tunnel_sender: Option<TunnelSender>,
    egfx_on_lossy_handle: Option<Arc<AtomicBool>>,
    egfx_on_udp_handle: Option<Arc<AtomicBool>>,
    demigrate_request: Option<Arc<AtomicBool>>,
    migrate_egfx: bool,
    /// Shared with the audio DVC factory, which attaches the dual audio DVCs.
    lossy_audio: LossyAudioConfig,
    /// What [`RdpServer::mt_offer`] set up for the connection now negotiating, until
    /// [`RdpServer::mt_begin_connection`] hands it to that connection's state.
    pending_offer: Option<PendingOffer>,
}

/// The lossy audio setup, shared by the server and its audio DVC factory.
#[derive(Clone, Default)]
struct LossyAudioConfig {
    formats: Arc<StdMutex<Option<Vec<AudioFormat>>>>,
    /// The format the reliable audio DVC negotiated. One per server, as before: a
    /// new connection's reliable DVC overwrites it once it negotiates.
    format: NegotiatedAudioFormat,
}

impl LossyAudioConfig {
    fn formats(&self) -> Option<Vec<AudioFormat>> {
        self.formats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn is_on(&self) -> bool {
        self.formats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
}

/// Attaches the dual audio DVCs (reliable handshake over TCP, lossy waves over the
/// DTLS tunnel) to each connection, when lossy audio is configured.
struct AudioDvcFactory(LossyAudioConfig);

impl DynamicChannelFactory for AudioDvcFactory {
    fn attach(&self, channels: dvc::DrdynvcServer) -> dvc::DrdynvcServer {
        match self.0.formats() {
            Some(formats) => channels
                .with_dynamic_channel(AudioReliableDvc::new(formats, self.0.format.clone()))
                .with_dynamic_channel(AudioLossyDvc::new()),
            None => channels,
        }
    }
}

/// The offer's cookie registration and the tunnel's inbound data, kept from the
/// offer until the connection's state exists.
struct PendingOffer {
    registration: Arc<StdMutex<OfferRegistration>>,
    inbound_rx: mpsc::UnboundedReceiver<Vec<u8>>,
}

/// What a connection reads of the server's configuration, copied when the
/// connection's state is created (macrdp sets it once, at startup).
struct MtConfig {
    cookies: Option<CookieRegistry>,
    tunnel_sender: Option<TunnelSender>,
    egfx_on_lossy_handle: Option<Arc<AtomicBool>>,
    egfx_on_udp_handle: Option<Arc<AtomicBool>>,
    demigrate_request: Option<Arc<AtomicBool>>,
    /// `set_migrate_egfx` or the legacy env fallback.
    migrate_egfx: bool,
    lossy_audio: bool,
    lossy_audio_format: NegotiatedAudioFormat,
}

/// One connection's multitransport state. Its drop is the connection's teardown.
#[derive(Default)]
pub(in crate::server) struct MtConnection {
    /// `None` only in a connection state built without the server (tests).
    config: Option<MtConfig>,
    /// What the RNG adapter registered for this connection's offer.
    offer: Option<Arc<StdMutex<OfferRegistration>>>,
    /// The in-flight request, once the acceptor reported sending it.
    migration: Option<MigrationState>,
    /// Raised by the listener when it binds this connection's tunnel.
    tunnel_bound: Option<Arc<AtomicBool>>,
    /// The tunnel's client-to-server data; the client loop drains it.
    tunnel_inbound_rx: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    /// The client sent a successful Initiate Multitransport Response; an empty
    /// Soft-Sync is owed (see [`ConnectionState::mt_on_response`]).
    soft_sync_owed: bool,
    #[cfg(feature = "egfx")]
    egfx_on_udp: bool,
    lossy_audio_block_no: u8,
    lossy_audio_streaming: bool,
    /// The Soft-Sync request named the lossy audio channel. Until the client has
    /// also acknowledged it, waves stay on TCP (see
    /// [`ConnectionState::mt_route_lossy_audio`]).
    lossy_audio_soft_synced: bool,
}

impl Drop for MtConnection {
    /// The connection ended, however it ended: retire its tunnel. Lowering the
    /// bound flag makes the listener treat the abandoned tunnel as a benign
    /// teardown (it ages out) rather than a death that would start the offer
    /// cooldown. Evicting the cookie here, not at activation, also covers a
    /// connection that died before activation, and stops a late tunnel bind from
    /// resurrecting a retired flag.
    fn drop(&mut self) {
        let Some(config) = self.config.take() else {
            return;
        };
        for handle in [
            &config.egfx_on_lossy_handle,
            &config.egfx_on_udp_handle,
            // A fresh connection retries UDP instead of instantly falling back.
            &config.demigrate_request,
        ]
        .into_iter()
        .flatten()
        {
            handle.store(false, Ordering::Relaxed);
        }
        if let Some(flag) = &self.tunnel_bound {
            flag.store(false, Ordering::Relaxed);
        }
        let cookie = self
            .offer
            .as_ref()
            .and_then(|r| r.lock().unwrap_or_else(std::sync::PoisonError::into_inner).cookie);
        if let (Some(cookie), Some(registry)) = (cookie, config.cookies.as_ref()) {
            registry.remove(&cookie);
        }
    }
}

impl MtConnection {
    fn tunnel_is_bound(&self) -> bool {
        self.tunnel_bound.as_ref().is_some_and(|f| f.load(Ordering::Relaxed))
    }

    /// Whether this connection is configured to move a channel onto the tunnel.
    fn has_channel_to_migrate(&self) -> bool {
        self.config.as_ref().is_some_and(|c| c.lossy_audio || c.migrate_egfx)
    }

    /// Claim the one Soft-Sync this connection gets. False when it's already spent or
    /// there's no request in flight.
    #[cfg(feature = "egfx")]
    fn claim_soft_sync(&mut self) -> bool {
        match self.migration.as_mut() {
            Some(state) if !state.soft_sync_sent => {
                state.soft_sync_sent = true;
                true
            }
            _ => false,
        }
    }

    /// Hand DVC messages to the listener for this connection's tunnel. The tunnel
    /// carries the BARE DRDYNVC PDU: `RDP_TUNNEL_DATA` is the framing, so each message
    /// is encoded unframed (a `CHANNEL_PDU_HEADER` would make the client misparse the
    /// stream). Best effort: with no sender or cookie the data is dropped.
    fn route_dvc_over_udp(&self, messages: Vec<SvcMessage>) -> ServerResult<()> {
        let Some(sender) = self.config.as_ref().and_then(|c| c.tunnel_sender.as_ref()) else {
            warn!("migrated channel data but no tunnel sender; dropping");
            return Ok(());
        };
        let Some(cookie) = self.migration.as_ref().map(|m| m.cookie) else {
            warn!("migrated channel data but no migration cookie; dropping");
            return Ok(());
        };
        let count = messages.len();
        for message in messages {
            sender.send(cookie, message.encode_unframed_pdu().map_err(ServerError::encode)?);
        }
        trace!(pdus = count, "routed DVC PDUs over the UDP tunnel");
        Ok(())
    }
}

/// The cookie and bound flag the RNG adapter registered, read back by the server.
#[derive(Default)]
struct OfferRegistration {
    cookie: Option<[u8; 16]>,
    tunnel_bound: Option<Arc<AtomicBool>>,
}

/// Supplies the acceptor's Initiate Multitransport Request with its cookie and
/// request id, registering the cookie with the listener *as it is generated*: the
/// client can only open the tunnel after receiving the request, so the listener
/// always knows the cookie first. The cookie is CSPRNG-generated, so it can't be
/// forged by someone who can see the predictable request id.
struct OfferRng {
    registry: CookieRegistry,
    inbound: Option<mpsc::UnboundedSender<Vec<u8>>>,
    registration: Arc<StdMutex<OfferRegistration>>,
}

impl MultitransportSecurityRng for OfferRng {
    fn fill_security_cookie(&mut self, cookie: &mut [u8; 16]) {
        if let Err(error) = getrandom::getrandom(cookie) {
            // The system RNG failing is near impossible; don't panic the server over
            // it. The binding still works (a registry match); only unpredictability
            // is lost in this degenerate case.
            tracing::error!(%error, "system RNG failed for the multitransport cookie; using a weak fallback");
            let seed = MT_REQUEST_ID.load(Ordering::Relaxed);
            for (i, b) in (0u32..).zip(cookie.iter_mut()) {
                *b = seed.wrapping_mul(2_654_435_761).wrapping_add(i).to_le_bytes()[0];
            }
        }
        let Some(inbound) = self.inbound.take() else {
            // A second request for the same connection never happens (the acceptor
            // sends one); keep the first registration rather than replace it.
            warn!("multitransport cookie requested twice for one connection; keeping the first");
            return;
        };
        let tunnel_bound = self.registry.register(*cookie, inbound);
        let mut registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        registration.cookie = Some(*cookie);
        registration.tunnel_bound = Some(tunnel_bound);
    }

    fn next_request_id(&mut self) -> u32 {
        MT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    }
}

/// Process-wide request id, so ids never repeat across connections.
static MT_REQUEST_ID: AtomicU32 = AtomicU32::new(1);

/// EXPERIMENTAL: migrate EGFX onto the tunnel (legacy env fallback for
/// `--udp-migrate-egfx`). Read once.
fn migrate_egfx_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| env_truthy("MACRDP_UDP_MIGRATE_EGFX"))
}

/// Isolation test: migrate EGFX onto the LOSSY (DTLS) tunnel instead of the reliable
/// one, to exercise the DTLS `RDP_TUNNEL_DATA` path with a proven payload. Read once.
#[cfg(feature = "egfx")]
fn migrate_egfx_lossy() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| env_truthy("MACRDP_UDP_MIGRATE_EGFX_LOSSY"))
}

fn tunnel_name(tunnel: SoftSyncTunnelType) -> &'static str {
    if tunnel == SoftSyncTunnelType::LOSSY_UDP {
        "lossy (UdpFecL)"
    } else if tunnel == SoftSyncTunnelType::RELIABLE_UDP {
        "reliable (UdpFecR)"
    } else {
        "unknown"
    }
}

// ---- Configuration (macrdp calls these once, at startup) ----

#[expect(
    clippy::multiple_inherent_impl,
    reason = "macrdp's methods stay out of server.rs so the fork's hooks there stay small"
)]
impl RdpServer {
    /// Never set together with upstream's `with_udp_transport`: both would offer UDP
    /// to the same connection.
    pub fn set_multitransport_provider(&mut self, provider: Option<Box<dyn MultitransportProvider>>) {
        if provider.is_some() && self.opts.udp_bind_addr.is_some() {
            warn!(
                "Both with_udp_transport and set_multitransport_provider are set; macrdp's offer replaces \
                 upstream's, and upstream's UDP accept still runs"
            );
        }
        self.macrdp.mt.provider = provider;
    }

    pub fn set_multitransport_cookie_registry(&mut self, registry: Option<CookieRegistry>) {
        self.macrdp.mt.cookies = registry;
    }

    pub fn set_multitransport_tunnel_sender(&mut self, sender: Option<TunnelSender>) {
        self.macrdp.mt.tunnel_sender = sender;
    }

    /// Raised while EGFX rides the LOSSY tunnel, so the H.264 pipeline can arm
    /// ack-driven IDR recovery.
    pub fn set_egfx_on_lossy_handle(&mut self, handle: Option<Arc<AtomicBool>>) {
        self.macrdp.mt.egfx_on_lossy_handle = handle;
    }

    /// Raised while EGFX rides a UDP tunnel, so the H.264 pipeline applies
    /// frame-ack-lag backpressure.
    pub fn set_egfx_on_udp_handle(&mut self, handle: Option<Arc<AtomicBool>>) {
        self.macrdp.mt.egfx_on_udp_handle = handle;
    }

    /// Set by the H.264 pipeline when the reliable tunnel has wedged; EGFX then moves
    /// back to TCP for the rest of the session.
    pub fn set_demigrate_request_handle(&mut self, handle: Option<Arc<AtomicBool>>) {
        self.macrdp.mt.demigrate_request = handle;
    }

    pub fn set_migrate_egfx(&mut self, on: bool) {
        self.macrdp.mt.migrate_egfx = on;
    }

    /// Registers the dual audio DVCs (reliable handshake over TCP, lossy waves over
    /// the DTLS tunnel) when `Some`.
    pub fn set_multitransport_lossy_audio_formats(&mut self, formats: Option<Vec<AudioFormat>>) {
        *self
            .macrdp
            .mt
            .lossy_audio
            .formats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = formats;
    }

    /// Adds the audio DVC factory to upstream's dynamic channel factories. Called
    /// once, by [`RdpServer::install_macrdp_channels`].
    pub(in crate::server) fn install_mt_channels(&mut self) {
        self.dynamic_channel_factories
            .push(Box::new(AudioDvcFactory(self.macrdp.mt.lossy_audio.clone())));
    }

    // ---- Connection lifecycle ----

    /// Before negotiation: offer UDP to this connection unless the tunnel-death
    /// cooldown says not to. Not called for a preemption candidate: it negotiates
    /// while the live connection runs, and upstream attaches nothing to it.
    pub(in crate::server) fn mt_offer(&mut self, acceptor: &mut Acceptor) {
        // A connection that never reached its state leaves its offer here.
        self.macrdp.mt.pending_offer = None;
        let Some(provider) = self.macrdp.mt.provider.as_ref() else {
            return;
        };
        let protocol = provider.requested_protocol();
        // After the listener declares a tunnel dead (an overlay network dropping
        // UDP), offers stay off for a cooldown, so the client's reconnect (mstsc
        // resets the session ~60 s after its tunnel dies) lands as stable plain TCP
        // instead of re-establishing a doomed tunnel and cycling.
        if self
            .macrdp
            .mt
            .cookies
            .as_ref()
            .is_some_and(CookieRegistry::multitransport_suppressed)
        {
            info!("multitransport offer SUPPRESSED (tunnel-death cooldown) \u{2014} this connection runs plain TCP");
            return;
        }

        let transport = match protocol {
            RequestedProtocol::UdpFecR => MultiTransportFlags::TRANSPORT_TYPE_UDP_FECR,
            RequestedProtocol::UdpFecL => MultiTransportFlags::TRANSPORT_TYPE_UDP_FECL,
        };
        acceptor.set_multitransport_offer(Some(transport | MultiTransportFlags::SOFT_SYNC_TCP_TO_UDP));
        acceptor.set_multitransport_requested_protocol(protocol);
        if let Some(registry) = self.macrdp.mt.cookies.clone() {
            // The listener forwards this tunnel's client-to-server data here, keyed
            // by cookie; the client loop drains the receiver.
            let (inbound, inbound_rx) = mpsc::unbounded_channel();
            let registration = Arc::new(StdMutex::new(OfferRegistration::default()));
            acceptor.set_multitransport_security_rng(Box::new(OfferRng {
                registry,
                inbound: Some(inbound),
                registration: Arc::clone(&registration),
            }));
            self.macrdp.mt.pending_offer = Some(PendingOffer {
                registration,
                inbound_rx,
            });
        }
    }

    /// The connection's state now exists: give it its multitransport state, so the
    /// state's drop tears the connection's tunnel down on every path out of it.
    pub(in crate::server) fn mt_begin_connection(&mut self, conn: &mut ConnectionState) {
        let mt = &mut self.macrdp.mt;
        let pending = mt.pending_offer.take();
        let config = MtConfig {
            cookies: mt.cookies.clone(),
            tunnel_sender: mt.tunnel_sender.clone(),
            egfx_on_lossy_handle: mt.egfx_on_lossy_handle.clone(),
            egfx_on_udp_handle: mt.egfx_on_udp_handle.clone(),
            demigrate_request: mt.demigrate_request.clone(),
            // Without `egfx` there is no EGFX channel to migrate.
            migrate_egfx: cfg!(feature = "egfx") && (mt.migrate_egfx || migrate_egfx_enabled()),
            lossy_audio: mt.lossy_audio.is_on(),
            lossy_audio_format: mt.lossy_audio.format.clone(),
        };
        let (offer, tunnel_inbound_rx) = match pending {
            Some(PendingOffer {
                registration,
                inbound_rx,
            }) => (Some(registration), Some(inbound_rx)),
            None => (None, None),
        };
        let mut connection = MtConnection::default();
        connection.config = Some(config);
        connection.offer = offer;
        connection.tunnel_inbound_rx = tunnel_inbound_rx;
        conn.macrdp.mt = connection;
    }
}

#[expect(
    clippy::multiple_inherent_impl,
    reason = "macrdp's methods stay out of server.rs so the fork's hooks there stay small"
)]
impl ConnectionState {
    /// After a finalize pass: record the request the acceptor sent, so the client's
    /// response can be matched and Soft-Sync fired once its tunnel binds.
    pub(in crate::server) fn mt_on_finalized(&mut self, acceptor: &Acceptor, reactivation: bool) {
        if reactivation {
            return;
        }
        let Some(request) = acceptor.multitransport_request() else {
            return;
        };
        let mt = &mut self.macrdp.mt;
        // Without a cookie registry there is no bound flag: the listener binds softly.
        mt.tunnel_bound = mt.offer.as_ref().and_then(|r| {
            r.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tunnel_bound
                .clone()
        });
        mt.migration = Some(MigrationState {
            request_id: request.request_id,
            cookie: request.security_cookie,
            protocol: request.requested_protocol,
            soft_sync_sent: false,
        });
        debug!(
            request_id = request.request_id,
            protocol = ?request.requested_protocol,
            soft_sync = acceptor.multitransport_soft_sync_negotiated(),
            "Server Initiate Multitransport Request was sent by the acceptor"
        );
    }

    /// The tunnel's client-to-server data, for the client loop to drain.
    pub(in crate::server) fn mt_take_tunnel_inbound(&mut self) -> Option<mpsc::UnboundedReceiver<Vec<u8>>> {
        self.macrdp.mt.tunnel_inbound_rx.take()
    }

    /// The client's Initiate Multitransport Response, from the message channel.
    ///
    /// mstsc does send `S_OK` here once the offer travels on the MCS message channel
    /// (the rebased acceptor's behaviour since the e258f6a0 pin; the June 2026 notes
    /// saw only tunnel creation). A connection gets exactly one Soft-Sync, so when
    /// something is configured to migrate (lossy audio, or EGFX) the response must
    /// NOT spend it on an empty request: [`Self::mt_soft_sync_on_egfx`] sends the one
    /// that names the channel once it is open. Spending it here left the lossy audio
    /// channel unmigrated, and mstsc dropped every wave streamed into the tunnel
    /// (silent audio since v0.10.0). With nothing to migrate, the response gets the
    /// empty Soft-Sync, from [`Self::mt_with_owed_soft_sync`].
    pub(in crate::server) fn mt_on_response(&mut self, response: &MultitransportResponsePdu) {
        let has_channel_to_migrate = self.macrdp.mt.has_channel_to_migrate();
        let mt = &mut self.macrdp.mt;
        let Some(state) = mt.migration.as_mut() else {
            return;
        };
        if state.request_id != response.request_id {
            warn!(
                got = response.request_id,
                in_flight = state.request_id,
                "Initiate Multitransport Response request_id mismatch; ignoring"
            );
            return;
        }
        if !response.is_success() {
            debug!(
                request_id = response.request_id,
                protocol = ?state.protocol,
                hr = format_args!("{:#010x}", response.hr_response),
                "client could not establish UDP; staying on TCP"
            );
            mt.migration = None;
            return;
        }
        if state.soft_sync_sent {
            debug!(
                request_id = response.request_id,
                "Initiate Multitransport Response S_OK after Soft-Sync; ignoring"
            );
            return;
        }
        if has_channel_to_migrate {
            debug!(
                request_id = response.request_id,
                "Initiate Multitransport Response S_OK \u{2014} leaving the Soft-Sync to the migration gate"
            );
            return;
        }
        state.soft_sync_sent = true;
        mt.soft_sync_owed = true;
    }

    /// Appends the empty Soft-Sync [`Self::mt_on_response`] owes, if any, to the
    /// message-channel handler's outputs.
    pub(in crate::server) fn mt_with_owed_soft_sync(
        &mut self,
        mut outputs: Vec<ConnectionOutput>,
        user_channel_id: u16,
    ) -> ServerResult<Vec<ConnectionOutput>> {
        if core::mem::take(&mut self.macrdp.mt.soft_sync_owed) {
            debug!("Initiate Multitransport Response S_OK \u{2014} Soft-Sync gate open");
            // Empty: EGFX migration is driven by the listener-bound gate, not this path.
            if let Some(frame) =
                self.mt_soft_sync_frame(user_channel_id, SoftSyncTunnelType::RELIABLE_UDP, Vec::new())?
            {
                outputs.push(ConnectionOutput::ResponseFrame(frame));
            }
        }
        Ok(outputs)
    }

    /// Route an EGFX batch over the tunnel once EGFX has migrated. Returns the batch
    /// back for TCP when it hasn't, or when the H.264 watchdog asked to de-migrate:
    /// the reliable tunnel wedged (acks silent while shipping), so EGFX goes back to
    /// TCP DRDYNVC for the rest of the session (mstsc renders EGFX on TCP after a
    /// Soft-Sync). One way; reset on reconnect.
    #[cfg(feature = "egfx")]
    pub(in crate::server) fn mt_route_egfx(
        &mut self,
        messages: Vec<SvcMessage>,
    ) -> ServerResult<Option<Vec<SvcMessage>>> {
        let mt = &mut self.macrdp.mt;
        if !mt.egfx_on_udp {
            return Ok(Some(messages));
        }
        let config = mt.config.as_ref();
        if config
            .and_then(|c| c.demigrate_request.as_ref())
            .is_some_and(|h| h.load(Ordering::Relaxed))
        {
            if let Some(handle) = config.and_then(|c| c.egfx_on_udp_handle.as_ref()) {
                handle.store(false, Ordering::Relaxed);
            }
            mt.egfx_on_udp = false;
            warn!(
                "EGFX-over-UDP reliable tunnel wedged \u{2014} watchdog de-migrating EGFX to TCP DRDYNVC for the rest of this session"
            );
            return Ok(Some(messages));
        }
        mt.route_dvc_over_udp(messages)?;
        Ok(None)
    }

    /// Called after EGFX ships over TCP. Once the tunnel is bound and EGFX is live
    /// (its DVC is open, the client is fully in DVC mode), send the one Soft-Sync this
    /// connection gets. mstsc signals multitransport success by creating the tunnel,
    /// never by a TCP response, so this is the real success gate.
    #[cfg(feature = "egfx")]
    pub(in crate::server) async fn mt_soft_sync_on_egfx(
        &mut self,
        writer: &mut impl FramedWrite,
        user_channel_id: u16,
    ) -> ServerResult<()> {
        if !self.macrdp.mt.tunnel_is_bound() {
            return Ok(());
        }
        let Some(config) = self.macrdp.mt.config.as_ref() else {
            return Ok(());
        };
        let lossy_audio = config.lossy_audio;
        let migrate = config.migrate_egfx;

        // Lossy audio: Soft-Sync the LOSSY audio DVC onto the LOSSY tunnel (the inverse
        // of EGFX, which goes to the reliable one). Its format handshake runs on the
        // reliable AUDIO_PLAYBACK_DVC over TCP: mstsc tears the socket down if formats
        // arrive on the lossy name. Wait for the channel to open before claiming the
        // one-time guard, so an unopened channel just retries on the next frame.
        if lossy_audio {
            let Some(audio_id) = self.mt_open_channel_id::<AudioLossyDvc>() else {
                return Ok(());
            };
            if !self.macrdp.mt.claim_soft_sync() {
                return Ok(());
            }
            debug!(
                audio_channel_id = audio_id,
                "UDP lossy tunnel bound \u{2014} Soft-Sync migrating AUDIO_PLAYBACK_LOSSY_DVC onto the LOSSY (UdpFecL) \
                 tunnel (waves only; the format handshake runs on the reliable AUDIO_PLAYBACK_DVC over TCP)"
            );
            self.mt_send_soft_sync(writer, user_channel_id, SoftSyncTunnelType::LOSSY_UDP, vec![audio_id])
                .await?;
            self.macrdp.mt.lossy_audio_soft_synced = true;
            return Ok(());
        }

        // EXPERIMENTAL (`--udp-migrate-egfx`, or `MACRDP_UDP_MIGRATE_EGFX`): name the
        // EGFX DVC so the client moves it onto the tunnel. Off, an empty Soft-Sync is
        // the proven safe spike: EGFX stays on TCP.
        let egfx_id = if migrate {
            let id = self.mt_open_channel_id::<GfxDvcBridge>();
            if id.is_none() {
                // Retry on the next frame rather than spend the one Soft-Sync on nothing.
                return Ok(());
            }
            id
        } else {
            None
        };
        if !self.macrdp.mt.claim_soft_sync() {
            return Ok(());
        }
        let tunnel = if migrate_egfx_lossy() {
            SoftSyncTunnelType::LOSSY_UDP
        } else {
            SoftSyncTunnelType::RELIABLE_UDP
        };
        if let Some(id) = egfx_id {
            let mt = &mut self.macrdp.mt;
            mt.egfx_on_udp = true;
            let config = mt.config.as_ref();
            if let Some(handle) = config.and_then(|c| c.egfx_on_udp_handle.as_ref()) {
                handle.store(true, Ordering::Relaxed);
            }
            // The reliable tunnel never drops; only the lossy one needs ack-driven
            // IDR recovery.
            if tunnel == SoftSyncTunnelType::LOSSY_UDP
                && let Some(handle) = config.and_then(|c| c.egfx_on_lossy_handle.as_ref())
            {
                handle.store(true, Ordering::Relaxed);
            }
            debug!(
                gfx_channel_id = id,
                "--udp-migrate-egfx: Soft-Sync will migrate the EGFX DVC onto the UDP tunnel"
            );
        }
        debug!("UDP tunnel bound + EGFX active \u{2014} Soft-Sync gate open");
        self.mt_send_soft_sync(writer, user_channel_id, tunnel, egfx_id.into_iter().collect())
            .await
    }

    /// The channel id of `T`'s DVC, once it's open.
    fn mt_open_channel_id<T: dvc::DvcServerProcessor + 'static>(&mut self) -> Option<u32> {
        let drdynvc = self.get_svc_processor::<dvc::DrdynvcServer>()?;
        drdynvc
            .get_channel_id_by_type::<T>()
            .filter(|&id| drdynvc.is_channel_opened(id))
    }

    /// Send a `DYNVC_SOFT_SYNC_REQUEST` over the DRDYNVC static channel on TCP.
    #[cfg(feature = "egfx")]
    async fn mt_send_soft_sync(
        &mut self,
        writer: &mut impl FramedWrite,
        user_channel_id: u16,
        tunnel: SoftSyncTunnelType,
        channel_ids: Vec<u32>,
    ) -> ServerResult<()> {
        let Some(data) = self.mt_soft_sync_frame(user_channel_id, tunnel, channel_ids)? else {
            return Ok(());
        };
        writer
            .write_all(&data)
            .await
            .map_err(|e| ServerError::io("write_all", e))
    }

    /// Encode a `DYNVC_SOFT_SYNC_REQUEST` for the DRDYNVC static channel on TCP.
    /// Empty `channel_ids` migrates nothing (everything stays on TCP). Channel
    /// routing stays macrdp's (see ironrdp-dvc's `request_soft_sync`), which is what
    /// keeps the TCP fallback for a wedged tunnel possible. `None`: nothing to send.
    fn mt_soft_sync_frame(
        &mut self,
        user_channel_id: u16,
        tunnel: SoftSyncTunnelType,
        channel_ids: Vec<u32>,
    ) -> ServerResult<Option<Vec<u8>>> {
        let Some(drdynvc_channel_id) = self.get_channel_id_by_type::<dvc::DrdynvcServer>() else {
            warn!("No DRDYNVC channel; cannot send Soft-Sync request");
            return Ok(None);
        };
        let Some(drdynvc) = self.get_svc_processor::<dvc::DrdynvcServer>() else {
            return Ok(None);
        };
        let migrating = !channel_ids.is_empty();
        let message = match drdynvc.request_soft_sync(tunnel, channel_ids) {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, "Soft-Sync request refused; staying on TCP");
                return Ok(None);
            }
        };
        let data = server_encode_svc_messages(vec![message], drdynvc_channel_id, user_channel_id)
            .map_err(ServerError::encode)?;
        if migrating {
            debug!(
                tunnel = tunnel_name(tunnel),
                "Sent DYNVC_SOFT_SYNC_REQUEST (migrating channels onto the UDP tunnel)"
            );
        } else {
            debug!(
                tunnel = tunnel_name(tunnel),
                "Sent DYNVC_SOFT_SYNC_REQUEST (empty channel list \u{2014} nothing migrated, stays on TCP)"
            );
        }
        Ok(Some(data))
    }

    /// Ship a wave over the lossy tunnel if that path is live (the reliable audio DVC
    /// negotiated a format, the tunnel is bound, the lossy DVC is open). Returns the
    /// wave back otherwise, for static RDPSND over TCP, so exactly one playback path
    /// is ever in use.
    pub(in crate::server) fn mt_route_lossy_audio(&mut self, data: Vec<u8>, ts: u32) -> Option<Vec<u8>> {
        let mt = &self.macrdp.mt;
        let Some(config) = mt.config.as_ref() else {
            return Some(data);
        };
        if !config.lossy_audio || config.tunnel_sender.is_none() || mt.migration.is_none() || !mt.tunnel_is_bound() {
            return Some(data);
        }
        let Some(format_no) = config.lossy_audio_format.get() else {
            return Some(data);
        };
        let Some(lossy_id) = self.mt_open_channel_id::<AudioLossyDvc>() else {
            return Some(data);
        };
        // The client reads the lossy channel from the tunnel only once it has
        // processed the Soft-Sync that moves it there; a wave sent earlier is
        // dropped (about the first half-second of every connection). Until the
        // client acknowledges the Soft-Sync over TCP, keep the waves on TCP.
        if !self.macrdp.mt.lossy_audio_soft_synced
            || !self
                .get_svc_processor::<dvc::DrdynvcServer>()
                .is_some_and(|drdynvc| drdynvc.soft_sync_response_received())
        {
            return Some(data);
        }
        let mt = &mut self.macrdp.mt;
        if !mt.lossy_audio_streaming {
            mt.lossy_audio_streaming = true;
            warn!(
                format_no,
                lossy_channel_id = lossy_id,
                "streaming Wave2 audio over the LOSSY UDP/DTLS tunnel (AUDIO_PLAYBACK_LOSSY_DVC) \u{2014} static rdpsnd now silent"
            );
        }
        let block_no = mt.lossy_audio_block_no;
        mt.lossy_audio_block_no = block_no.wrapping_add(1);
        let wave = crate::mt::audio_dvc::lossy_wave_dvc_message(block_no, ts, format_no, data);
        let routed = dvc::encode_dvc_messages(lossy_id, vec![wave], ChannelFlags::empty())
            .map_err(ServerError::encode)
            .and_then(|messages| mt.route_dvc_over_udp(messages));
        if let Err(error) = routed {
            warn!(%error, "lossy audio wave route over UDP tunnel failed");
        }
        None
    }

    /// One client-to-server PDU from the tunnel (a bare DRDYNVC PDU, e.g. an EGFX
    /// frame acknowledgement): feed it to the DRDYNVC processor and ship any replies
    /// back over the tunnel. Errors are logged: the optional UDP path must never end
    /// the TCP-authoritative session.
    ///
    /// Returns what the built-in dynamic channels received, as the TCP path does
    /// (see `ConnectionState::take_dvc_events`); the client migrates none of them.
    pub(in crate::server) fn mt_process_tunnel_inbound(&mut self, pdu: &[u8]) -> Vec<ConnectionOutput> {
        use ironrdp_svc::SvcProcessor as _;
        let Some(drdynvc) = self.get_svc_processor::<dvc::DrdynvcServer>() else {
            warn!("inbound tunnel data but no DRDYNVC channel; dropping");
            return Vec::new();
        };
        match drdynvc.process(pdu) {
            Ok(replies) => {
                if !replies.is_empty()
                    && let Err(error) = self.macrdp.mt.route_dvc_over_udp(replies)
                {
                    warn!(%error, "failed to ship tunnel reply PDUs");
                }
            }
            Err(error) => {
                warn!(%error, "failed to process inbound tunnel DRDYNVC PDU");
            }
        }
        self.take_dvc_events()
    }
}

#[cfg(test)]
mod tests {
    use core::any::TypeId;
    #[cfg(feature = "egfx")]
    use std::rc::Rc;

    use ironrdp_core::encode_vec;
    #[cfg(feature = "egfx")]
    use ironrdp_dvc::pdu::{
        CapabilitiesResponsePdu, CapsVersion, CreateResponsePdu, CreationStatus, DrdynvcClientPdu, SoftSyncResponsePdu,
    };
    use ironrdp_dvc::pdu::{DrdynvcServerPdu, SoftSyncChannelList, SoftSyncRequestPdu};
    #[cfg(feature = "egfx")]
    use ironrdp_svc::SvcProcessor as _;

    use super::*;
    #[cfg(feature = "egfx")]
    use crate::mt::{TunnelOutbound, tunnel_channel};
    use crate::server::PduContext;
    #[cfg(feature = "egfx")]
    use crate::server::{OutboundQueue, QueuedWriter};

    const DRDYNVC_CHANNEL_ID: u16 = 1005;
    const USER_CHANNEL_ID: u16 = 1002;
    const REQUEST_ID: u32 = 7;
    const COOKIE: [u8; 16] = [0x42; 16];

    fn audio_formats() -> Vec<AudioFormat> {
        vec![AudioFormat {
            format: ironrdp_rdpsnd::pdu::WaveFormat::PCM,
            n_channels: 2,
            n_samples_per_sec: 44_100,
            n_avg_bytes_per_sec: 176_400,
            n_block_align: 4,
            bits_per_sample: 16,
            data: None,
        }]
    }

    /// A DRDYNVC server whose dynamic channels the client has opened.
    #[cfg(feature = "egfx")]
    fn opened_drdynvc(drdynvc: dvc::DrdynvcServer) -> dvc::DrdynvcServer {
        let mut drdynvc = drdynvc;
        drdynvc.start().unwrap();
        let caps = encode_vec(&DrdynvcClientPdu::Capabilities(CapabilitiesResponsePdu::new(
            CapsVersion::V2,
        )))
        .unwrap();
        let creates = drdynvc.process(&caps).unwrap();
        for create in creates {
            let DrdynvcServerPdu::Create(create) =
                ironrdp_core::decode::<DrdynvcServerPdu>(&create.encode_unframed_pdu().unwrap()).unwrap()
            else {
                panic!("expected a Create Request");
            };
            let response = encode_vec(&DrdynvcClientPdu::Create(CreateResponsePdu::new(
                create.channel_id(),
                CreationStatus::OK,
            )))
            .unwrap();
            drdynvc.process(&response).unwrap();
        }
        drdynvc
    }

    fn config(lossy_audio: bool, migrate_egfx: bool) -> MtConfig {
        MtConfig {
            cookies: None,
            tunnel_sender: None,
            egfx_on_lossy_handle: None,
            egfx_on_udp_handle: None,
            demigrate_request: None,
            migrate_egfx,
            lossy_audio,
            lossy_audio_format: NegotiatedAudioFormat::new(),
        }
    }

    /// A connection with `drdynvc` joined and an Initiate Multitransport Request in flight.
    fn connection(config: MtConfig, drdynvc: dvc::DrdynvcServer) -> ConnectionState {
        let mut conn = ConnectionState::default();
        conn.static_channels.insert(drdynvc);
        conn.static_channels
            .attach_channel_id(TypeId::of::<dvc::DrdynvcServer>(), DRDYNVC_CHANNEL_ID);
        conn.macrdp.mt.config = Some(config);
        conn.macrdp.mt.migration = Some(MigrationState {
            request_id: REQUEST_ID,
            cookie: COOKIE,
            protocol: RequestedProtocol::UdpFecL,
            soft_sync_sent: false,
        });
        conn
    }

    fn soft_sync_request_frame(channel_lists: Vec<SoftSyncChannelList>) -> Vec<u8> {
        // DRDYNVC PDUs carry SHOW_PROTOCOL, the Soft-Sync request too.
        let request = SvcMessage::from(DrdynvcServerPdu::SoftSyncRequest(SoftSyncRequestPdu::new(
            channel_lists,
        )))
        .with_flags(ChannelFlags::SHOW_PROTOCOL);
        server_encode_svc_messages(vec![request], DRDYNVC_CHANNEL_ID, USER_CHANNEL_ID).unwrap()
    }

    #[cfg(feature = "egfx")]
    fn soft_sync_response(tunnels: Vec<SoftSyncTunnelType>) -> Vec<u8> {
        encode_vec(&DrdynvcClientPdu::SoftSyncResponse(SoftSyncResponsePdu::new(tunnels))).unwrap()
    }

    fn frames(outputs: &[ConnectionOutput]) -> Vec<&[u8]> {
        outputs
            .iter()
            .map(|output| match output {
                ConnectionOutput::ResponseFrame(frame) => frame.as_slice(),
                other => panic!("unexpected output {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_response_with_nothing_to_migrate_gets_the_empty_soft_sync_once() {
        let mut conn = connection(config(false, false), dvc::DrdynvcServer::new());

        conn.mt_on_response(&MultitransportResponsePdu::success(REQUEST_ID));
        let outputs = conn.mt_with_owed_soft_sync(Vec::new(), USER_CHANNEL_ID).unwrap();
        assert_eq!(frames(&outputs), vec![soft_sync_request_frame(Vec::new()).as_slice()]);

        // Owed once: the next message-channel PDU and a repeated response add nothing.
        assert!(
            conn.mt_with_owed_soft_sync(Vec::new(), USER_CHANNEL_ID)
                .unwrap()
                .is_empty()
        );
        conn.mt_on_response(&MultitransportResponsePdu::success(REQUEST_ID));
        assert!(
            conn.mt_with_owed_soft_sync(Vec::new(), USER_CHANNEL_ID)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn the_owed_soft_sync_answers_the_response_on_the_message_channel() {
        const MESSAGE_CHANNEL_ID: u16 = 1008;
        let mut conn = connection(config(false, false), dvc::DrdynvcServer::new());
        let frame = encode_vec(&ironrdp_pdu::x224::X224(ironrdp_pdu::mcs::McsMessage::SendDataRequest(
            ironrdp_pdu::mcs::SendDataRequest {
                initiator_id: USER_CHANNEL_ID,
                channel_id: MESSAGE_CHANNEL_ID,
                user_data: encode_vec(&MultitransportResponsePdu::success(REQUEST_ID))
                    .unwrap()
                    .into(),
            },
        )))
        .unwrap();
        let context = PduContext {
            io_channel_id: 1003,
            user_channel_id: USER_CHANNEL_ID,
            message_channel_id: Some(MESSAGE_CHANNEL_ID),
            udp_available: false,
            now_ms: 0,
        };
        let outputs = conn.dispatch_pdu(&context, ironrdp_pdu::Action::X224, &frame).unwrap();
        assert_eq!(frames(&outputs), vec![soft_sync_request_frame(Vec::new()).as_slice()]);
    }

    #[test]
    fn a_response_leaves_the_soft_sync_to_the_migration_gate_when_a_channel_will_migrate() {
        // (2026-11-13 regression) Spending the one Soft-Sync here left the lossy
        // audio channel unmigrated.
        for (lossy_audio, migrate_egfx) in [(true, false), (false, true)] {
            let mut conn = connection(config(lossy_audio, migrate_egfx), dvc::DrdynvcServer::new());
            conn.mt_on_response(&MultitransportResponsePdu::success(REQUEST_ID));
            assert!(
                conn.mt_with_owed_soft_sync(Vec::new(), USER_CHANNEL_ID)
                    .unwrap()
                    .is_empty()
            );
            assert!(!conn.macrdp.mt.migration.unwrap().soft_sync_sent);
        }
    }

    #[test]
    fn a_mismatched_or_failed_response_owes_no_soft_sync() {
        let mut conn = connection(config(false, false), dvc::DrdynvcServer::new());
        conn.mt_on_response(&MultitransportResponsePdu::success(REQUEST_ID + 1));
        assert!(conn.macrdp.mt.migration.is_some());
        assert!(!conn.macrdp.mt.soft_sync_owed);

        // E_ABORT: the client could not establish UDP.
        conn.mt_on_response(&MultitransportResponsePdu::abort(REQUEST_ID));
        assert!(conn.macrdp.mt.migration.is_none());
        assert!(
            conn.mt_with_owed_soft_sync(Vec::new(), USER_CHANNEL_ID)
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(feature = "egfx")]
    #[tokio::test]
    async fn waves_stay_on_tcp_until_the_lossy_soft_sync_is_acknowledged() {
        let (tunnel_sender, mut tunnel_rx) = tunnel_channel();
        let mut config = config(true, false);
        config.tunnel_sender = Some(tunnel_sender);
        config.lossy_audio_format.set(0);
        let drdynvc = opened_drdynvc(
            dvc::DrdynvcServer::new()
                .with_dynamic_channel(AudioReliableDvc::new(
                    audio_formats(),
                    config.lossy_audio_format.clone(),
                ))
                .with_dynamic_channel(AudioLossyDvc::new()),
        );
        let mut conn = connection(config, drdynvc);
        let queue = Rc::new(OutboundQueue::default());
        let mut writer = QueuedWriter::new(Rc::clone(&queue));

        // No tunnel yet: no Soft-Sync, waves on TCP.
        conn.mt_soft_sync_on_egfx(&mut writer, USER_CHANNEL_ID).await.unwrap();
        assert_eq!(queue.queued_bytes.get(), 0);
        assert_eq!(conn.mt_route_lossy_audio(vec![1], 0), Some(vec![1]));

        // The tunnel binds: the Soft-Sync names the lossy channel on the lossy tunnel.
        conn.macrdp.mt.tunnel_bound = Some(Arc::new(AtomicBool::new(true)));
        conn.mt_soft_sync_on_egfx(&mut writer, USER_CHANNEL_ID).await.unwrap();
        let lossy_id = conn.mt_open_channel_id::<AudioLossyDvc>().unwrap();
        let soft_sync = soft_sync_request_frame(vec![SoftSyncChannelList::new(
            SoftSyncTunnelType::LOSSY_UDP,
            vec![lossy_id],
        )]);
        assert_eq!(queue.queued_bytes.get(), soft_sync.len());

        // Sent but not acknowledged: still TCP.
        assert_eq!(conn.mt_route_lossy_audio(vec![2], 20), Some(vec![2]));
        assert!(tunnel_rx.try_recv().is_err());

        conn.get_svc_processor::<dvc::DrdynvcServer>()
            .unwrap()
            .process(&soft_sync_response(vec![SoftSyncTunnelType::LOSSY_UDP]))
            .unwrap();
        assert_eq!(conn.mt_route_lossy_audio(vec![3], 40), None);
        let TunnelOutbound { cookie, data } = tunnel_rx.try_recv().unwrap();
        assert_eq!(cookie, COOKIE);
        let wave = crate::mt::audio_dvc::lossy_wave_dvc_message(0, 40, 0, vec![3]);
        let expected = dvc::encode_dvc_messages(lossy_id, vec![wave], ChannelFlags::empty()).unwrap();
        assert_eq!(data, expected[0].encode_unframed_pdu().unwrap());

        // The one Soft-Sync is spent: no second request.
        conn.mt_soft_sync_on_egfx(&mut writer, USER_CHANNEL_ID).await.unwrap();
        assert_eq!(queue.queued_bytes.get(), soft_sync.len());
    }

    #[cfg(feature = "egfx")]
    #[test]
    fn the_watchdog_moves_egfx_back_to_tcp() {
        let (tunnel_sender, mut tunnel_rx) = tunnel_channel();
        let on_udp = Arc::new(AtomicBool::new(true));
        let demigrate = Arc::new(AtomicBool::new(false));
        let mut config = config(false, true);
        config.tunnel_sender = Some(tunnel_sender);
        config.egfx_on_udp_handle = Some(Arc::clone(&on_udp));
        config.demigrate_request = Some(Arc::clone(&demigrate));
        let mut conn = connection(config, dvc::DrdynvcServer::new());
        conn.macrdp.mt.egfx_on_udp = true;

        let batch = || vec![SvcMessage::from(vec![0xabu8; 8])];
        assert!(conn.mt_route_egfx(batch()).unwrap().is_none());
        assert_eq!(tunnel_rx.try_recv().unwrap().data, vec![0xab; 8]);

        demigrate.store(true, Ordering::Relaxed);
        assert_eq!(conn.mt_route_egfx(batch()).unwrap().map(|m| m.len()), Some(1));
        assert!(!on_udp.load(Ordering::Relaxed));
        // For the rest of the session, even once the request is lowered.
        demigrate.store(false, Ordering::Relaxed);
        assert!(conn.mt_route_egfx(batch()).unwrap().is_some());
        assert!(tunnel_rx.try_recv().is_err());
    }

    #[test]
    fn dropping_the_connection_retires_its_tunnel() {
        let registry = CookieRegistry::new();
        let handles: [Arc<AtomicBool>; 3] = core::array::from_fn(|_| Arc::new(AtomicBool::new(true)));
        let (inbound, _inbound_rx) = mpsc::unbounded_channel();
        let registration = Arc::new(StdMutex::new(OfferRegistration::default()));
        let mut rng = OfferRng {
            registry: registry.clone(),
            inbound: Some(inbound),
            registration: Arc::clone(&registration),
        };
        let mut cookie = [0u8; 16];
        rng.fill_security_cookie(&mut cookie);
        let tunnel_bound = registration.lock().unwrap().tunnel_bound.clone().unwrap();
        tunnel_bound.store(true, Ordering::Relaxed);

        let mut config = config(false, false);
        config.cookies = Some(registry.clone());
        config.egfx_on_lossy_handle = Some(Arc::clone(&handles[0]));
        config.egfx_on_udp_handle = Some(Arc::clone(&handles[1]));
        config.demigrate_request = Some(Arc::clone(&handles[2]));
        let mut conn = ConnectionState::default();
        conn.macrdp.mt.config = Some(config);
        conn.macrdp.mt.offer = Some(registration);
        conn.macrdp.mt.tunnel_bound = Some(Arc::clone(&tunnel_bound));
        drop(conn);

        assert!(handles.iter().all(|h| !h.load(Ordering::Relaxed)));
        assert!(!tunnel_bound.load(Ordering::Relaxed));
        assert!(registry.take(&cookie).is_none(), "the cookie was evicted");
    }

    #[test]
    fn the_audio_dvc_factory_attaches_both_channels_only_when_configured() {
        let config = LossyAudioConfig::default();
        let factory = AudioDvcFactory(config.clone());
        let drdynvc = factory.attach(dvc::DrdynvcServer::new());
        assert!(drdynvc.get_channel_id_by_type::<AudioLossyDvc>().is_none());

        *config.formats.lock().unwrap() = Some(audio_formats());
        let drdynvc = factory.attach(dvc::DrdynvcServer::new());
        assert!(drdynvc.get_channel_id_by_type::<AudioReliableDvc>().is_some());
        assert!(drdynvc.get_channel_id_by_type::<AudioLossyDvc>().is_some());
    }

    struct Offer;

    impl MultitransportProvider for Offer {
        fn requested_protocol(&self) -> RequestedProtocol {
            RequestedProtocol::UdpFecL
        }
    }

    fn acceptor() -> Acceptor {
        Acceptor::new(
            ironrdp_pdu::nego::SecurityProtocol::SSL,
            ironrdp_acceptor::DesktopSize {
                width: 1024,
                height: 768,
            },
            Vec::new(),
            None,
        )
    }

    #[tokio::test]
    async fn a_connection_gets_the_offer_its_negotiation_made() {
        let registry = CookieRegistry::new();
        let mut server = RdpServer::builder()
            .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
            .with_no_security()
            .with_no_input()
            .with_no_display()
            .build();
        server.set_multitransport_provider(Some(Box::new(Offer)));
        server.set_multitransport_cookie_registry(Some(registry.clone()));
        server.set_migrate_egfx(true);

        server.mt_offer(&mut acceptor());
        let mut conn = ConnectionState::default();
        server.mt_begin_connection(&mut conn);
        assert!(server.macrdp.mt.pending_offer.is_none());
        assert!(conn.macrdp.mt.offer.is_some());
        assert!(conn.mt_take_tunnel_inbound().is_some());
        assert_eq!(conn.macrdp.mt.has_channel_to_migrate(), cfg!(feature = "egfx"));

        // During the tunnel-death cooldown the next connection gets no offer.
        registry.suppress_multitransport(core::time::Duration::from_secs(60));
        server.mt_offer(&mut acceptor());
        let mut conn = ConnectionState::default();
        server.mt_begin_connection(&mut conn);
        assert!(conn.macrdp.mt.offer.is_none());
        assert!(conn.mt_take_tunnel_inbound().is_none());
    }

    #[test]
    fn the_builder_installs_the_audio_dvc_factory() {
        let server = RdpServer::builder()
            .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
            .with_no_security()
            .with_no_input()
            .with_no_display()
            .build();
        assert_eq!(server.dynamic_channel_factories.len(), 1);
    }
}
