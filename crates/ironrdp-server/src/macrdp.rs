//! (macrdp) macrdp's additions to [`RdpServer`], kept out of `server.rs`.
//!
//! `server.rs` holds only the hooks, each marked `(macrdp N)`.

use core::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ironrdp_acceptor::AcceptorResult;
use ironrdp_pdu::rdp::capability_sets::CapabilitySet;
use ironrdp_svc::server_encode_svc_messages;
use ironrdp_tokio::FramedWrite as _;
use rdpsnd::server::RdpsndServer;
use tokio::sync::{Mutex, MutexGuard, OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{debug, info, warn};

use super::{
    ConnectionOutput, ConnectionState, QueuedWriter, RdpServer, SharedConnectionHandler, WriteCompletion, rdpsnd,
    with_connection_handler,
};
#[cfg(feature = "egfx")]
use crate::ServerEvent;
use crate::audin::AudinChannelFactory;
use crate::rdcamera::RdCameraChannelFactory;
use crate::rdpdr_drive::RdpdrDriveChannelFactory;
use crate::rdpeusb::UrbdrcChannelFactory;
use crate::{
    AudinServerFactory, AudioWave, RdCameraServerFactory, RdpdrDriveMessage, RdpdrDriveServer, RdpdrDriveServerFactory,
    ServerError, ServerErrorExt as _, ServerResult, UrbdrcServerFactory, UrbdrcServerMessage,
};

// (macrdp 12) macrdp's UDP multitransport, under this module so it can extend
// `RdpServer` and `ConnectionState`.
#[cfg(feature = "multitransport")]
#[path = "mt/session.rs"]
mod mt_session;

/// (macrdp) Per-server state for macrdp's divergences, in one `RdpServer` field.
#[derive(Default)]
pub(super) struct MacrdpServerState {
    /// (2/8) Dedicated audio channel, read by the client loop's audio arm.
    audio_receiver: Option<Arc<Mutex<mpsc::Receiver<AudioWave>>>>,
    /// (15) Kernel TCP RTT sampled at accept, in ms (0 = unknown).
    link_rtt_ms: Option<Arc<AtomicU32>>,
    /// (12) macrdp's UDP multitransport: its configuration.
    #[cfg(feature = "multitransport")]
    mt: mt_session::MtState,
}

/// (macrdp) Per-connection state for macrdp's divergences, in one `ConnectionState`
/// field. Dropped with the connection's state.
#[cfg(feature = "multitransport")]
#[derive(Default)]
pub(super) struct MacrdpConnectionState {
    /// (12) macrdp's UDP multitransport: this connection's offer, tunnel and routing.
    mt: mt_session::MtConnection,
}

/// (macrdp 11) macrdp's own channel factories, set on the builder and handed to
/// [`RdpServer::install_macrdp_channels`]. They are kept alongside upstream's
/// equivalents (`with_rdpdr_factory`), which macrdp never sets. Never set both a
/// macrdp factory and its upstream counterpart: they register the same channel name.
#[derive(Default)]
pub(crate) struct MacrdpChannelFactories {
    pub(crate) rdpdr_drive: Option<Box<dyn RdpdrDriveServerFactory>>,
    /// (macrdp 16) Never set together with upstream's `with_usb_factory`.
    pub(crate) urbdrc: Option<Box<dyn UrbdrcServerFactory>>,
    /// (macrdp 19) MS-RDPECAM camera redirection; upstream has no server wiring for it.
    pub(crate) camera: Option<Box<dyn RdCameraServerFactory>>,
    /// (macrdp 25) Never set together with upstream's `with_rdpeai_factory`.
    pub(crate) audin: Option<Box<dyn AudinServerFactory>>,
}

/// (macrdp 2) Bounded to ~1 s of audio at the steady-state 42.78 waves/s
/// (1029 samples/wave at 44.1 kHz), so a stalled writer backpressures the
/// capture loop (losses land in the capture ring) instead of queueing here.
const AUDIO_CHANNEL_CAPACITY: usize = 50;

#[expect(
    clippy::multiple_inherent_impl,
    reason = "macrdp's methods stay out of server.rs so the fork's hooks there stay small"
)]
impl RdpServer {
    /// (macrdp) Installs macrdp's channel factories and its dedicated audio channel.
    /// Called once by the builder, right after [`Self::new`].
    pub(crate) fn install_macrdp_channels(&mut self, channels: MacrdpChannelFactories) {
        // (macrdp 11) Attached per connection after upstream's own channels,
        // through upstream's static channel factories.
        if let Some(mut rdpdr) = channels.rdpdr_drive {
            if self.rdpdr_factory.is_some() {
                warn!(
                    "Both with_rdpdr_factory and with_rdpdr_drive_factory are set; upstream's RDPDR \
                     server takes the RDPDR channel, and macrdp's drive and smart card redirection is off"
                );
            }
            rdpdr.set_sender(self.ev_sender.clone());
            self.static_channel_factories
                .push(Box::new(RdpdrDriveChannelFactory::new(rdpdr, self.ev_sender.clone())));
        }
        // (macrdp 12) The dual audio DVCs, through upstream's dynamic channel factories.
        #[cfg(feature = "multitransport")]
        self.install_mt_channels();
        // (macrdp 16) The main URBDRC channel, after the audio DVCs as before.
        if let Some(mut urbdrc) = channels.urbdrc {
            #[cfg(feature = "usb")]
            if self.usb_factory.is_some() {
                warn!(
                    "Both with_usb_factory and with_urbdrc_factory are set; the client is offered \
                     two URBDRC channels"
                );
            }
            urbdrc.set_sender(self.ev_sender.clone());
            self.dynamic_channel_factories
                .push(Box::new(UrbdrcChannelFactory::new(urbdrc)));
        }
        // (macrdp 19) The camera enumerator, after URBDRC as before.
        if let Some(mut camera) = channels.camera {
            camera.set_sender(self.ev_sender.clone());
            self.dynamic_channel_factories
                .push(Box::new(RdCameraChannelFactory::new(camera)));
        }
        // (macrdp 25) The microphone channel, after the camera as before.
        if let Some(audin) = channels.audin {
            if self.rdpeai_factory.is_some() {
                warn!(
                    "Both with_rdpeai_factory and with_audin_factory are set; the client is offered \
                     two AUDIO_INPUT channels"
                );
            }
            self.dynamic_channel_factories
                .push(Box::new(AudinChannelFactory::new(audin)));
        }
        // No sound factory: no rdpsnd channel, so no audio arm.
        if let Some(snd) = self.sound_factory.as_mut() {
            let (audio_sender, audio_receiver) = mpsc::channel::<AudioWave>(AUDIO_CHANNEL_CAPACITY);
            snd.set_audio_sender(audio_sender);
            self.macrdp.audio_receiver = Some(Arc::new(Mutex::new(audio_receiver)));
        }
    }

    /// (macrdp 2) Drops the waves a replaced session queued on the audio
    /// channel and never shipped; up to ~1 s of them may still be queued.
    pub(super) fn discard_stale_macrdp_audio(&self) {
        // If the receiver is somehow still held, skip: the next connection's
        // lag model drops stale waves anyway.
        if let Some(audio_receiver) = &self.macrdp.audio_receiver
            && let Ok(mut audio_receiver) = audio_receiver.try_lock()
        {
            let mut stale_waves = 0usize;
            while audio_receiver.try_recv().is_ok() {
                stale_waves += 1;
            }
            if stale_waves > 0 {
                debug!(stale_waves, "dropped audio the replaced session never shipped");
            }
        }
    }

    /// (macrdp 2) The audio channel's receiver, for [`AudioDispatch::new`].
    pub(super) fn macrdp_audio_receiver(&self) -> Option<Arc<Mutex<mpsc::Receiver<AudioWave>>>> {
        self.macrdp.audio_receiver.clone()
    }

    /// (macrdp 15) Install a cell the server fills with each accepted
    /// connection's kernel-smoothed TCP RTT in ms (0 = unknown).
    pub fn set_link_rtt_handle(&mut self, handle: Arc<AtomicU32>) {
        self.macrdp.link_rtt_ms = Some(handle);
    }

    /// (macrdp 15) Publish this connection's accept-time TCP RTT (0 = unknown).
    pub(super) fn store_link_rtt(&self, rtt_ms: u32) {
        if let Some(cell) = &self.macrdp.link_rtt_ms {
            cell.store(rtt_ms, Ordering::Relaxed);
            debug!(rtt_ms, "sampled TCP link RTT at accept");
        }
    }
}

/// (macrdp 15) The kernel's smoothed TCP RTT for an accepted connection, in
/// ms, via `getsockopt(TCP_CONNECTION_INFO)` (macOS). The kernel seeds srtt from the
/// SYN/SYN-ACK exchange, so a value exists right at accept. `None` on error or on
/// platforms without the option; a sub-ms LAN RTT maps to 1, keeping 0 for "unknown".
#[cfg(target_os = "macos")]
pub(super) fn tcp_srtt_ms(stream: &tokio::net::TcpStream) -> Option<u32> {
    use std::os::fd::AsRawFd as _;
    let fd = stream.as_raw_fd();
    // SAFETY: `tcp_connection_info` is a plain C struct, valid when zeroed.
    let mut info: libc::tcp_connection_info = unsafe { core::mem::zeroed() };
    let mut len = libc::socklen_t::try_from(size_of::<libc::tcp_connection_info>()).ok()?;
    // SAFETY: fd is a live TCP socket owned by `stream`; `info`/`len` describe a
    // properly sized, writable out-buffer for this socket option.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_CONNECTION_INFO,
            core::ptr::from_mut(&mut info).cast(),
            &mut len,
        )
    };
    // tcpi_srtt is already in milliseconds on macOS.
    (rc == 0).then(|| info.tcpi_srtt.max(1))
}

/// (macrdp 15) No kernel RTT outside macOS.
#[cfg(not(target_os = "macos"))]
pub(super) fn tcp_srtt_ms(_stream: &tokio::net::TcpStream) -> Option<u32> {
    None
}

/// (macrdp 2/3/8) The client loop's audio arm: one per connection.
///
/// Waves ship one at a time and in arrival order: the next wave is taken from
/// the channel only after the previous one is on the wire. A wave is queued
/// behind what the client loop already queued, never behind what it queues
/// later. While the writer stalls, waves stay in the bounded channel, so the
/// lag model sees the stall when the writer resumes.
pub(super) struct AudioDispatch<'a> {
    receiver: Option<MutexGuard<'a, mpsc::Receiver<AudioWave>>>,
    /// One permit: held by the queued wave until it is written or dropped.
    in_flight: Arc<Semaphore>,
    /// Fresh per connection.
    lag: AudioLagModel,
}

impl<'a> AudioDispatch<'a> {
    pub(super) async fn new(receiver: Option<&'a Mutex<mpsc::Receiver<AudioWave>>>) -> Self {
        let receiver = match receiver {
            Some(receiver) => Some(receiver.lock().await),
            None => None,
        };
        Self {
            receiver,
            in_flight: Arc::new(Semaphore::new(1)),
            lag: AudioLagModel::default(),
        }
    }

    /// Whether the arm has a channel to read.
    pub(super) fn is_open(&self) -> bool {
        self.receiver.is_some()
    }

    /// The next wave, once the previous one is written. `None`: the channel closed.
    ///
    /// Cancel safe: a canceled call releases its permit and takes no wave.
    pub(super) async fn next_wave(&mut self) -> Option<(OwnedSemaphorePermit, AudioWave)> {
        let receiver = self.receiver.as_mut()?;
        let permit = Arc::clone(&self.in_flight)
            .acquire_owned()
            .await
            .expect("the in-flight semaphore is never closed");
        receiver.recv().await.map(|wave| (permit, wave))
    }

    /// Runs the lag model on `wave` and queues it if admitted.
    pub(super) fn dispatch(
        &mut self,
        conn: &mut ConnectionState,
        writer: &mut QueuedWriter,
        user_channel_id: u16,
        wave: Option<(OwnedSemaphorePermit, AudioWave)>,
    ) -> ServerResult<()> {
        let Some((permit, (data, ts, duration_ms))) = wave else {
            // The embedder dropped its sender. Stop reading the channel; the
            // connection goes on.
            debug!("audio channel closed; stopping audio dispatch");
            self.receiver = None;
            return Ok(());
        };
        let wave_ms = AudioLagModel::wave_ms(data.len(), duration_ms);
        if !self.lag.admit(Instant::now(), wave_ms) {
            return Ok(());
        }
        // (macrdp 12) The lossy tunnel takes the wave once that path is live; static
        // RDPSND stays silent then (no double play). The permit drops here, so the
        // next wave is not held back.
        #[cfg(feature = "multitransport")]
        let Some(data) = conn.mt_route_lossy_audio(data, ts) else {
            self.lag.shipped(wave_ms);
            return Ok(());
        };
        let Some(rdpsnd) = conn.get_svc_processor::<RdpsndServer>() else {
            warn!("No rdpsnd channel, dropping wave");
            return Ok(());
        };
        // A wave captured before this connection's rdpsnd channel has
        // negotiated a format has nowhere to go yet.
        if !rdpsnd.is_ready() {
            debug!("Dropping wave: rdpsnd channel not negotiated yet");
            return Ok(());
        }
        let msgs = match rdpsnd.wave(data, ts) {
            Ok(msgs) => msgs,
            Err(error) => {
                warn!(?error, "rdpsnd.wave failed");
                return Ok(());
            }
        };
        let Some(channel_id) = conn.get_channel_id_by_type::<RdpsndServer>() else {
            warn!("rdpsnd SVC channel id missing");
            return Ok(());
        };
        let encoded =
            server_encode_svc_messages(msgs.into(), channel_id, user_channel_id).map_err(ServerError::encode)?;
        writer
            .queue_with_completion(encoded, WriteCompletion::Rdpsnd(permit))
            .map_err(|e| ServerError::io("write_all", e))?;
        // The next wave is admitted only after this one is written, so
        // counting it now gives the model the same view as counting it then.
        self.lag.shipped(wave_ms);
        Ok(())
    }
}

/// (macrdp 12) The client loop's arm for client-to-server data from macrdp's
/// tunnel (e.g. EGFX frame acks once EGFX migrated). Closed when there's no
/// tunnel, and always without the `multitransport` feature.
///
/// Taken from the connection's state when the client loop starts, so a
/// deactivation-reactivation pass ends it, as the old fork's per-loop future did.
pub(super) struct TunnelInbound {
    #[cfg(feature = "multitransport")]
    receiver: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl TunnelInbound {
    pub(super) fn take(conn: &mut ConnectionState) -> Self {
        #[cfg(not(feature = "multitransport"))]
        let _ = conn;
        Self {
            #[cfg(feature = "multitransport")]
            receiver: conn.mt_take_tunnel_inbound(),
        }
    }

    /// Whether the arm has a receiver to read.
    #[cfg_attr(
        not(feature = "multitransport"),
        expect(clippy::unused_self, reason = "no tunnel without the feature")
    )]
    pub(super) fn is_open(&self) -> bool {
        #[cfg(feature = "multitransport")]
        return self.receiver.is_some();
        #[cfg(not(feature = "multitransport"))]
        false
    }

    /// The next PDU. `None`: the listener dropped its sender. Cancel safe.
    pub(super) async fn recv(&mut self) -> Option<Vec<u8>> {
        #[cfg(feature = "multitransport")]
        if let Some(receiver) = self.receiver.as_mut() {
            return receiver.recv().await;
        }
        core::future::pending().await
    }

    /// Processes one PDU from [`Self::recv`].
    #[cfg_attr(
        not(feature = "multitransport"),
        expect(clippy::unused_self, reason = "no tunnel without the feature")
    )]
    pub(super) fn process(&mut self, conn: &mut ConnectionState, pdu: Option<Vec<u8>>) -> Vec<ConnectionOutput> {
        #[cfg(feature = "multitransport")]
        {
            let Some(pdu) = pdu else {
                self.receiver = None;
                return Vec::new();
            };
            conn.mt_process_tunnel_inbound(&pdu)
        }
        #[cfg(not(feature = "multitransport"))]
        {
            let _ = (conn, pdu);
            Vec::new()
        }
    }
}

/// (macrdp 2/3/8) The audio-lag model of the client loop's audio arm.
///
/// Tracks the client's projected playback buffer as audio shipped minus wall time
/// elapsed since the first wave. A wave that would push it past [`Self::MAX_LAG_MS`]
/// is dropped (the oldest go first, since waves arrive in order), so slow drift from
/// many small client pauses is caught, not just one big stall. When the writer stalls
/// (mstsc freezing the socket during a window resize), wall time outruns shipped
/// audio; past [`Self::RESYNC_DEFICIT_MS`] the model resyncs to live instead of
/// reading the gap as a starving client and shipping the whole stale backlog late.
#[derive(Default)]
struct AudioLagModel {
    shipped_ms: f64,
    clock_start: Option<Instant>,
}

impl AudioLagModel {
    const MAX_LAG_MS: f64 = 200.0;
    const RESYNC_DEFICIT_MS: f64 = 300.0;
    /// 16-bit stereo PCM at 44.1 kHz: 4 bytes/frame × 44 100 = 176.4 bytes per ms.
    const PCM_BYTES_PER_MS: f64 = 176.4;

    /// A wave's playback time. A compressed codec (AAC) passes its own duration: its
    /// byte length bears no fixed relation to playback time (a ~120-byte AU is ~23 ms).
    fn wave_ms(len: usize, duration_ms: Option<f64>) -> f64 {
        // A wave is far below 4 GiB.
        duration_ms.unwrap_or_else(|| f64::from(u32::try_from(len).unwrap_or(u32::MAX)) / Self::PCM_BYTES_PER_MS)
    }

    /// Whether to ship a wave of `wave_ms` at `now`.
    fn admit(&mut self, now: Instant, wave_ms: f64) -> bool {
        let start = *self.clock_start.get_or_insert(now);
        let elapsed_ms = (now - start).as_secs_f64() * 1000.0;
        let deficit_ms = elapsed_ms - self.shipped_ms;
        if deficit_ms > Self::RESYNC_DEFICIT_MS {
            debug!(target: "audio_backlog", deficit_ms, "writer stalled; resyncing audio clock to live");
            self.shipped_ms = elapsed_ms;
        }
        let projected_buffer_ms = self.shipped_ms + wave_ms - elapsed_ms;
        if projected_buffer_ms > Self::MAX_LAG_MS {
            debug!(
                target: "audio_backlog",
                projected_buffer_ms,
                wave_ms,
                "dropping wave (projected buffer over MAX_LAG_MS)"
            );
            return false;
        }
        true
    }

    /// Record that an admitted wave was shipped.
    fn shipped(&mut self, wave_ms: f64) {
        self.shipped_ms += wave_ms;
    }
}

/// (macrdp 11) Ships macrdp's RDPDR device-I/O requests on the RDPDR channel.
///
/// Not upstream's `StaticChannelWithCompletion`: that one sets `SHOW_PROTOCOL` on a
/// payload above `CHANNEL_CHUNK_LENGTH`, and Windows drops a multi-chunk RDPDR PDU
/// that carries it (a large `DeviceWriteRequest` is such a PDU). These messages go
/// out with no channel flags.
pub(super) async fn dispatch_rdpdr_drive(
    conn: &mut ConnectionState,
    writer: &mut QueuedWriter,
    user_channel_id: u16,
    message: RdpdrDriveMessage,
) -> ServerResult<()> {
    let RdpdrDriveMessage::SendMessages(messages) = message;
    let Some(channel_id) = conn.get_channel_id_by_type::<RdpdrDriveServer>() else {
        warn!("No RDPDR channel, dropping device-I/O request");
        return Ok(());
    };
    let data = server_encode_svc_messages(messages, channel_id, user_channel_id).map_err(ServerError::encode)?;
    writer
        .write_all(&data)
        .await
        .map_err(|e| ServerError::io("write_all", e))
}

/// (macrdp 16) Ships a [`UsbHandle`](crate::UsbHandle)'s transfer on its open
/// per-device channel. An encode error drops that transfer instead of ending the
/// session: a URB the codec rejects must not kill an opt-in feature's connection
/// (the same principle as the URBDRC `process()` decode tolerance). A socket error
/// stays fatal.
pub(super) async fn dispatch_urbdrc(
    conn: &mut ConnectionState,
    writer: &mut QueuedWriter,
    user_channel_id: u16,
    message: UrbdrcServerMessage,
) -> ServerResult<()> {
    let UrbdrcServerMessage::SendMessages { channel_id, messages } = message;
    let Some(drdynvc_channel_id) = conn.get_channel_id_by_type::<ironrdp_dvc::DrdynvcServer>() else {
        warn!("No DRDYNVC channel, dropping URBDRC transfer");
        return Ok(());
    };
    let data = match ironrdp_dvc::encode_dvc_messages(channel_id, messages, ironrdp_svc::ChannelFlags::empty())
        .and_then(|m| server_encode_svc_messages(m, drdynvc_channel_id, user_channel_id))
    {
        Ok(d) => d,
        Err(e) => {
            warn!(error = %e, "URBDRC: failed to encode a transfer request; dropping it (session kept alive)");
            return Ok(());
        }
    };
    writer
        .write_all(&data)
        .await
        .map_err(|e| ServerError::io("write_all", e))
}

/// (macrdp 18) Reports a connection's CredSSP outcome to the handler's
/// [`ConnectionHandler::on_authenticated`](crate::ConnectionHandler::on_authenticated).
///
/// `complete_security_upgrade` calls this for every connection, a preemption
/// candidate included, because both build their `ServerCredentialsHandler` with the
/// server's handler as its `auth_observer`.
pub(super) fn report_authentication(
    observer: Option<&SharedConnectionHandler>,
    result: &ironrdp_connector::ConnectorResult<()>,
) {
    with_connection_handler(observer, |h| {
        let reason = result.as_ref().err().map(ToString::to_string);
        h.on_authenticated(result.is_ok(), reason.as_deref());
    });
}

/// (macrdp 20) Client-fingerprint log: the identity the acceptor captured from
/// the GCC Client Core Data (acceptor divergence 4) plus the General-capset
/// platform, logged and passed to
/// [`ConnectionHandler::on_client_fingerprint`](crate::ConnectionHandler::on_client_fingerprint).
/// mstsc sends the real Windows build + WINDOWS; FreeRDP hardcodes build 2600; the
/// Windows Apps announce their host platform. Informational only: a client can
/// claim anything.
pub(super) fn report_client_fingerprint(handler: Option<&SharedConnectionHandler>, result: &AcceptorResult) {
    client_fingerprint(
        handler,
        &result.client_name,
        result.client_version,
        result.client_build,
        &result.capabilities,
    );
}

fn client_fingerprint(
    handler: Option<&SharedConnectionHandler>,
    client_name: &str,
    client_version: u32,
    client_build: u32,
    capabilities: &[CapabilitySet],
) {
    let platform = capabilities
        .iter()
        .find_map(|c| match c {
            CapabilitySet::General(g) => Some(format!("{:?}/{:?}", g.major_platform_type, g.minor_platform_type)),
            _ => None,
        })
        .unwrap_or_else(|| "unknown".to_owned());
    info!(
        client_name = %client_name,
        rdp_version = format_args!("{client_version:#x}"),
        client_build,
        platform = %platform,
        "client fingerprint"
    );
    with_connection_handler(handler, |h| {
        h.on_client_fingerprint(client_name, client_version, client_build, &platform);
    });
}

/// (macrdp 4) Reorders one event batch into three tiers: CLIPRDR, then EGFX
/// video and everything else, then RDPDR, since all of them share the one
/// outbound queue.
///
/// - CLIPRDR first: with H.264 on, a small FileContentsResponse otherwise queues
///   behind dozens of large video frames every batch, throttling a Mac-to-Windows
///   file copy to a crawl and freezing Explorer's synchronous paste read.
/// - RDPDR last: a large drive write (copying TO the redirected drive) otherwise
///   holds the writer ahead of the video frames in the same batch and stutters video.
///
/// Stable within each tier, so H.264 frames keep their order for the inter-frame
/// chain. CLIPRDR and RDPDR ride their own channels, so reordering them against EGFX
/// breaks no on-wire ordering. Audio from the audio channel is not in the batch: it
/// ships per wave from the client loop's audio arm, keeping its natural cadence.
#[cfg(feature = "egfx")]
pub(super) fn prioritize_batch(events: &mut [ServerEvent]) {
    fn tier(event: &ServerEvent) -> u8 {
        match event {
            // All three clipboard variants share tier 0, so the stable sort keeps their
            // order on the CLIPRDR channel.
            ServerEvent::Clipboard(_)
            | ServerEvent::ClipboardWithCompletion(..)
            | ServerEvent::ClipboardWithValidity { .. } => 0,
            ServerEvent::Rdpdr(_) | ServerEvent::RdpdrDrive(_) => 2,
            _ => 1,
        }
    }
    let has_egfx = events.iter().any(|e| matches!(e, ServerEvent::Egfx(_)));
    if has_egfx && events.iter().any(|e| tier(e) != 1) {
        // `sort_by_key` is stable.
        events.sort_by_key(tier);
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use super::*;
    // Imported above only with `egfx`.
    #[cfg(not(feature = "egfx"))]
    use crate::ServerEvent;

    mod urbdrc {
        use core::any::TypeId;
        use std::rc::Rc;

        use ironrdp_core::encode_vec;
        use ironrdp_dvc::pdu::{
            CapabilitiesResponsePdu, CapsVersion, CreateResponsePdu, CreationStatus, DataPdu, DrdynvcClientPdu,
            DrdynvcDataPdu, DrdynvcServerPdu,
        };
        use ironrdp_dvc::{DrdynvcServer, DvcMessage};
        use ironrdp_rdpeusb::pdu::sink::AddVirtualChannel;
        use ironrdp_svc::{ChannelFlags, SvcMessage, SvcProcessor as _};

        use super::super::super::OutboundQueue;
        use super::*;
        use crate::{URBDRC_CHANNEL_NAME, UrbdrcServer, UsbDeviceCallback};

        const DRDYNVC_CHANNEL_ID: u16 = 1005;
        const USER_CHANNEL_ID: u16 = 1002;

        struct Factory(Option<mpsc::UnboundedSender<ServerEvent>>);

        impl crate::ServerEventSender for Factory {
            fn set_sender(&mut self, sender: mpsc::UnboundedSender<ServerEvent>) {
                self.0 = Some(sender);
            }
        }

        impl UrbdrcServerFactory for Factory {
            fn build_processor(&self) -> UrbdrcServer {
                UrbdrcServer::with_sender(self.0.clone())
            }

            fn device_callback(&self) -> Option<UsbDeviceCallback> {
                Some(Arc::new(|_| {}))
            }
        }

        fn server() -> RdpServer {
            RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_no_security()
                .with_no_input()
                .with_no_display()
                .with_urbdrc_factory(Some(Box::new(Factory(None))))
                .build()
        }

        /// DRDYNVC with every channel opened through the caps and create handshake.
        fn opened_drdynvc(server: &RdpServer) -> DrdynvcServer {
            let mut drdynvc = server
                .dynamic_channel_factories
                .iter()
                .fold(DrdynvcServer::new(), |dvc, factory| factory.attach(dvc));
            drdynvc.start().unwrap();
            let caps = encode_vec(&DrdynvcClientPdu::Capabilities(CapabilitiesResponsePdu::new(
                CapsVersion::V2,
            )))
            .unwrap();
            for create in drdynvc.process(&caps).unwrap() {
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

        #[test]
        fn the_urbdrc_channel_is_attached_after_the_audio_dvcs() {
            #[cfg_attr(not(feature = "multitransport"), expect(unused_mut))]
            let mut server = server();
            #[cfg(feature = "multitransport")]
            server.set_multitransport_lossy_audio_formats(Some(vec![ironrdp_rdpsnd::pdu::AudioFormat {
                format: ironrdp_rdpsnd::pdu::WaveFormat::PCM,
                n_channels: 2,
                n_samples_per_sec: 44_100,
                n_avg_bytes_per_sec: 176_400,
                n_block_align: 4,
                bits_per_sample: 16,
                data: None,
            }]));
            let expected = if cfg!(feature = "multitransport") { 2 } else { 1 };
            assert_eq!(server.dynamic_channel_factories.len(), expected);
            let drdynvc = server
                .dynamic_channel_factories
                .iter()
                .fold(DrdynvcServer::new(), |dvc, factory| factory.attach(dvc));
            let urbdrc = drdynvc.get_channel_id_by_type::<UrbdrcServer>().unwrap();
            #[cfg(feature = "multitransport")]
            assert!(
                drdynvc
                    .get_channel_id_by_type::<crate::mt::audio_dvc::AudioLossyDvc>()
                    .unwrap()
                    < urbdrc
            );
            let _ = urbdrc;
        }

        #[tokio::test]
        async fn a_device_channel_and_its_transfers_reach_the_wire() {
            let mut server = server();
            let mut drdynvc = opened_drdynvc(&server);
            let urbdrc = drdynvc.get_channel_id_by_type::<UrbdrcServer>().unwrap();

            // The client announces a device on the main channel.
            let announce = encode_vec(&DrdynvcClientPdu::Data(DrdynvcDataPdu::Data(DataPdu::new(
                urbdrc,
                encode_vec(&AddVirtualChannel { msg_id: 1 }).unwrap(),
            ))))
            .unwrap();
            assert!(drdynvc.process(&announce).unwrap().is_empty());

            let mut conn = ConnectionState::default();
            conn.static_channels.insert(drdynvc);
            conn.static_channels
                .attach_channel_id(TypeId::of::<DrdynvcServer>(), DRDYNVC_CHANNEL_ID);
            let mut events = Vec::new();
            {
                let mut receiver = server.ev_receiver.lock().await;
                while let Ok(event) = receiver.try_recv() {
                    events.push(event);
                }
            }
            assert_eq!(events.len(), 1);
            assert!(matches!(events[0], ServerEvent::CreateDynamicChannel(_)));
            let transfer: DvcMessage = Box::new(AddVirtualChannel { msg_id: 2 });
            events.push(ServerEvent::Urbdrc(UrbdrcServerMessage::SendMessages {
                channel_id: 77,
                messages: vec![transfer],
            }));

            let queue = Rc::new(OutboundQueue::default());
            let mut writer = QueuedWriter::new(Rc::clone(&queue));
            server
                .dispatch_server_events(&mut conn, &mut events, &mut writer, 1003, USER_CHANNEL_ID, None, None)
                .await
                .unwrap();

            // URBDRC is the last channel registered, so the device gets the next id.
            let device = urbdrc + 1;
            let create = SvcMessage::from(DrdynvcServerPdu::Create(ironrdp_dvc::pdu::CreateRequestPdu::new(
                device,
                URBDRC_CHANNEL_NAME.into(),
            )))
            .with_flags(ChannelFlags::SHOW_PROTOCOL);
            let mut expected = server_encode_svc_messages(vec![create], DRDYNVC_CHANNEL_ID, USER_CHANNEL_ID).unwrap();
            let transfer = ironrdp_dvc::encode_dvc_messages(
                77,
                vec![Box::new(AddVirtualChannel { msg_id: 2 })],
                ChannelFlags::empty(),
            )
            .unwrap();
            expected.extend(server_encode_svc_messages(transfer, DRDYNVC_CHANNEL_ID, USER_CHANNEL_ID).unwrap());
            let written: Vec<u8> = queue
                .pending
                .borrow()
                .iter()
                .flat_map(|pdu| pdu.bytes.iter().copied())
                .collect();
            assert_eq!(written, expected);
        }
    }

    mod camera {
        use core::any::TypeId;
        use std::rc::Rc;

        use ironrdp_core::encode_vec;
        use ironrdp_dvc::DrdynvcServer;
        use ironrdp_dvc::pdu::{
            CapabilitiesResponsePdu, CapsVersion, CreateRequestPdu, CreateResponsePdu, CreationStatus, DataPdu,
            DrdynvcClientPdu, DrdynvcDataPdu, DrdynvcServerPdu,
        };
        use ironrdp_svc::{ChannelFlags, SvcMessage, SvcProcessor as _};

        use super::super::super::OutboundQueue;
        use super::*;
        use crate::{RdCameraServer, UrbdrcServer};

        const DRDYNVC_CHANNEL_ID: u16 = 1005;
        const USER_CHANNEL_ID: u16 = 1002;

        struct Camera {
            sender: Option<mpsc::UnboundedSender<ServerEvent>>,
            sinks: Arc<AtomicU32>,
        }

        impl crate::ServerEventSender for Camera {
            fn set_sender(&mut self, sender: mpsc::UnboundedSender<ServerEvent>) {
                self.sender = Some(sender);
            }
        }

        impl RdCameraServerFactory for Camera {
            fn build_processor(&self) -> RdCameraServer {
                RdCameraServer::with_sender(self.sender.clone())
            }

            fn build_sample_sink(&self) -> Option<Box<dyn crate::CameraSampleSink>> {
                self.sinks.fetch_add(1, Ordering::Relaxed);
                None
            }
        }

        struct Usb;

        impl crate::ServerEventSender for Usb {
            fn set_sender(&mut self, _: mpsc::UnboundedSender<ServerEvent>) {}
        }

        impl UrbdrcServerFactory for Usb {
            fn build_processor(&self) -> UrbdrcServer {
                UrbdrcServer::new()
            }
        }

        fn server(sinks: &Arc<AtomicU32>) -> RdpServer {
            RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_no_security()
                .with_no_input()
                .with_no_display()
                .with_urbdrc_factory(Some(Box::new(Usb)))
                .with_camera_factory(Some(Box::new(Camera {
                    sender: None,
                    sinks: Arc::clone(sinks),
                })))
                .build()
        }

        fn open_all(drdynvc: &mut DrdynvcServer, creates: Vec<SvcMessage>) {
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
        }

        struct Mic;

        impl AudinServerFactory for Mic {
            fn build_processor(&self) -> crate::AudinServer {
                crate::AudinServer::new(None)
            }
        }

        #[test]
        fn the_camera_and_mic_channels_are_attached_after_urbdrc() {
            let server = RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_no_security()
                .with_no_input()
                .with_no_display()
                .with_audin_factory(Some(Box::new(Mic)))
                .with_urbdrc_factory(Some(Box::new(Usb)))
                .with_camera_factory(Some(Box::new(Camera {
                    sender: None,
                    sinks: Arc::default(),
                })))
                .build();
            let drdynvc = server
                .dynamic_channel_factories
                .iter()
                .fold(DrdynvcServer::new(), |dvc, factory| factory.attach(dvc));
            let camera = drdynvc.get_channel_id_by_type::<RdCameraServer>().unwrap();
            let mic = drdynvc.get_channel_id_by_type::<crate::AudinServer>().unwrap();
            assert!(drdynvc.get_channel_id_by_type::<UrbdrcServer>().unwrap() < camera);
            assert!(camera < mic);
        }

        #[tokio::test]
        async fn an_announced_camera_gets_its_own_channel() {
            let sinks = Arc::new(AtomicU32::new(0));
            let mut server = server(&sinks);
            let mut drdynvc = server
                .dynamic_channel_factories
                .iter()
                .fold(DrdynvcServer::new(), |dvc, factory| factory.attach(dvc));
            drdynvc.start().unwrap();
            let caps = encode_vec(&DrdynvcClientPdu::Capabilities(CapabilitiesResponsePdu::new(
                CapsVersion::V2,
            )))
            .unwrap();
            let creates = drdynvc.process(&caps).unwrap();
            open_all(&mut drdynvc, creates);
            let enumerator = drdynvc.get_channel_id_by_type::<RdCameraServer>().unwrap();

            // DEVICE_ADDED_NOTIFICATION: "C\0" in UTF-16LE, then the channel name.
            let mut added = vec![1, 0x05, b'C', 0, 0, 0];
            added.extend_from_slice(b"RDCamera_Device_0\0");
            let added = encode_vec(&DrdynvcClientPdu::Data(DrdynvcDataPdu::Data(DataPdu::new(
                enumerator, added,
            ))))
            .unwrap();
            assert!(drdynvc.process(&added).unwrap().is_empty());

            let mut conn = ConnectionState::default();
            conn.static_channels.insert(drdynvc);
            conn.static_channels
                .attach_channel_id(TypeId::of::<DrdynvcServer>(), DRDYNVC_CHANNEL_ID);
            let mut events = Vec::new();
            {
                let mut receiver = server.ev_receiver.lock().await;
                while let Ok(event) = receiver.try_recv() {
                    events.push(event);
                }
            }
            assert_eq!(events.len(), 1);
            assert_eq!(sinks.load(Ordering::Relaxed), 0);

            let queue = Rc::new(OutboundQueue::default());
            let mut writer = QueuedWriter::new(Rc::clone(&queue));
            server
                .dispatch_server_events(&mut conn, &mut events, &mut writer, 1003, USER_CHANNEL_ID, None, None)
                .await
                .unwrap();
            // The sink is built once, when the device channel is created.
            assert_eq!(sinks.load(Ordering::Relaxed), 1);

            // The enumerator is the last channel registered, so the device gets the next id.
            let create = SvcMessage::from(DrdynvcServerPdu::Create(CreateRequestPdu::new(
                enumerator + 1,
                "RDCamera_Device_0".into(),
            )))
            .with_flags(ChannelFlags::SHOW_PROTOCOL);
            let expected = server_encode_svc_messages(vec![create], DRDYNVC_CHANNEL_ID, USER_CHANNEL_ID).unwrap();
            let written: Vec<u8> = queue
                .pending
                .borrow()
                .iter()
                .flat_map(|pdu| pdu.bytes.iter().copied())
                .collect();
            assert_eq!(written, expected);
        }
    }

    mod fingerprint {
        use core::cell::RefCell;
        use std::rc::Rc;
        use std::sync::Mutex as StdMutex;

        use ironrdp_pdu::rdp::capability_sets::{General, GeneralExtraFlags, MajorPlatformType, MinorPlatformType};

        use super::*;
        use crate::ConnectionHandler;

        type Seen = Arc<StdMutex<Vec<(String, u32, u32, String)>>>;

        struct Recorder(Seen);

        impl ConnectionHandler for Recorder {
            fn on_client_fingerprint(
                &mut self,
                client_name: &str,
                rdp_version: u32,
                client_build: u32,
                platform: &str,
            ) {
                self.0
                    .lock()
                    .unwrap()
                    .push((client_name.to_owned(), rdp_version, client_build, platform.to_owned()));
            }
        }

        fn handler(seen: &Seen) -> SharedConnectionHandler {
            let handler: Box<dyn ConnectionHandler> = Box::new(Recorder(Arc::clone(seen)));
            Rc::new(RefCell::new(handler))
        }

        #[test]
        fn the_handler_gets_the_client_s_identity_and_platform() {
            let seen = Seen::default();
            let handler = handler(&seen);
            let general = CapabilitySet::General(General {
                major_platform_type: MajorPlatformType::WINDOWS,
                minor_platform_type: MinorPlatformType::WINDOWS_NT,
                protocol_version: 0x0200,
                extra_flags: GeneralExtraFlags::empty(),
                refresh_rect_support: false,
                suppress_output_support: false,
            });
            client_fingerprint(Some(&handler), "pc", 0x0008_0004, 22621, &[general]);
            client_fingerprint(Some(&handler), "pc", 0x0008_0004, 2600, &[]);
            client_fingerprint(None, "pc", 0x0008_0004, 2600, &[]);

            let platform = format!("{:?}/{:?}", MajorPlatformType::WINDOWS, MinorPlatformType::WINDOWS_NT);
            assert_eq!(
                *seen.lock().unwrap(),
                vec![
                    ("pc".to_owned(), 0x0008_0004, 22621, platform),
                    ("pc".to_owned(), 0x0008_0004, 2600, "unknown".to_owned()),
                ]
            );
        }
    }

    mod link_rtt {
        use super::super::super::{ConnectionSource, IncomingConnection, NextConnection, TcpConnectionSource};
        use super::*;
        use crate::TransportTls;

        struct ClosedSource;

        #[async_trait::async_trait]
        impl ConnectionSource for ClosedSource {
            type Stream = tokio::io::DuplexStream;

            async fn accept(&mut self) -> std::io::Result<Option<IncomingConnection<Self::Stream>>> {
                Ok(None)
            }
        }

        #[tokio::test]
        async fn the_tcp_source_samples_the_rtt_at_accept() {
            let listener = tokio::net::TcpListener::bind((core::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            let mut source = TcpConnectionSource { listener };
            let _client = tokio::net::TcpStream::connect(addr).await.unwrap();
            let incoming = source.accept().await.unwrap().unwrap();
            // macOS only; a loopback RTT below 1 ms reads as 1.
            assert_eq!(incoming.link_rtt_ms.is_some(), cfg!(target_os = "macos"));
            assert_ne!(incoming.link_rtt_ms, Some(0));
        }

        async fn serve(server: &mut RdpServer, link_rtt_ms: Option<u32>) {
            let (stream, client) = tokio::io::duplex(64);
            drop(client);
            let mut incoming = IncomingConnection::new(stream, TransportTls::Managed, None);
            incoming.link_rtt_ms = link_rtt_ms;
            tokio::time::timeout(
                Duration::from_secs(5),
                server.serve_connection(NextConnection::Incoming(incoming), &mut ClosedSource),
            )
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn a_fresh_connection_publishes_its_rtt_or_unknown() {
            let mut server = RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_no_security()
                .with_no_input()
                .with_no_display()
                .build();
            let cell = Arc::new(AtomicU32::new(999));
            server.set_link_rtt_handle(Arc::clone(&cell));

            serve(&mut server, Some(42)).await;
            assert_eq!(cell.load(Ordering::Relaxed), 42);

            // A connection without a sample does not inherit the previous one's.
            serve(&mut server, None).await;
            assert_eq!(cell.load(Ordering::Relaxed), 0);
        }
    }

    mod authentication {
        use core::cell::RefCell;
        use std::rc::Rc;
        use std::sync::Mutex as StdMutex;

        use ironrdp_pdu::nego;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
        use tokio_rustls::rustls;

        use super::super::super::{
            ConnectionSource, IncomingConnection, NegotiationContext, NextConnection, negotiate_candidate,
        };
        use super::*;
        use crate::{ConnectionHandler, TransportTls};

        type Outcomes = Arc<StdMutex<Vec<(bool, Option<String>)>>>;

        struct Recorder(Outcomes);

        impl ConnectionHandler for Recorder {
            fn on_authenticated(&mut self, success: bool, reason: Option<&str>) {
                self.0
                    .lock()
                    .expect("outcomes lock poisoned")
                    .push((success, reason.map(ToOwned::to_owned)));
            }
        }

        struct NoCert;

        impl core::fmt::Debug for NoCert {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str("NoCert")
            }
        }

        impl rustls::server::ResolvesServerCert for NoCert {
            fn resolve(&self, _: rustls::server::ClientHello<'_>) -> Option<Arc<rustls::sign::CertifiedKey>> {
                None
            }
        }

        /// Hybrid security; the TLS acceptor is never used, because the streams
        /// below are `TransportTls::AlreadyDone`.
        fn hybrid() -> (tokio_rustls::TlsAcceptor, Vec<u8>) {
            let config =
                rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
                    .with_safe_default_protocol_versions()
                    .unwrap()
                    .with_no_client_auth()
                    .with_cert_resolver(Arc::new(NoCert));
            (tokio_rustls::TlsAcceptor::from(Arc::new(config)), vec![0; 32])
        }

        /// A client that asks for NLA, reads the Connection Confirm, then sends
        /// junk instead of a TSRequest, so CredSSP fails.
        async fn client_that_fails_credssp(mut stream: DuplexStream) {
            let request = nego::ConnectionRequest {
                nego_data: None,
                flags: nego::RequestFlags::empty(),
                protocol: nego::SecurityProtocol::HYBRID | nego::SecurityProtocol::HYBRID_EX,
                correlation_info: None,
            };
            let bytes = ironrdp_core::encode_vec(&ironrdp_pdu::x224::X224(request)).unwrap();
            stream.write_all(&bytes).await.unwrap();
            let mut confirm = [0u8; 64];
            let n = stream.read(&mut confirm).await.unwrap();
            assert!(n > 0, "no Connection Confirm");
            stream.write_all(&[0xff; 8]).await.unwrap();
            stream.shutdown().await.unwrap();
            // Keep the read half open until the server gives up.
            let mut rest = Vec::new();
            let _ = stream.read_to_end(&mut rest).await;
        }

        fn outcomes() -> (Outcomes, Rc<RefCell<Box<dyn ConnectionHandler>>>) {
            let outcomes = Outcomes::default();
            let handler: Box<dyn ConnectionHandler> = Box::new(Recorder(Arc::clone(&outcomes)));
            (outcomes, Rc::new(RefCell::new(handler)))
        }

        /// So CredSSP reads the client's TSRequest instead of failing at once.
        fn creds() -> Option<crate::Credentials> {
            Some(crate::Credentials {
                username: "user".to_owned(),
                password: "pass".to_owned(),
                domain: None,
            })
        }

        fn hybrid_ctx(handler: Rc<RefCell<Box<dyn ConnectionHandler>>>) -> NegotiationContext {
            let (acceptor, pub_key) = hybrid();
            let mut server = RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_hybrid(acceptor, pub_key)
                .with_no_input()
                .with_no_display()
                .build();
            server.set_credentials(creds());
            let mut ctx = server.negotiation_context();
            ctx.connection_handler = Some(handler);
            ctx
        }

        #[tokio::test]
        async fn a_candidate_s_failed_credssp_is_reported() {
            let (outcomes, handler) = outcomes();
            let ctx = hybrid_ctx(handler);
            let (stream, client) = tokio::io::duplex(4096);
            let client = tokio::spawn(client_that_fails_credssp(client));

            let candidate = tokio::time::timeout(
                Duration::from_secs(5),
                negotiate_candidate(&ctx, IncomingConnection::new(stream, TransportTls::AlreadyDone, None)),
            )
            .await
            .unwrap();
            assert!(candidate.is_none());
            client.await.unwrap();

            let outcomes = outcomes.lock().unwrap();
            assert_eq!(outcomes.len(), 1, "{outcomes:?}");
            assert!(!outcomes[0].0);
            assert!(outcomes[0].1.is_some());
        }

        #[tokio::test]
        async fn a_pre_tls_negotiation_error_is_not_a_failed_login() {
            let (outcomes, handler) = outcomes();
            let ctx = hybrid_ctx(handler);
            let (stream, mut client) = tokio::io::duplex(4096);
            client.write_all(&[0x03, 0x00, 0xff, 0xff, 0x41, 0x41]).await.unwrap();
            drop(client);

            let candidate = tokio::time::timeout(
                Duration::from_secs(5),
                negotiate_candidate(&ctx, IncomingConnection::new(stream, TransportTls::AlreadyDone, None)),
            )
            .await
            .unwrap();
            assert!(candidate.is_none());
            assert!(outcomes.lock().unwrap().is_empty());
        }

        struct ClosedSource;

        #[async_trait::async_trait]
        impl ConnectionSource for ClosedSource {
            type Stream = DuplexStream;

            async fn accept(&mut self) -> std::io::Result<Option<IncomingConnection<Self::Stream>>> {
                Ok(None)
            }
        }

        #[tokio::test]
        async fn a_served_connection_s_failed_credssp_is_reported() {
            let outcomes = Outcomes::default();
            let (acceptor, pub_key) = hybrid();
            let mut server = RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_hybrid(acceptor, pub_key)
                .with_no_input()
                .with_no_display()
                .with_connection_handler(Some(Box::new(Recorder(Arc::clone(&outcomes)))))
                .build();
            server.set_credentials(creds());
            let (stream, client) = tokio::io::duplex(4096);
            let client = tokio::spawn(client_that_fails_credssp(client));

            tokio::time::timeout(
                Duration::from_secs(5),
                server.serve_connection(
                    NextConnection::Incoming(IncomingConnection::new(stream, TransportTls::AlreadyDone, None)),
                    &mut ClosedSource,
                ),
            )
            .await
            .unwrap();
            client.await.unwrap();

            let outcomes = outcomes.lock().unwrap();
            assert_eq!(outcomes.len(), 1, "{outcomes:?}");
            assert!(!outcomes[0].0);
        }
    }

    #[tokio::test]
    async fn the_next_wave_waits_until_the_previous_one_is_written() {
        let (sender, receiver) = mpsc::channel::<AudioWave>(AUDIO_CHANNEL_CAPACITY);
        let receiver = Mutex::new(receiver);
        let mut audio = AudioDispatch::new(Some(&receiver)).await;
        sender.try_send((vec![0; 4], 0, None)).unwrap();
        sender.try_send((vec![1; 4], 1, None)).unwrap();

        let (in_flight, (_, ts, _)) = audio.next_wave().await.unwrap();
        assert_eq!(ts, 0);
        // The first wave still holds its permit (queued, not yet written).
        let blocked = tokio::time::timeout(Duration::from_millis(50), audio.next_wave()).await;
        assert!(
            blocked.is_err(),
            "a second wave was taken while the first was in flight"
        );
        // The canceled call took no wave.
        drop(in_flight);
        let (_, (_, ts, _)) = audio.next_wave().await.unwrap();
        assert_eq!(ts, 1);
    }

    #[tokio::test]
    async fn a_closed_channel_turns_the_audio_arm_off() {
        let (sender, receiver) = mpsc::channel::<AudioWave>(AUDIO_CHANNEL_CAPACITY);
        let receiver = Mutex::new(receiver);
        let mut audio = AudioDispatch::new(Some(&receiver)).await;
        drop(sender);
        let wave = audio.next_wave().await;
        assert!(wave.is_none());
        let queue = std::rc::Rc::new(super::super::OutboundQueue::default());
        let mut writer = QueuedWriter::new(std::rc::Rc::clone(&queue));
        audio
            .dispatch(&mut ConnectionState::default(), &mut writer, 1002, wave)
            .unwrap();
        assert!(!audio.is_open());
        assert_eq!(queue.queued_bytes.get(), 0);
    }

    #[tokio::test]
    async fn a_wave_without_an_rdpsnd_channel_is_dropped() {
        let (sender, receiver) = mpsc::channel::<AudioWave>(AUDIO_CHANNEL_CAPACITY);
        let receiver = Mutex::new(receiver);
        let mut audio = AudioDispatch::new(Some(&receiver)).await;
        sender.try_send((vec![0; 4], 0, None)).unwrap();
        let wave = audio.next_wave().await;
        let queue = std::rc::Rc::new(super::super::OutboundQueue::default());
        let mut writer = QueuedWriter::new(std::rc::Rc::clone(&queue));
        audio
            .dispatch(&mut ConnectionState::default(), &mut writer, 1002, wave)
            .unwrap();
        assert!(audio.is_open());
        assert_eq!(queue.queued_bytes.get(), 0);
        // The dropped wave released its permit.
        sender.try_send((vec![1; 4], 1, None)).unwrap();
        let next = tokio::time::timeout(Duration::from_millis(50), audio.next_wave()).await;
        assert!(next.is_ok());
    }

    #[tokio::test]
    async fn a_replaced_session_s_queued_waves_are_discarded() {
        type Slot = Arc<std::sync::Mutex<Option<mpsc::Sender<AudioWave>>>>;
        struct KeepSender(Slot);
        impl crate::ServerEventSender for KeepSender {
            fn set_sender(&mut self, _sender: mpsc::UnboundedSender<ServerEvent>) {}
        }
        impl crate::SoundServerFactory for KeepSender {
            fn build_backend(&self) -> Box<dyn crate::RdpsndServerHandler> {
                unreachable!("no connection in this test")
            }
            fn set_audio_sender(&mut self, audio_sender: mpsc::Sender<AudioWave>) {
                *self.0.lock().unwrap() = Some(audio_sender);
            }
        }

        let slot = Slot::default();
        let mut server = RdpServer::builder()
            .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
            .with_no_security()
            .with_no_input()
            .with_no_display()
            .with_sound_factory(Some(Box::new(KeepSender(Arc::clone(&slot)))))
            .build();
        let sender = slot
            .lock()
            .unwrap()
            .clone()
            .expect("the builder installs the audio sender");
        for ts in 0..3 {
            sender.try_send((vec![0; 4], ts, None)).unwrap();
        }

        server.discard_stale_session_events().await;

        let receiver = server.macrdp_audio_receiver().unwrap();
        assert!(receiver.lock().await.try_recv().is_err());
    }

    #[test]
    fn pcm_wave_ms_comes_from_its_byte_length() {
        // 1029 frames of 16-bit stereo PCM at 44.1 kHz.
        let ms = AudioLagModel::wave_ms(1029 * 4, None);
        assert!((ms - 23.333).abs() < 0.01, "{ms}");
    }

    #[test]
    fn aac_wave_ms_is_its_own_duration() {
        assert!((AudioLagModel::wave_ms(120, Some(21.333)) - 21.333).abs() < f64::EPSILON);
    }

    #[test]
    fn a_burst_is_admitted_up_to_max_lag() {
        let mut lag = AudioLagModel::default();
        let now = Instant::now();
        let mut admitted = 0;
        for _ in 0..20 {
            if lag.admit(now, 20.0) {
                lag.shipped(20.0);
                admitted += 1;
            }
        }
        // 10 waves of 20 ms fill the 200 ms budget; the rest are dropped.
        assert_eq!(admitted, 10);
    }

    #[test]
    fn a_writer_stall_resyncs_to_live() {
        let mut lag = AudioLagModel::default();
        let start = Instant::now();
        assert!(lag.admit(start, 20.0));
        lag.shipped(20.0);
        // The writer stalls for 1 s, then the backlog arrives at once.
        let after = start + Duration::from_secs(1);
        let mut admitted = 0;
        for _ in 0..50 {
            if lag.admit(after, 20.0) {
                lag.shipped(20.0);
                admitted += 1;
            }
        }
        // Resynced: one MAX_LAG_MS of waves goes out, not the whole second.
        assert_eq!(admitted, 10);
    }

    #[test]
    fn a_short_writer_stall_does_not_resync() {
        let mut lag = AudioLagModel::default();
        let start = Instant::now();
        assert!(lag.admit(start, 20.0));
        lag.shipped(20.0);
        // 250 ms deficit: under RESYNC_DEFICIT_MS, so the gap is read as the
        // client's buffer draining and the backlog fills it.
        let after = start + Duration::from_millis(270);
        let mut admitted = 0;
        for _ in 0..50 {
            if lag.admit(after, 20.0) {
                lag.shipped(20.0);
                admitted += 1;
            }
        }
        // 250 ms deficit + 200 ms budget = 22.5 waves of 20 ms.
        assert_eq!(admitted, 22);
    }

    mod rdpdr_drive {
        use core::any::TypeId;
        use core::sync::atomic::AtomicUsize;

        use ironrdp_acceptor::{Acceptor, DesktopSize};
        use ironrdp_pdu::nego::SecurityProtocol;
        use ironrdp_svc::{ChannelFlags, SvcMessage};

        use super::*;
        use crate::rdpdr_drive::IoRouter;
        use crate::{AnnouncedDevice, RdpdrBackendFactory, RdpdrHandle, RdpdrServerHandler};

        #[derive(Default)]
        struct Calls {
            set_sender: AtomicUsize,
            build_backend: AtomicUsize,
            set_handle: AtomicUsize,
        }

        struct Backend(Arc<Calls>);

        impl core::fmt::Debug for Backend {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                f.write_str("Backend")
            }
        }

        impl RdpdrServerHandler for Backend {
            fn set_handle(&mut self, _handle: RdpdrHandle) {
                self.0.set_handle.fetch_add(1, Ordering::Relaxed);
            }
            fn on_devices_announced(&mut self, _devices: &[AnnouncedDevice]) {}
        }

        struct Factory(Arc<Calls>);

        impl crate::ServerEventSender for Factory {
            fn set_sender(&mut self, _sender: mpsc::UnboundedSender<ServerEvent>) {
                self.0.set_sender.fetch_add(1, Ordering::Relaxed);
            }
        }

        impl RdpdrBackendFactory for Factory {
            fn build_backend(&self) -> Box<dyn RdpdrServerHandler> {
                self.0.build_backend.fetch_add(1, Ordering::Relaxed);
                Box::new(Backend(Arc::clone(&self.0)))
            }
            fn computer_name(&self) -> String {
                "mac".to_owned()
            }
        }

        impl RdpdrDriveServerFactory for Factory {}

        #[tokio::test]
        async fn the_drive_factory_attaches_a_processor_per_connection() {
            let calls = Arc::new(Calls::default());
            let server = RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_no_security()
                .with_no_input()
                .with_no_display()
                .with_rdpdr_drive_factory(Some(Box::new(Factory(Arc::clone(&calls)))))
                .build();
            assert_eq!(calls.set_sender.load(Ordering::Relaxed), 1);
            assert_eq!(server.static_channel_factories.len(), 1);

            for connection in 1..=2 {
                let mut acceptor = Acceptor::new(
                    SecurityProtocol::empty(),
                    DesktopSize {
                        width: 1024,
                        height: 768,
                    },
                    Vec::new(),
                    None,
                );
                let _ = server.attach_channels(&mut acceptor).await;
                assert_eq!(calls.build_backend.load(Ordering::Relaxed), connection);
                assert_eq!(calls.set_handle.load(Ordering::Relaxed), connection);
            }
        }

        #[test]
        fn no_drive_factory_adds_no_static_channel_factory() {
            let server = RdpServer::builder()
                .with_addr((core::net::Ipv4Addr::LOCALHOST, 0))
                .with_no_security()
                .with_no_input()
                .with_no_display()
                .build();
            assert!(server.static_channel_factories.is_empty());
        }

        fn joined_rdpdr(channel_id: u16) -> ConnectionState {
            let mut conn = ConnectionState::default();
            let backend = Box::new(Backend(Arc::default()));
            conn.static_channels
                .insert(RdpdrDriveServer::new(backend, "mac".to_owned(), IoRouter::default()));
            conn.static_channels
                .attach_channel_id(TypeId::of::<RdpdrDriveServer>(), channel_id);
            conn
        }

        #[tokio::test]
        async fn a_large_request_goes_out_without_show_protocol() {
            let payload = vec![0x5a; 3 * ironrdp_svc::CHANNEL_CHUNK_LENGTH];
            let mut conn = joined_rdpdr(1004);
            let queue = std::rc::Rc::new(super::super::super::OutboundQueue::default());
            let mut writer = QueuedWriter::new(std::rc::Rc::clone(&queue));
            dispatch_rdpdr_drive(
                &mut conn,
                &mut writer,
                1002,
                RdpdrDriveMessage::SendMessages(vec![SvcMessage::from(payload.clone())]),
            )
            .await
            .unwrap();

            let queued: Vec<u8> = queue
                .pending
                .borrow()
                .iter()
                .flat_map(|pdu| pdu.bytes.clone())
                .collect();
            let plain = server_encode_svc_messages(vec![SvcMessage::from(payload.clone())], 1004, 1002).unwrap();
            let flagged = server_encode_svc_messages(
                vec![SvcMessage::from(payload).with_flags(ChannelFlags::SHOW_PROTOCOL)],
                1004,
                1002,
            )
            .unwrap();
            assert_eq!(queued, plain);
            assert_ne!(queued, flagged);
        }

        #[tokio::test]
        async fn a_request_without_an_rdpdr_channel_is_dropped() {
            let queue = std::rc::Rc::new(super::super::super::OutboundQueue::default());
            let mut writer = QueuedWriter::new(std::rc::Rc::clone(&queue));
            dispatch_rdpdr_drive(
                &mut ConnectionState::default(),
                &mut writer,
                1002,
                RdpdrDriveMessage::SendMessages(vec![SvcMessage::from(vec![1, 2, 3])]),
            )
            .await
            .unwrap();
            assert_eq!(queue.queued_bytes.get(), 0);
        }
    }

    #[cfg(feature = "egfx")]
    mod batch_priority {
        use ironrdp_cliprdr::backend::ClipboardMessage;
        use ironrdp_rdpdr::server::RdpdrServerMessage;

        use super::*;
        use crate::EgfxServerMessage;

        fn egfx(n: usize) -> ServerEvent {
            ServerEvent::Egfx(EgfxServerMessage::SendMessages {
                messages: Vec::with_capacity(n),
            })
        }

        fn label(event: &ServerEvent) -> String {
            match event {
                ServerEvent::Clipboard(_) => "clip".to_owned(),
                ServerEvent::Rdpdr(_) => "rdpdr".to_owned(),
                ServerEvent::RdpdrDrive(_) => "drive".to_owned(),
                ServerEvent::Egfx(EgfxServerMessage::SendMessages { messages }) => {
                    format!("egfx{}", messages.capacity())
                }
                ServerEvent::LogonCompleted => "other".to_owned(),
                _ => unreachable!(),
            }
        }

        fn rdpdr() -> ServerEvent {
            ServerEvent::Rdpdr(RdpdrServerMessage::Close {
                device_id: 1,
                file_id: 1,
            })
        }

        fn drive() -> ServerEvent {
            ServerEvent::RdpdrDrive(RdpdrDriveMessage::SendMessages(Vec::new()))
        }

        fn clip() -> ServerEvent {
            ServerEvent::Clipboard(ClipboardMessage::SendInitiateCopy(Vec::new()))
        }

        #[test]
        fn a_batch_with_egfx_goes_cliprdr_first_and_rdpdr_last() {
            let mut events = vec![
                rdpdr(),
                egfx(1),
                clip(),
                ServerEvent::LogonCompleted,
                egfx(2),
                rdpdr(),
                egfx(3),
                clip(),
            ];
            prioritize_batch(&mut events);
            let order: Vec<_> = events.iter().map(label).collect();
            assert_eq!(
                order,
                ["clip", "clip", "egfx1", "other", "egfx2", "egfx3", "rdpdr", "rdpdr"]
            );
        }

        #[test]
        fn macrdp_rdpdr_sorts_last_with_upstream_rdpdr() {
            let mut events = vec![drive(), egfx(1), rdpdr(), clip(), drive(), egfx(2)];
            prioritize_batch(&mut events);
            let order: Vec<_> = events.iter().map(label).collect();
            assert_eq!(order, ["clip", "egfx1", "egfx2", "drive", "rdpdr", "drive"]);
        }

        #[test]
        fn a_batch_without_egfx_keeps_its_order() {
            let mut events = vec![rdpdr(), clip(), ServerEvent::LogonCompleted];
            prioritize_batch(&mut events);
            let order: Vec<_> = events.iter().map(label).collect();
            assert_eq!(order, ["rdpdr", "clip", "other"]);
        }
    }
}
