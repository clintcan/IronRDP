//! EGFX (Graphics Pipeline Extension) server integration.
//!
//! Provides the bridge between `ironrdp-egfx`'s `GraphicsPipelineServer` and
//! `ironrdp-server`'s `RdpServer`, enabling H.264 video streaming via DVC.
//!
//! The bridge pattern (`GfxDvcBridge`) wraps an `Arc<Mutex<GraphicsPipelineServer>>`
//! so the display handler can call `send_avc420_frame()` proactively while the
//! DVC infrastructure handles client messages (capability negotiation, frame acks).

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ironrdp_core::impl_as_any;
use ironrdp_dvc::{DvcMessage, DvcProcessor, DvcServerProcessor};
use ironrdp_egfx::server::{GraphicsPipelineHandler, GraphicsPipelineServer};
use ironrdp_pdu::PduResult;
use ironrdp_svc::SvcMessage;

use crate::autodetect::AutoDetectHandles;
use crate::server::ServerEventSender;

/// Shared handle to a `GraphicsPipelineServer`.
///
/// Uses `std::sync::Mutex` (not tokio) because `DvcProcessor` trait methods
/// are synchronous and cannot hold async locks.
pub type GfxServerHandle = Arc<Mutex<GraphicsPipelineServer>>;

/// What a connection hands to its EGFX backend, passed to the build methods of
/// [`GfxServerFactory`].
///
/// Each connection has its own handles, the same ones it hands to the display
/// through [`DisplayContext`](crate::DisplayContext).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GfxContext {
    /// The connection's auto-detect measurements, for flow control.
    pub autodetect: AutoDetectHandles,
}

/// Factory for creating EGFX graphics pipeline handlers.
///
/// Implements `ServerEventSender` so the factory can signal the server event loop
/// when EGFX frames are ready to be drained and sent.
pub trait GfxServerFactory: ServerEventSender + Send {
    /// Create a handler for EGFX callbacks (caps negotiation, frame acks).
    fn build_gfx_handler(&self, ctx: GfxContext) -> Box<dyn GraphicsPipelineHandler>;

    /// Create a bridge and shared server handle for proactive frame sending.
    ///
    /// When returning `Some`, the bridge is registered with DrdynvcServer for
    /// client messages, and the handle reaches the display through
    /// [`DisplayContext::gfx_handle`](crate::DisplayContext::gfx_handle) for
    /// direct frame submission. Returns `None` by default, falling back to
    /// `build_gfx_handler()`.
    fn build_server_with_handle(&self, _ctx: GfxContext) -> Option<(GfxDvcBridge, GfxServerHandle)> {
        None
    }
}

/// Build an EGFX bridge while always retaining a shared server handle.
pub(crate) fn build_server_and_handle(
    factory: &dyn GfxServerFactory,
    ctx: GfxContext,
) -> (GfxDvcBridge, GfxServerHandle) {
    if let Some((bridge, handle)) = factory.build_server_with_handle(ctx.clone()) {
        return (bridge, handle);
    }

    let handler = factory.build_gfx_handler(ctx);
    let server = Arc::new(Mutex::new(GraphicsPipelineServer::new(handler)));
    (GfxDvcBridge::new(Arc::clone(&server)), server)
}

/// DVC bridge wrapping a shared `GraphicsPipelineServer`.
///
/// Delegates all `DvcProcessor` methods to the inner server through a mutex,
/// enabling shared access from both the DVC layer and the display handler.
pub struct GfxDvcBridge {
    inner: GfxServerHandle,
    /// (macrdp divergence 14) Optional "decline the graphics pipeline" flag, set by the application's
    /// `GraphicsPipelineHandler` (synchronously during `process` — the inner
    /// server invokes the handler before returning). When true, `process`
    /// discards the inner server's output instead of shipping it — most
    /// importantly the `CapabilitiesConfirm`, so a client whose advertised
    /// capabilities the application can't serve (e.g. EGFX offered but AVC
    /// disabled on every capset) is never told the pipeline is active and
    /// keeps rendering legacy updates. Confirming caps and then sending legacy
    /// updates anyway is a protocol violation some clients (Windows App for
    /// Android) hard-disconnect on. `None` = never decline (default).
    decline_output: Option<Arc<AtomicBool>>,
}

impl GfxDvcBridge {
    pub fn new(server: GfxServerHandle) -> Self {
        Self {
            inner: server,
            decline_output: None,
        }
    }

    /// Like [`Self::new`] but with a shared decline flag (see the field note).
    pub fn with_decline_flag(server: GfxServerHandle, decline: Arc<AtomicBool>) -> Self {
        Self {
            inner: server,
            decline_output: Some(decline),
        }
    }

    pub fn server(&self) -> &GfxServerHandle {
        &self.inner
    }
}

impl_as_any!(GfxDvcBridge);

impl DvcProcessor for GfxDvcBridge {
    fn channel_name(&self) -> &str {
        ironrdp_egfx::CHANNEL_NAME
    }

    fn max_message_size(&self) -> Option<usize> {
        self.inner
            .lock()
            .expect("GfxServerHandle mutex poisoned")
            .max_message_size()
    }

    fn start(&mut self, channel_id: u32) -> PduResult<Vec<DvcMessage>> {
        self.inner
            .lock()
            .expect("GfxServerHandle mutex poisoned")
            .start(channel_id)
    }

    fn process(&mut self, channel_id: u32, payload: &[u8]) -> PduResult<Vec<DvcMessage>> {
        let out = self
            .inner
            .lock()
            .expect("GfxServerHandle mutex poisoned")
            .process(channel_id, payload)?;
        // The handler ran synchronously inside `process` above, so the decline
        // flag is current for the PDU just handled. Discarding the output here
        // (rather than not processing) keeps the inner server's state machine
        // consistent while the client is never told the pipeline came up.
        if let Some(declined) = &self.decline_output
            && declined.load(Ordering::Relaxed)
        {
            return Ok(Vec::new());
        }
        Ok(out)
    }

    fn close(&mut self, channel_id: u32) {
        self.inner
            .lock()
            .expect("GfxServerHandle mutex poisoned")
            .close(channel_id)
    }
}

impl DvcServerProcessor for GfxDvcBridge {}

/// Message for routing EGFX PDUs to the wire via `ServerEvent`.
#[derive(Debug)]
pub enum EgfxServerMessage {
    /// Pre-encoded DVC messages from `GraphicsPipelineServer::drain_output()`.
    SendMessages { messages: Vec<SvcMessage> },
}

impl core::fmt::Display for EgfxServerMessage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::SendMessages { messages } => {
                write!(f, "SendMessages(count={})", messages.len())
            }
        }
    }
}

/// The dynamic channel id EGFX is registered under: the bridge when the
/// factory handed out a frame handle, otherwise the server itself.
pub(crate) fn egfx_channel_id(drdynvc: &ironrdp_dvc::DrdynvcServer) -> Option<u32> {
    drdynvc
        .get_channel_id_by_type::<GfxDvcBridge>()
        .or_else(|| drdynvc.get_channel_id_by_type::<GraphicsPipelineServer>())
}

#[cfg(test)]
mod tests {
    use ironrdp_dvc::DrdynvcServer;
    use ironrdp_egfx::pdu::{CapabilitiesAdvertisePdu, CapabilitySet};

    use super::*;

    struct Handler;

    impl GraphicsPipelineHandler for Handler {
        fn capabilities_advertise(&mut self, _pdu: &CapabilitiesAdvertisePdu) {}

        fn on_ready(&mut self, _negotiated: &CapabilitySet) {}
    }

    struct LegacyFactory;

    impl ServerEventSender for LegacyFactory {
        fn set_sender(&mut self, _sender: tokio::sync::mpsc::UnboundedSender<crate::server::ServerEvent>) {}
    }

    impl GfxServerFactory for LegacyFactory {
        fn build_gfx_handler(&self, _: GfxContext) -> Box<dyn GraphicsPipelineHandler> {
            Box::new(Handler)
        }
    }

    #[test]
    fn legacy_factory_path_retains_clampable_handle() {
        let (_bridge, handle) = build_server_and_handle(
            &LegacyFactory,
            GfxContext {
                autodetect: AutoDetectHandles::default(),
            },
        );
        let mut server = handle.lock().expect("GfxServerHandle mutex poisoned");
        server.set_max_frames_in_flight(3);
        assert_eq!(
            server.set_client_frame_ack_limit(Some(core::num::NonZeroU32::new(1).expect("one is non-zero"))),
            1
        );
    }

    #[test]
    fn egfx_is_found_whichever_way_it_was_registered() {
        let server = Arc::new(Mutex::new(GraphicsPipelineServer::new(Box::new(Handler))));
        let bridged = DrdynvcServer::new().with_dynamic_channel(GfxDvcBridge::new(server));
        assert!(egfx_channel_id(&bridged).is_some());

        let direct = DrdynvcServer::new().with_dynamic_channel(GraphicsPipelineServer::new(Box::new(Handler)));
        assert!(egfx_channel_id(&direct).is_some());

        assert!(egfx_channel_id(&DrdynvcServer::new()).is_none());
    }

    // (macrdp 14) A handler that sets the decline flag while it sees the advertise.
    struct DecliningHandler(Arc<AtomicBool>);

    impl GraphicsPipelineHandler for DecliningHandler {
        fn capabilities_advertise(&mut self, _pdu: &CapabilitiesAdvertisePdu) {
            self.0.store(true, Ordering::Relaxed);
        }

        fn on_ready(&mut self, _negotiated: &CapabilitySet) {}
    }

    fn caps_advertise() -> Vec<u8> {
        use ironrdp_egfx::pdu::{CapabilitiesV8Flags, GfxPdu};

        let pdu = GfxPdu::CapabilitiesAdvertise(CapabilitiesAdvertisePdu::from_typed(&[CapabilitySet::V8 {
            flags: CapabilitiesV8Flags::empty(),
        }]));
        ironrdp_core::encode_vec(&pdu).expect("encode CapabilitiesAdvertise")
    }

    #[test]
    fn a_declined_pipeline_sends_no_capabilities_confirm() {
        let flag = Arc::new(AtomicBool::new(false));
        let server = Arc::new(Mutex::new(GraphicsPipelineServer::new(Box::new(DecliningHandler(
            Arc::clone(&flag),
        )))));
        let mut bridge = GfxDvcBridge::with_decline_flag(server, Arc::clone(&flag));
        let out = bridge.process(1, &caps_advertise()).expect("process");
        assert!(flag.load(Ordering::Relaxed));
        assert!(out.is_empty(), "the CapabilitiesConfirm must be discarded");
    }

    #[test]
    fn without_a_decline_the_capabilities_are_confirmed() {
        let flag = Arc::new(AtomicBool::new(false));
        let server = Arc::new(Mutex::new(GraphicsPipelineServer::new(Box::new(Handler))));
        let mut bridge = GfxDvcBridge::with_decline_flag(Arc::clone(&server), Arc::clone(&flag));
        assert!(!bridge.process(1, &caps_advertise()).expect("process").is_empty());

        let mut plain = GfxDvcBridge::new(Arc::new(Mutex::new(GraphicsPipelineServer::new(Box::new(Handler)))));
        assert!(!plain.process(1, &caps_advertise()).expect("process").is_empty());
    }
}
