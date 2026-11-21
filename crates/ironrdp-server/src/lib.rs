#![cfg_attr(doc, doc = include_str!("../README.md"))]
#![doc(html_logo_url = "https://cdnweb.devolutions.net/images/projects/devolutions/logos/devolutions-icon-shadow.svg")]
#![allow(clippy::arithmetic_side_effects)] // TODO: should we enable this lint back?

mod macros;

// (macrdp) macrdp's own channel modules, kept alongside upstream's
// equivalents (which stay inert: macrdp never sets their factories).
#[cfg(feature = "multitransport")]
mod mt;
mod rdcamera; // (macrdp 19)
mod rdpdr_drive;
mod rdpeusb;

pub mod autodetect;
mod builder;
mod capabilities;
mod clipboard;
mod display;
mod echo;
mod encoder;
mod error;
#[cfg(feature = "egfx")]
mod gfx;
mod handler;
pub mod heartbeat;
#[cfg(feature = "helper")]
mod helper;
mod multitransport;
mod rdpdr;
mod rdpeai;
mod rdpei;
mod server;
mod sound;
#[cfg(feature = "usb")]
mod urbdrc;

pub use clipboard::CliprdrServerFactory;
pub use display::{
    BitmapUpdate, ColorPointer, DesktopSize, DisplayContext, DisplayUpdate, Framebuffer, LargePointer, PixelFormat,
    RGBAPointer, RdpServerDisplay, RdpServerDisplayUpdates,
};
pub use echo::{EchoDvcBridge, EchoRoundTripMeasurement, EchoServerHandle, EchoServerMessage};
#[cfg(all(feature = "__test", feature = "qoiz"))]
pub use encoder::qoiz::Compressor as QoizCompressor;
#[cfg(feature = "__test")]
pub use encoder::{MAX_FASTPATH_UPDATE_SIZE, RemoteFxCodec, UpdateEncoder, UpdateEncoderCodecs};
pub use error::{ServerError, ServerErrorExt, ServerErrorKind, ServerResult, ServerResultExt};
#[cfg(feature = "egfx")]
pub use gfx::{EgfxServerMessage, GfxContext, GfxDvcBridge, GfxServerFactory, GfxServerHandle};
pub use handler::{KeyboardEvent, MouseButton, MouseEvent, RdpServerInputHandler};
#[cfg(feature = "helper")]
pub use helper::TlsIdentityCtx;
pub use ironrdp_acceptor::{Acceptor, CredentialOrigin, CredsspAuthIdentity, CredsspBackend, ReceivedCredentials};
pub use ironrdp_pdu::rdp::server_error_info::ErrorInfo;
pub use ironrdp_pdu::rdp::session_info::ServerAutoReconnect;
#[cfg(feature = "usb")]
pub use ironrdp_rdpeusb::io::{CompletionData, DeviceText, InternalIoControlPacket};
#[cfg(feature = "multitransport")]
pub use mt::dtls::DtlsServerContext;
#[cfg(feature = "multitransport")]
pub use mt::listener::{ListenerConfig, UdpMultitransportListener};
#[cfg(feature = "multitransport")]
pub use mt::{CookieRegistry, MultitransportProvider, TunnelSender, encode_initiate_request, tunnel_channel};
pub use rdcamera::{
    CameraSampleSink, RDCAMERA_CHANNEL_NAME, RdCameraDeviceProcessor, RdCameraServer, RdCameraServerFactory,
};
pub use rdpdr::{NoopRdpdrServerBackend, RdpdrServerBackend, RdpdrServerFactory, RdpdrServerMessage};
pub use rdpdr_drive::{
    AnnouncedDevice, DirEntry, RdpdrBackendFactory, RdpdrDriveMessage, RdpdrDriveServer, RdpdrDriveServerFactory,
    RdpdrHandle, RdpdrServerHandler, RdpdrStatus, SCARD_EJECT_CARD, SCARD_LEAVE_CARD, SCARD_RESET_CARD,
    SCARD_SHARE_DIRECT, SCARD_SHARE_EXCLUSIVE, SCARD_SHARE_SHARED, SCARD_UNPOWER_CARD,
};
pub use rdpeai::{NoopRdpeaiServerBackend, RdpeaiServerBackend, RdpeaiServerFactory, RdpeaiServerMessage};
pub use rdpei::{
    CsReadyFlags, CsReadyPdu, DismissHoveringTouchContactPdu, PenContact, PenContactDataFlags, PenContactFields,
    PenContactFlags, PenEventPdu, PenFlags, PenFrame, RdpInputProtocolVersion, RdpeiHandler, RdpeiServer,
    RdpeiServerFactory, ScReadyFeatures, TouchContact, TouchContactDataFlags, TouchContactFields, TouchContactFlags,
    TouchEventPdu, TouchFrame,
};
pub use rdpeusb::{
    DeviceDescriptor, URBDRC_CHANNEL_NAME, UrbdrcServer, UrbdrcServerFactory, UrbdrcServerMessage, UsbDeviceCallback,
    UsbHandle, UsbPipe,
};
#[cfg(feature = "__test")]
pub use server::audio_redirection_wanted;
#[cfg(feature = "__test")]
pub use server::autodetect_response_from_sub_header;
#[cfg(all(feature = "__test", feature = "egfx"))]
pub use server::autodetect_sub_header;
#[cfg(feature = "__test")]
pub use server::captured_credssp_identity;
pub use server::{
    AuthenticatedConnection, AutoReconnectCookieHandle, BoundConnection, ConnectionBinder, ConnectionHandler,
    ConnectionInfo, ConnectionPolicy, ConnectionSource, CredentialDecision, CredentialValidationError,
    CredentialValidator, Credentials, DynamicChannelFactory, ErrorInfoDisconnectHandle, ExactMatchCredentialValidator,
    IncomingConnection, NextConnection, PendingSession, PostConnectionAction, RdpServer, RdpServerHandle,
    RdpServerOptions, RdpServerSecurity, ServerEvent, ServerEventSender, ServerRedirectionHandle, SessionOutcome,
    StaticChannelFactory, TransportTls, WindowingOrders, WindowingOrdersStatus, pick_remotefx_entropy_coder,
};
#[cfg(feature = "__test")]
pub use server::{
    FINAL_FLUSH_TIMEOUT, MAX_QUEUED_REPLY_BYTES, OutboundQueue, QueuedWriter, client_max_request_size,
    dispatch_static_channel_events, drive_static_channel_events, pick_remotefx_codec,
};
pub use sound::{AudioWave, RdpsndServerHandler, RdpsndServerMessage, SoundServerFactory};
#[cfg(feature = "usb")]
pub use urbdrc::{
    CompletionFut, DeviceFactory, PendingHandle, PendingRequest, RdpUsbDeviceAnnounceInfo, UsbDeviceAnnounce,
    UsbDeviceHandle, UsbRedirDevice, UsbRequestCompletion,
};
#[cfg(feature = "__bench")]
pub mod bench {
    pub mod encoder {
        pub mod rfx {
            pub use crate::encoder::rfx::bench::{rfx_enc, rfx_enc_tile};
        }

        pub use crate::encoder::{RemoteFxCodec, UpdateEncoder, UpdateEncoderCodecs};
    }
}
