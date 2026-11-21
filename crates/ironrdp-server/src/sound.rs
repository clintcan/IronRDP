pub use ironrdp_rdpsnd::server::{RdpsndServerHandler, RdpsndServerMessage};

use tokio::sync::mpsc;

use crate::ServerEventSender;

/// (macrdp 2/8) One captured audio chunk on its way to the client: the encoded
/// audio bytes in the negotiated format (16-bit stereo interleaved PCM at
/// 44.1 kHz, or one AAC-LC access unit), the source-side timestamp in ms, and
/// the chunk's playback duration in ms.
///
/// It travels on a dedicated bounded channel, apart from the `ServerEvent`
/// stream, so the server ships each wave as it arrives and its lag model can
/// drop stale waves.
///
/// The third element is the chunk's duration in ms, or `None` to have the
/// server derive it from the byte length of uncompressed PCM. A compressed
/// codec (AAC) MUST set it: its byte length bears no fixed relation to
/// playback time, so the PCM-based lag model would read a near-empty buffer.
pub type AudioWave = (Vec<u8>, u32, Option<f64>);

pub trait SoundServerFactory: ServerEventSender {
    fn build_backend(&self) -> Box<dyn RdpsndServerHandler>;

    /// (macrdp 2) Hands the audio backend a dedicated bounded sender for wave
    /// PDUs. The default ignores it: such a backend keeps sending waves as
    /// `ServerEvent::Rdpsnd(RdpsndServerMessage::Wave(..))`, without the lag
    /// model.
    ///
    /// The channel holds ~1 s of audio at 42.78 waves/s, so the capture loop
    /// backpressures (drops at the capture side) instead of an unbounded queue
    /// growing while the writer stalls.
    fn set_audio_sender(&mut self, _audio_sender: mpsc::Sender<AudioWave>) {}
}
