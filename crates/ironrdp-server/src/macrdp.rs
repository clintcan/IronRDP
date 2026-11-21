//! (macrdp) macrdp's additions to [`RdpServer`], kept out of `server.rs`.
//!
//! `server.rs` holds only the hooks, each marked `(macrdp N)`.

use std::sync::Arc;
use std::time::Instant;

use ironrdp_svc::server_encode_svc_messages;
use rdpsnd::server::RdpsndServer;
use tokio::sync::{Mutex, MutexGuard, OwnedSemaphorePermit, Semaphore, mpsc};
use tracing::{debug, warn};

use super::{ConnectionState, QueuedWriter, RdpServer, WriteCompletion, rdpsnd};
use crate::{AudioWave, ServerError, ServerErrorExt as _, ServerResult};

/// (macrdp) Per-server state for macrdp's divergences, in one `RdpServer` field.
#[derive(Default)]
pub(super) struct MacrdpServerState {
    /// (2/8) Dedicated audio channel, read by the client loop's audio arm.
    audio_receiver: Option<Arc<Mutex<mpsc::Receiver<AudioWave>>>>,
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
    /// (macrdp) Installs macrdp's dedicated audio channel.
    /// Called once by the builder, right after [`Self::new`].
    pub(crate) fn install_macrdp_channels(&mut self) {
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

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use super::*;

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
            fn set_sender(&mut self, _sender: mpsc::UnboundedSender<crate::ServerEvent>) {}
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
}
