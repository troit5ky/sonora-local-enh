//! librespot's `Sink` over the shared paced output. librespot decodes on its own thread, so
//! every write here is finished audio; the pacing and the device watch live in `crate::sink`.

use librespot_playback::audio_backend::{Sink, SinkError, SinkResult};
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use librespot_playback::{NUM_CHANNELS, SAMPLE_RATE};
use tokio::sync::mpsc::UnboundedSender;

use crate::audio::Chain;
use crate::sink::{Cue, Paced, packet};

pub struct OutputSink {
    paced: Paced,
}

impl OutputSink {
    /// The sink librespot's player builder asks for. Without an output device it gets a silent
    /// one, so playback state still moves.
    pub fn boxed(cue: Cue, chain: Chain, changed: UnboundedSender<()>) -> Box<dyn Sink> {
        match Paced::open(cue.clone(), chain, changed) {
            Ok(paced) => Box::new(Self { paced }),
            Err(error) => {
                log::error!("sink: cannot open an output device: {error:#}");
                Box::new(Silence(cue))
            }
        }
    }
}

impl Sink for OutputSink {
    fn start(&mut self) -> SinkResult<()> {
        self.paced
            .play()
            .map_err(|error| SinkError::OnWrite(error.to_string()))
    }

    fn stop(&mut self) -> SinkResult<()> {
        self.paced.pause();
        Ok(())
    }

    fn write(&mut self, packeted: AudioPacket, converter: &mut Converter) -> SinkResult<()> {
        let samples = packeted
            .samples()
            .map_err(|error| SinkError::OnWrite(error.to_string()))?;
        let samples = converter.f64_to_f32(samples);
        let Some(samples) = packet(&samples, NUM_CHANNELS as u16, SAMPLE_RATE) else {
            return Ok(());
        };
        self.paced
            .write(samples)
            .and_then(|()| self.paced.drain())
            .map_err(|error| SinkError::OnWrite(error.to_string()))
    }
}

/// Swallows the audio when no output device opens. It still answers the cue, so playback state
/// moves on rather than waiting for sound that cannot come.
struct Silence(Cue);

impl Sink for Silence {
    fn write(&mut self, _packet: AudioPacket, _converter: &mut Converter) -> SinkResult<()> {
        self.0.admit();
        Ok(())
    }
}
