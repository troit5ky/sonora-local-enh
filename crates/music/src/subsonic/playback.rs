//! Playback for a Subsonic server. Two threads share the work the way the Spotify path does: a
//! tokio thread fetches, and one audio thread decodes and feeds `crate::sink::Paced`. Neither
//! the audio callback nor the runtime ever waits on the network, so a track starts as soon as
//! its first seconds are in.

use std::sync::mpsc::{Receiver, RecvTimeoutError, TryRecvError, channel};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use rodio::Source as _;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::audio::{Chain, Volume};
use crate::sink::{Cue, Paced, packet};
use crate::spectrum::Spectrum;
use crate::subsonic::client::SubsonicClient;
use crate::subsonic::stream::Stream;
use crate::{PlaybackConfig, PlaybackEvent, PlaybackEvents, PlaybackFactory, Player};

/// How many frames the decoder hands over at a time. Small enough that a skip is heard at once,
/// large enough that the queue is not rebuilt for every few samples.
const CHUNK: usize = 4096;

enum Command {
    Load {
        id: String,
        at: Option<Duration>,
        play: bool,
        seamless: bool,
    },
    Preload {
        id: String,
        segue: bool,
    },
    Play,
    Pause,
    Seek(Duration),
    Gain(f32),
}

/// What the audio thread is told to do. Everything that touches the network has happened by the
/// time one of these is sent.
enum Job {
    /// Decode this track from `at`, dropping whatever was playing.
    Play {
        id: String,
        stream: Stream,
        at: Duration,
        playing: bool,
    },
    /// Line this track up behind the current one, for a gapless join.
    Queue {
        id: String,
        stream: Stream,
        duration: Option<Duration>,
    },
    Resume,
    Pause,
    /// Move within the track being decoded. The bytes are already buffered, so this is local.
    Seek(Duration),
    Gain(f32),
}

pub struct Factory {
    client: SubsonicClient,
}

impl Factory {
    pub fn new(client: SubsonicClient) -> Self {
        Self { client }
    }
}

impl PlaybackFactory for Factory {
    fn start(&self, config: PlaybackConfig) -> (Box<dyn Player>, Box<dyn PlaybackEvents>) {
        let (commands, command_rx) = unbounded_channel();
        let (events, event_rx) = unbounded_channel();
        let client = self.client.clone();
        let spectrum = Spectrum::new();
        let engine_spectrum = spectrum.clone();
        let spawned = std::thread::Builder::new()
            .name("subsonic-playback".to_owned())
            .spawn(move || run(client, config, command_rx, events, engine_spectrum));
        if let Err(error) = spawned {
            log::error!("playback: cannot spawn subsonic engine thread: {error}");
        }
        (
            Box::new(Engine { commands, spectrum }),
            Box::new(Events(event_rx)),
        )
    }
}

struct Engine {
    commands: UnboundedSender<Command>,
    spectrum: Spectrum,
}

impl Player for Engine {
    fn load(&self, track_id: &str, at: Duration, seamless: bool) -> Result<()> {
        self.commands
            .send(Command::Load {
                id: track_id.to_owned(),
                at: (!at.is_zero()).then_some(at),
                play: true,
                seamless,
            })
            .context("cannot reach subsonic playback engine")
    }

    fn load_paused_at(&self, track_id: &str, at: Duration) -> Result<()> {
        self.commands
            .send(Command::Load {
                id: track_id.to_owned(),
                at: Some(at),
                play: false,
                seamless: false,
            })
            .context("cannot reach subsonic playback engine")
    }

    fn preload(&self, track_id: &str, segue: bool) -> Result<()> {
        self.commands
            .send(Command::Preload {
                id: track_id.to_owned(),
                segue,
            })
            .context("cannot reach subsonic playback engine")
    }

    fn play(&self) {
        self.commands.send(Command::Play).ok();
    }

    fn pause(&self) {
        self.commands.send(Command::Pause).ok();
    }

    fn seek(&self, position: Duration) {
        self.commands.send(Command::Seek(position)).ok();
    }

    fn set_gain(&self, gain: f32) {
        self.commands.send(Command::Gain(gain)).ok();
    }

    fn spectrum(&self) -> Option<Spectrum> {
        Some(self.spectrum.clone())
    }
}

struct Events(UnboundedReceiver<PlaybackEvent>);

#[async_trait]
impl PlaybackEvents for Events {
    async fn next(&mut self) -> Option<PlaybackEvent> {
        self.0.recv().await
    }
}

/// A track fetched and ready to decode.
#[derive(Clone)]
struct Loaded {
    stream: Stream,
    duration: Option<Duration>,
}

/// What the fetch of one track came back with, and whether anything still wants it.
struct Fetched {
    epoch: u64,
    id: String,
    segue: bool,
    result: Result<Loaded>,
}

fn run(
    client: SubsonicClient,
    config: PlaybackConfig,
    commands: UnboundedReceiver<Command>,
    events: UnboundedSender<PlaybackEvent>,
    spectrum: Spectrum,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            log::error!("playback: cannot build subsonic engine runtime: {error}");
            return;
        }
    };
    runtime.block_on(engine_loop(client, config, commands, events, spectrum));
}

async fn engine_loop(
    client: SubsonicClient,
    config: PlaybackConfig,
    mut commands: UnboundedReceiver<Command>,
    events: UnboundedSender<PlaybackEvent>,
    spectrum: Spectrum,
) {
    let (cue, mut written) = Cue::new();
    let (changed, mut gone) = unbounded_channel();
    let chain = Chain {
        volume: Volume::new(config.gain),
        equalizer: config.equalizer.clone(),
        spectrum,
    };
    let (jobs, job_rx) = channel::<Job>();

    let audio_cue = cue.clone();
    let audio_events = events.clone();
    let interval = config.position_interval;
    let spawned = std::thread::Builder::new()
        .name("subsonic-audio".to_owned())
        .spawn(move || audio_loop(job_rx, audio_cue, chain, changed, audio_events, interval));
    if let Err(error) = spawned {
        log::error!("playback: cannot spawn the subsonic audio thread: {error}");
        return;
    }

    // what the next admitted write means, held until the audio actually reaches the output
    let mut announcing: Option<PlaybackEvent> = None;
    let mut epoch = 0u64;
    let mut awaited: Option<u64> = None;
    let mut inflight: Option<tokio::task::AbortHandle> = None;
    let mut ahead: Option<(String, Loaded)> = None;
    let mut current: Option<String> = None;
    // handed to the audio thread for a gapless join, so a later seamless load knows it started
    let mut segued: Option<String> = None;
    // whether the listener wants sound. A play or pause during a fetch has to survive it, or
    // pressing play while a track loads would be forgotten by the time it arrives.
    let mut wanted = false;
    let (fetched, mut arrivals) = unbounded_channel::<Fetched>();

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    Command::Load { id, at, play, seamless } => {
                        let joined = segued.as_deref() == Some(id.as_str());
                        if seamless
                            && at.is_none()
                            && (current.as_deref() == Some(id.as_str()) || joined)
                        {
                            // already decoding, either still or through a gapless join
                            if joined {
                                current = segued.take();
                            }
                            jobs.send(Job::Resume).ok();
                            continue;
                        }
                        segued = None;
                        epoch += 1;
                        if let Some(handle) = inflight.take() {
                            handle.abort();
                        }
                        cue.clear();
                        current = Some(id.clone());
                        let position = at.unwrap_or_default();
                        wanted = play;
                        events.send(PlaybackEvent::Loading {
                            id: Some(id.clone()),
                            at: position,
                        }).ok();
                        announcing = Some(match play {
                            true => PlaybackEvent::Playing { id: Some(id.clone()), at: position },
                            false => PlaybackEvent::Paused { id: Some(id.clone()), at: position },
                        });

                        let held = ahead
                            .take_if(|(cached, _)| *cached == id)
                            .map(|(_, loaded)| loaded);
                        match held {
                            Some(loaded) => {
                                announce_length(&events, &id, &loaded);
                                jobs.send(Job::Play {
                                    id,
                                    stream: loaded.stream,
                                    at: position,
                                    playing: wanted,
                                }).ok();
                            }
                            None => {
                                ahead = None;
                                awaited = Some(epoch);
                                inflight = Some(
                                    spawn(&client, id, epoch, false, &fetched),
                                );
                            }
                        }
                    }
                    Command::Preload { id, segue } => {
                        let known = current.as_deref() == Some(id.as_str())
                            || ahead.as_ref().is_some_and(|(cached, _)| *cached == id);
                        if known || current.is_none() {
                            continue;
                        }
                        spawn(&client, id, epoch, segue, &fetched);
                    }
                    Command::Play => {
                        wanted = true;
                        announcing = announcing.map(playing_now);
                        jobs.send(Job::Resume).ok();
                    }
                    Command::Pause => {
                        wanted = false;
                        announcing = announcing.map(paused_now);
                        jobs.send(Job::Pause).ok();
                    }
                    Command::Seek(position) => {
                        if let Some(id) = current.clone() {
                            cue.clear();
                            announcing = Some(PlaybackEvent::Seeked {
                                id: Some(id),
                                at: position,
                            });
                            jobs.send(Job::Seek(position)).ok();
                        }
                    }
                    Command::Gain(level) => {
                        jobs.send(Job::Gain(level)).ok();
                    }
                }
            }
            arrival = arrivals.recv() => {
                let Some(Fetched { epoch: at, id, segue, result }) = arrival else { break };
                if at != epoch {
                    continue;
                }
                let loaded = match result {
                    Ok(loaded) => loaded,
                    Err(error) => {
                        log::warn!("playback: cannot load subsonic track {id}: {error:#}");
                        if awaited == Some(at) {
                            awaited = None;
                            inflight = None;
                            announcing = None;
                            events.send(PlaybackEvent::Unavailable { id: Some(id) }).ok();
                        }
                        continue;
                    }
                };
                if awaited == Some(at) && current.as_deref() == Some(id.as_str()) {
                    awaited = None;
                    inflight = None;
                    let position = announcing
                        .as_ref()
                        .and_then(position_of)
                        .unwrap_or_default();
                    announce_length(&events, &id, &loaded);
                    jobs.send(Job::Play {
                        id,
                        stream: loaded.stream,
                        at: position,
                        playing: wanted,
                    }).ok();
                    continue;
                }
                if segue {
                    segued = Some(id.clone());
                    jobs.send(Job::Queue {
                        id: id.clone(),
                        stream: loaded.stream.clone(),
                        duration: loaded.duration,
                    }).ok();
                }
                ahead = Some((id, loaded));
            }
            heard = written.recv() => {
                if heard.is_none() {
                    break;
                }
                if let Some(event) = announcing.take() {
                    events.send(event).ok();
                }
            }
            lost = gone.recv() => {
                if lost.is_some() {
                    events.send(PlaybackEvent::OutputChanged).ok();
                }
                return;
            }
        }
    }
}

/// The same announcement, but as a start. A play pressed while the track is still loading has
/// to change what its first audio will be reported as.
fn playing_now(event: PlaybackEvent) -> PlaybackEvent {
    match event {
        PlaybackEvent::Paused { id, at } => PlaybackEvent::Playing { id, at },
        held => held,
    }
}

fn paused_now(event: PlaybackEvent) -> PlaybackEvent {
    match event {
        PlaybackEvent::Playing { id, at } => PlaybackEvent::Paused { id, at },
        held => held,
    }
}

fn position_of(event: &PlaybackEvent) -> Option<Duration> {
    match event {
        PlaybackEvent::Playing { at, .. }
        | PlaybackEvent::Paused { at, .. }
        | PlaybackEvent::Seeked { at, .. } => Some(*at),
        _ => None,
    }
}

fn announce_length(events: &UnboundedSender<PlaybackEvent>, id: &str, loaded: &Loaded) {
    if let Some(duration) = loaded.duration {
        events
            .send(PlaybackEvent::Length {
                id: Some(id.to_owned()),
                duration,
            })
            .ok();
    }
}

fn spawn(
    client: &SubsonicClient,
    id: String,
    epoch: u64,
    segue: bool,
    fetched: &UnboundedSender<Fetched>,
) -> tokio::task::AbortHandle {
    let client = client.clone();
    let fetched = fetched.clone();
    tokio::spawn(async move {
        let result = fetch(&client, &id).await;
        fetched
            .send(Fetched {
                epoch,
                id,
                segue,
                result,
            })
            .ok();
    })
    .abort_handle()
}

/// Opens the stream and asks for the length at the same time, so neither round trip waits on
/// the other; the preroll usually covers the length lookup entirely.
async fn fetch(client: &SubsonicClient, id: &str) -> Result<Loaded> {
    let (stream, duration) = tokio::join!(
        async { Stream::open(client.open_stream(id).await?).await },
        client.duration(id),
    );
    Ok(Loaded {
        stream: stream?,
        duration,
    })
}

/// The track the audio thread is decoding, and where its samples land in real time.
struct Playing {
    id: String,
    decoder: rodio::Decoder<crate::subsonic::stream::Reader>,
    channels: u16,
    rate: u32,
    base: Duration,
    /// Samples queued before this track's first one. On a gapless join the one before it is
    /// still being heard, so its own position only starts once the count passes this.
    offset: u64,
}

impl Playing {
    fn mark(&self) -> Mark {
        Mark {
            id: self.id.clone(),
            channels: self.channels,
            rate: self.rate,
            base: self.base,
            offset: self.offset,
        }
    }
}

/// The track being heard, which is not always the one being decoded: at the end of a track the
/// decoder has moved on, or stopped, while the queue still holds the last seconds of it.
struct Mark {
    id: String,
    channels: u16,
    rate: u32,
    base: Duration,
    offset: u64,
}

impl Mark {
    /// Where the sound is: what the output has drawn from this track, from where it started.
    fn at(&self, cue: &Cue) -> Duration {
        let mine = cue.played().saturating_sub(self.offset);
        let frames = mine / u64::from(self.channels).max(1);
        self.base + Duration::from_secs_f64(frames as f64 / f64::from(self.rate).max(1.0))
    }
}

/// A track whose last sample is queued but not yet heard. The events for the join wait for it,
/// so one track's end and the next one's start land together, on the sample.
struct Join {
    ended: String,
    next: Option<(String, Option<Duration>)>,
    at: u64,
}

/// Decodes and writes until told otherwise. Every wait here is on the queue draining or on a
/// command, never on the network: the bytes are already arriving in the background.
fn audio_loop(
    jobs: Receiver<Job>,
    cue: Cue,
    chain: Chain,
    changed: UnboundedSender<()>,
    events: UnboundedSender<PlaybackEvent>,
    interval: Duration,
) {
    let mut paced = match Paced::open(cue.clone(), chain, changed) {
        Ok(paced) => paced,
        Err(error) => return log::error!("playback: cannot open audio output: {error:#}"),
    };

    let mut current: Option<Playing> = None;
    let mut written = 0u64;
    let mut joining: Option<Join> = None;
    let mut heard: Option<Mark> = None;
    let mut queued: Option<(String, Stream, Option<Duration>)> = None;
    let mut playing = false;
    let mut reported_at = Instant::now();

    loop {
        // a full queue or nothing to decode means waiting for work rather than spinning
        let job = match current.is_some() && !paced.full() {
            true => match jobs.try_recv() {
                Ok(job) => Some(job),
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => return,
            },
            false => match jobs.recv_timeout(Paced::poll()) {
                Ok(job) => Some(job),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => return,
            },
        };

        if let Some(join) = &joining
            && cue.played() >= join.at
        {
            let Join { ended, next, .. } = joining.take().unwrap_or_else(|| unreachable!());
            events.send(PlaybackEvent::Ended { id: Some(ended) }).ok();
            heard = current.as_ref().map(Playing::mark);
            match next {
                Some((id, duration)) => {
                    if let Some(duration) = duration {
                        events
                            .send(PlaybackEvent::Length {
                                id: Some(id.clone()),
                                duration,
                            })
                            .ok();
                    }
                    events
                        .send(PlaybackEvent::Playing {
                            id: Some(id),
                            at: Duration::ZERO,
                        })
                        .ok();
                }
                None => playing = false,
            }
        }

        // reported from the sound, so it keeps moving while the last seconds play out
        if playing
            && let Some(mark) = &heard
            && reported_at.elapsed() >= interval
            && !cue.cleared()
        {
            reported_at = Instant::now();
            events
                .send(PlaybackEvent::Position {
                    id: Some(mark.id.clone()),
                    at: mark.at(&cue),
                })
                .ok();
        }

        if let Some(job) = job {
            match job {
                Job::Play {
                    id,
                    stream,
                    at,
                    playing: start,
                } => {
                    queued = None;
                    joining = None;
                    written = 0;
                    current = begin(&id, &stream, at, 0);
                    heard = current.as_ref().map(Playing::mark);
                    playing = start && current.is_some();
                    match playing {
                        true if paced.play().is_err() => return,
                        true => {}
                        false => paced.pause(),
                    }
                    match current.is_some() {
                        // the next write is the new track, so it is the one worth reporting
                        true => cue.arm(),
                        false => {
                            events
                                .send(PlaybackEvent::Unavailable { id: Some(id) })
                                .ok();
                        }
                    }
                }
                Job::Queue {
                    id,
                    stream,
                    duration,
                } => queued = Some((id, stream, duration)),
                Job::Resume => {
                    playing = current.is_some();
                    if playing && paced.play().is_err() {
                        return;
                    }
                    if let Some(mark) = &heard {
                        events
                            .send(PlaybackEvent::Playing {
                                id: Some(mark.id.clone()),
                                at: mark.at(&cue),
                            })
                            .ok();
                    }
                }
                Job::Pause => {
                    playing = false;
                    paced.pause();
                    if let Some(mark) = &heard {
                        events
                            .send(PlaybackEvent::Paused {
                                id: Some(mark.id.clone()),
                                at: mark.at(&cue),
                            })
                            .ok();
                    }
                }
                Job::Seek(position) => {
                    if let Some(held) = &mut current {
                        if let Err(error) = held.decoder.try_seek(position) {
                            log::warn!("playback: cannot seek the subsonic track: {error}");
                        }
                        held.base = position;
                        held.offset = 0;
                        written = 0;
                        heard = Some(held.mark());
                        cue.arm();
                    }
                }
                Job::Gain(level) => paced.set_volume(level),
            }
            continue;
        }

        let Some(held) = &mut current else { continue };
        if !playing || paced.full() {
            continue;
        }

        let pulled = take(held, CHUNK);
        let Some(samples) = pulled else {
            // the decoder is done, but its last samples are still queued
            let ended = current.take().map(|held| held.id).unwrap_or_default();
            let next = queued.take().and_then(|(id, stream, duration)| {
                current = begin(&id, &stream, Duration::ZERO, written);
                current.as_ref().map(|_| (id, duration))
            });
            // both events wait for the queue to reach here, so the join lands on the sample
            joining = Some(Join {
                ended,
                next,
                at: written,
            });
            continue;
        };

        let Some(chunk) = packet(&samples, held.channels, held.rate) else {
            continue;
        };
        if paced.write(chunk).is_err() {
            return;
        }
        written += samples.len() as u64;
    }
}

/// Builds a decoder over a stream and places it at `at`. The bytes past the preroll are still
/// arriving, so this only reads the header.
fn begin(id: &str, stream: &Stream, at: Duration, offset: u64) -> Option<Playing> {
    let mut builder = rodio::Decoder::builder()
        .with_data(stream.reader())
        .with_seekable(true);
    if let Some(total) = stream.total() {
        builder = builder.with_byte_len(total);
    }
    let mut decoder = match builder.build() {
        Ok(decoder) => decoder,
        Err(error) => {
            log::warn!("playback: cannot decode the subsonic track {id}: {error}");
            return None;
        }
    };
    let channels = decoder.channels().get();
    let rate = decoder.sample_rate().get();
    if !at.is_zero()
        && let Err(error) = decoder.try_seek(at)
    {
        log::warn!(
            "playback: cannot start the subsonic track {id} at {}s: {error}",
            at.as_secs()
        );
    }

    Some(Playing {
        id: id.to_owned(),
        decoder,
        channels,
        rate,
        base: at,
        offset,
    })
}

/// Pulls up to `frames` frames out of the decoder. `None` means the track ended.
fn take(held: &mut Playing, frames: usize) -> Option<Vec<f32>> {
    let wanted = frames * usize::from(held.channels).max(1);
    let mut samples = Vec::with_capacity(wanted);
    for sample in held.decoder.by_ref().take(wanted) {
        samples.push(sample);
    }
    (!samples.is_empty()).then_some(samples)
}
