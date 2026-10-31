//! The core's live side: its link to the holder, one parsed screen per live
//! terminal, adoption after a restart, and what happens when a program ends.

use crate::api::CoreInfo;
use crate::holder_proto::{self as proto, Exit, Goodbye};
use crate::paths::Paths;
use crate::paths::{read_state, write_atomic};
use crate::platform::{self, Stream as UnixStream, signal};
use crate::relay::Relay;
use crate::terminals::Registry;
use crate::viewers::Viewer;
use ghostty_vt::Terminal as Screen;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Notify, oneshot};

pub struct Core {
    pub info: CoreInfo,
    pub started: Instant,
    pub stop: Notify,
    /// Set by `server.restart`: on stop, hand over instead of ending programs.
    pub restart: std::sync::atomic::AtomicBool,
    pub paths: Paths,
    pub state: Mutex<State>,
    /// Woken on every lifecycle change; waiters re-check what they need.
    pub changed: Notify,
    /// Work for the runtime that the holder thread can't do itself.
    pub jobs: tokio::sync::mpsc::UnboundedSender<crate::notes::Job>,
    pub jobs_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<crate::notes::Job>>>,
    /// Sweeps of lost terminals in flight: stop and restart wait for them.
    pub(crate) sweeps: Arc<std::sync::atomic::AtomicUsize>,
    /// Holder startup and goodbye cannot cross, or a stop could leave a new holder behind.
    holder_start: Mutex<()>,
    /// The registry changed in a way a crash may lose: `save_soon` writes it shortly.
    dirty: std::sync::atomic::AtomicBool,
    save_due: Notify,
}

#[derive(Default)]
pub struct State {
    pub reg: Registry,
    pub docs: std::collections::BTreeMap<String, crate::api::DocGetResult>,
    pub screens: HashMap<u32, Screen>,
    pub relays: HashMap<u32, Relay>,
    pub viewers: HashMap<u32, Vec<Viewer>>,
    pub events: crate::events::Events,
    pub gates: HashMap<u32, crate::input::Gate>,
    pub queues: HashMap<u32, Arc<tokio::sync::Mutex<()>>>,
    pub foreground: HashMap<u32, Vec<oneshot::Sender<i32>>>,
    /// Waits matching output: they get every byte viewers get.
    pub output_watch: HashMap<u32, Vec<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>,
    /// The turn of each caller's last send to a terminal, until a bare wait reports it.
    pub last_send: HashMap<(String, u32), u64>,
    /// Waits in progress by (caller, terminal): a parent waiting needs no note.
    pub waiting: HashMap<(String, u32), usize>,
    /// A message was typed into this agent while it worked (so it sits in its
    /// queue) and no wait of its has returned since: who sent it.
    pub messages: HashMap<u32, Option<String>>,
    /// Parents with a note delivery running.
    pub note_tasks: HashSet<u32>,
    /// When each terminal last printed anything, and how many times.
    pub last_output: HashMap<u32, Instant>,
    pub output_seq: HashMap<u32, u64>,
    /// Terminals a read is scrolling right now (viewers paused).
    pub stitching: HashSet<u32>,
    /// The last stitched read per terminal, good while it prints nothing:
    /// (output count after it, what it captured).
    pub stitched: HashMap<u32, (u64, crate::stitch::Capture)>,
    /// Every holder we talk to: one of our build takes new terminals, older ones drain.
    pub holders: Vec<Link>,
    pub pending: HashMap<u32, oneshot::Sender<Result<crate::terminals::Terminal, String>>>,
    pub killing: HashSet<u32>,
    pub stopping: bool,
    pub machines: Vec<crate::machines::Machine>,
    /// The line to each machine, by its core id, and the task keeping it up.
    pub lines: HashMap<String, crate::remote::LineState>,
    pub line_tasks: HashMap<String, tokio::task::AbortHandle>,
    /// Viewers here on terminals there, by (connection, channel here).
    pub relayed: HashMap<(usize, u32), crate::remote::Relayed>,
    /// Cores that dialed in, by id (kept), and the line to each while connected.
    pub peers: std::collections::BTreeMap<String, crate::remote::Peer>,
    pub peer_lines: HashMap<String, (usize, Arc<crate::remote::Line>)>,
    /// Notes from other machines for parents here, until typed.
    pub afar_notes: HashMap<u32, Vec<String>>,
    /// What a machine must hear once its line is back (a parent here finished).
    pub machine_outbox: HashMap<String, Vec<serde_json::Value>>,
    /// Each connected machine's terminals as seen from here, kept current by
    /// its events: machine id -> address (`rohan/t7`) -> record.
    pub mirrors: HashMap<String, std::collections::BTreeMap<String, serde_json::Value>>,
}

pub struct Link {
    /// Frames for the holder, written by their own thread: the core never
    /// blocks on the socket while holding its lock (the holder writes to us
    /// under its own lock; both waiting would freeze the session).
    writer: mpsc::Sender<Vec<u8>>,
    pub pid: u32,
    pub instance: String,
    /// Same build as this core: new terminals go here.
    pub current: bool,
    bye: mpsc::Sender<()>,
    bye_rx: Option<mpsc::Receiver<()>>,
}

impl State {
    /// The holder that runs terminal `id`.
    pub fn link(&self, id: u32) -> Option<&Link> {
        let instance = self.reg.terminals.get(&id)?.holder.as_deref()?;
        self.holders.iter().find(|l| l.instance == instance)
    }

    /// The holder new terminals go to.
    pub fn current(&self) -> Option<&Link> {
        self.holders.iter().find(|l| l.current)
    }
}

impl Link {
    pub fn send(&self, frame: &[u8]) {
        let _ = self.writer.send(frame.to_vec());
    }

    pub fn input(&self, id: u32, bytes: &[u8]) {
        if !bytes.is_empty() {
            self.send(&proto::frame(proto::INPUT, id, bytes));
        }
    }
}

const SAVE_SOON: Duration = Duration::from_secs(2);

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn finish_reason(killed: bool, stopping: bool, host_shutdown: bool) -> &'static str {
    if killed {
        "killed"
    } else if stopping {
        "server_stop"
    } else if host_shutdown {
        "host_shutdown"
    } else {
        "exited"
    }
}

impl Core {
    pub fn start(paths: Paths, info: CoreInfo) -> io::Result<Arc<Core>> {
        let reg: Registry = read_state(&paths.state.join("terminals.json"))?;
        let docs_path = paths.state.join("docs.json");
        let docs = read_state(&docs_path).unwrap_or_else(|e| {
            eprintln!("core: cannot load documents, starting empty: {e}");
            if let Err(e) = std::fs::rename(&docs_path, paths.state.join("docs.json.bad")) {
                eprintln!("core: cannot quarantine documents: {e}");
            }
            Default::default()
        });
        std::fs::create_dir_all(paths.state.join("screens"))?;
        let machines = read_state(&paths.state.join("machines.json"))?;
        let peers = read_state(&paths.state.join("peers.json"))?;
        let (jobs, jobs_rx) = tokio::sync::mpsc::unbounded_channel();
        let core = Arc::new(Core {
            info,
            started: Instant::now(),
            stop: Notify::new(),
            restart: Default::default(),
            paths,
            state: Mutex::new(State { reg, docs, machines, peers, ..Default::default() }),
            changed: Notify::new(),
            jobs,
            jobs_rx: Mutex::new(Some(jobs_rx)),
            sweeps: Default::default(),
            holder_start: Default::default(),
            dirty: Default::default(),
            save_due: Notify::new(),
        });
        core.attach_all()?;
        Ok(core)
    }

    /// Settings from config.toml, read fresh (a broken file means defaults here;
    /// `run` reports it).
    pub fn settings(&self) -> crate::config::Config {
        crate::config::load(&self.paths.config).unwrap_or_default()
    }

    pub fn headless(&self) -> (u16, u16) {
        self.settings().headless().unwrap_or((200, 120))
    }

    pub fn history_bytes(&self) -> usize {
        self.settings().history_bytes()
    }

    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn snapshot_path(&self, id: u32) -> PathBuf {
        self.paths.state.join("screens").join(format!("{}.snap", crate::terminals::address(id)))
    }

    /// Queues the registry for the background writer: callers hold the lock, and an fsync on a stalled disk took
    /// seconds with every RPC waiting behind it (Automancer, 31 Oct 2026). The newest registry wins.
    pub fn save(&self, st: &State) {
        self.dirty.store(false, std::sync::atomic::Ordering::SeqCst);
        let bytes = serde_json::to_vec(&st.reg).expect("registry serializes");
        static START: std::sync::Once = std::sync::Once::new();
        START.call_once(|| {
            std::thread::Builder::new().name("hc-save".into()).spawn(writer).expect("save thread starts");
        });
        let generation = WRITES.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *WRITES.pending.lock().unwrap_or_else(|p| p.into_inner()) =
            Some((generation, self.paths.state.join("terminals.json"), bytes));
        WRITES.ready.notify_one();
    }

    /// Written before it returns, for a restart or stop that reads it next; `sync` waits for the disk too.
    pub fn save_now(&self, st: &State, sync: bool) {
        self.dirty.store(false, std::sync::atomic::Ordering::SeqCst);
        let bytes = serde_json::to_vec(&st.reg).expect("registry serializes");
        let generation = WRITES.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        write_newest(generation, &self.paths.state.join("terminals.json"), &bytes, sync);
    }

    /// For frequent changes a crash may lose (the next start rebuilds them):
    /// saved within `SAVE_SOON`, once for a burst. Call with the lock held.
    pub fn save_soon(&self) {
        if !self.dirty.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.save_due.notify_one();
        }
    }

    pub async fn save_when_due(self: Arc<Self>) {
        loop {
            self.save_due.notified().await;
            tokio::time::sleep(SAVE_SOON).await;
            let st = self.lock();
            if self.dirty.load(std::sync::atomic::Ordering::SeqCst) {
                self.save(&st);
            }
        }
    }

    /// A holder of our build for new terminals, started if there is none.
    pub async fn ensure_holder(self: &Arc<Self>) -> io::Result<()> {
        let core = self.clone();
        tokio::task::spawn_blocking(move || core.start_holder()).await.map_err(io::Error::other)?
    }

    fn start_holder(self: &Arc<Self>) -> io::Result<()> {
        let _starting = self.holder_start.lock().unwrap_or_else(|p| p.into_inner());
        if self.lock().stopping || self.restart.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io::Error::other("the core is stopping or restarting"));
        }
        if self.lock().current().is_some() {
            return Ok(());
        }
        // 8 hex: unique enough among one session's holders, and socket paths are short (macOS: 104 bytes).
        let socket =
            format!("{}{}.sock", self.holder_prefix(), &crate::platform::random_hex()[..crate::paths::HOLDER_ID]);
        crate::client::spawn_detached(&self.paths, &["hold", "--socket", &socket], "holder.log")?;
        let stream = crate::client::wait_for_socket(socket.as_ref(), "holder").map_err(io::Error::other)?;
        self.take_over(stream, true).map(drop)
    }

    /// Holder sockets are `<session>.holder.<id>.sock`, next to `<session>.holder.sock`
    /// (older holders: a 32-hex id).
    fn holder_prefix(&self) -> String {
        let base = self.paths.holder_socket.display().to_string();
        format!("{}.", base.strip_suffix(".sock").unwrap_or(&base))
    }

    /// At startup: adopt from every holder still running. Live records no holder
    /// runs any more ended with it (a reboot, a killed holder).
    fn attach_all(self: &Arc<Self>) -> io::Result<()> {
        let prefix = self.holder_prefix();
        let dir = self.paths.holder_socket.parent().expect("socket has a dir");
        let mut sockets: Vec<PathBuf> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| e.path())
            .filter(|p| {
                let p = p.display().to_string();
                p == self.paths.holder_socket.display().to_string() || (p.starts_with(&prefix) && p.ends_with(".sock"))
            })
            .collect();
        sockets.sort();
        let mut adopted = HashSet::new();
        for socket in sockets {
            match platform::connect(&socket) {
                Ok(stream) => adopted.extend(self.take_over(stream, false)?),
                Err(_) => {
                    platform::remove_endpoint(&socket); // its holder is gone
                }
            }
        }
        let mut st = self.lock();
        let lost: Vec<u32> =
            st.reg.terminals.values().filter(|t| t.is_live() && !adopted.contains(&t.id())).map(|t| t.id()).collect();
        for id in lost {
            self.record_finish(&mut st, id, Exit { code: None, signal: None }, "stopped_by_restart");
        }
        // Whatever lost holders left running, even if a crash cut an earlier
        // cleanup short: sweeping again is harmless.
        let leftovers = st
            .reg
            .terminals
            .values()
            .filter(|t| matches!(t.reason.as_deref(), Some("holder_lost" | "stopped_by_restart")));
        self.sweep_in_background(leftovers.filter_map(|t| t.scope.clone()).collect());
        crate::exec::sweep_leftovers(&self.paths.state);
        self.prune(&mut st);
        self.save(&st);
        Ok(())
    }

    /// Takes over one holder and adopts what it runs; returns the adopted ids.
    fn take_over(self: &Arc<Self>, mut stream: UnixStream, require_current: bool) -> io::Result<Vec<u32>> {
        std::io::Write::write_all(
            &mut stream,
            &proto::json_frame(proto::HELLO, 0, &proto::Hello { version: proto::VERSION }),
        )?;
        let welcome: proto::Welcome = match proto::read(&mut stream)? {
            Some((proto::WELCOME, _, payload)) => proto::parse(&payload)?,
            _ => return Err(io::Error::other("holder sent no welcome")),
        };
        let mut tails = HashMap::new();
        for _ in &welcome.terminals {
            match proto::read(&mut stream)? {
                Some((proto::TAIL, id, bytes)) => tails.insert(id, bytes),
                _ => return Err(io::Error::other("holder sent no tail")),
            };
        }
        let (bye, bye_rx) = mpsc::channel();
        let build = crate::client::build_id();
        let current = welcome.build == build;
        if require_current && !current {
            io::Write::write_all(&mut stream, &proto::frame(proto::DRAIN, 0, &[]))?;
            return Err(io::Error::other(format!("holder build {} differs from core build {build}", welcome.build)));
        }
        let (writer, frames) = mpsc::channel::<Vec<u8>>();
        let mut socket = stream.try_clone()?;
        std::thread::spawn(move || {
            for frame in frames {
                if io::Write::write_all(&mut socket, &frame).is_err() {
                    break;
                }
            }
        });
        let link =
            Link { writer, pid: welcome.pid, instance: welcome.instance.clone(), current, bye, bye_rx: Some(bye_rx) };
        if !current {
            // Another build: it keeps what it runs, takes nothing new, exits when empty.
            link.send(&proto::frame(proto::DRAIN, 0, &[]));
        }
        let (adopted, nudge) = {
            let mut st = self.lock();
            st.holders.push(link);
            let adopted = self.adopt(&mut st, &welcome, tails);
            self.save(&st);
            adopted
        };
        let core = self.clone();
        let instance = welcome.instance;
        std::thread::spawn(move || core.read_holder(stream, instance));
        if !nudge.is_empty() {
            let core = self.clone();
            std::thread::spawn(move || core.nudge(nudge));
        }
        Ok(adopted)
    }

    /// Matches saved records with what one holder runs. Returns the adopted ids,
    /// and the terminals whose screens were rebuilt from the tail and need
    /// their program to redraw.
    fn adopt(
        &self,
        st: &mut State,
        welcome: &proto::Welcome,
        mut tails: HashMap<u32, Vec<u8>>,
    ) -> (Vec<u32>, Vec<(u32, u16, u16)>) {
        let instance = welcome.instance.as_str();
        let handover = welcome.previous == "handover";
        // Records from before holders were tracked per terminal name one holder for all.
        let legacy = st.reg.holder.clone();
        let ours =
            |t: &crate::terminals::Terminal| t.is_live() && t.holder.as_deref().or(legacy.as_deref()) == Some(instance);
        let mut nudge = Vec::new();
        let mut exited = Vec::new();
        let mut adopted = Vec::new();
        for h in &welcome.terminals {
            let id = h.id;
            if !st.reg.terminals.get(&id).is_some_and(ours) {
                // Spawned for a core that never recorded it: nobody can address it.
                let link = st.holders.iter().find(|l| l.instance == instance).expect("attached");
                link.send(&proto::json_frame(proto::SIGNAL, id, &proto::Signal { signal: signal::KILL }));
                link.send(&proto::frame(proto::RELEASE, id, &[]));
                continue;
            }
            adopted.push(id);
            st.reg.terminals.get_mut(&id).expect("ours").holder = Some(instance.to_string());
            let tail = tails.remove(&id).unwrap_or_default();
            let saved = handover
                .then(|| std::fs::read(self.snapshot_path(id)).ok())
                .flatten()
                .and_then(|bytes| Screen::restore(&bytes, self.history_bytes()));
            let exact = saved.is_some();
            let mut screen = saved.unwrap_or_else(|| Screen::new(h.cols, h.rows, self.history_bytes()));
            // Through a relay, like live output, so requests the screen doesn't keep are seen.
            let mut relay = Relay::default();
            relay.feed(&mut screen, &tail);
            relay.flush(&mut screen);
            // After a handover the tail is new output nobody answered yet; after a
            // crash it was answered long ago and replies would be typed as garbage.
            let replies = Relay::take_responses(&mut screen);
            if exact && let Some(link) = st.link(id) {
                link.input(id, &replies);
            }
            let reports = screen.take_status_reports();
            let rec = st.reg.terminals.get_mut(&id).expect("live id");
            screen.set_xtversion(&rec.identity);
            rec.title = screen.title();
            if let Some(on) = relay.win32_input {
                rec.win32_input = on;
            }
            // A crash tail repeats reports we already counted: take its final word,
            // but count at most the one turn that may have ended while we were down.
            let (turn, was, since) = (rec.turn, rec.status.as_ref().map(|s| s.state.clone()), rec.status_ms);
            for r in &reports {
                crate::status::apply(rec, r);
            }
            if !exact {
                let now = rec.status.as_ref().map(|s| s.state.clone());
                rec.turn = turn + (was.as_deref() == Some("working") && now.as_deref() != Some("working")) as u64;
                if now == was {
                    rec.status_ms = since;
                }
            }
            st.screens.insert(id, screen);
            rec.pid = Some(h.pid);
            rec.cols = h.cols;
            rec.rows = h.rows;
            rec.screen = if exact { "exact" } else { "inexact" }.into();
            match h.exit {
                Some(exit) => exited.push((id, exit)),
                None if !exact => nudge.push((id, h.cols, h.rows)),
                None => {}
            }
        }
        for (id, exit) in exited {
            self.finish(st, id, exit);
        }
        (adopted, nudge)
    }

    /// Resize down and back so full-screen programs repaint.
    fn nudge(&self, terminals: Vec<(u32, u16, u16)>) {
        let send = |id, cols, rows| {
            if let Some(link) = self.lock().link(id) {
                link.send(&proto::json_frame(proto::RESIZE, id, &proto::Size { cols, rows }));
            }
        };
        for &(id, cols, rows) in &terminals {
            send(id, cols, rows.saturating_sub(1).max(1));
        }
        std::thread::sleep(Duration::from_millis(50));
        for &(id, cols, rows) in &terminals {
            send(id, cols, rows);
        }
    }

    fn read_holder(self: Arc<Self>, mut stream: UnixStream, instance: String) {
        while let Ok(Some((kind, id, payload))) = proto::read(&mut stream) {
            let mut st = self.lock();
            match kind {
                proto::OUTPUT => self.output(&mut st, id, &payload),
                proto::SPAWNED | proto::SPAWN_FAILED => {
                    let result = if kind == proto::SPAWNED {
                        proto::parse::<proto::Spawned>(&payload).map(|s| s.pid).map_err(|e| e.to_string())
                    } else {
                        Err(proto::parse::<proto::SpawnFailed>(&payload).map_or_else(|e| e.to_string(), |f| f.error))
                    };
                    // Recorded here, on the thread that also handles exits, so `created`
                    // always comes before `finished`.
                    let waiting = st.pending.get(&id).is_some_and(|tx| !tx.is_closed());
                    let result = match (result, st.reg.terminals.get_mut(&id)) {
                        (Ok(pid), Some(t)) if t.is_live() => {
                            let restarted = t.reason.is_some();
                            t.pid = Some(pid);
                            t.program_started_ms = Some(now_ms());
                            t.reason = None;
                            t.exit_code = None;
                            t.signal = None;
                            let created = t.clone();
                            let public =
                                serde_json::to_value(crate::terminals::Public(created.clone())).unwrap_or_default();
                            self.emit(&mut st, if restarted { "restarted" } else { "created" }, id, public);
                            self.save(&st);
                            Ok(created)
                        }
                        (Ok(_), _) => {
                            // Its run gave up waiting: nobody can address it, so it must not live.
                            if let Some(link) = st.holders.iter().find(|l| l.instance == instance) {
                                link.send(&proto::json_frame(
                                    proto::SIGNAL,
                                    id,
                                    &proto::Signal { signal: signal::KILL },
                                ));
                                link.send(&proto::frame(proto::RELEASE, id, &[]));
                            }
                            Err("removed while starting".into())
                        }
                        (Err(e), Some(t)) if t.is_live() && (t.reason.is_some() || !waiting) => {
                            self.spawn_failed(&mut st, id);
                            Err(e)
                        }
                        (Err(e), _) => Err(e),
                    };
                    if let Some(tx) = st.pending.remove(&id) {
                        let _ = tx.send(result);
                    }
                }
                proto::EXITED => {
                    if let Ok(exit) = proto::parse::<Exit>(&payload) {
                        self.finish(&mut st, id, exit);
                    }
                }
                proto::FOREGROUND_IS => {
                    let pgid = proto::parse::<proto::Foreground>(&payload).map_or(-1, |f| f.pgid);
                    for tx in st.foreground.remove(&id).unwrap_or_default() {
                        let _ = tx.send(pgid);
                    }
                }
                proto::BYE => {
                    // It said goodbye: it takes no more work from us.
                    if let Some(at) = st.holders.iter().position(|l| l.instance == instance) {
                        let _ = st.holders.remove(at).bye.send(());
                    }
                    return;
                }
                _ => {}
            }
        }
        // The holder is gone (drained, or died), and with it every program it ran.
        let mut st = self.lock();
        let Some(at) = st.holders.iter().position(|l| l.instance == instance) else { return };
        let link = st.holders.remove(at);
        let theirs: Vec<u32> = st
            .reg
            .terminals
            .values()
            .filter(|t| t.is_live() && t.holder.as_deref() == Some(instance.as_str()))
            .map(|t| t.id())
            .collect();
        // Its programs may have outlived it (closing the PTY is only a hangup).
        self.sweep_in_background(theirs.iter().filter_map(|id| st.reg.terminals[id].scope.clone()).collect());
        for id in theirs {
            self.record_finish(&mut st, id, Exit { code: None, signal: None }, "holder_lost");
        }
        if link.current {
            for (_, tx) in st.pending.drain() {
                let _ = tx.send(Err("the holder exited".into()));
            }
        }
        self.save(&st);
        self.changed.notify_waiters();
    }

    /// A program ended: record it, keep its final screen on disk, let the holder forget it.
    pub fn finish(&self, st: &mut State, id: u32, exit: Exit) {
        let reason = finish_reason(st.killing.contains(&id), st.stopping, platform::host_shutdown_in_progress());
        if let Some(link) = st.link(id) {
            link.send(&proto::frame(proto::RELEASE, id, &[]));
        }
        self.record_finish(st, id, exit, reason);
        self.save(st);
        self.changed.notify_waiters();
    }

    pub(crate) fn record_finish(&self, st: &mut State, id: u32, exit: Exit, reason: &str) {
        if !st.reg.terminals.get(&id).is_some_and(|t| t.is_live()) {
            return;
        }
        let reason = if st.killing.remove(&id) {
            "killed"
        } else if reason == "exited" && st.reg.terminals[&id].keep && exit.code.is_none() && exit.signal.is_some() {
            "signaled"
        } else {
            reason
        };
        if let Some(t) = st.reg.terminals.get_mut(&id) {
            let change = crate::status::reset(t);
            self.status_changed(st, id, change);
        }
        st.reg.finish(id, exit.code, exit.signal, reason, now_ms());
        let finished_ms = st.reg.terminals.get(&id).and_then(|t| t.finished_ms);
        let end = serde_json::json!({"reason": reason, "exit_code": exit.code, "signal": exit.signal, "finished_ms": finished_ms});
        let pending = st.reg.terminals[&id].is_live();
        self.emit(st, if pending { "lost" } else { "finished" }, id, end.clone());
        if pending {
            let t = st.reg.terminals.get_mut(&id).expect("lost");
            t.news += 1;
            t.told_parent = false;
        } else {
            self.news(st, id);
            let _ = self.jobs.send(crate::notes::Job::Orphaned(id));
            crate::remote::orphaned_afar(self, st, id); // before `--rm` forgets where it reached
        }
        for v in st.viewers.remove(&id).unwrap_or_default() {
            v.conn.with(|out| out.finish(v.channel, end.to_string().as_bytes()));
        }
        if let (Some(relay), Some(screen)) = (st.relays.get_mut(&id), st.screens.get_mut(&id)) {
            relay.flush(screen);
        }
        st.relays.remove(&id);
        if let Some(snapshot) = st.screens.remove(&id).and_then(|s| s.snapshot())
            && let Err(e) = write_atomic(&self.snapshot_path(id), &snapshot)
        {
            eprintln!("core: cannot save screen of {}: {e}", crate::terminals::address(id));
        }
        // `--rm`: gone now, unless its parent is waiting for the exit code.
        let rm = st.reg.terminals.get(&id).is_some_and(|t| {
            !t.is_live() && t.rm && !t.parent.as_ref().is_some_and(|p| st.waiting.contains_key(&(p.clone(), id)))
        });
        if rm {
            self.remove(st, id);
        }
        self.prune(st);
    }

    /// Forgets finished terminals past their retention. Runs when one finishes,
    /// at startup and on every list, so a quiet core keeps nothing too long.
    pub fn prune(&self, st: &mut State) {
        for gone in st.reg.expired(now_ms()) {
            self.remove(st, gone);
        }
    }

    /// The screen text of a live or finished terminal.
    pub fn screen_text(&self, st: &State, id: u32) -> Option<String> {
        match st.screens.get(&id) {
            Some(screen) => Some(screen.plain_text()),
            None => std::fs::read(self.snapshot_path(id))
                .ok()
                .and_then(|bytes| Screen::restore(&bytes, self.history_bytes()))
                .map(|s| s.plain_text()),
        }
    }

    pub fn remove(&self, st: &mut State, id: u32) {
        // Gone before it is announced: a fresh snapshot for a subscriber that
        // fell behind must not bring it back.
        let Some(was) = st.reg.terminals.remove(&id) else { return };
        self.emit_from(st, "removed", id, serde_json::json!({}), Some(&was));
        let _ = std::fs::remove_file(self.snapshot_path(id));
        let _ = std::fs::remove_dir_all(crate::put::dir(&self.paths.state, id));
        self.save(st);
    }

    pub fn signal(&self, st: &State, id: u32, signal: i32) {
        if let Some(link) = st.link(id) {
            link.send(&proto::json_frame(proto::SIGNAL, id, &proto::Signal { signal }));
        }
    }

    /// Waits until `done` holds or the timeout passes; returns whether it held.
    pub async fn wait_for(&self, timeout: Duration, done: impl Fn(&State) -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if done(&self.lock()) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return done(&self.lock());
            }
        }
    }

    /// Hang up, then escalate. Returns once the program ended or KILL timed out.
    pub async fn kill(&self, ids: &[u32]) {
        let all_done = |st: &State| {
            ids.iter().all(|id| !st.reg.terminals.get(id).is_some_and(|t| t.is_live() && t.holder.is_some()))
        };
        {
            let mut st = self.lock();
            for &id in ids {
                if st
                    .reg
                    .terminals
                    .get(&id)
                    .is_some_and(|t| t.is_live() && (t.holder.is_none() || (t.reason.is_some() && t.pid.is_none())))
                {
                    let reason = if st.killing.contains(&id) { "killed" } else { "server_stop" };
                    if let Some(link) = st.link(id) {
                        link.send(&proto::json_frame(proto::SIGNAL, id, &proto::Signal { signal: signal::KILL }));
                        link.send(&proto::frame(proto::RELEASE, id, &[]));
                    }
                    self.record_finish(&mut st, id, Exit { code: None, signal: None }, reason);
                }
            }
            self.save(&st);
        }
        for (signal, wait) in [(signal::HUP, 500), (signal::TERM, 500), (signal::KILL, 2000)] {
            {
                let st = self.lock();
                for &id in ids {
                    self.signal(&st, id, signal);
                }
            }
            if self.wait_for(Duration::from_millis(wait), all_done).await {
                return;
            }
        }
    }

    /// Ends every link on purpose and waits until each holder confirms.
    fn goodbye(&self, mode: Goodbye) {
        let _starting = self.holder_start.lock().unwrap_or_else(|p| p.into_inner());
        let byes: Vec<mpsc::Receiver<()>> = {
            let mut st = self.lock();
            st.holders
                .iter_mut()
                .filter_map(|link| {
                    link.send(&proto::json_frame(proto::GOODBYE, 0, &mode));
                    link.bye_rx.take()
                })
                .collect()
        };
        for rx in byes {
            // A holder told to shut down first ends every leftover (up to ~3 s).
            let _ = rx.recv_timeout(Duration::from_secs(6));
        }
    }

    /// Ends what lost terminals left running, in the background but counted:
    /// stop and restart wait for it.
    fn sweep_in_background(&self, scopes: Vec<String>) {
        if scopes.is_empty() {
            return;
        }
        self.sweeps.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let sweeps = self.sweeps.clone();
        std::thread::spawn(move || {
            crate::sweep::sweep(&scopes);
            sweeps.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        });
    }

    fn wait_for_sweeps(&self) {
        let deadline = Instant::now() + Duration::from_secs(6);
        while self.sweeps.load(std::sync::atomic::Ordering::SeqCst) > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    /// Planned restart: the holder keeps programs; we save every screen exactly.
    pub fn handover(&self) {
        self.wait_for_sweeps();
        self.goodbye(Goodbye::Handover);
        self.wait_for_sweeps(); // a holder that died during goodbye left one
        let mut st = self.lock();
        st.holders.clear();
        let State { screens, relays, .. } = &mut *st;
        for (id, screen) in screens.iter_mut() {
            if let Some(relay) = relays.get_mut(id) {
                relay.flush(screen);
            }
            if let Some(snapshot) = screen.snapshot() {
                let _ = crate::paths::write_replace(&self.snapshot_path(*id), &snapshot);
            }
        }
        self.save_now(&st, false);
    }

    /// Server stop: end every program, then let the holder exit.
    pub async fn shutdown(&self) {
        let live: Vec<u32> = {
            let mut st = self.lock();
            st.stopping = true;
            st.reg.terminals.values().filter(|t| t.is_live()).map(|t| t.id()).collect()
        };
        self.changed.notify_waiters(); // execs end
        self.kill(&live).await;
        crate::exec::wait_all().await;
        self.wait_for_sweeps();
        self.goodbye(Goodbye::Shutdown);
        self.wait_for_sweeps(); // a holder that died during goodbye left one
        let mut st = self.lock();
        for id in live {
            self.record_finish(&mut st, id, Exit { code: None, signal: None }, "server_stop");
        }
        self.save_now(&st, true);
    }
}

/// The registry writes waiting for the `hc-save` thread, and the newest generation on disk.
struct Writes {
    pending: Mutex<Option<(u64, PathBuf, Vec<u8>)>>,
    ready: Condvar,
    written: Mutex<u64>,
    next: AtomicU64,
}

static WRITES: Writes =
    Writes { pending: Mutex::new(None), ready: Condvar::new(), written: Mutex::new(0), next: AtomicU64::new(1) };

/// Writes unless a newer generation is already on disk, so a late older write never replaces a newer one.
fn write_newest(generation: u64, path: &Path, bytes: &[u8], sync: bool) {
    let mut written = WRITES.written.lock().unwrap_or_else(|p| p.into_inner());
    if generation <= *written {
        return;
    }
    let wrote = if sync { write_atomic(path, bytes) } else { crate::paths::write_replace(path, bytes) };
    match wrote {
        Ok(()) => *written = generation,
        Err(e) => eprintln!("core: cannot save terminals: {e}"),
    }
}

fn writer() {
    loop {
        let (generation, path, bytes) = {
            let mut pending = WRITES.pending.lock().unwrap_or_else(|p| p.into_inner());
            loop {
                if let Some(next) = pending.take() {
                    break next;
                }
                pending = WRITES.ready.wait(pending).unwrap_or_else(|p| p.into_inner());
            }
        };
        write_newest(generation, &path, &bytes, true);
    }
}

#[cfg(test)]
mod tests;
