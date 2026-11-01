//! `machine.exec`: one program without a terminal, and exactly what it wrote.
//! Nothing it started outlives it: its own process group (a job on Windows)
//! and scope token, swept whenever it ends. The token is kept in the state dir
//! until swept, so a core that crashed mid-exec sweeps it at its next start.

use crate::api::{ExecParams, ExecResult};
use crate::rpc::RpcError;
use crate::runtime::Core;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::watch;

pub const MAX_STDOUT: usize = 8 << 20;
pub const MAX_STDERR: usize = 1 << 20;
pub const MAX_STDIN: usize = 8 << 20;
pub const DEFAULT_TIMEOUT: f64 = 60.0;
const MAX_TIMEOUT: f64 = 600.0;
/// Execs allowed at once; `HC_MAX_EXECS` in the server's environment overrides it (Automancer: upstream's 16 refused
/// a burst of lane starts on a 20-core machine, 31 Oct 2026). 1024 ran at once on auto-vps under load without a failure.
const DEFAULT_MAX_RUNNING: usize = 1024;

pub fn max_running() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("HC_MAX_EXECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_MAX_RUNNING)
    })
}

/// Execs running now, cleanup included.
pub fn running() -> usize {
    RUNNING.load(Ordering::SeqCst)
}
/// How long output may still come after everything was ended (a process that
/// escaped the sweep may hold a pipe).
const DRAIN: Duration = Duration::from_secs(2);

static RUNNING: AtomicUsize = AtomicUsize::new(0);

/// One of the `max_running()` places, given back on drop: held until
/// everything the exec started was ended, so a stopping core waits for that.
struct Slot;

impl Slot {
    /// Cleanup counts as running too, past the limit.
    fn cleanup() -> Slot {
        RUNNING.fetch_add(1, Ordering::SeqCst);
        Slot
    }

    fn take() -> Result<Slot, RpcError> {
        if RUNNING.fetch_add(1, Ordering::SeqCst) >= max_running() {
            RUNNING.fetch_sub(1, Ordering::SeqCst);
            return Err(RpcError::new(
                "unavailable",
                format!("{} execs are running already", max_running()),
                "try again when one is done",
            ));
        }
        Ok(Slot)
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        RUNNING.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Seconds to run, checked before anything starts.
pub fn timeout(secs: Option<f64>) -> Result<Duration, RpcError> {
    match secs.unwrap_or(DEFAULT_TIMEOUT) {
        s if s > 0.0 && s <= MAX_TIMEOUT => Ok(Duration::from_secs_f64(s)),
        s => Err(RpcError::invalid_params(format!("timeout must be more than 0 and at most {MAX_TIMEOUT} s, not {s}"))),
    }
}

/// What runs: `argv` as given, or `line` in this machine's shell.
fn program(p: &ExecParams) -> Result<Vec<String>, RpcError> {
    match (&p.line, p.argv.is_empty()) {
        (Some(line), true) => Ok(crate::platform::shell_line(line)),
        (None, false) => Ok(p.argv.clone()),
        _ => Err(RpcError::invalid_params("give either argv or line")),
    }
}

/// The core's environment without hc's own marks, plus `extra`, plus its
/// token; `PWD` is its folder as the caller named it (as for terminals).
fn env(
    base: BTreeMap<String, String>,
    extra: BTreeMap<String, String>,
    token: &str,
    cwd: &str,
) -> BTreeMap<String, String> {
    let hc_own = ["HC_TERMINAL_ID", crate::sweep::VAR, crate::sweep::RUNTIME];
    let mut env: BTreeMap<String, String> = base
        .into_iter()
        .filter(|(k, _)| !crate::api::IDENTITY_VARS.contains(&k.as_str()) && !hc_own.contains(&k.as_str()))
        .collect();
    if let Some(lang) = crate::locale::for_programs(&env) {
        env.insert("LANG".into(), lang);
    }
    env.insert("PWD".into(), cwd.into());
    env.extend(extra);
    env.insert(crate::sweep::VAR.into(), token.into());
    env
}

fn tokens_dir(state: &Path) -> PathBuf {
    state.join("execs")
}

/// Pairs a start with its outcome, including a dropped request. File IO stays off the core thread.
struct Audit {
    record: Option<Value>,
    log: PathBuf,
    /// Before a process tree takes over token cleanup.
    file: Option<PathBuf>,
}

impl Audit {
    fn append(log: &Path, mut record: Value, event: &str, outcome: Value) -> std::io::Result<()> {
        record["event"] = event.into();
        record.as_object_mut().expect("exec record").extend(outcome.as_object().expect("exec outcome").clone());
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        std::fs::OpenOptions::new().create(true).append(true).open(log)?.write_all(&bytes)
    }

    async fn finish(mut self, outcome: Value) {
        let record = self.record.take().expect("unfinished exec");
        let log = self.log.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) = Self::append(&log, record, "exec_end", outcome) {
                eprintln!("core: cannot log exec end: {e}");
            }
        })
        .await;
    }
}

impl Drop for Audit {
    fn drop(&mut self) {
        if let Some(record) = self.record.take() {
            let log = self.log.clone();
            let file = self.file.clone();
            std::thread::spawn(move || {
                if let Err(e) = Self::append(&log, record, "exec_end", json!({"reason": "cancelled"})) {
                    eprintln!("core: cannot log cancelled exec: {e}");
                }
                if let Some(file) = file {
                    let _ = std::fs::remove_file(file);
                }
            });
        }
    }
}

/// At start: sweep what execs of a crashed core left running.
pub fn sweep_leftovers(state: &Path) {
    let dir = tokens_dir(state);
    let files: Vec<PathBuf> = std::fs::read_dir(&dir).into_iter().flatten().flatten().map(|e| e.path()).collect();
    if files.is_empty() {
        return;
    }
    let slot = Slot::cleanup();
    std::thread::spawn(move || {
        let _slot = slot;
        let tokens: Vec<String> = files.iter().filter_map(|f| Some(f.file_name()?.to_str()?.to_string())).collect();
        crate::sweep::sweep(&tokens);
        files.iter().for_each(|f| drop(std::fs::remove_file(f)));
    });
}

/// The folder `cwd` names: from this machine's home for a caller from afar,
/// else from the core's folder; that start when none is given.
/// Use HOME, not the core's cwd: the underlying home directory may have been replaced.
pub fn folder(cwd: Option<&str>, from_afar: bool) -> String {
    let start = match (from_afar, std::env::var("HOME")) {
        (true, Ok(home)) => home,
        _ => std::env::current_dir().map(|d| d.display().to_string()).unwrap_or_else(|_| "/".into()),
    };
    match cwd {
        Some(cwd) => Path::new(&start).join(cwd).display().to_string(),
        None => start,
    }
}

/// Runs it to the end (exit, timeout, or the core stopping) and returns what it wrote.
pub async fn run(core: &Arc<Core>, p: ExecParams, origin: Option<String>) -> Result<ExecResult, RpcError> {
    let argv = program(&p)?;
    let limit = timeout(p.timeout)?;
    let input = match &p.input {
        Some(data) => Some(
            crate::put::decode(data)
                .filter(|b| b.len() <= MAX_STDIN)
                .ok_or_else(|| RpcError::invalid_params("input: base64, at most 8 MB raw"))?,
        ),
        None => None,
    };
    crate::api::not_stopping(&core.lock())?;
    let slot = Slot::take()?;
    let from_afar = origin.is_some();
    let origin = origin.unwrap_or_else(|| core.info.name.clone());
    let cwd = folder(p.cwd.as_deref(), from_afar);
    let token = crate::sweep::token();
    let caller = p.caller.as_ref().map(|caller| if from_afar { caller.clone() } else { format!("{origin}/{caller}") });
    let record = json!({
        "token": token, "origin_machine": origin, "caller_terminal": caller,
        "argv0": argv[0], "started_ms": crate::runtime::now_ms(),
    });
    let dir = tokens_dir(&core.paths.state);
    let file = dir.join(&token);
    let (d, f) = (dir.clone(), file.clone());
    let log = core.paths.state.join("core.log");
    let mut audit = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&d)?;
        std::fs::write(&f, serde_json::to_vec(&record)?)?;
        if let Err(e) = Audit::append(&log, record.clone(), "exec_start", json!({})) {
            let _ = std::fs::remove_file(&f);
            return Err(e);
        }
        Ok(Audit { record: Some(record), log, file: Some(f) })
    })
    .await
    .map_err(|e| RpcError::internal(e.to_string()))?
    .map_err(|e| RpcError::internal(format!("cannot note the exec: {e}")))?;
    let mut cmd = tokio::process::Command::new(&argv[0]);
    #[cfg(windows)]
    if crate::platform::is_shell_line(&argv) {
        cmd.args(&argv[1..4]).raw_arg(format!("\"{}\"", argv[4]));
    } else {
        cmd.args(&argv[1..]);
    }
    #[cfg(unix)]
    cmd.args(&argv[1..]);
    cmd.current_dir(&cwd)
        .env_clear()
        .envs(env(crate::platform::program_env().await, crate::api::labels(&p.env), &token, &cwd))
        .stdin(if input.is_some() { std::process::Stdio::piped() } else { std::process::Stdio::null() })
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    crate::platform::exec_group(&mut cmd);
    let started = Instant::now();
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            let _ = std::fs::remove_file(&file);
            audit.finish(json!({"reason": "spawn_failed"})).await;
            let what = if std::path::Path::new(&cwd).is_dir() { argv[0].clone() } else { format!("folder {cwd}") };
            return Err(RpcError::new("spawn_failed", format!("cannot start {what}: {e}"), ""));
        }
    };
    // Never run uncontained.
    let group = match crate::platform::ExecGroup::new(&child) {
        Ok(group) => group,
        Err(e) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            let _ = std::fs::remove_file(&file);
            audit.finish(json!({"reason": "containment_failed"})).await;
            return Err(RpcError::new("spawn_failed", format!("cannot contain {}: {e}", argv[0]), ""));
        }
    };
    let (out, err) = (child.stdout.take().expect("piped"), child.stderr.take().expect("piped"));
    let stdin = child.stdin.take();
    let mut tree = Tree { child: Some(child), group, token, file, slot: Some(slot) };
    audit.file = None;
    let (ended, ended_rx) = watch::channel(false);
    let main = async {
        let stop = stopping(core);
        // Not reaped yet: its group stays its own until everything is ended.
        let timed_out = tokio::select! {
            () = crate::platform::exited(tree.child()) => false,
            _ = tokio::time::sleep(limit) => true,
            _ = stop => true,
        };
        tree.end().await;
        let _ = ended.send(true);
        (tree.child().wait().await.ok(), timed_out)
    };
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let ((status, timed_out), cut_out, cut_err, ()) = tokio::join!(
        main,
        capture(out, &mut stdout, MAX_STDOUT, ended_rx.clone()),
        capture(err, &mut stderr, MAX_STDERR, ended_rx.clone()),
        feed(stdin, input.unwrap_or_default(), ended_rx)
    );
    audit
        .finish(json!({
            "code": status.as_ref().and_then(|s| s.code()),
            "signal": status.as_ref().and_then(crate::platform::exit_signal),
            "timed_out": timed_out,
        }))
        .await;
    if core.lock().stopping {
        return Err(RpcError::new("unavailable", "the core stopped during the exec", ""));
    }
    Ok(ExecResult {
        code: status.and_then(|s| s.code()),
        signal: status.as_ref().and_then(crate::platform::exit_signal),
        stdout: crate::put::encode(&stdout),
        stderr: crate::put::encode(&stderr),
        truncated: cut_out || cut_err,
        timed_out,
        ms: started.elapsed().as_millis() as u64,
    })
}

/// Waits until the core is stopping.
async fn stopping(core: &Core) {
    loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if core.lock().stopping {
            return;
        }
        changed.await;
    }
}

/// A stopping core waits (a while) for its execs to end their trees.
pub async fn wait_all() {
    let deadline = Instant::now() + Duration::from_secs(6);
    while RUNNING.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Reads a pipe to its end, keeping the first `max` bytes; returns whether
/// it dropped any. Gives up `DRAIN` after everything was ended.
async fn capture(
    mut pipe: impl AsyncRead + Unpin,
    keep: &mut Vec<u8>,
    max: usize,
    mut ended: watch::Receiver<bool>,
) -> bool {
    let mut buf = vec![0u8; 64 << 10];
    let mut cut = false;
    let give_up = async move {
        let _ = ended.wait_for(|e| *e).await;
        tokio::time::sleep(DRAIN).await;
    };
    tokio::pin!(give_up);
    loop {
        let n = tokio::select! {
            n = pipe.read(&mut buf) => n.unwrap_or(0),
            _ = &mut give_up => return cut,
        };
        if n == 0 {
            return cut;
        }
        let room = max - keep.len();
        keep.extend_from_slice(&buf[..n.min(room)]);
        cut |= n > room;
    }
}

/// Writes `input` to its stdin, then closes it; gives up once everything was
/// ended (or it stopped reading).
async fn feed(stdin: Option<tokio::process::ChildStdin>, input: Vec<u8>, mut ended: watch::Receiver<bool>) {
    let Some(mut stdin) = stdin else { return };
    tokio::select! {
        _ = tokio::io::AsyncWriteExt::write_all(&mut stdin, &input) => {}
        _ = ended.wait_for(|e| *e) => {}
    }
}

/// Everything one exec started: its group or job, its token, and its slot
/// (none once its ending has started).
struct Tree {
    child: Option<tokio::process::Child>,
    group: crate::platform::ExecGroup,
    token: String,
    file: PathBuf,
    slot: Option<Slot>,
}

impl Tree {
    fn child(&mut self) -> &mut tokio::process::Child {
        self.child.as_mut().expect("its child until dropped")
    }

    /// Ends it all, once; the slot goes when that is done. `child`, when
    /// given, is reaped only after that (its group id stays its own).
    fn ender(&mut self, child: Option<tokio::process::Child>) -> Option<impl FnOnce() + Send + 'static> {
        let slot = self.slot.take()?;
        let (group, token, file) = (self.group.clone(), self.token.clone(), self.file.clone());
        Some(move || {
            group.end();
            crate::sweep::sweep(&[token]);
            group.kill();
            drop(child);
            let _ = std::fs::remove_file(file);
            drop(slot);
        })
    }

    async fn end(&mut self) {
        if let Some(ender) = self.ender(None) {
            let _ = tokio::task::spawn_blocking(ender).await;
        }
    }
}

impl Drop for Tree {
    /// Its caller went away: end it all without waiting here.
    fn drop(&mut self) {
        let child = self.child.take();
        if let Some(ender) = self.ender(child) {
            self.group.end();
            std::thread::spawn(ender);
        }
    }
}
