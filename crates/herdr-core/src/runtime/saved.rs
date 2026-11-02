//! terminals.json and the journal beside it. The file holds the whole registry; a save appends (synced) only the
//! terminals that changed since the last write to terminals.journal, and a start replays it. Rewriting the whole
//! file on every status change wrote about 635 KB/s for some 25 agents (Automancer, 2 Nov 2026). The journal folds
//! back into the file once it outgrows it, at the first save after a start, and at every handover and stop, so a
//! planned restart into an older build still reads one complete file.

use crate::paths::{read_state, write_atomic, write_replace};
use crate::terminals::{Registry, Terminal};
use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

/// The registry as saved: each terminal serialized on its own, so a save can tell which ones changed.
pub struct Saved {
    next_id: u32,
    holder: Option<String>,
    terminals: BTreeMap<u32, Vec<u8>>,
}

impl Saved {
    pub fn of(reg: &Registry) -> Saved {
        let terminals =
            reg.terminals.iter().map(|(id, t)| (*id, serde_json::to_vec(t).expect("terminal serializes"))).collect();
        Saved { next_id: reg.next_id, holder: reg.holder.clone(), terminals }
    }

    /// terminals.json: the registry's own layout, naming the journal that continues it.
    fn file(&self, journal: &str) -> Vec<u8> {
        let head = format!(
            r#"{{"next_id":{},"holder":{},"journal":"{journal}","terminals":"#,
            self.next_id,
            json(&self.holder)
        );
        let mut out = head.into_bytes();
        object(&mut out, self.terminals.iter());
        out.push(b'}');
        out
    }

    /// One journal line: what changed since `was`, or nothing.
    fn line(&self, was: &Saved, journal: &str) -> Option<Vec<u8>> {
        let set = self.terminals.iter().filter(|(id, t)| was.terminals.get(id) != Some(t));
        let gone: Vec<u32> = was.terminals.keys().filter(|id| !self.terminals.contains_key(id)).copied().collect();
        if set.clone().next().is_none() && gone.is_empty() && self.next_id == was.next_id && self.holder == was.holder {
            return None;
        }
        let head = format!(
            r#"{{"journal":"{journal}","next_id":{},"holder":{},"gone":{},"set":"#,
            self.next_id,
            json(&self.holder),
            json(&gone)
        );
        let mut out = head.into_bytes();
        object(&mut out, set);
        out.extend_from_slice(b"}\n");
        Some(out)
    }
}

fn json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).expect("serializes")
}

fn object<'a>(out: &mut Vec<u8>, terminals: impl Iterator<Item = (&'a u32, &'a Vec<u8>)>) {
    out.push(b'{');
    for (n, (id, t)) in terminals.enumerate() {
        if n > 0 {
            out.push(b',');
        }
        out.extend_from_slice(format!("\"{id}\":").as_bytes());
        out.extend_from_slice(t);
    }
    out.push(b'}');
}

#[derive(serde::Deserialize)]
struct Line {
    journal: String,
    next_id: u32,
    holder: Option<String>,
    gone: Vec<u32>,
    set: BTreeMap<u32, Terminal>,
}

fn journal_path(file: &Path) -> PathBuf {
    file.with_extension("journal")
}

/// The saved registry: `file` with its journal replayed. A line a crash cut short ends the replay; lines of an
/// older journal, left by a crash during a fold, are not this file's.
pub fn load(file: &Path) -> io::Result<Registry> {
    let mut reg: Registry = read_state(file)?;
    let Some(journal) = reg.journal.clone() else { return Ok(reg) };
    let bytes = match std::fs::read(journal_path(file)) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(reg),
        read => read?,
    };
    for raw in bytes.split(|b| *b == b'\n') {
        let Ok(line) = serde_json::from_slice::<Line>(raw) else { break };
        if line.journal == journal {
            reg.next_id = line.next_id;
            reg.holder = line.holder;
            for id in line.gone {
                reg.terminals.remove(&id);
            }
            reg.terminals.extend(line.set);
        }
    }
    Ok(reg)
}

/// What is on disk for a file this process wrote.
struct Last {
    saved: Saved,
    journal: String,
    file_len: usize,
    journal_len: usize,
}

/// Per file, the newest generation on disk and what it holds.
type Disk = HashMap<PathBuf, (u64, Option<Last>)>;

/// The saves waiting for the `hc-save` thread, and what is on disk.
struct Writes {
    pending: Mutex<Option<(u64, PathBuf, Saved)>>,
    ready: Condvar,
    disk: Mutex<Option<Disk>>,
    next: AtomicU64,
    /// The newest generation written, or given up on.
    settled: AtomicU64,
}

static WRITES: Writes = Writes {
    pending: Mutex::new(None),
    ready: Condvar::new(),
    disk: Mutex::new(None),
    next: AtomicU64::new(1),
    settled: AtomicU64::new(0),
};

/// Queues `saved` for the `hc-save` thread; a newer save replaces one it has not written yet.
pub fn queue(file: PathBuf, saved: Saved) {
    static START: std::sync::Once = std::sync::Once::new();
    START.call_once(|| {
        std::thread::Builder::new().name("hc-save".into()).spawn(writer).expect("save thread starts");
    });
    let generation = WRITES.next.fetch_add(1, Ordering::SeqCst);
    *WRITES.pending.lock().unwrap_or_else(|p| p.into_inner()) = Some((generation, file, saved));
    WRITES.ready.notify_one();
}

/// Writes `saved` as one complete file before it returns, for a restart or stop that reads it next.
pub fn fold_now(file: &Path, saved: Saved, sync: bool) {
    let generation = WRITES.next.fetch_add(1, Ordering::SeqCst);
    write_newest(generation, file, saved, true, sync);
}

/// Writes unless a newer generation is already on disk, so a late older write never replaces a newer one.
fn write_newest(generation: u64, file: &Path, saved: Saved, fold: bool, sync: bool) {
    let mut disk = WRITES.disk.lock().unwrap_or_else(|p| p.into_inner());
    let (on_disk, last) = disk.get_or_insert_default().entry(file.to_path_buf()).or_default();
    if generation > *on_disk {
        // Taken: after a failed write, the next one folds whatever the journal holds.
        let wrote = match last.take().filter(|_| !fold) {
            None => fold_into(file, saved, sync),
            Some(mut was) => match saved.line(&was.saved, &was.journal) {
                None => Ok(was),
                Some(line) if was.journal_len + line.len() <= was.file_len => append(&journal_path(file), &line, sync)
                    .map(|()| {
                        was.journal_len += line.len();
                        was.saved = saved;
                        was
                    }),
                Some(_) => fold_into(file, saved, sync),
            },
        };
        match wrote {
            Ok(now) => (*on_disk, *last) = (generation, Some(now)),
            Err(e) => eprintln!("core: cannot save terminals: {e}"),
        }
    }
    WRITES.settled.fetch_max(generation, Ordering::SeqCst);
}

/// The whole registry into `file`, starting a new journal; the old one goes after.
fn fold_into(file: &Path, saved: Saved, sync: bool) -> io::Result<Last> {
    let journal = crate::platform::random_hex();
    let bytes = saved.file(&journal);
    if sync {
        write_atomic(file, &bytes)?
    } else {
        write_replace(file, &bytes)?
    }
    match std::fs::remove_file(journal_path(file)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    Ok(Last { saved, journal, file_len: bytes.len(), journal_len: 0 })
}

fn append(path: &Path, line: &[u8], sync: bool) -> io::Result<()> {
    let mut journal = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    journal.write_all(line)?;
    if sync { journal.sync_data() } else { Ok(()) }
}

/// Waits (up to 10 s) until the newest save has reached the disk: `hc-save` takes a write off the queue before it
/// writes it, so an empty queue alone does not mean the write finished. Tests call it before removing their state
/// folder.
#[cfg(test)]
pub fn flush_saves() {
    use std::time::{Duration, Instant};
    let until = Instant::now() + Duration::from_secs(10);
    while Instant::now() < until {
        if WRITES.settled.load(Ordering::SeqCst) >= WRITES.next.load(Ordering::SeqCst) - 1 {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn writer() {
    loop {
        let (generation, file, saved) = {
            let mut pending = WRITES.pending.lock().unwrap_or_else(|p| p.into_inner());
            loop {
                if let Some(next) = pending.take() {
                    break next;
                }
                pending = WRITES.ready.wait(pending).unwrap_or_else(|p| p.into_inner());
            }
        };
        write_newest(generation, &file, saved, false, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminals::NewTerminal;

    fn registry(n: usize) -> Registry {
        let mut reg = Registry::default();
        for i in 0..n {
            let new = NewTerminal {
                argv: vec!["claude".into(), "--model".into(), "opus".into()],
                cwd: format!("/opt/automancer/worktrees/lanes/ln-{i:08x}"),
                cols: 200,
                rows: 50,
                ..Default::default()
            };
            reg.create(new, 1_793_612_104_924).unwrap();
        }
        reg
    }

    fn scratch() -> PathBuf {
        let dir = PathBuf::from(format!("/var/tmp/hc-saved-{}", crate::platform::random_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn json_of(reg: &Registry) -> serde_json::Value {
        serde_json::json!({"next_id": reg.next_id, "holder": reg.holder, "terminals": reg.terminals})
    }

    #[test]
    fn a_status_change_appends_that_terminal_only_and_a_start_reads_it_back() {
        let dir = scratch();
        let file = dir.join("terminals.json");
        let mut reg = registry(100);
        reg.holder = Some("h1".into());
        fold_now(&file, Saved::of(&reg), true);
        let folded = std::fs::read(&file).unwrap();
        let mut appended = 0;
        for turn in 1..=40u64 {
            let t = reg.terminals.get_mut(&(1 + turn as u32 % 100)).unwrap();
            t.turn = turn;
            t.title = format!("turn {turn}");
            let before = std::fs::metadata(journal_path(&file)).map_or(0, |m| m.len());
            write_newest(WRITES.next.fetch_add(1, Ordering::SeqCst), &file, Saved::of(&reg), false, true);
            appended += std::fs::metadata(journal_path(&file)).unwrap().len() - before;
            assert_eq!(std::fs::read(&file).unwrap(), folded, "terminals.json is not rewritten for one status change");
        }
        // About one record a change, where every change used to rewrite all 100.
        assert!(
            appended < 40 * folded.len() as u64 / 50,
            "{appended} bytes for 40 changes of a {} byte file",
            folded.len()
        );
        reg.terminals.remove(&7);
        reg.next_id += 1;
        write_newest(WRITES.next.fetch_add(1, Ordering::SeqCst), &file, Saved::of(&reg), false, true);
        assert_eq!(json_of(&load(&file).unwrap()), json_of(&reg));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_journal_folds_back_once_it_outgrows_the_file() {
        let dir = scratch();
        let file = dir.join("terminals.json");
        let mut reg = registry(3);
        fold_now(&file, Saved::of(&reg), true);
        for turn in 1..=20 {
            reg.terminals.get_mut(&1).unwrap().turn = turn;
            write_newest(WRITES.next.fetch_add(1, Ordering::SeqCst), &file, Saved::of(&reg), false, true);
            let journal = std::fs::metadata(journal_path(&file)).map_or(0, |m| m.len());
            assert!(journal <= std::fs::metadata(&file).unwrap().len(), "journal of {journal} bytes after {turn}");
        }
        assert_eq!(json_of(&load(&file).unwrap()), json_of(&reg));
        fold_now(&file, Saved::of(&reg), false);
        assert!(!journal_path(&file).exists(), "a handover leaves one complete file");
        let plain: Registry = read_state(&file).unwrap();
        assert_eq!(json_of(&plain), json_of(&reg), "an older build reads it without the journal");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_crash_mid_line_or_mid_fold_loses_only_the_unfinished_write() {
        let dir = scratch();
        let file = dir.join("terminals.json");
        let mut reg = registry(3);
        fold_now(&file, Saved::of(&reg), true);
        reg.terminals.get_mut(&2).unwrap().turn = 5;
        write_newest(WRITES.next.fetch_add(1, Ordering::SeqCst), &file, Saved::of(&reg), false, true);
        let saved = json_of(&reg);
        // A crash in the middle of the next line.
        reg.terminals.get_mut(&2).unwrap().turn = 6;
        let mut torn = std::fs::read(journal_path(&file)).unwrap();
        torn.extend_from_slice(&Saved::of(&reg).line(&Saved::of(&registry(3)), "x").unwrap()[..40]);
        std::fs::write(journal_path(&file), &torn).unwrap();
        assert_eq!(json_of(&load(&file).unwrap()), saved);
        // A crash after a fold wrote the file but before it removed the old journal: those lines are stale.
        let mut older = reg.terminals[&2].clone();
        older.turn = 1;
        let stale = Saved {
            next_id: reg.next_id,
            holder: None,
            terminals: BTreeMap::from([(2, serde_json::to_vec(&older).unwrap())]),
        };
        fold_now(&file, Saved::of(&reg), true);
        let journal = String::from_utf8(stale.line(&Saved::of(&Registry::default()), "older").unwrap()).unwrap();
        std::fs::write(journal_path(&file), journal).unwrap();
        assert_eq!(load(&file).unwrap().terminals[&2].turn, 6);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
