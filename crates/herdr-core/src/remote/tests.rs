use super::*;
use crate::events::{Filter, Sink};
use crate::paths::Paths;
use std::sync::atomic::AtomicBool;

#[test]
fn remote_misses_and_renames_keep_subscribers_on_the_current_machine() {
    let root = std::path::PathBuf::from(format!(
        "/var/tmp/hc-doc-missed-{}-{}",
        std::process::id(),
        crate::platform::random_hex()
    ));
    let paths = Paths {
        socket: root.join("core.sock"),
        holder_socket: root.join("holder.sock"),
        state: root.clone(),
        config: root.join("config.toml"),
    };
    paths.create_dirs().unwrap();
    let core = Core::start(paths, crate::api::CoreInfo { id: "here".into(), name: "here".into() }).unwrap();
    let machine = Machine {
        name: "rohan".into(),
        target: "rohan".into(),
        via: false,
        temporary: false,
        session: None,
        id: "remote".into(),
        host: "rohan".into(),
        version: "test".into(),
    };
    let (tx, mut rx) = mpsc::channel(4);
    let (filtered_tx, mut filtered_rx) = mpsc::channel(4);
    {
        let mut st = core.lock();
        st.events.seq = 7;
        st.machines.push(machine.clone());
        core.subscribe(&mut st, Filter::default(), Sink::Plain { tx, behind: Arc::new(AtomicBool::new(false)) });
        core.subscribe(
            &mut st,
            Filter { terminal: Some("t1".into()), ..Default::default() },
            Sink::Plain { tx: filtered_tx, behind: Arc::new(AtomicBool::new(false)) },
        );
    }
    let (frames, _rx) = mpsc::unbounded_channel();
    let line = Line::new(frames, &json!({}));
    mirror_events(&core, &machine, &line, b"{\"method\":\"missed\",\"params\":{\"seq\":9999,\"terminals\":[]}}\n");
    let msg: Value = serde_json::from_str(&rx.try_recv().expect("local missed notification")).unwrap();
    assert_eq!(msg["method"], "missed");
    assert_eq!(msg["params"]["seq"], 7);
    assert!(filtered_rx.try_recv().is_err());
    // Who typed last travels with the mirror: a person's keys and an agent's input apart.
    mirror_replace(&core, &machine, vec![json!({"address": "t1", "state": "live"})]);
    rx.try_recv().expect("created");
    mirror_events(&core, &machine, &line, b"{\"method\":\"event\",\"params\":{\"type\":\"input\",\"terminal\":\"t1\",\"data\":{\"input_ms\":1793000000000}}}\n");
    mirror_events(&core, &machine, &line, b"{\"method\":\"event\",\"params\":{\"type\":\"sent\",\"terminal\":\"t1\",\"data\":{\"sent_ms\":1793000000001,\"sends\":3}}}\n");
    let typed: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
    let sent: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
    assert_eq!((typed["params"]["type"].as_str(), sent["params"]["type"].as_str()), (Some("input"), Some("sent")));
    let record = core.lock().mirrors[&machine.id]["rohan/t1"].clone();
    assert_eq!(
        (record["input_ms"].as_u64(), record["sent_ms"].as_u64(), record["sends"].as_u64()),
        (Some(1793000000000), Some(1793000000001), Some(3))
    );
    mirror_replace(&core, &machine, vec![json!({"address":"t1", "state":"live", "status":{"state":"working"}})]);
    rx.try_recv().unwrap();
    let conn = Arc::new(Conn::default());
    conn.with(|out| {
        out.open(1);
        assert!(out.data_lines(1, &vec![b'\n'; wire::MAX_PENDING]));
    });
    {
        let mut st = core.lock();
        core.subscribe(
            &mut st,
            Filter { terminal: Some("renamed/t1".into()), ..Default::default() },
            Sink::Framed { conn: conn.clone(), channel: 1 },
        );
        st.machines[0].name = "renamed".into();
        rename_mirror(&core, &mut st, &machine.id, "rohan", "renamed");
    }
    let mut bytes = Vec::new();
    conn.with(|out| {
        while let Some(frame) = out.next_frame() {
            bytes.extend_from_slice(&frame[wire::HEADER..]);
        }
    });
    let missed = bytes
        .split(|b| *b == b'\n')
        .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
        .find(|line| line["method"] == "missed")
        .expect("backpressure snapshot");
    assert_eq!(missed["params"]["terminals"][0]["address"], "renamed/t1");
    let removed: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
    let created: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
    assert_eq!(removed["params"]["terminal"], "rohan/t1");
    assert_eq!(removed["params"]["type"], "removed");
    assert_eq!(created["params"]["terminal"], "renamed/t1");
    assert_eq!(created["params"]["data"]["status"]["state"], "working");
    mirror_events(&core, &machine, &line, b"{\"method\":\"event\",\"params\":{\"type\":\"status\",\"terminal\":\"t1\",\"data\":{\"status\":{\"state\":\"blocked\"}}}}\n");
    let status: Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
    assert_eq!(status["params"]["terminal"], "renamed/t1");
    assert_eq!(core.lock().mirrors[&machine.id]["renamed/t1"]["status"]["state"], "blocked");
    core.lock().machines.clear();
    mirror_event(&core, &machine, json!({"type": "doc", "data": {"name": "app", "version": 1}}));
    assert!(rx.try_recv().is_err(), "removed machines must not emit doc events");
    drop(core);
    crate::paths::flush_later(); // the hc-files thread may still be writing into it
    crate::runtime::flush_saves(); // so may the hc-save thread (terminals.json)
    std::fs::remove_dir_all(root).unwrap();
}
