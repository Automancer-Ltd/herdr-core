#![cfg(unix)] // Windows: tests/windows.rs

mod common;

use common::*;

#[test]
fn a_file_lands_beside_the_terminal_keeps_its_name_and_goes_with_it() {
    let rohan = Session::new();
    let mut s = Session::new();
    s.env.push(fake_ssh(&s, &[("rohan", &rohan)]));
    s.json(&["machine", "add", "rohan"]);
    let image: Vec<u8> = (0..=255u8).cycle().take(300_000).collect();
    let file = s.root.join("shot.png");
    std::fs::write(&file, &image).unwrap();

    let here = run(&s, &["sleep", "100"]);
    let first = s.json(&["put", &here, file.to_str().unwrap()]);
    let path = first["path"].as_str().unwrap();
    assert!(path.starts_with(&s.root.join("state").display().to_string()) && path.ends_with("/shot.png"), "{first}");
    assert_eq!((std::fs::read(path).unwrap(), first["bytes"].as_u64()), (image.clone(), Some(300_000)));
    let second = s.json(&["put", &here, file.to_str().unwrap()]);
    assert!(second["path"].as_str().unwrap().ends_with("/shot-2.png"), "{second}");

    let there = run(&s, &["--on", "rohan", "sleep", "100"]);
    let afar = s.json(&["put", &there, file.to_str().unwrap()]);
    let afar_path = afar["path"].as_str().unwrap();
    assert!(afar_path.starts_with(&rohan.root.display().to_string()), "lands on rohan: {afar}");
    assert_eq!(std::fs::read(afar_path).unwrap(), image);

    s.json(&["kill", &here]);
    s.json(&["rm", &here]);
    wait_until("file removed with the terminal", || !std::path::Path::new(path).exists());

    std::fs::write(&file, vec![0u8; 9 << 20]).unwrap();
    assert_eq!(
        s.err(&["--json", "put", &there, file.to_str().unwrap()]).1["error"]["kind"],
        "invalid_params",
        "too big"
    );
    // Too big even to carry to rohan: refused here, and the line lives on.
    std::fs::write(&file, vec![0u8; 13 << 20]).unwrap();
    assert_eq!(s.err(&["--json", "put", &there, file.to_str().unwrap()]).1["error"]["kind"], "invalid_params");
    std::fs::write(&file, b"small").unwrap();
    s.json(&["put", &there, file.to_str().unwrap()]);
}
