mod raft;
use std::thread;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: raft_node <node_id>");
        return;
    }
    let node_id = args[1].parse::<u64>().unwrap();

    match node_id {
        "1" => {
            thread::spawn {||
                raft::heartbeat("127.0.0.1:5001");

            }

        }
        "2" => {
            thread::spawn { ||
                raft::detect_heartbeat("127.0.0.1:5001")
            }
        }
        _ => {
            eprintln!("not enough nodes yet");

        }
    }
    loop {
    thread::sleep(Duration::from_secs(1));
    }
}
