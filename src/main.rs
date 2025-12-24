// GOAL IS TO GET A LOCK FREE VERISON WORKING by the end of 2025

mod store;
mod server;
mod clients;
mod logs;
//mod raft;

fn main() {
    let db = store::new();
    logs::create_log(&db);
    server::run(db);
}
