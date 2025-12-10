// GOAL IS TO GET A LOCK FREE VERISON WORKING by the end of 2025


mod store;
mod server;
mod clients;
mod logs;

fn main() {
    let db = store::new();
    server::run(db);
}
