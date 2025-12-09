// main server side database
use std::net::TcpListener;
use std::sync::Arc;
use crate::store::Database;
use crate::clients;

pub fn run(db: Database) {

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("db listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // this is of vital importance, clone 
                // creates a copy of the db reference, &db
                // the move keyword is then used with spawn 
                // to handle multiple threads concurrently
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    clients::read_stream(stream, db);
                });
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}
