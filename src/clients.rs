// this is my client handling file 

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use crate::store::{self, Database};


pub fn read_stream(mut stream: TcpStream, db: Database){ 

    let reader = BufReader::new(stream.try_clone().unwrap());

    //error pattern match, common thread
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        
        let response = parse_command(&line, &db);
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    }
}
pub fn parse_command(command: &str, db: &Database) -> String {
    let parts: Vec<&str> = command.split_whitespace().collect();

    if parts.is_empty() {
        return "ERROR: empty command".to_string();
    }
    
    match parts[0].to_uppercase().as_str() {
        "SET" => {
            if parts.len() < 3 {
                return "ERROR: SET requires key and value".to_string();
            }
            let key = parts[1].to_string();
            let value = parts[2..].join(" ");
            let flag = true;
            store::set(db, key, value,flag);
            "OK".to_string()
        }
        "GET" => {
            if parts.len() < 2 {
                return "ERROR: GET requires key".to_string();
            }
            let key = parts[1];
            match store::get(db, key){
                Some(value) => value,
                None => "NIL".to_string(),
            }
            
        }
        "DEL" => {
            if parts.len() < 2 {
                return "ERROR: DEL requires key".to_string();
            }
            let key = parts[1];
            let flag = true;
            if store::delete(&db.clone(), key, flag){
                "OK".to_string()
            }                    
            else {
                "NIL".to_string()
            }

        }
        _ => format!("ERROR: unknown command '{}'", parts[0]),
    }
}

/*pub fn write_response(&self) -> Option<>{

}*/

