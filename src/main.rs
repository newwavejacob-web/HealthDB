// GOAL IS TO GET A LOCK FREE VERISON WORKING by the end of 2025


mod store;
mod server;
mod clients;


fn main() {
    let db = store::new();
    server::run(db);
}
/*
fn handle_client(mut stream: TcpStream, db: Database) {
    //unwrap and read our cloned stream
    let reader = BufReader::new(stream.try_clone().unwrap());
   
    //error pattern match, common thread
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        
        let response = process_command(&line, &db);
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    }
}

fn process_command(command: &str, db: &Database) -> String {
    //collect is an iterator used to collect some given thing or collection and put it in another
    //collections
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
            
            let mut map = db.lock().unwrap();
            map.insert(key, value);
            "OK".to_string()
        }
        "GET" => {
            if parts.len() < 2 {
                return "ERROR: GET requires key".to_string();
            }
            let key = parts[1];
            
            let map = db.lock().unwrap();

            match map.get(key) {
                Some(value) => value.clone(),
                None => "NIL".to_string(),
            }
        }
        "DEL" => {
            if parts.len() < 2 {
                return "ERROR: DEL requires key".to_string();
            }
            let key = parts[1];
            
            let mut map = db.lock().unwrap();
            match map.remove(key) {
                Some(_) => "OK".to_string(),
                None => "NIL".to_string(),
            }
        }
        _ => format!("ERROR: unknown command '{}'", parts[0]),
    }
};
*/
