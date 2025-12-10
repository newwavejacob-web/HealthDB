// actually storage operations??
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub type Database = Arc<Mutex<HashMap<String, String>>>;
// i gotta write every operation in here
// instead of saving the hashmap every time, we save the operations we do to the hashmap to load
// back in

pub fn new() -> Database {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(db: &Database, key: String, value: String) {
    let mut map = db.lock().unwrap();
    map.insert(key, value);
}
//option return type is how we get some and none if get works or not
pub fn get(db: &Database, key: &str) -> Option<String> {
    let map = db.lock().unwrap();
    map.get(key).cloned() 
}
pub fn delete(db: Database, key: &str) -> bool {
    let mut map = db.lock().unwrap();
    map.remove(key).is_some() 
}
/*
            //set main body
            let key = parts[1].to_string();
            let value = parts[2..].join(" ");
            
            let mut map = db.lock().unwrap();
            map.insert(key, value);
            "OK".to_string()
            let key = parts[1];

            //get main body 
            let map = db.lock().unwrap();
            match map.get(key) {
                Some(value) => value.clone(),
                None => "NIL".to_string(),
            }

            // delete main body code
            let key = parts[1];
            let mut map = db.lock().unwrap();
            match map.remove(key) {
                Some(_) => "OK".to_string(),
                None => "NIL".to_string(),
            }
*/
