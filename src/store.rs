// actually storage operations??
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::logs;

/*pub struct Database {
    db: Arc<Mutex<HashMap<String, String>>>,// isLoggable: bool,} */
                                            //
pub type Database = Arc<Mutex<HashMap<String, String>>>;

//impl Database {
pub fn new() -> Database {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(db: &Database, key: String, value: String, log_flag: bool) {
    println!("store::set called with log_flag: {}", log_flag);
    if log_flag {
        logs::log_set(&key, &value).unwrap();
    }
        let mut map = db.lock().unwrap();
        map.insert(key, value);
}
//option return type is how we get some and none if get works or not
pub fn get(db: &Database, key: &str) -> Option<String> {
    println!("store::get called");
    let map = db.lock().unwrap();
    map.get(key).cloned() 
}
pub fn delete(db: &Database, key: &str, log_flag: bool) -> bool {
    println!("store::del called with log_flag: {}", log_flag);
    if log_flag {
        logs::log_del(&key);
        raft::replication::make_append_entries(state);
    }
        let mut map = db.lock().unwrap();
        map.remove(key).is_some()
}
//}

/ actually storage operations??
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::logs;

/*pub struct Database {
    db: Arc<Mutex<HashMap<String, String>>>,// isLoggable: bool,} */
                                            //
pub type Database = Arc<Mutex<HashMap<String, String>>>;

//impl Database {
pub fn new() -> Database {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn set(db: &Database, key: String, value: String, log_flag: bool) {
    println!("store::set called with log_flag: {}", log_flag);
    if log_flag {
        logs::log_set(&key, &value).unwrap();
    }
        let mut map = db.lock().unwrap();
        map.insert(key, value);
}
//option return type is how we get some and none if get works or not
pub fn get(db: &Database, key: &str) -> Option<String> {
    println!("store::get called");
    let map = db.lock().unwrap();
    map.get(key).cloned() 
}
pub fn delete(db: &Database, key: &str, log_flag: bool) -> bool {
    println!("store::del called with log_flag: {}", log_flag);
    if log_flag {
        logs::log_del(&key);
        raft::replication::make_append_entries(state);
    }
        let mut map = db.lock().unwrap();
        map.remove(key).is_some()
}
//}

