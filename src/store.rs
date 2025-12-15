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
    if log_flag {
        logs::log_set(&key, &value)?;
    }
        let mut map = db.lock().unwrap();
        map.insert(key, value);
}
//option return type is how we get some and none if get works or not
pub fn get(db: &Database, key: &str) -> Option<String> {
    let map = db.lock().unwrap();
    map.get(key).cloned() 
}
pub fn delete(db: &Database, key: &str, value: &str, log_flag: bool) -> bool {
    if log_flag {
        logs::log_del(&key, &value)?;
    }
        let mut map = db.lock().unwrap();
        map.remove(key).is_some()
}
//}

