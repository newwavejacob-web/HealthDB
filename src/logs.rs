use crate::store::Database;
use crate::store;
use crate::clients::parse_command;
use std::fs::{OpenOptions, File};
use std::io::{BufRead, BufReader, Write};
use std::error::Error;

pub fn create_log() {
    let file_result = File::open("log.txt");
    
    // both arms of a match must return the same type
    match file_result {
        Ok(file) => println!("logfile already created"),
        Err(_) => {
            File::create("log.txt");
            println!("created new Log file");
        }
    }
}
// call these in store.rs, when called it will append to the log file
pub fn log_set(key: &str, value: &str) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all(format!("SET {} {}\n", key, value).as_bytes())?;
    println!("Appended to log");
    Ok(())
}
pub fn log_del(key: &str, value: &str) -> Result<(), Box<dyn Error>>  {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all(format!("DEL {} {}\n", key, value).as_bytes())?;
    println!("Appended to log");
    Ok(())
}

// this is the tricky part. how do i make it so that if my db detects the log file != empty, then
// it reloads all of the operations on the hash map

pub fn reload() -> Result<(), Box<dyn Error>> {
   let file = File::open("log.txt")?;
   let db = store::new(); // literally the error I JUST FUCKING GOT 
   parse_log(&file, &db);
   Ok(())
}
pub fn parse_log(file: &File, db: &Database)-> Result<(), Box<dyn Error>>{
   let reader = BufReader::new(file);
   for line in reader.lines() {
    let line_result = line?;
    let response = parse_log_command(&line_result,&db); //just implement clients?  YES YOU JUST
                                                            //IMPLEMENT CLIENTS YOU FUCK WHEN YOU IMPORT
                                                            //YOU HAVE TO USE CLIENTS:: OR STORE:: FUCK
   }
   Ok(())
}
pub fn parse_log_command(command: &str, db: &Database) {
    let parts: Vec<&str> = command.split_whitespace().collect();

   /* if parts.is_empty() {
        return "ERROR: empty command".to_string();
    }*/
    
    match parts[0].to_uppercase().as_str() {
        "SET" => {
          /*  if parts.len() < 3 {
                return "ERROR: SET requires key and value".to_string();
            }*/
            let key = parts[1].to_string();
            let value = parts[2..].join(" ");
            store::set(db, key, value);
        }
        "DEL" => {
            let key = parts[1];

            store::delete(db.clone(), key);

        }
        _ => return,
   }
}
/* or wait, can i jsut do parse command and just reload everythiinig on teh database 
 * NO, we actually can't. if we call the normal commands, we will jsut be double logging, so we
 * must write our own logic.
}*/
