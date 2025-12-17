use crate::store::Database;
use crate::store;

use std::fs::{OpenOptions, File};
use std::io::{BufRead, BufReader, Write};
use std::error::Error;
use crc32fast::Hasher;

pub fn create_log(db: &Database) {
    let file_result = File::open("log.txt");
    // both arms of a match must return the same type
    match file_result {
        Ok(file) => {
            match reload(&file, &db){
                Ok(_) => println!("logfile already created, reload done"),
                Err(e) => eprintln!("Reload failed: {}", e),
            }
        }
        Err(_) => {
            File::create("log.txt");
            println!("created new Log file");
        }
    }
}
// call these in store.rs, when called it will append to the log file
pub fn log_set(key: &str, value: &str) -> Result<(), Box<dyn Error>> {
    println!("LOG_SET CALLED with key: {}, value: {}", key, value);
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;

    let mut hasher = Hasher::new();
    let file_write = format!("SET {} {}\n", key, value);  
    let byte_size = file_write.len();

    let file_write_check = file_write.as_bytes();  
    hasher.update(file_write_check);
    let checksum = hasher.finalize();
    println!("CRC32 checksum: {}", checksum); 

    file.write_all(format!("{} SET {} {} {}\n", byte_size, key, value, checksum).as_bytes())?;
    println!("Appended to log");
    Ok(())
}
pub fn log_del(key: &str) -> Result<(), Box<dyn Error>>  {
    println!("LOG_Del CALLED with key: {} ", key);
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    let mut hasher = Hasher::new();
    let file_write = format!("DEL {}\n", key);
    let byte_size = file_write.len();
    
    let file_write_check = file_write.as_bytes();  
    hasher.update(file_write_check);
    let checksum = hasher.finalize();
    println!("CRC32 checksum: {}", checksum); 

    file.write_all(format!("{} DEL {} {}\n", byte_size, key, checksum).as_bytes())?;
    println!("Appended to log");
    Ok(())
}

// need to use a flag to know whether we append or reload, add handling
// RELOAD WHENEVER LOGS DONT MATCH DATABASE? IM PRETTY SURE aka when its just new dont overthink it
pub fn reload(log_file: &File, db: &Database) -> Result<(), Box<dyn Error>> {
// let db = store::new(); // literally the error I JUST FUCKING GOT
// only needs to be done in main right? since be are taking the new shit and jsjt adding to it.
parse_log(&log_file, &db);
Ok(())
}
pub fn parse_log(file: &File, db: &Database)-> Result<(), Box<dyn Error>>{
let reader = BufReader::new(file);
    for line in reader.lines() {
        let line_result = line?;
        //let response = parse_log_command(&line_result,&db); //just implement clients?  YES YOU JUST
        let parts: Vec<&str> = line_result.split_whitespace().collect();

        let lastidx = parts.len() - 1;
        match parts[1].to_uppercase().as_str() {
            "SET" => {

                let payload_string = format!("{} {} {}\n", parts[1], parts[2], parts[3..lastidx].join(" "));
                let payload = payload_string.as_bytes();

                let mut hasher = Hasher::new();
                hasher.update(payload);
                let checksum = hasher.finalize();
                println!("CRC32 checksum: {}", checksum); 

                let write_check = parts[lastidx].parse::<u32>()?;
                if checksum != write_check {
                    eprintln!("Checksum doesn't match, Recheck Data for tampering."); // must add proper error handling
                    continue;
                }

                let key = parts[2].to_string();
                let value = parts[3].to_string();
                let flag = false;

                store::set(&db, key, value, flag);

            }
            "DEL" => {

                let payload_string = format!("{} {}\n", parts[1], parts[2..lastidx].join(" "));
                let payload = payload_string.as_bytes();

                let mut hasher = Hasher::new();
                hasher.update(payload);
                let checksum = hasher.finalize();

                println!("CRC32 checksum: {}", checksum); 
                let write_check = parts[lastidx].parse::<u32>()?;
                if checksum != write_check {
                    eprintln!("Checksum doesn't match, Recheck Data for tampering."); // must add proper error handling
                    continue;
                }

                let key = parts[2];
                let flag = false;
                store::delete(&db.clone(), key, flag);
            }
            _ => eprintln!("Wall reload complete"),
        }
    }
    Ok(())
}

