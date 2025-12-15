use crate::store::Database;
use crate::store;
use crate::clients::parse_command;
use std::fs::{OpenOptions, File};
use std::io::{BufRead, BufReader, Write};
use std::error::Error;
use crc32fast::Hasher;

struct WAL {
    log_file: File,
    log_flag: bool,
}

impl WAL {
    pub fn create_log() {
        let file_result = File::open("log.txt");
        let WAL::log_file = file_result; 
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


        let mut hasher = Hasher::new();
        let file_write = format!("SET {} {}\n", key, value).as_bytes();  
        let byte_size = file_write.len();
        hasher.update(file_write);
        let checksum = hasher.finalize();
//        println!("CRC32 checksum: {}", checksum); 
        file.write_all(format!("{} SET {} {} {}\n", byte_size, key, value, checksum).as_bytes())?;
        println!("Appended to log");
        Ok(())
    }
    pub fn log_del(key: &str, value: &str) -> Result<(), Box<dyn Error>>  {
        let mut file = OpenOptions::new()
            .append(true)
            .open("log.txt")?;
        
        let mut hasher = Hasher::new();
        let file_write = format!("DEL {} {}", key, value).as_bytes();  
        let byte_size = file_write.len();
        hasher.update(file_write);
        let checksum = hasher.finalize();
//        println!("CRC32 checksum: {}", checksum); 
        file.write_all(format!("{} DEL {} {} {}\n", byte_size, key, value, checksum).as_bytes())?;
        println!("Appended to log");
        Ok(())
    }

    // need to use a flag to know whether we append or reload, add handling
    // WE RELOAD WHENEVER LOGS DONT MATCH DATABASE? IM PRETTY SURE
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
        //let response = parse_log_command(&line_result,&db); //just implement clients?  YES YOU JUST
        let parts: Vec<&str> = line_result.split_whitespace().collect();
        let lastidx = parts.len() - 1;
        let payload = format!("{} {} {} ", parts[1], parts[2], parts[3..lastidx-1].join(" "));
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let checksum = hasher.finalize();
        println!("CRC32 checksum: {}", checksum); 
       
        if checksum != parts[lastidx] {
            eprintln!("Checksum doesn't match, Recheck Data for tampering."); // must add proper error handling
        }
        // verify checksum
    // we have to switch this doing the length check, then the reverse checksum, then we do the
    // reload operations. they have to be native to this code as i will get double logs if im
    // calling store ::, need to own it in here`    
        match parts[1].to_uppercase().as_str() {
            "SET" => {
            /*  if parts.len() < 3 {
                    return "ERROR: SET requires key and value".to_string();
                }*/
                let key = parts[2];
                let value = parts[3];
                store::set(&db, &key, &value, flag);
            }
            "DEL" => {
                let key = parts[3];
                store::delete(&db.clone(), &key, &value, flag);

            }
            _ => return,
    }
    }
                                                                //IMPLEMENT CLIENTS YOU FUCK WHEN YOU IMPORT
                                                                //YOU HAVE TO USE CLIENTS:: OR STORE:: FUCK
    }
    Ok(())
    }
    /*pub fn parse_log_command(command: &str, db: &Database) {
        let parts: Vec<&str> = command.split_whitespace().collect();
        let lastidx = parts.len() - 1;
        let payload = format!("{} {} {} ", parts[1], parts[2], parts[3..lastidx-1]).join(" ");
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let checksum = hasher.finalize();
        println!("CRC32 checksum: {}", checksum); 
       
        if checksum != parts[lastidx] {
            return; // must add proper error handling
        }
        // verify checksum
    // we have to switch this doing the length check, then the reverse checksum, then we do the
    // reload operations. they have to be native to this code as i will get double logs if im
    // calling store ::, need to own it in here`    
        match parts[1].to_uppercase().as_str() {
            "SET" => {
            /*  if parts.len() < 3 {
                    return "ERROR: SET requires key and value".to_string();
                }*/
                let key = parts[2];
                let value = parts[3];
                store::set(db, key, value);
            }
            "DEL" => {
                let key = parts[3];
                store::delete(db.clone(), key);

            }
            _ => return,
    }
    }*/
   

