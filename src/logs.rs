use crate::store::Database;
use crate::clients;
use std::fs::{OpenOptions, File};
use std::io::{BufRead BufReader, Write};
use std::error::Error;

pub fn create_log() {
    let file_result = File::open("log.txt");

    match file_result {
        Ok(file) => println!("logfile already created"),
        Err(error) => let result = File::create("log.txt"),
    }
}
// call these in store.rs, when called it will append to the log file
pub fn log_set(key: &str, value: &str) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all("b\n  SET {key} {value}")?;
    println!("Appended to log");
    Ok(())
}
pub fn log_get( key: &str, value: &str) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all("b\n GET {key} {value}")?;
    println!("Appended to log");
    Ok(())

}
pub fn log_del(key: &str, value: &str) -> Result<(), Box<dyn Error>>  {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all("b\n DEL {key} {value}")?;
    println!("Appended to log");
    Ok(())
}

// this is the tricky part. how do i make it so that if my db detects the log file != empty, then
// it reloads all of the operations on the hash map
pub fn reload() -> Result<(), Box<dyn Error>> {
   let file = File::open("log.txt")?;
   parse_log(&file, &db);
   Ok(())
}
pub fn parse_log(file: &File, db: &Database){
   let reader = BufReader::new(file);
   for line in reader.lines() {
    let mut buff = String::new();
    reader.read_line(&mut buff)?;
    let response = parse_command(&line,&db); //just implement clients? 
   }
}
/* or wait, can i jsut do parse command and just reload everythiinig on teh database 
pub fn parse_log_command(command: &str, db: &Database){
    let parts: Vec<&str> = command.split_whitespace().collect();

    if parts.is_empty() {
        return "ERROR"
    }
}*/
