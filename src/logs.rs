use crate::store;
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
    
    file.write_all("b\n {length} {checksum} SET {key} {value}")?;
    println!("Appended to log");
    Ok(())
}
pub fn log_get( key: &str, value: &str) -> Result<(), Box<dyn Error>> {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all("b\n {length} {checksum} GET {key} {value}")?;
    println!("Appended to log");
    Ok(())

}
pub fn log_del(key: &str, value: &str) -> Result<(), Box<dyn Error>>  {
    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    
    file.write_all("b\n {length} {checksum} DEL {key} {value}")?;
    println!("Appended to log");
    Ok(())
}

// this is the tricky part. how do i make it so that if my db detects the log file != empty, then
// it reloads all of the operations on the hash map
pub fn reload() -> Result<(), Box<dyn Error>> {
   let file = File::open("log.txt")?;
   parse_log(&file);
   Ok(())
}
pub fn parse_log(file: &File){
   let reader = BufReader::new(file);
   for line in reader.lines() {
    let mut buff = String::new();
    reader.read_line(&mut buff)?;

   }
}
