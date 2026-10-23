use crate::raft::LogEntry;

use crc32fast::Hasher;

// call these in store.rs, when called it will append to the log file
pub fn log_set(key: &str, value: &str, term: u64) -> LogEntry {
    println!("LOG_SET CALLED with key: {}, value: {}", key, value);
/*    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;


    file.write_all(format!("{} SET {} {} {}\n", byte_size, key, value, checksum).as_bytes())?;
    println!("Appended to log");*/

    let mut hasher = Hasher::new();
    let file_write = format!("SET {} {}\n", key, value);  
    let byte_size = file_write.len();

    let file_write_check = file_write.as_bytes();  
    hasher.update(file_write_check);
    let checksum = hasher.finalize();
    println!("CRC32 checksum: {}", checksum); 

    LogEntry {
        data: format!("{} SET {} {} {}\n", byte_size, key, value, checksum).as_bytes().to_vec(),
        term, // ?? what is this how do i find this
    }
}
pub fn log_del(key: &str, term: u64) -> LogEntry  {
    println!("LOG_Del CALLED with key: {} ", key);
/*    let mut file = OpenOptions::new()
        .append(true)
        .open("log.txt")?;
    

    file.write_all(format!("{} DEL {} {}\n", byte_size, key, checksum).as_bytes())?;
    println!("Appended to log");
*/
    let mut hasher = Hasher::new();
    let file_write = format!("DEL {}\n", key);
    let byte_size = file_write.len();
    
    let file_write_check = file_write.as_bytes();  
    hasher.update(file_write_check);
    let checksum = hasher.finalize();
    println!("CRC32 checksum: {}", checksum); 

    LogEntry {
        data: format!("{} DEL {} {}\n", byte_size, key, checksum).as_bytes().to_vec(),
        term, // ?? what is this how do i find this
              // i think i have to send the data over to the individual servers then manipulate it
              // reasonably. Have to make sure the servers insert the term into the struct once we
              // pass it over to raft. 
    }
}

// written by a new leader at the start of its term. applying it does nothing.
pub fn log_noop(term: u64) -> LogEntry {
    let mut hasher = Hasher::new();
    let file_write = "NOOP\n";
    hasher.update(file_write.as_bytes());
    let checksum = hasher.finalize();

    LogEntry {
        data: format!("{} NOOP {}\n", file_write.len(), checksum).as_bytes().to_vec(),
        term,
    }
}

// checks a stored entry ("<len> <payload> <crc>\n") against its length and CRC32.
// the payload is sliced out exactly rather than re-joined from tokens, so the check
// doesn't depend on how the value was spaced.
pub fn checksum_ok(data: &str) -> bool {
    let line = match data.strip_suffix('\n') {
        Some(l) => l,
        None => return false,
    };
    let (len, rest) = match line.split_once(' ') {
        Some(p) => p,
        None => return false,
    };
    let (payload, crc) = match rest.rsplit_once(' ') {
        Some(p) => p,
        None => return false,
    };
    let payload = format!("{}\n", payload);

    let mut hasher = Hasher::new();
    hasher.update(payload.as_bytes());
    len.parse::<usize>().ok() == Some(payload.len()) && crc.parse::<u32>().ok() == Some(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksums_round_trip() {
        for entry in [log_set("k", "two words", 1), log_del("k", 1), log_noop(1)] {
            assert!(checksum_ok(std::str::from_utf8(&entry.data).unwrap()));
        }
    }

    #[test]
    fn checksum_catches_tampering() {
        let entry = log_set("k", "v", 1);
        let tampered = String::from_utf8(entry.data).unwrap().replace(" v ", " x ");
        assert!(!checksum_ok(&tampered));
    }
}
