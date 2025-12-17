use crate::logs;
use std::fs::File;
use std::io::BufReader;
pub fn reload2 (&db)

let file = File::open("snapshot.txt");
let reader = BufReader::new(file);
    for line in reader.lines() {
        let line_result = line?;
        //let response = parse_log_command(&line_result,&db); //just implement clients?  YES YOU JUST
        let parts: Vec<&str> = line_result.split_whitespace().collect();
//no clue what this logic is gonna be like
        for part in parts {
            let db = format!("{} \n", parts[1]).as_bytes();
        }
}

let log_file = File::open("log.txt");
// then do 
logs::reload(log_file, &db);


}
}
