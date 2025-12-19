// this is where i will write the code to connect different servers, making them #[cfg(test)]
enum NodeType(){
    leader: ,
    follower: ,
    candidate: ,
}
pub struct Node(){
    isUP: bool,
    isNodeType: NodeType,
    message: //TcpStream?
}


//lets use serde to serialize our shit. get the machines talking to one another.
pub fn node_listen(db: Database) {

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    println!("db listening on 127.0.0.1:6379");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
               
                std::thread::spawn(move || {
                // do some shit here
                })
            }
            Err(e) => eprintln!("Connection failed: {}", e),
        }
    }
}
pub fn node_talk(&mut stream: TcpStream, db: Database) {
    let reader = BufReader::new(stream.try_clone().unwrap());

    //error pattern match, common thread
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        
        //do some raft shit here
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(b"\n").unwrap();
    }

}
//learn how to use serde in rust
//shit in the real world just communicates by sending messages, tcp, http, request, response
//
