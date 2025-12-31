mod raft;

#[tokio::test]
async fn main(){
    // so how do we test our raft code?
    // spawn some nodes and try to get em talking??
    
    let addr1 = "127.0.0.1:5001";
    let addr2 = "127.0.0.1:5002";
    let addr3 = "127.0.0.1:5003";

    let peers = vec![
        addr1.to_string();
        addr2.to_string();
        addr3.to_string();
    ]

        let args: Vec<string> = std::env::args().collect();
        let my_port = &args[1];

        let addr = format!("127.0.0.1:{}", my_port);

        //spawn listener: (rpc_handler()), and try sending a message
}
