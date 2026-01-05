//send_rpc, run_rpc_server,handle_connectio
//
//research her this is THE MAIN FUCKNIG SHIT
//I HAVE TO SEND EVERYTHING TO EVERYTHING, WE NEED MORE CONCURRENCY NOT JUST COMMUNCATION

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use crate::raft::messages::RaftMsg;
use std::error::Error;

// THIS IS MY tCP LISteNER TASK
//TODO have to make this my listener for RPC
pub async fn rpc_handler(addr: &str, event_tx: mpsc::Sender<RaftEvent>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        println!("Connecton from {}", addr);
        let tx = event_tx.clone();
        tokio::spawn(handle_connection(stream, tx));
    }
}

pub async fn send_rpc(addr: &str, msg: RaftMsg) -> Result<RaftMsg, Box<dyn Error>> {
    let mut stream = TcpStream::connect(addr).await?;

    let bytes = bincode::serialize(&msg)?;

    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;

    read_rpc(&mut stream).await 
    
}
pub async fn read_rpc(stream: &mut TcpStream) -> Result<RaftMsg, Box<dyn Error>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let response: RaftMsg = bincode::deserialize(&buf)?; 
    Ok(response) 
    
}

pub async fn handle_connection(
    mut stream: TcpStream,
    event_tx: mpsc::Sender<(RaftMsg, oneshot::Sender<RaftMsg>)>
){
    // match makes error handling really easy
    let msg = match read_rpc(&mut stream).await {
        Ok(m) => m,
        Err(_) => return,
    };

    let (resp_tx, resp_rx) = oneshot::channel();
    if event_tx.send((msg, resp_tx)).await.is_err() { return; }

    if let Ok(response) = resp_rx.await {
        let bytes = bincode::serialize(&response).unwrap();
        let len = bytes.len() as u32;
        let _ = stream.write_all(&len.to_be_bytes()).await;
        let _ = stream.write_all(&bytes).await;
    }
}
