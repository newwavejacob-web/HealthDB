//send_rpc, run_rpc_server,handle_connectio
//
//research her this is THE MAIN FUCKNIG SHIT
//I HAVE TO SEND EVERYTHING TO EVERYTHING, WE NEED MORE CONCURRENCY NOT JUST COMMUNCATION

use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep, timeout};
use crate::raft::messages::RaftMsg;
use crate::tls::{Tls, server_name};
use std::error::Error;
use std::sync::Arc;

pub type RpcError = Box<dyn Error + Send + Sync>;

// these have to fit inside one heartbeat interval (125ms) so a dead or slow peer can't
// stall the leader long enough for followers to time out: 3 x 30ms + 5ms + 10ms backoff = 105ms
pub const RPC_TIMEOUT: Duration = Duration::from_millis(30);
pub const RPC_ATTEMPTS: u32 = 3;
const RETRY_BACKOFF: Duration = Duration::from_millis(5);

// one request/response, each attempt bounded by RPC_TIMEOUT, retried with exponential backoff.
// with tls, every attempt is a mutually-authenticated TLS connection.
pub async fn send_rpc(addr: &str, msg: RaftMsg, tls: Option<&Tls>) -> Result<RaftMsg, RpcError> {
    let mut backoff = RETRY_BACKOFF;
    let mut attempt = 1;
    loop {
        let err: RpcError = match timeout(RPC_TIMEOUT, send_rpc_once(addr, &msg, tls)).await {
            Ok(Ok(response)) => return Ok(response),
            Ok(Err(e)) => e,
            Err(_) => format!("rpc to {} timed out after {:?}", addr, RPC_TIMEOUT).into(),
        };
        if attempt == RPC_ATTEMPTS {
            return Err(format!("{} (gave up after {} attempts)", err, RPC_ATTEMPTS).into());
        }
        sleep(backoff).await;
        backoff *= 2;
        attempt += 1;
    }
}

async fn send_rpc_once(addr: &str, msg: &RaftMsg, tls: Option<&Tls>) -> Result<RaftMsg, RpcError> {
    let stream = TcpStream::connect(addr).await?;
    match tls {
        Some(tls) => {
            let mut stream = tls.connector.connect(server_name(addr)?, stream).await?;
            exchange(&mut stream, msg).await
        }
        None => {
            let mut stream = stream;
            exchange(&mut stream, msg).await
        }
    }
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, msg: &RaftMsg) -> Result<RaftMsg, RpcError> {
    write_rpc(stream, msg.clone()).await?;
    read_rpc(stream).await
}

// sends every (peer, msg) at once and returns the results once they've all finished or
// given up, so the slowest peer bounds a round instead of the sum of all of them.
pub async fn broadcast(requests: Vec<(String, RaftMsg)>, tls: Option<Arc<Tls>>) -> Vec<(String, Result<RaftMsg, RpcError>)> {
    let mut set = JoinSet::new();
    for (peer, msg) in requests {
        let tls = tls.clone();
        set.spawn(async move {
            let result = send_rpc(&peer, msg, tls.as_deref()).await;
            (peer, result)
        });
    }
    let mut results = Vec::new();
    while let Some(joined) = set.join_next().await {
        if let Ok(result) = joined {
            results.push(result);
        }
    }
    results
}

// the sender may have already timed out and hung up, so a failed write is not fatal
pub async fn write_rpc<S: AsyncWrite + Unpin>(stream: &mut S, msg: RaftMsg) -> Result<(), RpcError> {
    let bytes = bincode::serialize(&msg)?;
    let len = bytes.len() as u32;

    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    // a TLS stream buffers, so push the frame out before waiting on the reply
    stream.flush().await?;
    Ok(())
}

// incoming side gets the same deadline so a peer that connects and stalls can't block the node
pub async fn read_rpc_with_timeout<S: AsyncRead + Unpin>(stream: &mut S) -> Result<RaftMsg, RpcError> {
    timeout(RPC_TIMEOUT, read_rpc(stream)).await.map_err(|_| -> RpcError { "timed out reading rpc".into() })?
}

pub async fn read_rpc<S: AsyncRead + Unpin>(stream: &mut S) -> Result<RaftMsg, RpcError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let response: RaftMsg = bincode::deserialize(&buf)?; 
    Ok(response) 
}

// THIS IS MY tCP LISteNER TASK
//TODO have to make this my listener for RPC
/*pub async fn run_listener(addr: &str, event_tx: mpsc::Sender<RaftEvent>) {
    let listener = TcpListener::bind(addr).await.unwrap();
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        println!("Connecton from {}", addr);
        let tx = event_tx.clone();
        tokio::spawn(handle_connection(stream, tx));
    }
}
// use this channel approach from massively conncurent rpcs. work on this when you have time.
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
}*/

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::messages::RequestVoteResponse;
    use tokio::net::TcpListener;
    use std::time::Instant;

    fn vote_reply() -> RaftMsg {
        RaftMsg::RequestVoteResponse(RequestVoteResponse { current_term: 7, voted: true })
    }

    #[tokio::test]
    async fn gives_up_on_a_peer_that_never_answers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // accept connections and hold them open without ever replying
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop { held.push(listener.accept().await.unwrap().0); }
        });

        let start = Instant::now();
        let result = send_rpc(&addr, vote_reply(), None).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(elapsed >= RPC_TIMEOUT * RPC_ATTEMPTS);
        assert!(elapsed < Duration::from_millis(125), "took {:?}, longer than a heartbeat", elapsed);
    }

    #[tokio::test]
    async fn retries_after_a_dropped_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            // first attempt: hang up without replying
            drop(listener.accept().await.unwrap());
            // second attempt: answer properly
            let (mut stream, _) = listener.accept().await.unwrap();
            read_rpc(&mut stream).await.unwrap();
            write_rpc(&mut stream, vote_reply()).await.unwrap();
        });

        match send_rpc(&addr, vote_reply(), None).await {
            Ok(RaftMsg::RequestVoteResponse(r)) => assert_eq!(r.current_term, 7),
            other => panic!("expected a vote response, got {:?}", other.map_err(|e| e.to_string())),
        }
    }
}
