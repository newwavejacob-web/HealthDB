//send_rpc, run_rpc_server,handle_connectio
//
//research her this is THE MAIN FUCKNIG SHIT
//I HAVE TO SEND EVERYTHING TO EVERYTHING, WE NEED MORE CONCURRENCY NOT JUST COMMUNCATION

// THIS IS MY tCP LISteNER TASK
//TODO have to make this my listener for RPC
pub async fn rpc_handler(&self, event_tx: mpsc::Sender<RaftEvent>) {
    let listener = TcpListener::bind(...);
    loop {
        let stream = listener.accept();
        let msg = deserialize_from(stream);

        event_tx.send(RaftEvent::IncomingRpc(msg)).await;
    }
}


