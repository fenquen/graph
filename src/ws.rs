use std::{
    collections::HashMap,
    env,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use anyhow::{anyhow, Result};

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, stream::TryStreamExt, StreamExt};
use log::debug;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use strum_macros::Display;

use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use crate::graph_error::GraphError;
use crate::graph_value::GraphValue;
use crate::session::Session;

#[derive(Serialize, Deserialize)]
pub struct GraphWsRequest {
    pub requestType: RequestType,
    pub sql: Option<String>,
}

impl GraphWsRequest {}

#[derive(Serialize, Deserialize)]
// #[serde(untagged)] // 如果使用的话 对应的json变为null
pub enum RequestType {
    ExecuteSql
}

pub async fn init() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9673").await?;

    while let Ok((tcpStream, remoteAddr)) = listener.accept().await {
        tokio::spawn(handleConn(tcpStream, remoteAddr));
    }

    Ok(())
}

pub trait MessageExtGraph {
    fn readText(&self);
}

async fn handleConn(tcpStream: TcpStream, remoteAddr: SocketAddr) -> Result<()> {
    let callback = |req: &Request, mut response: Response| {
        debug!("received a new ws handshake");
        debug!("the request's path is: {}", req.uri().path());
        debug!("the request's headers are:");
        for (ref header, _value) in req.headers() {
            debug!("* {}: {:?}", header, _value);
        }

        let headers = response.headers_mut();
        headers.append("MyCustomHeader", ":)".parse().unwrap());

        Ok(response)
    };

    let wsStream = tokio_tungstenite::accept_hdr_async(tcpStream, callback).await?;
    println!("ws connection established from: {}", remoteAddr);


    let (writeStream, mut readStream) = wsStream.split();

    loop {
        let readOption = readStream.next().await;
        if let None = readOption { // eof
            continue;
        }

        let readResult = readOption.unwrap();
        if let Err(e) = readResult {
            println!("{}", anyhow::Error::msg(e));
            break;
        }

        if let Message::Text(text) = readResult.unwrap() {
            let deserialResult = serde_json::from_str::<GraphWsRequest>(&text);

            if let Err(e) = deserialResult {
                // 使用debug会同时打印message和stack
                println!("{:?}", anyhow::Error::new(GraphError::new(&e.to_string())));
                continue;
            }

            let graphWsRequest = deserialResult.unwrap();
            match graphWsRequest.requestType {
                RequestType::ExecuteSql => {
                    if let None = graphWsRequest.sql {
                        continue;
                    }

                    let sql = graphWsRequest.sql.unwrap();

                    if sql.is_empty() || sql.starts_with("--") {
                        continue;
                    }

                    let mut session = Session::new();

                    match session.executeSql(&sql) {
                        Ok(selectResultToFront) => {
                            let a = serde_json::to_string(&selectResultToFront)?;
                        }
                        Err(error) => println!("{:?}",error),
                    }
                }
            }
        }
    }

    // let readSend = readStream.try_for_each(|message| {
    //   println!("received a message from {}: {}", remoteAddr, message.to_text()?);
    // future::ok(())
    //});

    println!("{} disconnected", &remoteAddr);

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::graph_error::GraphError;
    use crate::graph_value::GraphValue;
    use crate::ws::{GraphWsRequest, RequestType};
    use anyhow::Result;

    #[test]
    pub fn a() {
        println!("{}", serde_json::to_string(&GraphWsRequest {
            requestType: RequestType::ExecuteSql,
            sql: Some("aaaa".to_string()),
        }).unwrap());
    }
}