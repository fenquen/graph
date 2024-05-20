use std::{
    collections::HashMap,
    env,
    io::Error as IoError,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use anyhow::{anyhow, Result};
use futures::Sink;

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, SinkExt, stream::TryStreamExt, StreamExt};
use futures_util::stream::SplitSink;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use strum_macros::Display;

use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;
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
        log::debug!("received a new ws handshake");
        log::debug!("the request's path is: {}", req.uri().path());
        log::debug!("the request's headers are:");
        for (ref header, _value) in req.headers() {
            log::debug!("* {}: {:?}", header, _value);
        }

        let headers = response.headers_mut();
        headers.append("myCustomHeader", ":)".parse().unwrap());

        Ok(response)
    };

    let wsStream = tokio_tungstenite::accept_hdr_async(tcpStream, callback).await?;
    log::info!("ws connection established from: {}", remoteAddr);

    let (mut writeStream, mut readStream) = wsStream.split();

    let mut session = Session::new();

    loop {
        let readOption = readStream.next().await;
        if let None = readOption { // eof
            break;
        }

        let readResult = readOption.unwrap();
        if let Err(e) = readResult {
            log::info!("{:?}", anyhow::Error::new(e));
            break;
        }

        if let Message::Text(text) = readResult.unwrap() {
            if let Err(e) = a(&mut writeStream, &text, &mut session).await {
                // 使用debug会同时打印message和stack
                log::info!("{:?}",e);
            }
        }

        async fn a(writeStream: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
                   text: &str,
                   session: &mut Session<'_>) -> Result<()> {
            let deserialResult = serde_json::from_str::<GraphWsRequest>(text);
            if let Err(e) = deserialResult {
                return Err(anyhow::Error::new(GraphError::new(&e.to_string())));
            }

            let graphWsRequest = deserialResult.unwrap();
            match graphWsRequest.requestType {
                RequestType::ExecuteSql => {
                    if let None = graphWsRequest.sql {
                        return Ok(());
                    }

                    let sql = graphWsRequest.sql.unwrap();
                    if sql.is_empty() || sql.starts_with("--") {
                        return Ok(());
                    }

                    let selectResultToFront = session.executeSql(&sql)?;

                    // 如果sql是只有单个的小sql的话,那么[[]] 可以变为 []
                    let json = if selectResultToFront.len() == 1 {
                        serde_json::to_string(&selectResultToFront[0])?
                    } else {
                        serde_json::to_string(&selectResultToFront)?
                    };

                    writeStream.send(Message::Text(json)).await?;
                }
            }

            Ok(())
        }
    }

    log::info!("ws client:{} disconnected", &remoteAddr);

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