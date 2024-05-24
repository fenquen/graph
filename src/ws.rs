use std::{collections::HashMap, env, fmt, io::Error as IoError, net::SocketAddr, sync::{Arc, Mutex}};
use std::fmt::{Display, Formatter};
use anyhow::{anyhow, Result};
use futures::Sink;

use futures_channel::mpsc::{unbounded, UnboundedSender};
use futures_util::{future, pin_mut, SinkExt, stream::TryStreamExt, StreamExt};
use futures_util::stream::SplitSink;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::WebSocketStream;
use crate::command_executor::SelectResultToFront;
use crate::{config, parser, throw};
use crate::graph_error::GraphError;
use crate::graph_value::GraphValue;
use crate::session::Session;

#[derive(Deserialize, Serialize)]
pub struct GraphWsRequest {
    pub requestType: RequestType,
    pub sql: Option<String>,
}

impl GraphWsRequest {}

#[derive(Serialize, Deserialize)]
// #[serde(untagged)] // 如果使用的话 对应的json变为null
pub enum RequestType {
    ExecuteSql,
    Begin,
    TestParser,
}

#[derive(Serialize, Deserialize, Default)]
pub struct GraphWsResponse {
    success: bool,
    errorMsg: Option<String>,
    data: Option<SelectResultToFront>,
}

impl GraphWsResponse {
    pub fn success() -> GraphWsResponse {
        GraphWsResponse {
            success: true,
            ..Default::default()
        }
    }

    pub fn fail(errorMsg: &impl ToString) -> GraphWsResponse {
        GraphWsResponse {
            errorMsg: Some(errorMsg.to_string()),
            ..Default::default()
        }
    }

    pub fn successWithData(data: SelectResultToFront) -> GraphWsResponse {
        GraphWsResponse {
            success: true,
            errorMsg: None,
            data: Some(data),
        }
    }
}

impl Display for GraphWsResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match serde_json::to_string(self) {
            Ok(s) => write!(f, "{}", s),
            Err(e) => {
                log::info!("{:?}",e);
                Err(fmt::Error)
            }
        }
    }
}

pub async fn init() -> Result<()> {
    let listener = TcpListener::bind(config::CONFIG.wsAddr.as_str()).await?;

    log::info!("server started, ws listen on: {}",config::CONFIG.wsAddr);

    while let Ok((tcpStream, remoteAddr)) = listener.accept().await {
        tokio::spawn(processConn(tcpStream, remoteAddr));
    }

    Ok(())
}

async fn processConn(tcpStream: TcpStream, remoteAddr: SocketAddr) -> Result<()> {
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
            if let Err(e) = processGraphWsRequest(&mut writeStream, &text, &mut session, &remoteAddr).await {
                // 使用debug会同时打印message和stack
                log::info!("{:?}", e);
                writeStream.send(Message::Text(GraphWsResponse::fail(&e).to_string())).await?;
            }
        }

        async fn processGraphWsRequest(writeStream: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
                                       text: &str,
                                       session: &mut Session<'_>,
                                       remoteAddr: &SocketAddr) -> Result<()> {
            let graphWsRequest = serde_json::from_str::<GraphWsRequest>(text);
            if let Err(e) = graphWsRequest {
                return Err(anyhow::Error::new(GraphError::new(&e.to_string())));
            }

            let graphWsRequest = graphWsRequest.unwrap();

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

                    writeStream.send(Message::Text(GraphWsResponse::successWithData(selectResultToFront).to_string())).await?;
                }
                RequestType::TestParser => {
                    if remoteAddr.ip().is_loopback() == false {
                        throw!("test parser request can only be from localhost");
                    }

                    if let None = graphWsRequest.sql {
                        return Ok(());
                    }

                    let sql = graphWsRequest.sql.unwrap();
                    if sql.is_empty() || sql.starts_with("--") {
                        return Ok(());
                    }

                    parser::parse(&sql)?;

                    writeStream.send(Message::Text(GraphWsResponse::success().to_string())).await?;
                }
                _ => {}
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
    use crate::ws::{GraphWsRequest, GraphWsResponse, RequestType};
    use anyhow::Result;

    #[test]
    pub fn a() {
        println!("{}", serde_json::to_string(&GraphWsRequest {
            requestType: RequestType::ExecuteSql,
            sql: Some("aaaa".to_string()),
        }).unwrap());
    }
}