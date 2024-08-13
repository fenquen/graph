#![allow(non_snake_case)]

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Result;
use lazy_static::lazy_static;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use crate::{config, raft};
use crate::raft::GraphRaftNode;

const MTU_SIZE: usize = 1500;

pub fn init() -> Result<(JoinHandle<Result<()>>, JoinHandle<Result<()>>)> {
    let multicastHostIpv4 = Ipv4Addr::from_str(&config::CONFIG.raftConfig.multicastHost)?;
    let multicastAddr = SocketAddrV4::new(multicastHostIpv4, config::CONFIG.raftConfig.multicastPort);

    let udpSocket = buildSocket()?;
    udpSocket.join_multicast_v4(&multicastHostIpv4, &Ipv4Addr::from_str(&config::CONFIG.raftConfig.multicastInterfaceHost)?)?;

    let udpSocket = Arc::new(udpSocket);

    let c = udpSocket.clone();
    let joinHandleSender =
        thread::Builder::new().name("multicast-sender".to_string()).spawn(move || {
            let jsonByte = serde_json::to_vec(raft::THIS_GRAPH_NODE.deref())?;

            loop {
                c.send_to(jsonByte.as_slice(), multicastAddr.to_string())?;
                thread::sleep(Duration::from_millis(1000));
            }

            Result::<()>::Ok(())
        })?;

    let joinHandleReceiver =
        thread::Builder::new().name("multicast-receiver".to_string()).spawn(move || {
            let mut buf = [0; MTU_SIZE];

            loop {
                let (len, _) = udpSocket.recv_from(&mut buf).unwrap();

                let raftNode = serde_json::from_slice::<GraphRaftNode>(&buf[..len])?;
                raft::ONLINE_RAFT_ID_RAFT_NODE.write().unwrap().insert(raftNode.id, raftNode);

                // log::info!("receive: {}", String::from_utf8_lossy(&buf[..len]).deref());

                thread::sleep(Duration::from_millis(1000));
            }

            Result::<()>::Ok(())
        })?;

    Ok((joinHandleSender, joinHandleReceiver))
}

fn buildSocket() -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_multicast_if_v4(&Ipv4Addr::from_str(&config::CONFIG.raftConfig.multicastInterfaceHost)?)?;
    // socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    socket.set_nonblocking(false)?;
    socket.bind(&SockAddr::from(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), config::CONFIG.raftConfig.multicastPort)))?;
    Ok(socket.into())
}