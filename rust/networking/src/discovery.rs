use std::{
    io,
    net::{Ipv6Addr, SocketAddr, SocketAddrV6},
    sync::Arc,
    time::Duration,
};

use bytemuck::{Pod, Zeroable};
use parking_lot::Mutex;
use tokio::{net::UdpSocket, sync::mpsc};
use tracing::{debug, info, trace, warn};
use zenoh::config::ZenohId;

const GROUP: Ipv6Addr = Ipv6Addr::new(0xff12, 0, 0, 0, 0, 0, 0xe0a1, 0xde89);

pub trait Message: Pod {
    const KIND: Kind;
    fn header() -> Header {
        Header {
            magic: *b"EXO",
            kind: Self::KIND as u8,
        }
    }
    fn write_into(&self, buf: &mut [u8]) {
        let total = size_of::<Header>() + size_of::<Self>();
        assert!(total <= buf.len());
        buf[0..size_of::<Header>()].copy_from_slice(bytemuck::bytes_of(&Self::header()));
        buf[size_of::<Header>()..total].copy_from_slice(bytemuck::bytes_of(self));
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
// packet & version
pub enum Kind {
    Hello = 0,
    WhatsUp = 1,
}

#[derive(Debug, Clone, Copy)]
pub struct Discovered {
    pub zid: ZenohId,
    pub addr: (Ipv6Addr, u32),
}

pub struct UnknownKind;
impl TryFrom<u8> for Kind {
    type Error = UnknownKind;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Kind::Hello),
            1 => Ok(Kind::WhatsUp),
            _ => Err(UnknownKind),
        }
    }
}
#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Header {
    magic: [u8; 3],
    kind: u8,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct Hello {
    pub nonce: [u8; 8],
}
impl Hello {
    const fn buf_size() -> usize {
        size_of::<Header>() + size_of::<Self>()
    }
}
impl Message for Hello {
    const KIND: Kind = Kind::Hello;
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Pod, Zeroable)]
pub struct WhatsUp {
    pub nonce: [u8; 8],
    pub zid: [u8; 16],
}
impl WhatsUp {
    const fn buf_size() -> usize {
        size_of::<Header>() + size_of::<Self>()
    }
}
impl Message for WhatsUp {
    const KIND: Kind = Kind::WhatsUp;
}

pub struct Discovery {
    sock: Arc<UdpSocket>,
    ifaces: Mutex<Vec<SocketAddr>>,
    last_nonce: Mutex<[u8; 8]>,
    discovered: mpsc::Sender<Discovered>,
    port: u16,
    zid: ZenohId,
}

impl Discovery {
    pub async fn new(zid: ZenohId) -> io::Result<(Self, mpsc::Receiver<Discovered>)> {
        let sock = Arc::new(UdpSocket::bind("[::]:52414").await?);
        sock.set_multicast_loop_v6(false)?;
        let (send, recv) = mpsc::channel(10);
        Ok((
            Self {
                sock,
                ifaces: Default::default(),
                last_nonce: Default::default(),
                port: 52414,
                zid,
                discovered: send,
            },
            recv,
        ))
    }

    pub fn enable_iface(&self, iface_idx: u32) -> io::Result<()> {
        self.sock.join_multicast_v6(&GROUP, iface_idx).inspect(|_| {
            self.ifaces.lock().push(SocketAddr::V6(SocketAddrV6::new(
                GROUP, self.port, 0, iface_idx,
            )))
        })
    }

    pub fn disable_iface(&self, iface_idx: u32) -> io::Result<()> {
        self.ifaces.lock().retain(|addr| {
            if let SocketAddr::V6(v6) = addr {
                v6.scope_id() != iface_idx
            } else {
                true
            }
        });
        self.sock.leave_multicast_v6(&GROUP, iface_idx)
    }

    pub async fn respond_loop(&self) -> io::Result<()> {
        let mut buf = [0u8; 100];
        loop {
            let (bytes_read, addr) = self.sock.recv_from(&mut buf).await?;
            trace!(
                "raw recv: {bytes_read} bytes from {addr}: {:02x?}",
                &buf[..bytes_read]
            );
            if bytes_read < size_of::<Header>() {
                trace!("dropped: early EOF");
                continue;
            };
            let header: &Header = bytemuck::from_bytes(&buf[0..size_of::<Header>()]);
            if header.magic != *b"EXO" {
                trace!("dropped: wrong magic");
                continue;
            };
            match header.kind.try_into() {
                Ok(Kind::Hello) => {
                    let total = Hello::buf_size();
                    if bytes_read != total {
                        trace!("dropped: hello wrong size");
                        continue;
                    }
                    let hello: &Hello = bytemuck::from_bytes(&buf[size_of::<Header>()..total]);
                    if hello.nonce == *self.last_nonce.lock() {
                        trace!("dropped: local hello nonce");
                        continue;
                    }

                    // reply
                    let mut reply_buf = [0u8; WhatsUp::buf_size()];
                    let reply = WhatsUp {
                        nonce: hello.nonce,
                        zid: self.zid.to_le_bytes(),
                    };
                    reply.write_into(&mut reply_buf);

                    for i in 0..4 {
                        if self
                            .sock
                            .send_to(&reply_buf, addr)
                            .await
                            .inspect_err(|e| warn!("send to {addr} failed: {e}"))
                            .is_ok_and(|sent| sent == WhatsUp::buf_size())
                        {
                            info!(
                                "sent {} bytes to {addr} after {i} attempts",
                                WhatsUp::buf_size(),
                            );
                            break;
                        };
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                }
                Ok(Kind::WhatsUp) => {
                    let total = WhatsUp::buf_size();
                    if bytes_read != total {
                        trace!("dropped: whatsup wrong size");
                        continue;
                    }
                    let whats_up: &WhatsUp = bytemuck::from_bytes(&buf[size_of::<Header>()..total]);
                    if whats_up.nonce == [0u8; 8] || whats_up.nonce != *self.last_nonce.lock() {
                        trace!("dropped: stale nonce");
                        continue;
                    }
                    let SocketAddr::V6(v6) = addr else {
                        trace!("dropped: v4 addr used");
                        continue;
                    };
                    let Ok(zid) = ZenohId::try_from(&whats_up.zid[..]) else {
                        trace!("dropped: zenoh conversion failed");
                        continue;
                    };
                    if zid == self.zid {
                        trace!("dropped: self zenoh id");
                        continue;
                    }
                    // discovered
                    let Ok(_) = self
                        .discovered
                        .send(Discovered {
                            zid,
                            addr: (*v6.ip(), v6.scope_id()),
                        })
                        .await
                    else {
                        return Ok(());
                    };
                }
                Err(_) => {
                    warn!("message with unknown kind {header:?}")
                }
            }
        }
    }

    pub async fn announce(&self) -> io::Result<()> {
        let nonce = rand::random();
        *self.last_nonce.lock() = nonce;
        let hello = Hello { nonce };

        let mut buf = [0u8; Hello::buf_size()];
        hello.write_into(&mut buf);

        let addrs = self.ifaces.lock().clone();
        debug!("announcing {hello:?} to {addrs:?}");
        for addr in addrs {
            let sent = self.sock.send_to(&buf, addr).await?;
            trace!("sent {sent} to {addr}");
        }
        Ok(())
    }
}
