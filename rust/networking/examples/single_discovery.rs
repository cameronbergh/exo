use std::{env::args, io, sync::Arc, time::Duration};

use nix::net::if_::if_nameindex;
use tokio::time;
use tracing::info;
use zenoh::config::ZenohId;

use networking::discovery::Discovery;

#[tokio::main]
async fn main() -> io::Result<()> {
    zenoh::init_log_from_env_or("networking::discovery=trace,info");

    let if_name = args().nth(1).expect("pass an interface name");

    let iface_idx: u32 = if_nameindex()
        .expect("no ifaces")
        .into_iter()
        .filter_map(|iface| {
            if iface.name().to_string_lossy() == *if_name {
                Some(iface.index())
            } else {
                None
            }
        })
        .next()
        .expect("no iface found");
    let zid = ZenohId::try_from(&rand::random::<[u8; 16]>()[..]).expect("told ya so");

    println!("starting peer");
    println!("  zid       = {zid:?}");
    println!("  iface_idx = {iface_idx}");

    let (discovery, mut discovered_rx) = Discovery::new(zid).await?;
    let discovery = Arc::new(discovery);

    discovery.enable_iface(iface_idx)?;

    {
        let discovery = Arc::clone(&discovery);

        tokio::spawn(async move {
            if let Err(err) = discovery.respond_loop().await {
                eprintln!("discovery receive loop stopped: {err}");
            }
        });
        info!("spawned respond loop")
    }

    {
        let discovery = Arc::clone(&discovery);

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                if let Err(err) = discovery.announce().await {
                    eprintln!("announce failed: {err}");
                }
            }
        });
        info!("spawned announce loop")
    }

    println!("running; start the same binary on another host/interface");

    while let Some(peer) = discovered_rx.recv().await {
        let (ip, scope_id) = peer.addr;

        println!(
            "discovered peer: zid={:?} addr={} scope_id={}",
            peer.zid, ip, scope_id,
        );
    }

    Ok(())
}
