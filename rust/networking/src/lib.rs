use std::{
    collections::HashMap,
    env,
    net::{IpAddr, SocketAddr},
    ops::{Deref, DerefMut},
};

use netwatcher::{Interface, IpRecord, Update, UpdateDiff, WatchHandle};
use tokio::{
    sync::mpsc,
    task::{AbortHandle, JoinHandle, JoinSet},
};
pub use zenoh::{Config, config::ZenohId};
use zenoh::{
    Result, Session as ZSession,
    config::{Locator, WhatAmI},
    internal::runtime::Runtime,
};
use zenoh_plugin_storage_manager::StoragesPlugin;
use zenoh_plugin_trait::PluginsManager;

pub mod swarm;

pub struct Session {
    pub session: ZSession,
    _watch_all_handle: WatchAllHandle,
}
impl Deref for Session {
    type Target = ZSession;
    fn deref(&self) -> &Self::Target {
        &self.session
    }
}
impl DerefMut for Session {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.session
    }
}
impl Drop for WatchAllHandle {
    fn drop(&mut self) {
        self._async.abort();
    }
}

pub fn cfg(identity: u128) -> Result<zenoh::Config> {
    let namespace = env::var("EXO_ZENOH_NAMESPACE").unwrap_or_else(|_| "exo".to_string());
    let mut cfg = zenoh::Config::default();
    // todo: cleanup
    cfg.insert_json5("id", &format!("\"{identity:x}\""))?;
    // mesh mode - "client" and "server" also exist
    cfg.insert_json5("mode", "\"peer\"")?;
    cfg.insert_json5("scouting/multicast/enabled", "true")?;
    cfg.insert_json5("scouting/multicast/autoconnect", "[]")?;
    cfg.insert_json5("scouting/gossip/multihop", "true")?;
    cfg.insert_json5("namespace", &format!("{namespace:?}"))?;
    cfg.insert_json5("transport/link/tx/batch_size", "9216")?;
    cfg.insert_json5("timestamping/enabled", "true")?;
    cfg.insert_json5("plugins/storage_manager/__required__", "true")?;
    cfg.insert_json5(
        "plugins/storage_manager/storages/mem1",
        r#"{
            key_expr: "storage/mem1/**",
            strip_prefix: "storage/mem1",
            volume: "memory",
            replication: {
                interval: 2,
            }
        }"#,
    )?;
    Ok(cfg)
}

pub async fn open(cfg: zenoh::Config) -> Result<Session> {
    let mut plugins = PluginsManager::static_plugins_only();
    plugins.declare_static_plugin::<StoragesPlugin, _>("storage_manager", true);
    let mut runtime = zenoh::internal::runtime::RuntimeBuilder::new(cfg)
        .plugins_manager(plugins)
        .build()
        .await?;
    let session = zenoh::session::init(runtime.clone().into()).await?;
    runtime.start().await?;
    let _watch_all_handle = watch_all(runtime)?;
    Ok(Session {
        session,
        _watch_all_handle,
    })
}

pub async fn watch_iface(runtime: Runtime, index: u32, name: String) -> Result<()> {
    let mut cfg = zenoh::Config::default();
    cfg.insert_json5("scouting/multicast/interface", &format!("\"{name}\""))?;
    log::info!("starting scout on iface={name}");
    let scout = zenoh::scout(WhatAmI::Peer, cfg).await?;
    while let Ok(hello) = scout.recv_async().await {
        if hello.zid() == runtime.zid() {
            continue;
        }
        // nb: currently only propagates scoped ll v6 addresses
        let locators = hello
            .locators()
            .iter()
            .cloned()
            .filter_map(|mut locator| {
                append_iface_to_unicast_ll_v6(locator.address().as_str(), index).map({
                    move |addr| {
                        locator.address_mut().set(&*addr)?;
                        Ok(locator)
                    }
                })
            })
            .collect::<Result<Vec<Locator>>>()?;
        runtime.connect_peer(&hello.zid().into(), &*locators).await;
    }
    Ok(())
}

fn append_iface_to_unicast_ll_v6(addr: &str, iface_idx: u32) -> Option<String> {
    if addr.contains('%') {
        return None;
    }

    let socket = addr.parse::<SocketAddr>().ok()?;

    let IpAddr::V6(ip) = socket.ip() else {
        return None;
    };

    if !ip.is_unicast_link_local() {
        return None;
    }

    Some(format!("[{ip}%{iface_idx}]:{}", socket.port()))
}

struct WatchAllHandle {
    _sync: WatchHandle,
    _async: JoinHandle<Result<()>>,
}
fn watch_all(runtime: Runtime) -> Result<WatchAllHandle> {
    let (send, mut recv) = mpsc::unbounded_channel();
    let _sync = netwatcher::watch_interfaces_with_callback(move |u| _ = send.send(u))?;
    let _async = tokio::task::spawn(async move {
        let mut js = JoinSet::<Result<()>>::new();
        let mut handles = HashMap::<u32, AbortHandle>::new();

        while let Some(Update {
            mut interfaces,
            diff:
                UpdateDiff {
                    added,
                    modified,
                    removed,
                },
            ..
        }) = recv.recv().await
        {
            for idx in removed {
                if let Some(ah) = handles.remove(&idx) {
                    ah.abort();
                }
            }
            for Interface {
                index, name, ips, ..
            } in modified
                .into_keys()
                .chain(added.into_iter())
                .filter_map(|i| interfaces.remove(&i))
            {
                if let Some(ah) = handles.remove(&index) {
                    ah.abort();
                }
                if ips.into_iter().any(is_valid_v6) {
                    let abort_handle = js.spawn(watch_iface(runtime.clone(), index, name));
                    handles.insert(index, abort_handle);
                }
            }

            // Drain completed tasks and propagate errors lazily.
            // AbortHandles are drained only when necessary.
            while let Some(r) = js.try_join_next() {
                match r {
                    Ok(Err(e)) => {
                        log::error!("iface watcher failed with {e}");
                        return Err(e);
                    }
                    Err(e) if e.is_panic() => {
                        log::error!("iface watcher panicked with {e}");
                        return Err(e.into());
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    });
    Ok(WatchAllHandle { _sync, _async })
}

fn is_valid_v6(record: IpRecord) -> bool {
    record.ip.is_ipv6() && !record.ip.is_loopback() && !record.ip.is_multicast()
}
