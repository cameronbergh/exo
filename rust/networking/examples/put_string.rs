use networking;
use zenoh::Result;

#[tokio::main]
async fn main() -> Result<()> {
    zenoh::init_log_from_env_or("info");
    log::info!("Opening session...");
    let cfg = networking::cfg(rand::random())?;
    let session = networking::open(cfg).await?;
    let _tok = session
        .liveliness()
        .declare_token(format!("nodes/{}/live", session.zid()))
        .await?;
    let key_expr = "storage/mem1/name";
    let payload = "me";

    log::info!("Putting Data ('{key_expr}': '{payload}')...");
    session.put(key_expr, payload).await?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}
