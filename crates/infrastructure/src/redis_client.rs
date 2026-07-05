use redis::{aio::ConnectionManager, Client};

pub async fn create_connection_manager(redis_url: &str) -> redis::RedisResult<ConnectionManager> {
    let client = Client::open(redis_url)?;
    ConnectionManager::new(client).await
}

pub const JOBS_CHANNEL: &str = "jobs:events";
pub const DLQ_CHANNEL: &str = "dlq:events";

pub async fn publish(
    conn: &mut ConnectionManager,
    channel: &str,
    message: &str,
) -> redis::RedisResult<()> {
    redis::cmd("PUBLISH")
        .arg(channel)
        .arg(message)
        .query_async(conn)
        .await
}
