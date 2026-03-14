use anyhow::Result;
use std::net::TcpListener;
use std::process::Command;
use tokio::time::{sleep, Duration};
use tracing::info;

/// Allocate a random free TCP port.
pub fn allocate_free_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Start an ephemeral Postgres container on a random port.
/// Returns `(container_name, host_port)`.
pub async fn start_postgres_container() -> Result<(String, u16)> {
    ensure_docker_daemon().await?;

    let port = allocate_free_port()?;
    let name = format!("rindexer_pg_{}_{port}", std::process::id());

    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &name,
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "-e",
            "POSTGRES_USER=postgres",
            "-e",
            "POSTGRES_DB=postgres",
            "-p",
            &format!("{}:5432", port),
            "postgres:16",
        ])
        .output();

    let out = match out {
        Ok(o) => o,
        Err(_) => return Err(anyhow::anyhow!("Docker not available")),
    };
    if !out.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to start postgres container: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }

    for _ in 0..40 {
        if port_open(port).await {
            return Ok((name, port));
        }
        sleep(Duration::from_millis(250)).await;
    }

    let _ = stop_postgres_container(&name).await;
    Err(anyhow::anyhow!("Postgres did not become ready on port {}", port))
}

/// Wait for Postgres to accept connections via `tokio-postgres`.
pub async fn wait_for_postgres_ready(port: u16, timeout_seconds: u64) -> Result<()> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_seconds);
    let conn_str =
        format!("host=localhost port={} user=postgres password=postgres dbname=postgres", port);

    while start.elapsed() < timeout {
        if let Ok((client, connection)) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await
        {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            if client.simple_query("SELECT 1").await.is_ok() {
                info!("Postgres ready on port {}", port);
                return Ok(());
            }
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!("Postgres not ready on port {} after {}s", port, timeout_seconds))
}

/// Build the standard set of Postgres env vars for rindexer.
pub fn postgres_env_vars(port: u16) -> Vec<(String, String)> {
    let database_url = format!("postgresql://postgres:postgres@localhost:{}/postgres", port);
    vec![
        ("POSTGRES_HOST".into(), "localhost".into()),
        ("POSTGRES_PORT".into(), port.to_string()),
        ("POSTGRES_USER".into(), "postgres".into()),
        ("POSTGRES_PASSWORD".into(), "postgres".into()),
        ("POSTGRES_DB".into(), "postgres".into()),
        ("DATABASE_URL".into(), database_url),
    ]
}

/// Ensure the Docker daemon is running.
pub async fn ensure_docker_daemon() -> Result<()> {
    if docker_info_ok() {
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        let _ = Command::new("open").args(["-g", "-a", "Docker"]).output();
    }

    if which("colima") {
        let _ = Command::new("colima").arg("start").output();
    }

    for _ in 0..60 {
        if docker_info_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(anyhow::anyhow!("Docker daemon not available after waiting"))
}

fn docker_info_ok() -> bool {
    Command::new("docker").arg("info").output().map(|o| o.status.success()).unwrap_or(false)
}

fn which(bin: &str) -> bool {
    Command::new("which").arg(bin).output().map(|o| o.status.success()).unwrap_or(false)
}

pub async fn stop_postgres_container(name: &str) -> Result<()> {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
    Ok(())
}

/// Start an ephemeral ClickHouse container on a random HTTP port.
/// Returns `(container_name, http_port)`.
pub async fn start_clickhouse_container() -> Result<(String, u16)> {
    ensure_docker_daemon().await?;

    let http_port = allocate_free_port()?;
    let name = format!("rindexer_ch_{}_{http_port}", std::process::id());

    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &name,
            "-e",
            "CLICKHOUSE_DB=default",
            "-e",
            "CLICKHOUSE_USER=default",
            "-e",
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
            "-p",
            &format!("{http_port}:8123"),
            "--ulimit",
            "nofile=262144:262144",
            "clickhouse/clickhouse-server:24.8",
        ])
        .output();

    let out = match out {
        Ok(o) => o,
        Err(_) => return Err(anyhow::anyhow!("Docker not available")),
    };
    if !out.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to start ClickHouse container: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }

    for _ in 0..40 {
        if port_open(http_port).await {
            return Ok((name, http_port));
        }
        sleep(Duration::from_millis(250)).await;
    }

    let _ = stop_container(&name).await;
    Err(anyhow::anyhow!("ClickHouse did not become ready on port {}", http_port))
}

/// Wait for ClickHouse to accept HTTP queries.
pub async fn wait_for_clickhouse_ready(port: u16, timeout_seconds: u64) -> Result<()> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_seconds);
    let url = format!("http://localhost:{}/?query=SELECT%201", port);
    let client = reqwest::Client::new();

    while start.elapsed() < timeout {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                info!("ClickHouse ready on port {}", port);
                return Ok(());
            }
        }
        sleep(Duration::from_millis(250)).await;
    }

    Err(anyhow::anyhow!("ClickHouse not ready on port {} after {}s", port, timeout_seconds))
}

/// Build the standard set of ClickHouse env vars for rindexer.
pub fn clickhouse_env_vars(port: u16) -> Vec<(String, String)> {
    vec![
        ("CLICKHOUSE_URL".into(), format!("http://localhost:{port}")),
        ("CLICKHOUSE_USER".into(), "default".into()),
        ("CLICKHOUSE_PASSWORD".into(), String::new()),
        ("CLICKHOUSE_DB".into(), "default".into()),
    ]
}

/// Generic container stop (works for any Docker container).
pub async fn stop_container(name: &str) -> Result<()> {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
    Ok(())
}

async fn port_open(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok()
}
