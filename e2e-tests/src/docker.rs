use anyhow::Result;
use std::net::TcpListener;
use std::process::Command;
use tokio::time::{sleep, Duration};

pub async fn start_postgres_container() -> Result<(String, u16)> {
    // Ensure Docker daemon is running (try to start it programmatically if not)
    ensure_docker_daemon().await?;

    // Pick a free local port
    let port = allocate_free_port()?;
    let name = format!("rindexer_pg_{}_{port}", std::process::id());

    // Run container
    let status = Command::new("docker")
        .args([
            "run", "-d",
            "--name", &name,
            "-e", "POSTGRES_PASSWORD=postgres",
            "-e", "POSTGRES_USER=postgres",
            "-e", "POSTGRES_DB=postgres",
            "-p", &format!("{}:5432", port),
            "postgres:16",
        ])
        .output();

    let out = match status {
        Ok(o) => o,
        Err(_) => return Err(anyhow::anyhow!("Docker not available")),
    };
    if !out.status.success() {
        return Err(anyhow::anyhow!(
            "Failed to start postgres container: {}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }

    // Wait for port to be ready
    for _ in 0..40 {
        if port_open(port).await {
            return Ok((name, port));
        }
        sleep(Duration::from_millis(250)).await;
    }

    // Cleanup on failure
    let _ = stop_postgres_container(&name).await;
    Err(anyhow::anyhow!("Postgres did not become ready on port {}", port))
}

/// Ensure the Docker daemon is running. Attempts to start Docker Desktop (macOS)
/// or Colima if available. Times out after ~30s if daemon is unavailable.
pub async fn ensure_docker_daemon() -> Result<()> {
    // If docker info works, we're done
    if docker_info_ok() { return Ok(()); }

    // Try to start Docker Desktop on macOS
    #[cfg(target_os = "macos")]
    {
        let _ = Command::new("open").args(["-g", "-a", "Docker"]).output();
    }

    // Try Colima if installed
    if which("colima") {
        let _ = Command::new("colima").arg("start").output();
    }

    // Poll for docker daemon readiness
    for _ in 0..60 {
        if docker_info_ok() { return Ok(()); }
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

fn allocate_free_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

async fn port_open(port: u16) -> bool {
    tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok()
}


