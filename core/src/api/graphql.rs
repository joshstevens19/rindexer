use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use reqwest::{Client, Error};
use serde_json::{json, Value};
use tokio::sync::{oneshot, oneshot::Sender};
use tracing::{error, info};

use crate::{
    database::{
        generate::generate_indexer_contract_schema_name, postgres::client::connection_string,
    },
    helpers::set_thread_no_logging,
    indexer::Indexer,
    manifest::graphql::GraphQLSettings,
};

/// Check if there are any table name conflicts across contracts.
/// Returns true if any two contracts have tables with the same name.
fn has_table_name_conflicts(indexer: &Indexer) -> bool {
    let mut seen_table_names: HashSet<String> = HashSet::new();

    for contract in &indexer.contracts {
        if let Some(tables) = &contract.tables {
            for table in tables {
                if !seen_table_names.insert(table.name.clone()) {
                    // Table name already seen in another contract
                    return true;
                }
            }
        }
    }

    false
}

pub struct GraphqlOverrideSettings {
    pub enabled: bool,
    pub override_port: Option<u16>,
}

fn get_graphql_exe() -> Result<PathBuf, Box<dyn std::error::Error>> {
    // Try the build-time path first (for development)
    let build_path = PathBuf::from(env!("RINDEXER_GRAPHQL_EXE"));
    if build_path.exists() {
        return Ok(build_path);
    }

    // Fall back to embedded binary (for deployed/installed versions)
    const GRAPHQL_BINARY: &[u8] = include_bytes!(env!("RINDEXER_GRAPHQL_EMBED"));

    let temp_dir = std::env::temp_dir();
    let exe_name = if cfg!(windows) {
        format!("rindexer-graphql-{}.exe", std::process::id())
    } else {
        format!("rindexer-graphql-{}", std::process::id())
    };
    let temp_path = temp_dir.join(exe_name);

    fs::write(&temp_path, GRAPHQL_BINARY)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&temp_path)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&temp_path, perms)?;
    }

    Ok(temp_path)
}

#[allow(dead_code)]
pub struct GraphQLServer {
    pid: u32,
}

#[derive(thiserror::Error, Debug)]
pub enum StartGraphqlServerError {
    #[error("Can not read database environment variable: {0}")]
    UnableToReadDatabaseUrl(#[from] env::VarError),

    #[error("Could not start up GraphQL server {0}")]
    GraphQLServerStartupError(String),
}

pub async fn start_graphql_server(
    indexer: &Indexer,
    settings: &GraphQLSettings,
) -> Result<GraphQLServer, StartGraphqlServerError> {
    info!("Starting GraphQL server");

    let schemas: Vec<String> = indexer
        .contracts
        .iter()
        .map(move |contract| {
            generate_indexer_contract_schema_name(
                &indexer.name,
                &contract.before_modify_name_if_filter_readonly(),
            )
        })
        .collect();

    let connection_string = connection_string()?;
    let port = settings.port;
    let graphql_endpoint = format!("http://localhost:{}/graphql", &port);
    let graphql_playground = format!("http://localhost:{}/playground", &port);

    let rindexer_graphql_exe = get_graphql_exe().map_err(|_| {
        StartGraphqlServerError::GraphQLServerStartupError(
            "rindexer-graphql executable not found".to_string(),
        )
    })?;

    // Check if port is available
    if std::net::TcpListener::bind(format!("0.0.0.0:{}", port)).is_err() {
        error!(
            "Could not start GraphQL API - port {} is already in use. To kill: `kill $(lsof -t -i:{})` or set a different port in graphql.port YAML",
            port, port
        );
        return Err(StartGraphqlServerError::GraphQLServerStartupError(format!(
            "Port {} in use",
            port
        )));
    }

    // Check if there are table name conflicts - if so, skip node alias plugin
    let skip_node_alias_plugin = has_table_name_conflicts(indexer);
    if skip_node_alias_plugin {
        info!(
            "Detected table name conflicts across contracts - disabling node alias plugin to avoid GraphQL type conflicts"
        );
    }

    let (tx, rx) = oneshot::channel();
    let tx_arc = Arc::new(Mutex::new(Some(tx)));

    spawn_start_server(
        tx_arc,
        rindexer_graphql_exe,
        connection_string,
        schemas.join(","),
        Arc::new(port),
        settings.filter_only_on_indexed_columns,
        settings.disable_advanced_filters,
        skip_node_alias_plugin,
    );

    // Wait for the initial server startup
    let pid = rx.await.map_err(|e| {
        StartGraphqlServerError::GraphQLServerStartupError(format!(
            "Failed to receive initial PID: {e}"
        ))
    })?;

    perform_health_check(&graphql_endpoint, &graphql_playground).await?;

    Ok(GraphQLServer { pid })
}

static MANUAL_STOP: AtomicBool = AtomicBool::new(false);

/// Signal the GraphQL server to stop its restart loop
pub fn stop_graphql_server() {
    MANUAL_STOP.store(true, Ordering::SeqCst);
}

#[allow(clippy::too_many_arguments)]
fn spawn_start_server(
    tx_arc: Arc<Mutex<Option<Sender<u32>>>>,
    rindexer_graphql_exe: PathBuf,
    connection_string: String,
    schemas: String,
    port: Arc<u16>,
    filter_only_on_indexed_columns: bool,
    disable_advanced_filters: bool,
    skip_node_alias_plugin: bool,
) {
    tokio::spawn(async move {
        loop {
            if MANUAL_STOP.load(Ordering::SeqCst) {
                break;
            }

            match start_server(
                &rindexer_graphql_exe,
                &connection_string,
                &schemas,
                &port,
                filter_only_on_indexed_columns,
                disable_advanced_filters,
                skip_node_alias_plugin,
            )
            .await
            {
                Ok(child) => {
                    let pid = child.id();
                    let child_arc = Arc::new(Mutex::new(Some(child)));
                    let child_inner_for_thread = Arc::clone(&child_arc);

                    if let Some(tx) = tx_arc.lock().expect("Failed to lock tx arc").take() {
                        if let Err(e) = tx.send(pid) {
                            error!("Failed to send PID: {}", e);
                            break;
                        }
                    }

                    let port_inner = Arc::clone(&port);

                    tokio::spawn(async move {
                        set_thread_no_logging();
                        match child_inner_for_thread.lock() {
                            Ok(mut guard) => match guard.as_mut() {
                                Some(ref mut child) => match child.wait() {
                                    Ok(status) => {
                                        if status.success() {
                                            info!(
                                                "🦀GraphQL API ready at http://0.0.0.0:{}/",
                                                port_inner
                                            );
                                        } else {
                                            error!("GraphQL: Could not start up API: Child process exited with errors");
                                        }
                                    }
                                    Err(e) => {
                                        error!("GraphQL: Failed to wait on child process: {}", e);
                                    }
                                },
                                None => error!("GraphQL: Child process is None"),
                            },
                            Err(e) => {
                                error!("GraphQL: Failed to lock child process for waiting: {}", e);
                            }
                        }
                    });

                    if let Err(e) = child_arc
                        .lock()
                        .expect("Failed to lock child arc")
                        .as_mut()
                        .expect("Failed to get child")
                        .wait()
                    {
                        error!("Failed to wait on child process: {}", e);
                    }

                    if !MANUAL_STOP.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to start GraphQL server: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

async fn start_server(
    rindexer_graphql_exe: &Path,
    connection_string: &str,
    schemas: &str,
    port: &u16,
    filter_only_on_indexed_columns: bool,
    disable_advanced_filters: bool,
    skip_node_alias_plugin: bool,
) -> Result<Child, String> {
    Command::new(rindexer_graphql_exe)
        .arg(connection_string)
        .arg(schemas)
        .arg(port.to_string())
        // graphql_limit
        .arg("1000")
        // graphql_timeout
        .arg("10000")
        .arg(filter_only_on_indexed_columns.to_string())
        .arg(disable_advanced_filters.to_string())
        .arg(skip_node_alias_plugin.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit()) // Show errors from GraphQL server
        .spawn()
        .map_err(|e| e.to_string())
}

async fn perform_health_check(
    graphql_endpoint: &str,
    graphql_playground: &str,
) -> Result<(), StartGraphqlServerError> {
    let client = Client::new();
    let health_check_query = json!({
        "query": "query MyQuery { nodeId }"
    });
    let mut health_check_attempts = 0;
    while health_check_attempts < 40 {
        match client.post(graphql_endpoint).json(&health_check_query).send().await {
            Ok(response) if response.status().is_success() => {
                let response_json: Result<Value, Error> = response.json().await;
                match response_json {
                    Ok(response_json) => {
                        if response_json.get("errors").is_none() {
                            info!(
                                "🦀 GraphQL API ready at {} Playground - {} 🦀",
                                graphql_endpoint, graphql_playground
                            );
                            return Ok(());
                        } else {
                            error!(
                                "GraphQL health check returned errors: {:?}",
                                response_json.get("errors")
                            );
                        }
                    }
                    Err(e) => {
                        error!("GraphQL health check JSON parse error: {}", e);
                    }
                }
            }
            Ok(response) => {
                error!("GraphQL health check failed with status: {}", response.status());
            }
            Err(_) => {
                // Connection error - server might not be ready yet
                if health_check_attempts == 0 {
                    info!("Waiting for GraphQL server to start...");
                }
            }
        }
        health_check_attempts += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    error!("GraphQL API did not become ready in time after {} attempts", health_check_attempts);
    Err(StartGraphqlServerError::GraphQLServerStartupError(
        "GraphQL API did not become ready in time".to_string(),
    ))
}

// Do not need now with the main shutdown keeping around in-case
// fn setup_ctrlc_handler(child_arc: Arc<Mutex<Option<Child>>>) {
//     ctrlc::set_handler(move || {
//         MANUAL_STOP.store(true, Ordering::SeqCst);
//         if let Ok(mut guard) = child_arc.lock() {
//             if let Some(child) = guard.as_mut() {
//                 if let Err(e) = kill_process_tree(child.id()) {
//                     error!("Failed to kill child process: {}", e);
//                 } else {
//                     info!("GraphQL server process killed");
//                 }
//             }
//         }
//         std::process::exit(0);
//     })
//     .expect("Error setting Ctrl-C handler");
// }

// Do not need now with the main shutdown keeping around in-case
// fn kill_process_tree(pid: u32) -> Result<(), String> {
//     if cfg!(target_os = "windows") {
//         Command::new("taskkill")
//             .args(["/PID", &pid.to_string(), "/T", "/F"])
//             .output()
//             .map_err(|e| e.to_string())?;
//     } else {
//         Command::new("pkill")
//             .args(["-TERM", "-P", &pid.to_string()])
//             .output()
//             .map_err(|e| e.to_string())?;
//     }
//     Ok(())
// }
