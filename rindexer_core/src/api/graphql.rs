use crate::api::playground;
use crate::database::postgres::{connection_string, indexer_contract_schema_name};
use crate::helpers::set_thread_no_logging;
use crate::indexer::Indexer;
use reqwest::Client;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, thread};
use tracing::{error, info};

pub struct GraphQLServerDetails {
    pub settings: GraphQLServerSettings,
}

pub struct GraphQLServerSettings {
    port: Option<usize>,
    #[allow(dead_code)]
    watch: Option<bool>,
}

impl GraphQLServerSettings {
    pub fn port(port: usize) -> Self {
        Self {
            port: Some(port),
            watch: Some(false),
        }
    }
}

impl Default for GraphQLServerSettings {
    fn default() -> Self {
        Self {
            port: Some(5005),
            watch: Some(false),
        }
    }
}

fn get_postgraphile_path() -> PathBuf {
    let postgraphile_filename = match std::env::consts::OS {
        "windows" => "postgraphile-win.exe",
        "macos" => "postgraphile-macos",
        _ => "postgraphile-linux",
    };

    let mut paths = vec![];

    // Assume `resources` directory is in the same directory as the executable (installed)
    if let Ok(executable_path) = env::current_exe() {
        let mut path = executable_path.clone();
        path.pop(); // Remove the executable name
        path.push("resources");
        path.push(postgraphile_filename);
        paths.push(path);

        // Also consider when running from within the `rindexer_core` directory
        let mut path = executable_path;
        path.pop(); // Remove the executable name
        path.pop(); // Remove the 'release' or 'debug' directory
        path.push("resources");
        path.push(postgraphile_filename);
        paths.push(path);
    }

    // Check additional common paths
    if let Ok(home_dir) = env::var("HOME") {
        let mut path = PathBuf::from(home_dir);
        path.push(".rindexer");
        path.push("resources");
        path.push(postgraphile_filename);
        paths.push(path);
    }

    // Return the first valid path
    for path in &paths {
        if path.exists() {
            return path.clone();
        }
    }

    // If none of the paths exist, return the first one with useful error message
    paths
        .into_iter()
        .next()
        .expect("Failed to determine postgraphile path")
}
#[allow(dead_code)]
pub struct GraphQLServer {
    child: Arc<Mutex<Child>>,
    pid: u32,
}

#[derive(thiserror::Error, Debug)]
pub enum StartGraphqlServerError {
    #[error("Can not read database environment variable: {0}")]
    UnableToReadDatabaseUrl(env::VarError),

    #[error("Could not start up GraphQL server {0}")]
    GraphQLServerStartupError(String),
}

pub async fn start_graphql_server(
    indexer: &Indexer,
    settings: GraphQLServerSettings,
) -> Result<GraphQLServer, StartGraphqlServerError> {
    info!("Starting GraphQL server");

    let schemas: Vec<String> = indexer
        .contracts
        .iter()
        .map(move |contract| indexer_contract_schema_name(&indexer.name, &contract.name))
        .collect();

    let connection_string =
        connection_string().map_err(StartGraphqlServerError::UnableToReadDatabaseUrl)?;
    let port = settings.port.unwrap_or(5005).to_string();
    let graphql_endpoint = format!("http://localhost:{}/graphql", port);

    let postgraphile_path = get_postgraphile_path();
    if !postgraphile_path.exists() {
        return Err(StartGraphqlServerError::GraphQLServerStartupError(
            "Postgraphile executable not found".to_string(),
        ));
    }

    let child = Command::new(postgraphile_path)
        .arg("postgraphile")
        .arg("-c")
        .arg(connection_string)
        .arg("--host")
        .arg("0.0.0.0")
        .arg("--port")
        .arg(&port)
        .arg("--watch")
        .arg("--schema")
        .arg(schemas.join(","))
        .arg("--no-ignore-indexes") // seems to not work
        // .arg("--default-role")
        // .arg(database_user)
        //.arg("--enhance-graphiql")
        .arg("--disable-graphiql")
        .arg("--cors")
        .arg("--disable-default-mutations")
        .arg("--retry-on-init-fail")
        .arg("--dynamic-json")
        .arg("--disable-graphiql")
        .arg("--enable-query-batching")
        // .arg("--subscriptions")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| StartGraphqlServerError::GraphQLServerStartupError(e.to_string()))?;

    let pid = child.id();
    let child_arc = Arc::new(Mutex::new(child));

    ctrlc::set_handler(move || {
        kill_process_tree(pid).expect("Failed to kill child process");
        info!("GraphQL server process killed");
    })
    .expect("Error setting Ctrl-C handler");

    let child_clone_for_thread = Arc::clone(&child_arc);
    thread::spawn(move || {
        set_thread_no_logging();
        match child_clone_for_thread.lock() {
            Ok(mut guard) => match guard.wait() {
                Ok(status) => {
                    if status.success() {
                        info!("🚀 GraphQL API ready at http://0.0.0.0:{}/", port);
                    } else {
                        error!("GraphQL: Could not start up API: Child process exited with errors");
                    }
                }
                Err(e) => {
                    error!("GraphQL: Failed to wait on child process: {}", e);
                }
            },
            Err(e) => {
                error!("GraphQL: Failed to lock child process for waiting: {}", e);
            }
        }
    });

    let playground_endpoint = playground::run_in_child_thread(&graphql_endpoint);

    // Health check to ensure API is ready
    let client = Client::new();
    let health_check_query = json!({
        "query": "query MyQuery { nodeId }"
    });
    let mut health_check_attempts = 0;
    while health_check_attempts < 40 {
        match client
            .post(&graphql_endpoint)
            .json(&health_check_query)
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                let response_json: Value = response.json().await.unwrap();
                if response_json.get("errors").is_none() {
                    info!(
                        "🚀 GraphQL API ready at {} Playground - {}",
                        graphql_endpoint, playground_endpoint
                    );
                    break;
                }
            }
            _ => {}
        }
        health_check_attempts += 1;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    if health_check_attempts >= 40 {
        error!("GraphQL API did not become ready in time");
        return Err(StartGraphqlServerError::GraphQLServerStartupError(
            "GraphQL API did not become ready in time".to_string(),
        ));
    }

    Ok(GraphQLServer {
        child: child_arc,
        pid,
    })
}

fn kill_process_tree(pid: u32) -> Result<(), String> {
    if cfg!(target_os = "windows") {
        Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
            .map_err(|e| e.to_string())?;
    } else {
        Command::new("pkill")
            .args(["-TERM", "-P", &pid.to_string()])
            .output()
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}
