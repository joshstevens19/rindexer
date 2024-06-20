use crate::database::postgres::{connection_string, indexer_contract_schema_name};
use crate::indexer::Indexer;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::{env, thread};
use thiserror::Error;
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

#[allow(dead_code)]
pub struct GraphQLServer {
    child: Arc<Mutex<Child>>,
    pid: u32,
}

#[derive(Error, Debug)]
pub enum StartGraphqlServerError {
    #[error("Can not read database environment variable: {0}")]
    UnableToReadDatabaseUrl(env::VarError),

    #[error("Could not start up GraphQL server {0}")]
    GraphQLServerStartupError(String),
}

/// Starts the GraphQL server with the given settings.
///
/// # Arguments
///
/// * `indexers` - A slice of `Indexer` structs representing the schemas to be indexed.
/// * `settings` - The settings for configuring the server.
///
/// # Returns
///
/// Returns a `Result` with the `GraphQLServer` on success, or an `StartGraphqlServerError` on failure.
pub fn start_graphql_server(
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

    let child = Command::new("npx")
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
        // .arg("--default-role")
        // .arg(database_user)
        .arg("--enhance-graphiql")
        .arg("--cors")
        .arg("--disable-default-mutations")
        .arg("--retry-on-init-fail")
        .arg("--dynamic-json")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| StartGraphqlServerError::GraphQLServerStartupError(e.to_string()))?;

    let pid = child.id();
    let child_arc = Arc::new(Mutex::new(child));

    ctrlc::set_handler(move || {
        kill_process_tree(pid).expect("Failed to kill child process");
        info!("GraphQL server process killed");
    })
    .expect("Error setting Ctrl-C handler");

    let port_clone = port.clone();
    let child_clone_for_thread = Arc::clone(&child_arc);
    thread::spawn(move || match child_clone_for_thread.lock() {
        Ok(mut guard) => match guard.wait() {
            Ok(status) => {
                if status.success() {
                    info!("🚀 GraphQL API ready at http://0.0.0.0:{}/", port_clone);
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
    });

    Ok(GraphQLServer {
        child: child_arc,
        pid,
    })
}

/// Kills the process tree for the given PID.
///
/// # Arguments
///
/// * `pid` - The process ID of the root process to kill.
///
/// # Returns
///
/// Returns a `Result` indicating success or an error message on failure.
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
