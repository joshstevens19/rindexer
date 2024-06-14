use crate::database::postgres::{connection_string, indexer_contract_schema_name};
use crate::manifest::yaml::Indexer;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::{error, info};

pub struct GraphQLServerDetails {
    pub settings: GraphQLServerSettings,
}

pub struct GraphQLServerSettings {
    port: Option<usize>,
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

pub struct GraphQLServer {
    child: Arc<Mutex<Child>>,
    pid: u32,
}

impl Drop for GraphQLServer {
    fn drop(&mut self) {
        kill_process_tree(self.pid).expect("Failed to kill child process");
        error!("GraphQL server process killed");
    }
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
/// Returns a `Result` with the `GraphQLServer` on success, or an error message on failure.
pub fn start_graphql_server(
    indexers: &[Indexer],
    settings: GraphQLServerSettings,
) -> Result<GraphQLServer, String> {
    info!("Starting GraphQL server");

    let schemas: Vec<String> = indexers
        .iter()
        .flat_map(|indexer| {
            indexer
                .contracts
                .iter()
                .map(move |contract| indexer_contract_schema_name(&indexer.name, &contract.name))
        })
        .collect();

    let connection_string = connection_string().map_err(|e| e.to_string())?;
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
        .map_err(|e| e.to_string())?;

    let pid = child.id();
    let child_arc = Arc::new(Mutex::new(child));

    ctrlc::set_handler(move || {
        kill_process_tree(pid).expect("Failed to kill child process");
        info!("GraphQL server process killed");
    })
    .expect("Error setting Ctrl-C handler");

    let port_clone = port.clone();
    let child_clone_for_thread = Arc::clone(&child_arc);
    thread::spawn(move || {
        let status = child_clone_for_thread
            .lock()
            .unwrap()
            .wait()
            .expect("Failed to wait on child process");

        if status.success() {
            info!("🚀 GraphQL API ready at http://0.0.0.0:{}/", port_clone);
        } else {
            panic!("Could not start up API");
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
