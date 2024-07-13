use std::{path::PathBuf, process::Command, thread, time::Duration};

use rindexer::{
    manifest::{
        core::ProjectType,
        yaml::{read_manifest, YAML_CONFIG_NAME},
    },
    rindexer_info, setup_info_logger, start_rindexer_no_code, GraphqlOverrideSettings,
    IndexerNoCodeDetails, PostgresClient, StartNoCodeDetails,
};

use crate::{
    cli_interface::StartSubcommands,
    console::{print_error_message},
    rindexer_yaml::validate_rindexer_yaml_exist,
};

fn check_docker_compose_status(project_path: &PathBuf, max_retries: u32) -> Result<(), String> {
    let mut retries = 0;

    while retries < max_retries {
        let ps_status = Command::new("docker")
            .args(["compose", "ps"])
            .current_dir(project_path)
            .output()
            .map_err(|e| {
                let error = format!("Failed to check docker compose status: {}", e);
                print_error_message(&error);
                error
            })?;

        if ps_status.status.success() {
            let output = String::from_utf8_lossy(&ps_status.stdout);
            rindexer_info!("Docker compose ps output:\n{}", output);
            if !output.contains("Exit") && output.contains("Up") {
                rindexer_info!("All containers are up and running.");
                return Ok(());
            }
        } else {
            let error = format!("docker compose ps exited with status: {}", ps_status.status);
            print_error_message(&error);
        }

        retries += 1;
        thread::sleep(Duration::from_millis(200));
        rindexer_info!("Waiting for docker compose containers to start...");
    }

    Err("Docker containers did not start successfully within the given retries.".into())
}

fn start_docker_compose(project_path: &PathBuf) -> Result<(), String> {
    if !project_path.exists() {
        return Err(format!("Project path does not exist: {:?}", project_path));
    }

    let status = Command::new("docker")
        .args(["compose", "up", "-d"])
        .current_dir(project_path)
        .status()
        .map_err(|e| {
            let error = format!("Docker could not startup the postgres container: {}", e);
            print_error_message(&error);
            error
        })?;

    if !status.success() {
        let error = format!("docker compose exited with status: {}", status);
        print_error_message(&error);
        return Err(error);
    }

    rindexer_info!("Docker starting up the postgres container..");
    
    check_docker_compose_status(project_path, 200)
}

pub async fn start(
    project_path: PathBuf,
    command: &StartSubcommands,
) -> Result<(), Box<dyn std::error::Error>> {
    setup_info_logger();

    validate_rindexer_yaml_exist();

    let manifest = read_manifest(&project_path.join(YAML_CONFIG_NAME)).map_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e));
        e
    })?;

    if manifest.storage.postgres_enabled() {
        let client = PostgresClient::new().await;
        if client.is_err() {
            // find if docker-compose.yml is present in parent
            let docker_compose_path = project_path.join("docker-compose.yml");
            if !docker_compose_path.exists() {
                return Err(
                    "The DATABASE_URL mapped is not running please make sure it is correct".into()
                );
            }

            match start_docker_compose(&project_path) {
                Ok(_) => {
                    rindexer_info!("Docker postgres containers started up successfully");
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
            // print_error_message("Could not connect to the postgres database, please make sure it
            // is running. If running locally you can run docker compose up -d");
        }
    }

    match manifest.project_type {
        ProjectType::Rust => {
            let project_cargo_manifest_path = project_path.join("Cargo.toml");
            let status = Command::new("cargo")
                .arg("run")
                .arg("--manifest-path")
                .arg(project_cargo_manifest_path)
                .arg(match command {
                    StartSubcommands::Indexer => "-- --indexer".to_string(),
                    StartSubcommands::Graphql { port } => match port {
                        Some(port) => format!("-- --graphql --port={}", port),
                        None => "-- --graphql".to_string(),
                    },
                    StartSubcommands::All { port } => match port {
                        Some(port) => format!("-- --port={}", port),
                        None => "".to_string(),
                    },
                })
                .status()
                .expect("Failed to execute cargo run.");

            if !status.success() {
                panic!("cargo run failed with status: {:?}", status);
            }
        }
        ProjectType::NoCode => match command {
            StartSubcommands::Indexer => {
                let details = StartNoCodeDetails {
                    manifest_path: &project_path.join(YAML_CONFIG_NAME),
                    indexing_details: IndexerNoCodeDetails { enabled: true },
                    graphql_details: GraphqlOverrideSettings {
                        enabled: false,
                        override_port: None,
                    },
                };

                start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the server: {}", e));
                    e
                })?;
            }
            StartSubcommands::Graphql { port } => {
                let details = StartNoCodeDetails {
                    manifest_path: &project_path.join(YAML_CONFIG_NAME),
                    indexing_details: IndexerNoCodeDetails { enabled: false },
                    graphql_details: GraphqlOverrideSettings {
                        enabled: true,
                        override_port: port.as_ref().and_then(|port| port.parse().ok()),
                    },
                };

                start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the indexer: {}", e));
                    e
                })?;
            }
            StartSubcommands::All { port } => {
                let details = StartNoCodeDetails {
                    manifest_path: &project_path.join(YAML_CONFIG_NAME),
                    indexing_details: IndexerNoCodeDetails { enabled: true },
                    graphql_details: GraphqlOverrideSettings {
                        enabled: true,
                        override_port: port.as_ref().and_then(|port| port.parse().ok()),
                    },
                };

                let _ = start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the server: {}", e));
                });
            }
        },
    }

    Ok(())
}
