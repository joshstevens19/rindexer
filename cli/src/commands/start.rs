use std::{env, path::PathBuf, process::Command, thread, time::Duration};

use colored::Colorize;
use rindexer::{
    apply_clickhouse_schema_change, apply_schema_change, detect_clickhouse_schema_changes,
    detect_schema_changes,
    manifest::{
        core::ProjectType,
        yaml::{read_manifest, YAML_CONFIG_NAME},
    },
    resolve_table_column_types, rindexer_error, rindexer_info, setup_info_logger,
    start_rindexer_no_code, ClickhouseClient, ClickhouseSchemaChange, GraphqlOverrideSettings,
    IndexerNoCodeDetails, PostgresClient, SchemaChange, StartNoCodeDetails,
};

use crate::{
    cli_interface::StartSubcommands,
    console::{print_error_message, print_warn_message, prompt_yes_no},
    rindexer_yaml::validate_rindexer_yaml_exist,
};

fn check_postgres_connection(conn_str: &str, max_retries: u32) -> Result<(), String> {
    let mut retries = 0;

    while retries < max_retries {
        let status = Command::new("pg_isready").args(["-d", conn_str]).output().map_err(|e| {
            let error = format!("Failed to check Postgres status: {e}");
            rindexer_error!(error);
            error
        })?;

        if status.status.success() {
            return Ok(());
        }

        retries += 1;
        thread::sleep(Duration::from_millis(500));
        rindexer_info!(
            "Waiting for Postgres to become available this may take a few attempts... attempt: {}",
            retries
        );
    }

    Err("Postgres did not become available within the given retries.".into())
}

fn check_docker_compose_status(project_path: &PathBuf, max_retries: u32) -> Result<(), String> {
    let mut retries = 0;

    while retries < max_retries {
        let ps_status = Command::new("docker")
            .args(["compose", "ps"])
            .current_dir(project_path)
            .output()
            .map_err(|e| {
                let error = format!("Failed to check docker compose status: {e}");
                print_error_message(&error);
                error
            })?;

        if ps_status.status.success() {
            let output = String::from_utf8_lossy(&ps_status.stdout);
            if !output.contains("Exit") && output.contains("Up") {
                rindexer_info!("All containers are up and running.");

                return if let Ok(conn_str) = env::var("DATABASE_URL") {
                    check_postgres_connection(&conn_str, max_retries).map_err(|e| {
                        let error = format!("Failed to connect to PostgresSQL: {e}");
                        rindexer_error!(error);
                        error
                    })
                } else {
                    let error = "DATABASE_URL not set.".to_string();
                    rindexer_error!(error);
                    Err(error)
                };
            }
        } else {
            let error = format!("docker compose ps exited with status: {}", ps_status.status);
            rindexer_error!(error);
        }

        retries += 1;
        thread::sleep(Duration::from_millis(200));
        rindexer_info!("Waiting for docker compose containers to start...");
    }

    Err("Docker containers did not start successfully within the given retries.".into())
}

/// Handles schema synchronization for custom tables.
/// Returns Ok(true) if we should continue, Ok(false) if user cancelled.
async fn handle_schema_sync(
    client: &PostgresClient,
    manifest: &rindexer::manifest::core::Manifest,
    auto_yes: bool,
) -> Result<bool, String> {
    let changes = detect_schema_changes(client, manifest).await?;

    if changes.is_empty() {
        return Ok(true);
    }

    println!("\n{}", "[rindexer] Schema changes detected:".cyan().bold());

    for change in &changes {
        match change {
            SchemaChange::AddColumn {
                table_full_name,
                column_name,
                column_type,
                default_value,
            } => {
                // Auto-apply new columns
                let default_str = match default_value {
                    Some(v) => format!(" DEFAULT {}", v),
                    None => " DEFAULT NULL".to_string(),
                };
                println!(
                    "  {} Adding column '{}' ({}){} to table '{}'",
                    "✓".green(),
                    column_name.yellow(),
                    column_type,
                    default_str.cyan(),
                    table_full_name
                );

                if let Err(e) = apply_schema_change(client, change).await {
                    println!("    {} Failed to add column: {}", "✗".red(), e);
                    return Err(e);
                }
                println!("    {} Column added successfully", "→".green());
            }
            SchemaChange::RemoveColumn { table_full_name, column_name } => {
                println!(
                    "\n  {} Column '{}' exists in database but not in YAML for table '{}'",
                    "?".yellow(),
                    column_name.yellow(),
                    table_full_name
                );

                let should_delete = if auto_yes {
                    println!("    {} Auto-confirming deletion (--yes flag)", "→".cyan());
                    true
                } else {
                    prompt_yes_no(
                        "    Delete this column? This will permanently remove data",
                        false,
                    )
                };

                if should_delete {
                    if let Err(e) = apply_schema_change(client, change).await {
                        println!("    {} Failed to delete column: {}", "✗".red(), e);
                        print_warn_message(&format!("    Column kept. Error: {}", e));
                    } else {
                        println!("    {} Column deleted", "→".green());
                    }
                } else {
                    println!("    {} Column kept (rindexer will ignore it)", "→".cyan());
                }
            }
            SchemaChange::ChangePrimaryKey {
                table_full_name,
                current_pk_columns,
                new_pk_columns,
            } => {
                println!(
                    "\n  {} Primary key change detected for table '{}':",
                    "?".yellow(),
                    table_full_name
                );
                println!("    Current: ({})", current_pk_columns.join(", ").red());
                println!("    New:     ({})", new_pk_columns.join(", ").green());

                let should_change = if auto_yes {
                    println!("    {} Auto-confirming PK change (--yes flag)", "→".cyan());
                    true
                } else {
                    prompt_yes_no(
                        "    Change primary key? This may fail if data has duplicates",
                        false,
                    )
                };

                if should_change {
                    match apply_schema_change(client, change).await {
                        Ok(_) => {
                            println!("    {} Primary key updated successfully", "→".green());
                        }
                        Err(e) => {
                            println!("    {} Failed to change primary key: {}", "✗".red(), e);
                            print_error_message(
                                "    Hint: Existing data may have duplicate values for the new PK columns.",
                            );
                            print_error_message(
                                "    You may need to manually clean up data or adjust your schema.",
                            );
                            return Err(e);
                        }
                    }
                } else {
                    print_warn_message(
                        "    Primary key change skipped. Schema mismatch may cause issues.",
                    );
                }
            }
            SchemaChange::ColumnTypeChanged {
                table_full_name,
                column_name,
                current_type,
                new_type,
            } => {
                println!(
                    "\n  {} Column type change detected for '{}' in table '{}':",
                    "!".red().bold(),
                    column_name.yellow(),
                    table_full_name
                );
                println!("    Current: {}", current_type.red());
                println!("    New:     {}", new_type.green());
                print_warn_message(
                    "    Type changes require manual migration. Please backup your data and handle this manually.",
                );
            }
        }
    }

    println!();
    Ok(true)
}

/// Handles schema synchronization for custom tables in ClickHouse.
/// Returns Ok(true) if we should continue, Ok(false) if user cancelled.
async fn handle_clickhouse_schema_sync(
    client: &ClickhouseClient,
    manifest: &rindexer::manifest::core::Manifest,
    auto_yes: bool,
) -> Result<bool, String> {
    let changes = detect_clickhouse_schema_changes(client, manifest).await?;

    if changes.is_empty() {
        return Ok(true);
    }

    println!("\n{}", "[rindexer] ClickHouse schema changes detected:".cyan().bold());

    for change in &changes {
        match change {
            ClickhouseSchemaChange::AddColumn {
                table_full_name,
                column_name,
                column_type,
                default_value,
            } => {
                // Auto-apply new columns
                let default_str = match default_value {
                    Some(v) => format!(" DEFAULT {}", v),
                    None => String::new(),
                };
                println!(
                    "  {} Adding column '{}' ({}){} to table '{}'",
                    "✓".green(),
                    column_name.yellow(),
                    column_type,
                    default_str.cyan(),
                    table_full_name
                );

                if let Err(e) = apply_clickhouse_schema_change(client, change).await {
                    println!("    {} Failed to add column: {}", "✗".red(), e);
                    return Err(e);
                }
                println!("    {} Column added successfully", "→".green());
            }
            ClickhouseSchemaChange::RemoveColumn { table_full_name, column_name } => {
                println!(
                    "\n  {} Column '{}' exists in database but not in YAML for table '{}'",
                    "?".yellow(),
                    column_name.yellow(),
                    table_full_name
                );

                let should_delete = if auto_yes {
                    println!("    {} Auto-confirming deletion (--yes flag)", "→".cyan());
                    true
                } else {
                    prompt_yes_no(
                        "    Delete this column? This will permanently remove data",
                        false,
                    )
                };

                if should_delete {
                    if let Err(e) = apply_clickhouse_schema_change(client, change).await {
                        println!("    {} Failed to delete column: {}", "✗".red(), e);
                        print_warn_message(&format!("    Column kept. Error: {}", e));
                    } else {
                        println!("    {} Column deleted", "→".green());
                    }
                } else {
                    println!("    {} Column kept (rindexer will ignore it)", "→".cyan());
                }
            }
            ClickhouseSchemaChange::ChangeOrderBy {
                table_full_name,
                current_order_by,
                new_order_by,
            } => {
                println!(
                    "\n  {} ORDER BY change detected for table '{}':",
                    "?".yellow(),
                    table_full_name
                );
                println!("    Current: ({})", current_order_by.join(", ").red());
                println!("    New:     ({})", new_order_by.join(", ").green());

                let should_change = if auto_yes {
                    println!("    {} Auto-confirming ORDER BY change (--yes flag)", "→".cyan());
                    true
                } else {
                    prompt_yes_no(
                        "    Change ORDER BY? This may fail if data has duplicates",
                        false,
                    )
                };

                if should_change {
                    match apply_clickhouse_schema_change(client, change).await {
                        Ok(_) => {
                            println!("    {} ORDER BY updated successfully", "→".green());
                        }
                        Err(e) => {
                            println!("    {} Failed to change ORDER BY: {}", "✗".red(), e);
                            print_error_message(
                                "    Hint: Existing data may have duplicate values for the new ORDER BY columns.",
                            );
                            print_error_message(
                                "    You may need to manually clean up data or adjust your schema.",
                            );
                            return Err(e);
                        }
                    }
                } else {
                    print_warn_message(
                        "    ORDER BY change skipped. Schema mismatch may cause issues.",
                    );
                }
            }
            ClickhouseSchemaChange::ColumnTypeChanged {
                table_full_name,
                column_name,
                current_type,
                new_type,
            } => {
                println!(
                    "\n  {} Column type change detected for '{}' in table '{}':",
                    "!".red().bold(),
                    column_name.yellow(),
                    table_full_name
                );
                println!("    Current: {}", current_type.red());
                println!("    New:     {}", new_type.green());
                print_warn_message(
                    "    Type changes require manual migration. Please backup your data and handle this manually.",
                );
            }
        }
    }

    println!();
    Ok(true)
}

fn start_docker_compose(project_path: &PathBuf) -> Result<(), String> {
    if !project_path.exists() {
        return Err(format!("Project path does not exist: {project_path:?}"));
    }

    let status = Command::new("docker")
        .args(["compose", "up", "-d"])
        .current_dir(project_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| {
            let error = format!("Docker command could not be executed make sure docker is running on the machine: {e}");
            print_error_message(&error);
            error
        })?;

    if !status.success() {
        let error = "Docker compose could not startup the postgres container, please make sure docker is running on the machine".to_string();
        rindexer_error!(error);
        return Err(error);
    }

    rindexer_info!("Docker starting up the postgres container..");

    check_docker_compose_status(project_path, 200)
}

pub async fn start(
    project_path: PathBuf,
    command: &StartSubcommands,
    auto_yes: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    setup_info_logger();

    validate_rindexer_yaml_exist(&project_path);

    let mut manifest = read_manifest(&project_path.join(YAML_CONFIG_NAME)).map_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {e}"));
        e
    })?;

    if manifest.storage.postgres_enabled() {
        let client = PostgresClient::new().await;
        if client.is_err() {
            // find if docker-compose.yml is present in parent
            let docker_compose_path = project_path.join("docker-compose.yml");
            if !docker_compose_path.exists() {
                return Err(
                    "The DATABASE_URL mapped is not running please make sure it is correct".into(),
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
        }

        // Schema sync for no-code projects with custom tables
        if manifest.project_type == ProjectType::NoCode {
            let has_tables = manifest.contracts.iter().any(|c| c.tables.is_some());
            if has_tables {
                // Resolve column types from ABI before schema sync
                if let Err(e) = resolve_table_column_types(&project_path, &mut manifest) {
                    print_error_message(&format!("Could not resolve column types: {e}"));
                    return Err(e.to_string().into());
                }

                // Need to get a fresh client for schema sync
                let client = PostgresClient::new().await.map_err(|e| {
                    print_error_message(&format!("Could not connect to postgres: {e}"));
                    e
                })?;

                if let Err(e) = handle_schema_sync(&client, &manifest, auto_yes).await {
                    print_error_message(&format!("Schema sync failed: {e}"));
                    return Err(e.into());
                }
            }
        }
    }

    // ClickHouse schema sync for no-code projects with custom tables
    if manifest.storage.clickhouse_enabled() && manifest.project_type == ProjectType::NoCode {
        let has_tables = manifest.contracts.iter().any(|c| c.tables.is_some());
        if has_tables {
            // Resolve column types from ABI before schema sync (if not already done for postgres)
            if !manifest.storage.postgres_enabled() {
                if let Err(e) = resolve_table_column_types(&project_path, &mut manifest) {
                    print_error_message(&format!("Could not resolve column types: {e}"));
                    return Err(e.to_string().into());
                }
            }

            let client = ClickhouseClient::new().await.map_err(|e| {
                print_error_message(&format!("Could not connect to ClickHouse: {e}"));
                e
            })?;

            if let Err(e) = handle_clickhouse_schema_sync(&client, &manifest, auto_yes).await {
                print_error_message(&format!("ClickHouse schema sync failed: {e}"));
                return Err(e.into());
            }
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
                        Some(port) => format!("-- --graphql --port={port}"),
                        None => "-- --graphql".to_string(),
                    },
                    StartSubcommands::All { port } => match port {
                        Some(port) => format!("-- --graphql --indexer --port={port}"),
                        None => "-- --graphql --indexer".to_string(),
                    },
                })
                .status()
                .expect("Failed to execute cargo run.");

            if !status.success() {
                panic!("cargo run failed with status: {status:?}");
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
                    print_error_message(&format!("Error starting the server: {e}"));
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
                    print_error_message(&format!("Error starting the indexer: {e}"));
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
                    print_error_message(&format!("Error starting the server: {e}"));
                });
            }
        },
    }

    Ok(())
}
