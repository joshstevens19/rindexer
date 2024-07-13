use std::path::PathBuf;

use rindexer::{
    drop_tables_for_indexer_sql,
    manifest::yaml::{read_manifest, YAML_CONFIG_NAME},
    PostgresClient,
};
use tokio::fs::remove_dir_all;

use crate::console::{
    print_error_message, print_success_message, print_warn_message, prompt_for_input_list,
};

pub async fn handle_delete_command(
    project_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    print_warn_message(&format!(
        "This will delete all data in the postgres database and csv files for the project at: {}",
        project_path.display()
    ));
    print_warn_message(
        "This operation can not be reverted. Make sure you know what you are doing.",
    );
    let manifest = read_manifest(&project_path.join(YAML_CONFIG_NAME)).map_err(|e| {
        print_error_message(&format!("Could read the rindexer.yaml please make sure you are running the command with rindexer.yaml in root: trace: {}", e));
        e
    })?;

    let postgres_enabled = manifest.storage.postgres_enabled();
    let csv_enabled = manifest.storage.csv_enabled();

    if !postgres_enabled && !csv_enabled {
        print_success_message("No storage enabled. Nothing to delete.");
        return Ok(());
    }

    if postgres_enabled {
        let postgres_delete = prompt_for_input_list(
            "Are you sure you wish to delete the database data (it can not be reverted)?",
            &["yes".to_string(), "no".to_string()],
            None,
        );

        if postgres_delete == "yes" {
            let postgres_client = PostgresClient::new().await.map_err(|e| {
                print_error_message(&format!("Could not connect to Postgres, make sure your connection string is mapping in the .env correctly: trace: {}", e));
                e
            })?;
            let sql = drop_tables_for_indexer_sql(&project_path, &manifest.to_indexer());

            postgres_client.batch_execute(sql.as_str()).await.map_err(|e| {
                print_error_message(&format!("Could not delete tables from Postgres make sure your connection string is mapping in the .env correctly: trace: {}", e));
                e
            })?;

            print_success_message(
                "\n\nSuccessfully deleted all data from the postgres database.\n\n",
            );
        }
    }

    if csv_enabled {
        let csv_delete = prompt_for_input_list(
            "Are you sure you wish to delete the csv data (it can not be reverted)?",
            &["yes".to_string(), "no".to_string()],
            None,
        );

        if csv_delete == "yes" {
            if let Some(csv) = &manifest.storage.csv {
                let path = &project_path.join(&csv.path);
                // if no csv exist we will just look like it cleared it
                if path.exists() {
                    remove_dir_all(&project_path.join(path)).await.map_err(|e| {
                        print_error_message(&format!("Could not delete csv files: trace: {}", e));
                        e
                    })?;
                }

                print_success_message("\n\nSuccessfully deleted all csv files.\n\n");
            } else {
                print_error_message("CSV storage is not enabled so no storage can be deleted. Please enable it in the YAML configuration file.");
            }
        }
    }

    Ok(())
}
