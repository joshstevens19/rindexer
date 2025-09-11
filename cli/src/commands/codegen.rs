use std::path::PathBuf;

use crate::{
    cli_interface::CodegenSubcommands,
    console::{print_error_message, print_success_message},
    rindexer_yaml::validate_rindexer_yaml_exist,
};
use rindexer::{
    format_all_files_for_project, generate_graphql_queries,
    generator::build::{generate_rindexer_handlers, generate_rindexer_typings},
    manifest::{
        core::ProjectType,
        graphql::default_graphql_port,
        yaml::{read_manifest, YAML_CONFIG_NAME},
    },
};

pub async fn handle_codegen_command(
    project_path: PathBuf,
    subcommand: &CodegenSubcommands,
) -> Result<(), Box<dyn std::error::Error>> {
    if let CodegenSubcommands::GraphQL { endpoint } = subcommand {
        let default_url = format!("http://localhost:{}/graphql", default_graphql_port());
        let url = endpoint.as_deref().unwrap_or(&default_url);
        generate_graphql_queries(url, &project_path).await.map_err(|e| {
            print_error_message(&format!("Failed to generate graphql queries: {e}"));
            e
        })?;

        print_success_message("Generated graphql queries.");

        return Ok(());
    }

    validate_rindexer_yaml_exist(&project_path);

    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let manifest = read_manifest(&rindexer_yaml_path).map_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {e}"));
        e
    })?;
    if manifest.project_type == ProjectType::NoCode {
        let error = "This command is not supported for no-code projects, please migrate to a project to use this.";
        print_error_message(error);
        return Err(error.into());
    }

    match subcommand {
        CodegenSubcommands::Typings => {
            generate_rindexer_typings(&manifest, &rindexer_yaml_path, true).map_err(|e| {
                print_error_message(&format!("Failed to generate rindexer typings: {e}"));
                e
            })?;
            format_all_files_for_project(project_path);
            print_success_message("Generated rindexer typings.");
        }
        CodegenSubcommands::Indexer => {
            generate_rindexer_handlers(manifest, &rindexer_yaml_path, true).map_err(|e| {
                print_error_message(&format!("Failed to generate rindexer indexer handlers: {e}"));
                e
            })?;
            format_all_files_for_project(project_path);
            print_success_message("Generated rindexer indexer handlers.");
        }
        CodegenSubcommands::GraphQL { endpoint: _endpoint } => {
            unreachable!("This should not be reachable");
        }
    }

    Ok(())
}
