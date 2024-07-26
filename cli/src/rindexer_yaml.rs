use std::{fs, path::Path};

use rindexer::manifest::yaml::YAML_CONFIG_NAME;

use crate::console::print_error_message;

pub fn rindexer_yaml_exists(project_path: &Path) -> bool {
    fs::metadata(project_path.join(YAML_CONFIG_NAME)).is_ok()
}

pub fn rindexer_yaml_does_not_exist(project_path: &Path) -> bool {
    !rindexer_yaml_exists(project_path)
}

pub fn validate_rindexer_yaml_exist(project_path: &Path) {
    if rindexer_yaml_does_not_exist(project_path) {
        print_error_message("rindexer.yaml does not exist in the current directory. Please use rindexer new to create a new project.");
        std::process::exit(1);
    }
}
