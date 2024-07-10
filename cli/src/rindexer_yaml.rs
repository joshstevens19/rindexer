use crate::console::print_error_message;
use rindexer::manifest::yaml::YAML_CONFIG_NAME;
use std::fs;

pub fn rindexer_yaml_exists() -> bool {
    fs::metadata(YAML_CONFIG_NAME).is_ok()
}

pub fn rindexer_yaml_does_not_exist() -> bool {
    !rindexer_yaml_exists()
}

pub fn validate_rindexer_yaml_exist() {
    if rindexer_yaml_does_not_exist() {
        print_error_message("rindexer.yaml does not exist in the current directory. Please use rindexer new to create a new project.");
        std::process::exit(1);
    }
}
