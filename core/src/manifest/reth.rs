use serde::{Deserialize, Serialize};

/// Configuration for Reth node and ExEx
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RethConfig {
    /// Whether to enable Reth integration
    pub enabled: bool,
    
    /// CLI args for reth node command
    #[serde(default)]
    pub cli_args: Vec<String>,
}

impl RethConfig {
    /// Convert to reth CLI, parsing the stored args
    pub fn to_cli(&self) -> Result<reth::cli::Cli, String> {
        use reth::cli::Cli;
        
        let mut args = vec!["reth".to_string(), "node".to_string()];
        args.extend(self.cli_args.clone());
        
        Cli::try_parse_args_from(&args)
            .map_err(|e| format!("Failed to parse reth CLI args: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_deserialize_basic_config() {
        let yaml = r#"
            enabled: true
            cli_args: []
        "#;
        
        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert!(config.cli_args.is_empty());
    }
    
    #[test]
    fn test_deserialize_with_custom_ipc_path() {
        let yaml = r#"
            enabled: true
            cli_args: ["--ipcpath", "/custom/path.ipc"]
        "#;
        
        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.cli_args, vec!["--ipcpath", "/custom/path.ipc"]);
        
        // Test that to_cli works and extracts the IPC path
        let cli = config.to_cli().unwrap();
        if let reth::cli::Commands::Node(node_cmd) = &cli.command {
            assert_eq!(node_cmd.rpc.ipcpath, "/custom/path.ipc");
        } else {
            panic!("Expected node command");
        }
    }
    
    #[test]
    fn test_deserialize_with_http_enabled() {
        let yaml = r#"
            enabled: true
            cli_args: ["--http", "--http.addr", "0.0.0.0"]
        "#;
        
        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        
        // Build CLI to verify HTTP settings
        let cli = config.to_cli().unwrap();
        if let reth::cli::Commands::Node(node_cmd) = &cli.command {
            assert!(node_cmd.rpc.http);
            assert_eq!(node_cmd.rpc.http_addr.to_string(), "0.0.0.0");
        } else {
            panic!("Expected node command");
        }
    }
    
    #[test]
    fn test_ipc_disabled() {
        let yaml = r#"
            enabled: true
            cli_args: ["--ipcdisable"]
        "#;
        
        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        
        // Should parse successfully but IPC should be disabled
        let cli = config.to_cli().unwrap();
        if let reth::cli::Commands::Node(node_cmd) = &cli.command {
            assert!(node_cmd.rpc.ipcdisable);
        } else {
            panic!("Expected node command");
        }
    }
    
    #[test]
    fn test_serialize_includes_config() {
        let yaml = r#"
            enabled: true
            cli_args: ["--http"]
        "#;
        
        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        let serialized = serde_yaml::to_string(&config).unwrap();
        
        // Should contain both fields
        assert!(serialized.contains("enabled: true"));
        assert!(serialized.contains("cli_args:"));
        assert!(serialized.contains("--http"));
    }
}