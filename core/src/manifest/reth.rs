use reth::cli::Cli;
use serde::{Deserialize, Serialize};

/// Default value for logging field
fn default_true() -> bool {
    true
}

/// Configuration for Reth node and ExEx
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RethConfig {
    /// Whether to enable Reth integration
    pub enabled: bool,

    /// Whether to show reth logs in stdout
    #[serde(default = "default_true")]
    pub logging: bool,

    /// CLI args as "key value" strings (e.g., "--datadir /path/to/data")
    #[serde(default)]
    pub cli_args: Vec<String>,
}

impl RethConfig {
    /// Create RethConfig from CLI arguments, validating them through reth's CLI parser
    pub fn from_cli_args(args: Vec<String>) -> Result<Self, String> {
        use reth::cli::Cli;

        // Validate args by parsing through reth's CLI
        let mut full_args = vec!["reth".to_string(), "node".to_string()];
        full_args.extend(args.clone());

        // This will error if args are invalid
        let _cli = Cli::try_parse_args_from(&full_args)
            .map_err(|e| format!("Failed to parse reth CLI args: {e}"))?;

        // Store args in space-separated format for clean YAML
        let cli_args = Self::combine_args_with_values(&args);

        Ok(Self { enabled: true, logging: true, cli_args })
    }

    /// Combine CLI flags with their values into space-separated strings
    /// e.g., ["--http", "--datadir", "/path"] -> ["--http", "--datadir /path"]
    fn combine_args_with_values(args: &[String]) -> Vec<String> {
        let mut combined = Vec::new();
        let mut i = 0;

        while i < args.len() {
            let arg = &args[i];
            if arg.starts_with("--") {
                if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                    // Combine flag and value
                    combined.push(format!("{} {}", arg, args[i + 1]));
                    i += 2;
                } else {
                    // Just the flag
                    combined.push(arg.to_string());
                    i += 1;
                }
            } else {
                // Skip non-flag arguments
                i += 1;
            }
        }

        combined
    }

    /// Convert to reth CLI, parsing the stored args
    pub fn to_cli(&self) -> Result<reth::cli::Cli, String> {
        let args = self.to_cli_args();
        Cli::try_parse_args_from(&args).map_err(|e| format!("Failed to parse reth CLI args: {e}"))
    }

    /// Convert to reth CLI args
    pub fn to_cli_args(&self) -> Vec<String> {
        let mut args = vec!["reth".to_string()];

        // Add node subcommand
        args.push("node".to_string());

        // Add --quiet if logging is disabled
        if !self.logging {
            println!("adding --quiet");
            args.push("--quiet".to_string());
        }

        // Split space-separated args back into individual arguments
        for arg in &self.cli_args {
            if let Some(space_idx) = arg.find(' ') {
                let (flag, value) = arg.split_at(space_idx);
                args.push(flag.to_string());
                args.push(value.trim().to_string());
            } else {
                args.push(arg.to_string());
            }
        }

        args
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
            cli_args:
                - --ipcpath /custom/path.ipc
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.cli_args, vec!["--ipcpath /custom/path.ipc"]);

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
            cli_args:
                - --http
                - --http.addr 0.0.0.0
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.cli_args, vec!["--http", "--http.addr 0.0.0.0"]);

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
            cli_args:
                - --ipcdisable
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.cli_args, vec!["--ipcdisable"]);

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
            cli_args:
                - --http
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        let serialized = serde_yaml::to_string(&config).unwrap();

        // Should contain both fields
        assert!(serialized.contains("enabled: true"));
        assert!(serialized.contains("cli_args:"));
        assert!(serialized.contains("--http"));
    }

    #[test]
    fn test_complex_cli_args() {
        let yaml = r#"
            enabled: true
            cli_args:
                - --authrpc.jwtsecret /Users/skanda/secrets/jwt.hex
                - --authrpc.addr 127.0.0.1
                - --authrpc.port 8551
                - --datadir /Volumes/T9/reth
                - --metrics 127.0.0.1:9001
                - --chain sepolia
                - --http
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert_eq!(config.cli_args.len(), 7);
        assert!(config
            .cli_args
            .contains(&"--authrpc.jwtsecret /Users/skanda/secrets/jwt.hex".to_string()));
        assert!(config.cli_args.contains(&"--http".to_string()));

        // Test that to_cli_args produces correct output
        let args = config.to_cli_args();
        assert!(args.contains(&"--authrpc.jwtsecret".to_string()));
        assert!(args.contains(&"/Users/skanda/secrets/jwt.hex".to_string()));
        assert!(args.contains(&"--http".to_string()));
    }

    #[test]
    fn test_from_cli_args() {
        let args = vec![
            "--http".to_string(),
            "--datadir".to_string(),
            "/path/to/data".to_string(),
            "--chain".to_string(),
            "sepolia".to_string(),
            "--ipcdisable".to_string(),
        ];

        let config = RethConfig::from_cli_args(args).unwrap();
        assert!(config.enabled);
        assert_eq!(
            config.cli_args,
            vec!["--http", "--datadir /path/to/data", "--chain sepolia", "--ipcdisable"]
        );

        // Verify it can be converted back to CLI
        let cli = config.to_cli().unwrap();
        assert!(matches!(cli.command, reth::cli::Commands::Node(_)));
    }

    #[test]
    fn test_logging_config() {
        // Test with logging enabled (default)
        let yaml = r#"
            enabled: true
            cli_args:
                - --http
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert!(config.logging); // Should default to true

        let args = config.to_cli_args();
        assert!(!args.contains(&"--quiet".to_string()));

        // Test with logging disabled
        let yaml = r#"
            enabled: true
            logging: false
            cli_args:
                - --http
        "#;

        let config: RethConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.enabled);
        assert!(!config.logging);

        let args = config.to_cli_args();
        assert!(args.contains(&"--quiet".to_string()));
        // --quiet should come before other args
        let quiet_index = args.iter().position(|arg| arg == "--quiet").unwrap();
        let http_index = args.iter().position(|arg| arg == "--http").unwrap();
        assert!(quiet_index < http_index);
    }
}
