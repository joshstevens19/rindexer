use super::{chat::ChatConfig, core::Manifest, stream::StreamsConfig};

fn require(configured: bool, available: bool, feature: &'static str) -> Result<(), String> {
    if configured && !available {
        return Err(format!("Configured integration requires the `{feature}` Cargo feature"));
    }
    Ok(())
}

fn validate_streams(streams: &StreamsConfig) -> Result<(), String> {
    require(streams.sns.is_some(), cfg!(feature = "sns"), "sns")?;
    require(streams.redis.is_some(), cfg!(feature = "redis"), "redis")?;
    require(streams.rabbitmq.is_some(), cfg!(feature = "rabbitmq"), "rabbitmq")
}

fn validate_chat(chat: &ChatConfig) -> Result<(), String> {
    require(chat.discord.is_some(), cfg!(feature = "discord"), "discord")?;
    require(chat.telegram.is_some(), cfg!(feature = "telegram"), "telegram")
}

pub(super) fn validate(manifest: &Manifest) -> Result<(), String> {
    require(manifest.storage.postgres_enabled(), cfg!(feature = "postgres"), "postgres")?;
    require(manifest.storage.clickhouse_enabled(), cfg!(feature = "clickhouse"), "clickhouse")?;
    require(manifest.phantom.is_some(), cfg!(feature = "phantom"), "phantom")?;
    require(
        manifest.networks.iter().any(|network| network.hypersync.is_some()),
        cfg!(feature = "hypersync"),
        "hypersync",
    )?;
    for contract in &manifest.contracts {
        if let Some(streams) = &contract.streams {
            validate_streams(streams)?;
        }
        if let Some(chat) = &contract.chat {
            validate_chat(chat)?;
        }
    }
    if manifest.native_transfers.enabled {
        if let Some(streams) = &manifest.native_transfers.streams {
            validate_streams(streams)?;
        }
        if let Some(chat) = &manifest.native_transfers.chat {
            validate_chat(chat)?;
        }
    }
    // GraphQL settings alone do not start a server. start_rindexer validates
    // the actual GraphqlOverrideSettings.enabled request instead.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(extra: &str) -> Manifest {
        serde_yaml::from_str(&format!(
            "name: features\nproject_type: rust\n{}contracts: []\n{extra}",
            if extra.starts_with("networks:") { "" } else { "networks: []\n" },
        ))
        .unwrap()
    }

    #[test]
    fn minimal_manifest_needs_no_optional_features() {
        assert!(validate(&manifest("")).is_ok());
        assert!(validate(&manifest("storage: {postgres: {enabled: false}}")).is_ok());
        assert!(validate(&manifest("graphql: {port: 3001}")).is_ok());
    }

    #[test]
    fn configured_integrations_require_their_features() {
        for (config, feature, enabled) in [
            ("storage: {postgres: {enabled: true}}", "postgres", cfg!(feature = "postgres")),
            ("storage: {clickhouse: {enabled: true}}", "clickhouse", cfg!(feature = "clickhouse")),
            ("networks: [{name: test, chain_id: 1, rpc: http://localhost:8545, hypersync: {}}]", "hypersync", cfg!(feature = "hypersync")),
            ("native_transfers: {enabled: true, streams: {redis: {connection_uri: 'redis://localhost', streams: []}}}", "redis", cfg!(feature = "redis")),
            ("native_transfers: {enabled: true, streams: {rabbitmq: {url: 'amqp://localhost', exchanges: []}}}", "rabbitmq", cfg!(feature = "rabbitmq")),
            ("phantom: {dyrpc: {api_key: test}}", "phantom", cfg!(feature = "phantom")),
            (
                "native_transfers: {enabled: true, streams: {sns: {aws_config: {region: test, access_key: test, secret_key: test}, topics: []}}}",
                "sns",
                cfg!(feature = "sns"),
            ),
            (
                "native_transfers: {enabled: true, chat: {telegram: []}}",
                "telegram",
                cfg!(feature = "telegram"),
            ),
            (
                "native_transfers: {enabled: true, chat: {discord: []}}",
                "discord",
                cfg!(feature = "discord"),
            ),
        ] {
            let parsed = manifest(config);
            // Disabled integrations must survive serialization so validation
            // cannot silently lose a user's configuration.
            let roundtrip: Manifest =
                serde_yaml::from_str(&serde_yaml::to_string(&parsed).unwrap()).unwrap();
            let result = validate(&roundtrip);
            if enabled {
                assert!(result.is_ok(), "{feature}: {result:?}");
            } else {
                assert!(result.unwrap_err().contains(&format!("`{feature}`")));
            }
        }
    }
}
