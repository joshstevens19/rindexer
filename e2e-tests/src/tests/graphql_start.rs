use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct GraphqlStartTests;

impl TestModule for GraphqlStartTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_graphql_service_starts",
            "Start ALL services with Postgres enabled and verify GraphQL stays up",
            graphql_service_starts_test,
        )
        .with_timeout(180)]
    }
}

fn graphql_service_starts_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running GraphQL Startup Test");

        let mut config = context.create_minimal_config();
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        let (container_name, pg_port) = crate::docker::start_postgres_container()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start postgres container: {}", e))?;
        context.register_container(container_name.clone());

        crate::docker::wait_for_postgres_ready(pg_port, 10).await?;

        // Allocate a dynamic GraphQL port
        let gql_port = crate::docker::allocate_free_port()?;

        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }
        r = r.with_env("GRAPHQL_PORT", &gql_port.to_string())
            .with_env("PORT", &gql_port.to_string());

        r.start_all().await?;
        context.rindexer = Some(r.clone());

        let gql_url = r
            .wait_for_graphql_url(20)
            .await
            .ok_or_else(|| anyhow::anyhow!("GraphQL URL not found in logs"))?;
        info!("GraphQL URL: {}", gql_url);

        if !r.is_running() {
            return Err(anyhow::anyhow!("Rindexer all services process is not running"));
        }

        info!("GraphQL Startup Test PASSED: start_all with Postgres, GraphQL running");
        Ok(())
    })
}
