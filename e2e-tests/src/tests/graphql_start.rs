use anyhow::Result;
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct GraphqlStartTests;

impl TestModule for GraphqlStartTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_graphql_service_starts",
                "Start ALL services with Postgres enabled and verify GraphQL stays up",
                graphql_service_starts_test,
            ).with_timeout(180),
        ]
    }
}

fn graphql_service_starts_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running GraphQL Startup Test");

        // Build minimal config with Postgres enabled (no contracts needed)
        let mut config = context.create_minimal_config();
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        // Write config to project
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        // Start ephemeral Postgres
        let (container_name, pg_port) = crate::docker::start_postgres_container().await
            .map_err(|e| anyhow::anyhow!("Failed to start postgres container: {}", e))?;
        // Wait for Postgres readiness
        {
            let mut ready = false;
            for _ in 0..40 {
                if tokio_postgres::connect(
                    &format!("host=localhost port={} user=postgres password=postgres dbname=postgres", pg_port),
                    tokio_postgres::NoTls,
                ).await.is_ok() { ready = true; break; }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            if !ready { return Err(anyhow::anyhow!("Postgres did not become ready in time")); }
        }

        // Start ALL services with DB env and explicit GraphQL port
        let mut r = crate::rindexer_client::RindexerInstance::new(&context.rindexer_binary, context.project_path.clone())
            .with_env("POSTGRES_HOST", "localhost")
            .with_env("POSTGRES_PORT", &pg_port.to_string())
            .with_env("POSTGRES_USER", "postgres")
            .with_env("POSTGRES_PASSWORD", "postgres")
            .with_env("POSTGRES_DB", "postgres")
            .with_env("DATABASE_URL", &format!("postgres://postgres:postgres@localhost:{}/postgres", pg_port))
            .with_env("GRAPHQL_PORT", "3001")
            .with_env("PORT", "3001");
        r.start_all().await?;
        context.rindexer = Some(r.clone());

        // Wait for GraphQL URL
        let gql_url = r.wait_for_graphql_url(20).await
            .ok_or_else(|| anyhow::anyhow!("GraphQL URL not found in logs"))?;
        info!("GraphQL URL: {}", gql_url);

        // Ensure process is running
        if !r.is_running() {
            return Err(anyhow::anyhow!("Rindexer all services process is not running"));
        }

        // Cleanup PG container
        let _ = crate::docker::stop_postgres_container(&container_name).await;

        info!("✓ GraphQL Startup Test PASSED: start_all with Postgres, GraphQL running");
        Ok(())
    })
}


