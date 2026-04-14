use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::B256;
use anyhow::Context;
use clickhouse::Row;
use serde::Deserialize;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;

use super::window::BlockChainWindow;

pub struct ReorgBlockHashPersistence {
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
}

impl ReorgBlockHashPersistence {
    pub fn new(
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
    ) -> Self {
        Self { postgres, clickhouse }
    }

    /// Load all entries from the persisted `reorg_block_hashes` table into a new
    /// `BlockChainWindow`. Priority: postgres > clickhouse.
    pub async fn load(
        &self,
        network: &str,
        max_window_size: usize,
    ) -> anyhow::Result<BlockChainWindow> {
        let mut window = BlockChainWindow::try_new(max_window_size)?;

        if let Some(postgres) = &self.postgres {
            let query = r#"
                SELECT block_number, block_hash, parent_hash
                FROM rindexer_internal.reorg_block_hashes
                WHERE network = $1
                ORDER BY block_number ASC"#;

            let rows = postgres
                .query(query, &[&network])
                .await
                .context("Failed to load reorg_block_hashes from postgres")?;

            for row in rows {
                let block_number: i64 = row.get("block_number");
                let block_hash_str: String = row.get("block_hash");
                let parent_hash_str: String = row.get("parent_hash");

                let block_hash = B256::from_str(&block_hash_str).with_context(|| {
                    format!(
                        "Failed to parse block_hash '{}' at block {}",
                        block_hash_str, block_number
                    )
                })?;
                let parent_hash = B256::from_str(&parent_hash_str).with_context(|| {
                    format!(
                        "Failed to parse parent_hash '{}' at block {}",
                        parent_hash_str, block_number
                    )
                })?;

                window.insert(block_number as u64, block_hash, parent_hash);
            }

            return Ok(window);
        }

        if let Some(clickhouse) = &self.clickhouse {
            #[derive(Row, Deserialize)]
            struct ReorgBlockHashRow {
                block_number: u64,
                block_hash: String,
                parent_hash: String,
            }

            let query = format!(
                r#"SELECT block_number, block_hash, parent_hash
                 FROM rindexer_internal.reorg_block_hashes FINAL
                 WHERE network = '{}'
                 ORDER BY block_number ASC"#,
                network
            );

            let rows = clickhouse
                .query_all::<ReorgBlockHashRow>(&query)
                .await
                .context("Failed to load reorg_block_hashes from clickhouse")?;

            for row in rows {
                let block_hash = B256::from_str(&row.block_hash).with_context(|| {
                    format!(
                        "Failed to parse block_hash '{}' at block {}",
                        row.block_hash, row.block_number
                    )
                })?;
                let parent_hash = B256::from_str(&row.parent_hash).with_context(|| {
                    format!(
                        "Failed to parse parent_hash '{}' at block {}",
                        row.parent_hash, row.block_number
                    )
                })?;

                window.insert(row.block_number, block_hash, parent_hash);
            }

            return Ok(window);
        }

        Ok(window)
    }

    /// Persist a single new block entry. Uses upsert for postgres, simple insert
    /// for clickhouse.
    pub async fn insert_block(
        &self,
        network: &str,
        block_number: u64,
        block_hash: &str,
        parent_hash: &str,
    ) -> anyhow::Result<()> {
        if let Some(postgres) = &self.postgres {
            let query = r#"INSERT INTO rindexer_internal.reorg_block_hashes
                         (network, block_number, block_hash, parent_hash)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (network, block_number)
                         DO UPDATE SET block_hash = EXCLUDED.block_hash,
                         parent_hash = EXCLUDED.parent_hash"#;

            let block_number_i64 = block_number as i64;
            postgres
                .execute(query, &[&network, &block_number_i64, &block_hash, &parent_hash])
                .await
                .with_context(|| {
                    format!("Failed to insert block {} into postgres", block_number)
                })?;
        }

        if let Some(clickhouse) = &self.clickhouse {
            let query = format!(
                "INSERT INTO rindexer_internal.reorg_block_hashes \
                 (network, block_number, block_hash, parent_hash) \
                 VALUES ('{}', {}, '{}', '{}')",
                network, block_number, block_hash, parent_hash
            );

            clickhouse.execute(&query).await.with_context(|| {
                format!("Failed to insert block {} into clickhouse", block_number)
            })?;
        }

        Ok(())
    }

    /// Delete entries older than the given block number.
    pub async fn prune(&self, network: &str, older_than: u64) -> anyhow::Result<()> {
        if let Some(postgres) = &self.postgres {
            let query = r#"DELETE FROM rindexer_internal.reorg_block_hashes
                         WHERE network = $1 AND block_number < $2"#;

            let older_than_i64 = older_than as i64;
            postgres
                .execute(query, &[&network, &older_than_i64])
                .await
                .context("Failed to prune reorg_block_hashes in postgres")?;
        }

        if let Some(clickhouse) = &self.clickhouse {
            let query = format!(
                r#"ALTER TABLE rindexer_internal.reorg_block_hashes DELETE
                 WHERE network = '{}' AND block_number < {}"#,
                network, older_than
            );

            clickhouse
                .execute(&query)
                .await
                .context("Failed to prune reorg_block_hashes in clickhouse")?;
        }

        Ok(())
    }
}
