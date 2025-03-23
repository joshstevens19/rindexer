use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use ethers::types::{BlockNumber, U256, U64};
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::{
    event::{callback_registry::TxInformation, contract_setup::NetworkContract},
    provider::JsonRpcCachedProvider,
};

/// Stores reference point for a network (known block number and timestamp)
#[derive(Clone, Debug)]
struct ReferencePoint {
    block: U64,
    timestamp: U256,
}

/// Stores calibration data for block time estimation
#[derive(Clone, Debug)]
struct BlockTimeCalibration {
    avg_block_time_ms: f64,      // Average time between blocks in milliseconds
    calibration_block: U64,      // Block number used for calibration
    calibration_timestamp: U256, // Timestamp of calibration block (in seconds)
    last_update: u64,            // When calibration was last updated
    is_hardcoded: bool,          // Whether this uses hardcoded values
}

impl Default for BlockTimeCalibration {
    fn default() -> Self {
        Self {
            avg_block_time_ms: 0.0,
            calibration_block: U64::zero(),
            calibration_timestamp: U256::zero(),
            last_update: 0,
            is_hardcoded: false,
        }
    }
}

/// Block time estimator for predicting timestamps
struct BlockTimeEstimator {
    calibrations: Arc<Mutex<HashMap<String, BlockTimeCalibration>>>,
    network_to_chain_id: Arc<Mutex<HashMap<String, U256>>>,
    known_block_times: HashMap<U256, f64>, // block times in milliseconds
    reference_points: HashMap<U256, ReferencePoint>,
}

impl Clone for BlockTimeEstimator {
    fn clone(&self) -> Self {
        Self {
            calibrations: Arc::clone(&self.calibrations),
            network_to_chain_id: Arc::clone(&self.network_to_chain_id),
            known_block_times: self.known_block_times.clone(),
            reference_points: self.reference_points.clone(),
        }
    }
}

impl BlockTimeEstimator {
    /// Create a new block time estimator with preconfigured network data
    fn new() -> Self {
        let mut known_block_times = HashMap::new();
        // Ethereum mainnet
        known_block_times.insert(U256::from(1), 12100.0);
        // TODO: add more well known ones

        let mut reference_points = HashMap::new();
        // Ethereum mainnet
        reference_points.insert(
            U256::from(1),
            ReferencePoint {
                block: U64::from(20111643),
                timestamp: U256::from(1718628959), // GMT: Monday, 17 June 2024 12:55:59
            },
        );

        // TODO: add more reference ones

        Self {
            calibrations: Arc::new(Mutex::new(HashMap::new())),
            network_to_chain_id: Arc::new(Mutex::new(HashMap::new())),
            known_block_times,
            reference_points,
        }
    }

    async fn init(
        &self,
        network_contracts: &[NetworkContract],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        {
            let mut calibrations = self.calibrations.lock().await;
            let mut network_to_chain_id = self.network_to_chain_id.lock().await;

            for network in network_contracts {
                let chain_id = network.cached_provider.get_chain_id().await?;
                let network_name = network.network.clone();

                network_to_chain_id.insert(network_name.clone(), chain_id);

                if let Some(&known_time_ms) = self.known_block_times.get(&chain_id) {
                    let mut calibration = BlockTimeCalibration::default();
                    calibration.avg_block_time_ms = known_time_ms;
                    calibration.is_hardcoded = true;

                    if let Some(reference) = self.reference_points.get(&chain_id) {
                        calibration.calibration_block = reference.block;
                        calibration.calibration_timestamp = reference.timestamp;
                        calibration.last_update = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        info!(
                            "Using hardcoded block time for network {} (chain ID: {}): {:.1} ms",
                            network_name, chain_id, known_time_ms
                        );
                    }

                    calibrations.insert(network_name.clone(), calibration);
                } else {
                    // We'll calculate this from recent blocks
                    info!(
                        "Calculating block time for network: {} (chain ID: {})",
                        network_name, chain_id
                    );

                    calibrations.insert(network_name.clone(), BlockTimeCalibration::default());
                }
            }
        }

        let mut handles = vec![];

        for network in network_contracts {
            let needs_calibration = {
                let calibrations = self.calibrations.lock().await;
                let entry = calibrations.get(&network.network);

                entry.map_or(true, |calib| {
                    // Need calibration if:
                    // 1. Not hardcoded, or
                    // 2. Hardcoded but missing reference point
                    !calib.is_hardcoded || calib.calibration_timestamp == U256::zero()
                })
            };

            if needs_calibration {
                let estimator = self.clone();
                let provider = Arc::clone(&network.cached_provider);
                let name = network.network.clone();

                let handle = tokio::spawn(async move {
                    match estimator.update_calibration(provider, &name, 5).await {
                        Ok(_) => {
                            info!("Initial calibration successful for {}", name);
                            true
                        }
                        Err(e) => {
                            info!("Initial calibration failed for {}: {}", name, e);
                            false
                        }
                    }
                });

                handles.push(handle);
            }
        }

        let _ =
            tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(handles)).await;

        Ok(())
    }

    async fn update_calibration(
        &self,
        provider: Arc<JsonRpcCachedProvider>,
        network_name: &str,
        sample_size: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let calibration = {
            let calibrations = self.calibrations.lock().await;
            calibrations.get(network_name).cloned().unwrap_or_default()
        };

        // Get latest block info
        let latest_block = provider.get_block_number().await?;
        let block = provider
            .get_block(BlockNumber::Number(latest_block))
            .await?
            .ok_or("Failed to get latest block")?;

        // Update the calibration point
        let mut updated_calibration = calibration.clone();
        updated_calibration.calibration_block = latest_block;
        updated_calibration.calibration_timestamp = block.timestamp;
        updated_calibration.last_update = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if calibration.is_hardcoded {
            let mut calibrations = self.calibrations.lock().await;
            calibrations.insert(network_name.to_string(), updated_calibration);
            return Ok(());
        }

        let mut recent_blocks = Vec::with_capacity(sample_size);
        recent_blocks.push((latest_block, block.timestamp));

        let mut block_fetch_tasks = Vec::with_capacity(sample_size - 1);

        for i in 1..sample_size {
            let block_number = latest_block.as_u64().saturating_sub(i as u64);
            let provider_clone = provider.clone();

            let task = tokio::spawn(async move {
                match provider_clone.get_block(BlockNumber::Number(block_number.into())).await {
                    Ok(Some(block)) => Some((U64::from(block_number), block.timestamp)),
                    _ => None,
                }
            });

            block_fetch_tasks.push(task);
        }

        for task in futures::future::join_all(block_fetch_tasks).await {
            if let Ok(Some(block_data)) = task {
                recent_blocks.push(block_data);
            }
        }

        if recent_blocks.len() < 2 {
            return Err("Not enough blocks to calculate average".into());
        }

        recent_blocks.sort_by_key(|&(num, _)| num);

        let mut total_time_ms = 0.0;
        let mut total_blocks = 0.0;

        for i in 1..recent_blocks.len() {
            let (prev_num, prev_time) = recent_blocks[i - 1];
            let (curr_num, curr_time) = recent_blocks[i];

            let block_diff = curr_num.as_u64().saturating_sub(prev_num.as_u64()) as f64;
            if block_diff > 0.0 {
                let time_diff_ms =
                    (curr_time.as_u64().saturating_sub(prev_time.as_u64())) as f64 * 1000.0;
                total_time_ms += time_diff_ms;
                total_blocks += block_diff;
            }
        }

        if total_blocks > 0.0 {
            let new_average_ms = total_time_ms / total_blocks;
            updated_calibration.avg_block_time_ms = new_average_ms;

            let mut calibrations = self.calibrations.lock().await;
            calibrations.insert(network_name.to_string(), updated_calibration);

            info!("Updated block time for {}: {:.1} ms", network_name, new_average_ms);
            Ok(())
        } else {
            Err("Failed to calculate average block time".into())
        }
    }

    async fn start_periodic_updates(&self, networks: Vec<NetworkContract>) {
        let update_interval = Duration::from_secs(600); // 10 minutes
        let estimator = self.clone();

        tokio::spawn(async move {
            let mut first_run = true;
            let mut interval = tokio::time::interval(update_interval);

            loop {
                // Skip waiting on the first run, wait on subsequent runs
                if !first_run {
                    interval.tick().await;
                }
                first_run = false;

                let update_tasks = networks.iter().map(|network| {
                    let network_name = network.network.clone();
                    let provider = Arc::clone(&network.cached_provider);
                    let estimator_clone = estimator.clone();

                    tokio::spawn(async move {
                        if let Err(e) =
                            estimator_clone.update_calibration(provider, &network_name, 5).await
                        {
                            info!("Error updating block time for {}: {}", network_name, e);
                        }
                    })
                });

                let _ = tokio::time::timeout(
                    Duration::from_secs(60),
                    futures::future::join_all(update_tasks),
                )
                .await;
            }
        });
    }

    /// Estimate a timestamp for a block number
    async fn estimate_timestamp(&self, network_name: &str, block_number: &U64) -> U256 {
        // Try to get calibration data
        let calibration = {
            let calibrations = self.calibrations.lock().await;
            calibrations.get(network_name).cloned()
        };

        // If we have valid calibration data, calculate timestamp
        if let Some(calibration) = calibration {
            if calibration.calibration_timestamp > U256::zero() &&
                calibration.avg_block_time_ms > 0.0
            {
                // Calculate block difference (can be positive or negative)
                let block_diff_i64 = if *block_number >= calibration.calibration_block {
                    // Future block (relative to reference)
                    block_number.as_u64().saturating_sub(calibration.calibration_block.as_u64())
                        as i64
                } else {
                    // Historical block (relative to reference)
                    -((calibration.calibration_block.as_u64().saturating_sub(block_number.as_u64()))
                        as i64)
                };

                // Convert milliseconds to seconds for timestamp calculation
                // Block timestamps are in seconds, so we divide by 1000
                let time_diff_seconds =
                    (block_diff_i64 as f64 * calibration.avg_block_time_ms / 1000.0) as i64;

                // For historical blocks, time_diff_seconds will be negative
                // For future blocks, time_diff_seconds will be positive
                // This works correctly in both directions
                if let Some(estimated) =
                    calibration.calibration_timestamp.as_u64().checked_add_signed(time_diff_seconds)
                {
                    return U256::from(estimated);
                }
            }
        }

        unreachable!(
            "Should never get here - Failed to estimate timestamp for block {} on network {}",
            block_number, network_name
        );
    }

    /// Check if timestamp prediction is ready for a network
    async fn is_ready(&self, network_name: &str) -> bool {
        let calibrations = self.calibrations.lock().await;

        if let Some(calibration) = calibrations.get(network_name) {
            calibration.calibration_timestamp > U256::zero() && calibration.avg_block_time_ms > 0.0
        } else {
            false
        }
    }
}

// Global estimator instance
lazy_static::lazy_static! {
    static ref TIMESTAMP_ESTIMATOR: Mutex<Option<BlockTimeEstimator>> = Mutex::new(None);
}

/// Initialize and start the timestamp prediction system
pub fn start_predictable_timestamps(network_contracts: Vec<NetworkContract>) {
    info!("Starting predictable timestamps system");

    tokio::spawn(async move {
        let estimator = BlockTimeEstimator::new();

        if let Err(e) = estimator.init(&network_contracts).await {
            error!("Failed to initialize timestamp estimator: {}", e);
            return;
        }

        // Store in global instance first so it's available
        {
            let mut global = TIMESTAMP_ESTIMATOR.lock().await;
            *global = Some(estimator.clone());
        }

        // Start background updates
        estimator.start_periodic_updates(network_contracts).await;

        info!("Predictable timestamps system started successfully");
    });
}

/// Get timestamp for a transaction
/// If block_timestamp is available in the transaction, use that
/// Otherwise estimate based on block number
pub async fn get_block_timestamp(tx_information: &TxInformation) -> DateTime<Utc> {
    if let Some(timestamp) = tx_information.block_timestamp {
        let known_timestamp = timestamp.as_u64();
        return DateTime::<Utc>::from_timestamp(known_timestamp as i64, 0)
            .expect("get_block_timestamp - invalid timestamp");
    }

    get_timestamp_for_block(&tx_information.network, &tx_information.block_number).await
}

/// Get a block timestamp directly by network name and block number
/// This is useful for cases where you don't have a TxInformation struct
pub async fn get_timestamp_for_block(network_name: &str, block_number: &U64) -> DateTime<Utc> {
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(10);

    loop {
        let global = TIMESTAMP_ESTIMATOR.lock().await;

        if let Some(estimator) = global.as_ref() {
            let is_ready = estimator.is_ready(network_name).await;

            if is_ready {
                let estimate =
                    estimator.estimate_timestamp(network_name, block_number).await.as_u64();

                return DateTime::<Utc>::from_timestamp(estimate as i64, 0)
                    .expect("get_timestamp_for_block - invalid timestamp");
            }
        }

        drop(global);

        if start_time.elapsed() > timeout {
            panic!("Timed out waiting for timestamp prediction for network: {}", network_name);
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
