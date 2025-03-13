use std::ops::RangeInclusive;

use alloy_primitives::BlockNumber;
use alloy_rpc_types::Filter;

pub fn extract_block_range(filter: &Filter) -> eyre::Result<RangeInclusive<BlockNumber>> {
    let (from_opt, to_opt) = filter.block_option.as_range();

    let from_block = from_opt
        .and_then(|tag| tag.as_number())
        .ok_or_else(|| eyre::eyre!("Invalid from_block in filter"))?;

    let to_block = to_opt
        .and_then(|tag| tag.as_number())
        .ok_or_else(|| eyre::eyre!("Invalid to_block in filter"))?;

    Ok(from_block..=to_block)
}
