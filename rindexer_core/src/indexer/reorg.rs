use ethers::types::{U256, U64};

pub fn reorg_safe_distance_for_chain(chain_id: &U256) -> U64 {
    match chain_id.as_u64() {
        1 => U64::from(12),
        _ => U64::from(64),
    }
}

// pub fn compute_best_block_number(
//     reorg_safe_distance: U64,
//     end_block: U64,
//     latest_block_number: U64,
// ) -> U64 {
//     let best_block_number = current_block_number + reorg_safe_distance;
//     if best_block_number > latest_block_number {
//         latest_block_number
//     } else {
//         best_block_number
//     }
// }
