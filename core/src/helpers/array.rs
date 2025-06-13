use std::collections::HashSet;

pub fn chunk_hashset<T: Clone + Eq + std::hash::Hash>(
    set: HashSet<T>,
    chunk_size: usize,
) -> Vec<HashSet<T>> {
    let mut chunks = Vec::with_capacity(set.len().div_ceil(chunk_size));
    let mut current_chunk = HashSet::with_capacity(chunk_size);

    for item in set {
        current_chunk.insert(item);

        if current_chunk.len() == chunk_size {
            chunks.push(std::mem::replace(&mut current_chunk, HashSet::with_capacity(chunk_size)));
        }
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks
}
