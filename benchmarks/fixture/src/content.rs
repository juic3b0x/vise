use sha2::{Digest, Sha256};

// Counter-based SplitMix64: deterministic, seekable, non-cryptographic test data.
// Explicit wrapping is part of the PRNG, not unchecked request arithmetic.
pub fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

pub fn fill(seed: u64, file: usize, offset: u64, output: &mut [u8]) {
    let key = mix(seed ^ mix(file as u64));
    for (position, byte) in (offset..).zip(output.iter_mut()) {
        let word = mix(key ^ (position / 8)).to_le_bytes();
        if let Some(value) = word.get((position % 8) as usize) {
            *byte = *value;
        }
    }
}

pub fn digest(seed: u64, file: usize, size: u64) -> String {
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    let mut offset = 0;
    while offset < size {
        let count = ((size - offset).min(buffer.len() as u64)) as usize;
        if let Some(bytes) = buffer.get_mut(..count) {
            fill(seed, file, offset, bytes);
            hash.update(bytes);
        }
        offset += count as u64;
    }
    format!("{:x}", hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_match_whole_content_at_unaligned_offsets() {
        let mut whole = vec![0; 1027];
        fill(42, 7, 0, &mut whole);
        for start in [0, 1, 7, 8, 1001] {
            let mut part = [0; 19];
            fill(42, 7, start as u64, &mut part);
            assert_eq!(whole.get(start..start + 19), Some(part.as_slice()));
        }
        let mut other = [0; 19];
        fill(43, 7, 0, &mut other);
        assert_ne!(whole.get(..19), Some(other.as_slice()));
    }
}
