use super::*;
use sha3::{Digest, Keccak256};

pub const BLOOM_BYTE_LENGTH: usize = 256;

// See Section 4.3.1 "Transaction Receipt" of the Yellow Paper
fn m3_2048(bloom: &mut Bloom, x: &[u8]) {
    let hash = Keccak256::digest(x);
    let h = hash.as_slice();
    for i in [0, 2, 4] {
        let bit = (h[i + 1] as usize + ((h[i] as usize) << 8)) & 0x7FF;
        bloom.0[BLOOM_BYTE_LENGTH - 1 - bit / 8] |= 1 << (bit % 8);
    }
}

pub fn logs_bloom<'a, It>(logs: It) -> Bloom
where
    It: IntoIterator<Item = &'a Log>,
{
    let mut bloom = Bloom::zero();
    for log in logs {
        m3_2048(&mut bloom, log.address.as_bytes());
        for topic in &log.topics {
            m3_2048(&mut bloom, topic.as_bytes());
        }
    }
    bloom
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn hardcoded_bloom() {
        let logs = vec![
            Log {
                address: hex!("22341ae42d6dd7384bc8584e50419ea3ac75b83f").into(),
                topics: vec![hex!(
                    "04491edcd115127caedbd478e2e7895ed80c7847e903431f94f9cfa579cad47f"
                )
                .into()],
                data: vec![].into(),
            },
            Log {
                address: hex!("e7fb22dfef11920312e4989a3a2b81e2ebf05986").into(),
                topics: vec![
                    hex!("7f1fef85c4b037150d3675218e0cdb7cf38fea354759471e309f3354918a442f").into(),
                    hex!("d85629c7eaae9ea4a10234fed31bc0aeda29b2683ebe0c1882499d272621f6b6").into(),
                ],
                data: hex::decode("2d690516512020171c1ec870f6ff45398cc8609250326be89915fb538e7b")
                    .unwrap()
                    .into(),
            },
        ];
        assert_eq!(
            logs_bloom(&logs),
            Bloom::from(hex!(
                "000000000000000000810000000000000000000000000000000000020000000000000000000000000000008000"
                "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                "000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000"
                "000000000000000000000000000000000000000000000000000000280000000000400000800000004000000000"
                "000000000000000000000000000000000000000000000000000000000000100000100000000000000000000000"
                "00000000001400000000000000008000000000000000000000000000000000"
            ))
        );
    }
}
