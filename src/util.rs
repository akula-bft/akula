use bytes::{Bytes, BytesMut};
use ethereum_types::*;
use ethnum::U256;
use num_traits::Zero;
use serde::{
    de::{self, Error},
    Deserialize,
};
use std::{
    borrow::Borrow,
    fmt::{self, Formatter},
};

pub fn static_left_pad<const LEN: usize>(unpadded: &[u8]) -> [u8; LEN] {
    assert!(unpadded.len() <= LEN);

    let mut v = [0; LEN];
    v[LEN - unpadded.len()..].copy_from_slice(unpadded);
    v
}

fn pad<const LEFT: bool>(buffer: Bytes, min_size: usize) -> Bytes {
    if buffer.len() >= min_size {
        return buffer;
    }

    let point = if LEFT { min_size - buffer.len() } else { 0 };

    let mut b = BytesMut::with_capacity(min_size);
    b.resize(min_size, 0);
    b[point..point + buffer.len()].copy_from_slice(&buffer[..]);
    b.freeze()
}

pub fn left_pad(buffer: Bytes, min_size: usize) -> Bytes {
    pad::<true>(buffer, min_size)
}

pub fn right_pad(buffer: Bytes, min_size: usize) -> Bytes {
    pad::<false>(buffer, min_size)
}

pub fn u256_to_h256(v: U256) -> H256 {
    H256(v.to_be_bytes())
}

pub fn h256_to_u256(v: impl Borrow<H256>) -> U256 {
    U256::from_be_bytes(v.borrow().0)
}

pub fn zeroless_view(v: &impl AsRef<[u8]>) -> &[u8] {
    let v = v.as_ref();
    &v[v.iter().take_while(|b| b.is_zero()).count()..]
}

pub fn hex_to_bytes(s: &str) -> Result<Bytes, hex::FromHexError> {
    hex::decode(s).map(From::from)
}

pub fn write_hex_string<B: AsRef<[u8]>>(b: &B, f: &mut Formatter) -> fmt::Result {
    write!(f, "0x{}", hex::encode(b))
}

pub fn deserialize_hexstr_as_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    let d = if let Some(stripped) = s.strip_prefix("0x") {
        u64::from_str_radix(stripped, 16)
    } else {
        s.parse()
    }
    .map_err(|e| de::Error::custom(format!("{}/{}", e, s)))?;

    Ok(d)
}

pub mod hexbytes {
    use serde::Serializer;

    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        Ok(hex::decode(s.strip_prefix("0x").unwrap_or(&s))
            .map_err(D::Error::custom)?
            .into())
    }

    pub fn serialize<S>(b: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{}", hex::encode(b)))
    }
}

pub fn version_string() -> String {
    format!(
        "akula/v{}-{}-{}-{}/{}/rustc{}",
        env!("VERGEN_BUILD_SEMVER"),
        env!("VERGEN_GIT_BRANCH"),
        env!("VERGEN_GIT_SHA_SHORT"),
        env!("VERGEN_GIT_COMMIT_DATE"),
        env!("VERGEN_CARGO_TARGET_TRIPLE"),
        env!("VERGEN_RUSTC_SEMVER")
    )
}

#[cfg(test)]
pub mod test_util {
    use std::future::Future;
    use tokio::runtime::Builder;

    pub fn run_test<F: Future<Output = ()> + Send + 'static>(f: F) {
        Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(64 * 1024 * 1024)
            .build()
            .unwrap()
            .block_on(async move { tokio::spawn(f).await.unwrap() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use bytes_literal::bytes;
    use hex_literal::hex;

    #[test]
    fn padding() {
        assert_eq!(right_pad(bytes!("a5").to_vec().into(), 3), bytes!("a50000"));
        assert_eq!(
            right_pad(bytes!("5a0b54d5dc17e0aadc383d2db4").to_vec().into(), 3),
            bytes!("5a0b54d5dc17e0aadc383d2db4")
        );

        assert_eq!(left_pad(bytes!("a5").to_vec().into(), 3), bytes!("0000a5"));
        assert_eq!(
            left_pad(bytes!("5a0b54d5dc17e0aadc383d2db4").to_vec().into(), 3),
            bytes!("5a0b54d5dc17e0aadc383d2db4")
        );

        let mut repeatedly_padded = right_pad(bytes!("b8c4").to_vec().into(), 3);
        assert_eq!(repeatedly_padded, bytes!("b8c400"));
        repeatedly_padded.advance(1);
        assert_eq!(repeatedly_padded, bytes!("c400"));
        repeatedly_padded = right_pad(repeatedly_padded, 4);
        assert_eq!(repeatedly_padded, bytes!("c4000000"));

        repeatedly_padded = left_pad(bytes!("b8c4").to_vec().into(), 3);
        assert_eq!(repeatedly_padded, bytes!("00b8c4"));
        repeatedly_padded.truncate(repeatedly_padded.len() - 1);
        assert_eq!(repeatedly_padded, bytes!("00b8"));
        repeatedly_padded = left_pad(repeatedly_padded, 4);
        assert_eq!(repeatedly_padded, bytes!("000000b8"));
    }

    #[test]
    fn zeroless_view_test() {
        assert_eq!(
            zeroless_view(&H256::from(hex!(
                "0000000000000000000000000000000000000000000000000000000000000000"
            ))),
            &bytes!("") as &[u8]
        );
        assert_eq!(
            zeroless_view(&H256::from(hex!(
                "000000000000000000000000000000000000000000000000000000000004bc00"
            ))),
            &bytes!("04bc00") as &[u8]
        );
    }
}
