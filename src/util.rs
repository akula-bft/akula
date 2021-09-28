use bytes::Bytes;
use ethereum_types::*;
use num_traits::Zero;
use serde::{
    de::{self, Error},
    Deserialize,
};
use static_bytes::BytesMut;
use std::{
    borrow::Borrow,
    fmt::{self, Formatter},
};

fn pad<const LEFT: bool>(buffer: Bytes<'_>, min_size: usize) -> Bytes<'_> {
    if buffer.len() >= min_size {
        return buffer;
    }

    let point = if LEFT { min_size - buffer.len() } else { 0 };

    let mut b = BytesMut::with_capacity(min_size);
    b.resize(min_size, 0);
    b[point..point + buffer.len()].copy_from_slice(&buffer[..]);
    b.freeze().into()
}

pub fn left_pad(buffer: Bytes<'_>, min_size: usize) -> Bytes<'_> {
    pad::<true>(buffer, min_size)
}

pub fn right_pad(buffer: Bytes<'_>, min_size: usize) -> Bytes<'_> {
    pad::<false>(buffer, min_size)
}

pub fn u256_to_h256(v: U256) -> H256 {
    H256(v.into())
}

pub fn h256_to_u256(v: impl Borrow<H256>) -> U256 {
    U256::from_big_endian(&v.borrow().0)
}

pub fn zeroless_view(v: &impl AsRef<[u8]>) -> &[u8] {
    let v = v.as_ref();
    &v[v.iter().take_while(|b| b.is_zero()).count()..]
}

pub fn write_hex_string<B: AsRef<[u8]>>(b: &B, f: &mut Formatter) -> fmt::Result {
    write!(f, "0x{}", hex::encode(b))
}

pub fn deserialize_str_as_bytes<'de, D>(deserializer: D) -> Result<Bytes<'static>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    Ok(hex::decode(s.strip_prefix("0x").unwrap_or(&s))
        .map_err(D::Error::custom)?
        .into())
}

#[cfg(test)]
pub mod test_util {
    use std::future::Future;
    use tokio::runtime::Builder;

    pub fn run_test<F: Future<Output = ()> + Send + 'static>(f: F) {
        Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(32 * 1024 * 1024)
            .build()
            .unwrap()
            .block_on(async move { tokio::spawn(f).await.unwrap() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use hex_literal::hex;

    #[test]
    fn padding() {
        assert_eq!(right_pad(hex!("a5").to_vec().into(), 3), hex!("a50000"));
        assert_eq!(
            right_pad(hex!("5a0b54d5dc17e0aadc383d2db4").to_vec().into(), 3),
            hex!("5a0b54d5dc17e0aadc383d2db4")
        );

        assert_eq!(left_pad(hex!("a5").to_vec().into(), 3), hex!("0000a5"));
        assert_eq!(
            left_pad(hex!("5a0b54d5dc17e0aadc383d2db4").to_vec().into(), 3),
            hex!("5a0b54d5dc17e0aadc383d2db4")
        );

        let mut repeatedly_padded = right_pad(hex!("b8c4").to_vec().into(), 3);
        assert_eq!(repeatedly_padded, hex!("b8c400"));
        repeatedly_padded.advance(1);
        assert_eq!(repeatedly_padded, hex!("c400"));
        repeatedly_padded = right_pad(repeatedly_padded, 4);
        assert_eq!(repeatedly_padded, hex!("c4000000"));

        repeatedly_padded = left_pad(hex!("b8c4").to_vec().into(), 3);
        assert_eq!(repeatedly_padded, hex!("00b8c4"));
        repeatedly_padded.truncate(repeatedly_padded.len() - 1);
        assert_eq!(repeatedly_padded, hex!("00b8"));
        repeatedly_padded = left_pad(repeatedly_padded, 4);
        assert_eq!(repeatedly_padded, hex!("000000b8"));
    }

    #[test]
    fn zeroless_view_test() {
        assert_eq!(
            zeroless_view(&H256::from(hex!(
                "0000000000000000000000000000000000000000000000000000000000000000"
            ))),
            &hex!("") as &[u8]
        );
        assert_eq!(
            zeroless_view(&H256::from(hex!(
                "000000000000000000000000000000000000000000000000000000000004bc00"
            ))),
            &hex!("04bc00") as &[u8]
        );
    }
}
