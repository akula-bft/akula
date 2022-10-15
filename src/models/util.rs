use crate::models::*;
use anyhow::{bail, format_err};
use arrayvec::ArrayVec;
use bytes::*;
use eio::{FromBytes, ToBytes};

pub fn variable_to_compact<const N: usize, T: ToBytes<N>>(v: T) -> ArrayVec<u8, N> {
    v.to_be_bytes()
        .into_iter()
        .skip_while(|&v| v == 0)
        .collect()
}

pub fn variable_from_compact<const N: usize, T: Default + FromBytes<N>>(
    mut buf: &[u8],
    len: u8,
) -> anyhow::Result<(T, &[u8])> {
    let len = len as usize;
    if len > N {
        bail!("len too big");
    }
    if buf.len() < len {
        bail!("input too short");
    }

    let mut value = T::default();
    if len > 0 {
        let mut arr = [0; N];
        arr[N - len..].copy_from_slice(&buf[..len]);
        value = T::from_be_bytes(arr);
        buf.advance(len);
    }

    Ok((value, buf))
}

pub fn h160_from_compact(mut buf: &[u8]) -> anyhow::Result<(H160, &[u8])> {
    let v = H160::from_slice(
        buf.get(..20)
            .ok_or_else(|| format_err!("input too short"))?,
    );
    buf.advance(20);
    Ok((v, buf))
}

pub fn h256_from_compact(mut buf: &[u8]) -> anyhow::Result<(H256, &[u8])> {
    let v = H256::from_slice(
        buf.get(..32)
            .ok_or_else(|| format_err!("input too short"))?,
    );
    buf.advance(32);
    Ok((v, buf))
}
