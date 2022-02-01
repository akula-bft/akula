use crate::{chain::protocol_param::param, crypto::*, models::*, util::*};
use arrayref::array_ref;
use bytes::{Buf, Bytes};
use evmodin::Revision;
use num_bigint::BigUint;
use num_traits::Zero;
use ripemd::*;
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message, SECP256K1,
};
use sha2::*;
use sha3::*;
use std::{
    cmp::min,
    convert::TryFrom,
    io::{repeat, Read},
    mem::size_of,
};
use substrate_bn::*;

pub type GasFunction = fn(Bytes, Revision) -> Option<u64>;
pub type RunFunction = fn(Bytes) -> Option<Bytes>;

pub struct Contract {
    pub gas: GasFunction,
    pub run: RunFunction,
}

pub const CONTRACTS: [Contract; NUM_OF_ISTANBUL_CONTRACTS] = [
    Contract {
        gas: ecrecover_gas,
        run: ecrecover_run,
    },
    Contract {
        gas: sha256_gas,
        run: sha256_run,
    },
    Contract {
        gas: ripemd160_gas,
        run: ripemd160_run,
    },
    Contract {
        gas: id_gas,
        run: id_run,
    },
    Contract {
        gas: expmod_gas,
        run: expmod_run,
    },
    Contract {
        gas: bn_add_gas,
        run: bn_add_run,
    },
    Contract {
        gas: bn_mul_gas,
        run: bn_mul_run,
    },
    Contract {
        gas: snarkv_gas,
        run: snarkv_run,
    },
    Contract {
        gas: blake2_f_gas,
        run: blake2_f_run,
    },
];

pub const NUM_OF_FRONTIER_CONTRACTS: usize = 4;
pub const NUM_OF_BYZANTIUM_CONTRACTS: usize = 8;
pub const NUM_OF_ISTANBUL_CONTRACTS: usize = 9;

fn ecrecover_gas(_: Bytes, _: Revision) -> Option<u64> {
    Some(3_000)
}

fn ecrecover_run_inner(mut input: Bytes) -> Option<Bytes> {
    if input.len() < 128 {
        let mut input2 = input.as_ref().to_vec();
        input2.resize(128, 0);
        input = input2.into();
    }

    let v = U256::from_be_bytes(*array_ref!(input, 32, 32));
    let r = H256(*array_ref!(input, 64, 32));
    let s = H256(*array_ref!(input, 96, 32));

    if !is_valid_signature(r, s) {
        return None;
    }

    let mut sig = [0; 64];
    sig[..32].copy_from_slice(&r.0);
    sig[32..].copy_from_slice(&s.0);

    let odd = if v == 28 {
        true
    } else if v == 27 {
        false
    } else {
        return None;
    };

    let sig =
        RecoverableSignature::from_compact(&sig, RecoveryId::from_i32(odd.into()).ok()?).ok()?;

    let public = &SECP256K1
        .recover_ecdsa(&Message::from_slice(&input[..32]).ok()?, &sig)
        .ok()?;

    let mut out = vec![0; 32];
    out[12..].copy_from_slice(&Keccak256::digest(&public.serialize_uncompressed()[1..])[12..]);

    Some(out.into())
}

fn ecrecover_run(input: Bytes) -> Option<Bytes> {
    Some(ecrecover_run_inner(input).unwrap_or_else(Bytes::new))
}

fn sha256_gas(input: Bytes, _: Revision) -> Option<u64> {
    Some(60 + 12 * ((input.len() as u64 + 31) / 32))
}
fn sha256_run(input: Bytes) -> Option<Bytes> {
    Some(Sha256::digest(&input).to_vec().into())
}

fn ripemd160_gas(input: Bytes, _: Revision) -> Option<u64> {
    Some(600 + 120 * ((input.len() as u64 + 31) / 32))
}
fn ripemd160_run(input: Bytes) -> Option<Bytes> {
    let mut b = [0; 32];
    b[12..].copy_from_slice(&Ripemd160::digest(&input)[..]);
    Some(b.to_vec().into())
}

fn id_gas(input: Bytes, _: Revision) -> Option<u64> {
    Some(15 + 3 * ((input.len() as u64 + 31) / 32))
}
fn id_run(input: Bytes) -> Option<Bytes> {
    Some(input)
}

fn mult_complexity_eip198(x: U256) -> U256 {
    let x_squared = x * x;
    if x <= 64 {
        x_squared
    } else if x <= 1024 {
        (x_squared >> 2) + 96.as_u256() * x - 3072.as_u256()
    } else {
        (x_squared >> 4) + 480.as_u256() * x - 199680.as_u256()
    }
}

fn mult_complexity_eip2565(max_length: U256) -> U256 {
    let words = (max_length + 7) >> 3; // ⌈max_length/8⌉
    words * words
}

fn expmod_gas(mut input: Bytes, rev: Revision) -> Option<u64> {
    let min_gas = if rev < Revision::Berlin { 0 } else { 200 };

    input = right_pad(input, 3 * 32);

    let base_len256 = U256::from_be_bytes(*array_ref!(input, 0, 32));
    let exp_len256 = U256::from_be_bytes(*array_ref!(input, 32, 32));
    let mod_len256 = U256::from_be_bytes(*array_ref!(input, 64, 32));

    if base_len256 == 0 && mod_len256 == 0 {
        return Some(min_gas);
    }

    let base_len = usize::try_from(base_len256).ok()?;
    let exp_len = usize::try_from(exp_len256).ok()?;
    u64::try_from(mod_len256).ok()?;

    input.advance(3 * 32);

    let mut exp_head = U256::ZERO; // first 32 bytes of the exponent

    if input.len() > base_len {
        let mut exp_input = right_pad(input.slice(base_len..min(base_len + 32, input.len())), 32);
        if exp_len < 32 {
            exp_input = exp_input.slice(..exp_len);
            exp_input = left_pad(exp_input, 32);
        }
        exp_head = U256::from_be_bytes(*array_ref!(exp_input, 0, 32));
    }

    let bit_len = 256 - exp_head.leading_zeros();

    let mut adjusted_exponent_len = U256::ZERO;
    if exp_len > 32 {
        adjusted_exponent_len = (8 * (exp_len - 32)).as_u256();
    }
    if bit_len > 1 {
        adjusted_exponent_len += (bit_len - 1).as_u256();
    }

    if adjusted_exponent_len == 0 {
        adjusted_exponent_len = U256::ONE;
    }

    let max_length = std::cmp::max(mod_len256, base_len256);

    let gas = {
        if rev < Revision::Berlin {
            mult_complexity_eip198(max_length) * adjusted_exponent_len
                / U256::from(param::G_QUAD_DIVISOR_BYZANTIUM)
        } else {
            mult_complexity_eip2565(max_length) * adjusted_exponent_len
                / U256::from(param::G_QUAD_DIVISOR_BERLIN)
        }
    };

    Some(std::cmp::max(min_gas, u64::try_from(gas).ok()?))
}

fn expmod_run(input: Bytes) -> Option<Bytes> {
    let mut input = right_pad(input, 3 * 32);

    let base_len = usize::try_from(u64::from_be_bytes(*array_ref!(input, 24, 8))).unwrap();
    let exponent_len = usize::try_from(u64::from_be_bytes(*array_ref!(input, 56, 8))).unwrap();
    let modulus_len = usize::try_from(u64::from_be_bytes(*array_ref!(input, 88, 8))).unwrap();

    if modulus_len == 0 {
        return Some(Bytes::new());
    }

    input.advance(96);
    let input = right_pad(input, base_len + exponent_len + modulus_len);

    let base = BigUint::from_bytes_be(&input[..base_len]);
    let exponent = BigUint::from_bytes_be(&input[base_len..base_len + exponent_len]);
    let modulus = BigUint::from_bytes_be(
        &input[base_len + exponent_len..base_len + exponent_len + modulus_len],
    );

    let mut out = vec![0; modulus_len];
    if modulus.is_zero() {
        return Some(out.into());
    }

    let b = base.modpow(&exponent, &modulus).to_bytes_be();

    out[modulus_len - b.len()..].copy_from_slice(&b);

    Some(out.into())
}

fn bn_add_gas(_: Bytes, rev: Revision) -> Option<u64> {
    Some({
        if rev >= Revision::Istanbul {
            150
        } else {
            500
        }
    })
}

fn parse_fr_point(r: &mut impl Read) -> Option<substrate_bn::Fr> {
    let mut buf = [0; 32];

    r.read_exact(&mut buf[..]).ok()?;
    substrate_bn::Fr::from_slice(&buf).ok()
}

fn parse_bn_point(r: &mut impl Read) -> Option<substrate_bn::G1> {
    use substrate_bn::*;

    let mut buf = [0; 32];

    r.read_exact(&mut buf).unwrap();
    let x = Fq::from_slice(&buf[..]).ok()?;

    r.read_exact(&mut buf).unwrap();
    let y = Fq::from_slice(&buf[..]).ok()?;

    Some({
        if x.is_zero() && y.is_zero() {
            G1::zero()
        } else {
            AffineG1::new(x, y).ok()?.into()
        }
    })
}

fn bn_add_run(input: Bytes) -> Option<Bytes> {
    let mut input = Read::chain(input.as_ref(), repeat(0));

    let a = parse_bn_point(&mut input)?;
    let b = parse_bn_point(&mut input)?;

    let mut out = [0u8; 64];
    if let Some(sum) = AffineG1::from_jacobian(a + b) {
        sum.x().to_big_endian(&mut out[..32]).unwrap();
        sum.y().to_big_endian(&mut out[32..]).unwrap();
    }

    Some(out.to_vec().into())
}

fn bn_mul_gas(_: Bytes, rev: Revision) -> Option<u64> {
    Some({
        if rev >= Revision::Istanbul {
            6_000
        } else {
            40_000
        }
    })
}
fn bn_mul_run(input: Bytes) -> Option<Bytes> {
    let mut input = Read::chain(input.as_ref(), repeat(0));

    let a = parse_bn_point(&mut input)?;
    let b = parse_fr_point(&mut input)?;

    let mut out = [0u8; 64];
    if let Some(product) = AffineG1::from_jacobian(a * b) {
        product.x().to_big_endian(&mut out[..32]).unwrap();
        product.y().to_big_endian(&mut out[32..]).unwrap();
    }

    Some(out.to_vec().into())
}

const SNARKV_STRIDE: u8 = 192;

fn snarkv_gas(input: Bytes, rev: Revision) -> Option<u64> {
    let k = input.len() as u64 / SNARKV_STRIDE as u64;
    Some({
        if rev >= Revision::Istanbul {
            34_000 * k + 45_000
        } else {
            80_000 * k + 100_000
        }
    })
}
fn snarkv_run(input: Bytes) -> Option<Bytes> {
    if input.len() % usize::from(SNARKV_STRIDE) != 0 {
        return None;
    }

    let k = input.len() / usize::from(SNARKV_STRIDE);

    let ret_val = if input.is_empty() {
        U256::ONE
    } else {
        let mut mul = Gt::one();
        for i in 0..k {
            let a_x = Fq::from_slice(&input[i * 192..i * 192 + 32]).ok()?;
            let a_y = Fq::from_slice(&input[i * 192 + 32..i * 192 + 64]).ok()?;
            let b_a_y = Fq::from_slice(&input[i * 192 + 64..i * 192 + 96]).ok()?;
            let b_a_x = Fq::from_slice(&input[i * 192 + 96..i * 192 + 128]).ok()?;
            let b_b_y = Fq::from_slice(&input[i * 192 + 128..i * 192 + 160]).ok()?;
            let b_b_x = Fq::from_slice(&input[i * 192 + 160..i * 192 + 192]).ok()?;

            let b_a = Fq2::new(b_a_x, b_a_y);
            let b_b = Fq2::new(b_b_x, b_b_y);
            let b = if b_a.is_zero() && b_b.is_zero() {
                G2::zero()
            } else {
                G2::from(AffineG2::new(b_a, b_b).ok()?)
            };
            let a = if a_x.is_zero() && a_y.is_zero() {
                G1::zero()
            } else {
                G1::from(AffineG1::new(a_x, a_y).ok()?)
            };
            mul = mul * pairing(a, b);
        }

        if mul == Gt::one() {
            U256::ONE
        } else {
            U256::ZERO
        }
    };

    Some(ret_val.to_be_bytes().to_vec().into())
}

fn blake2_f_gas(input: Bytes, _: Revision) -> Option<u64> {
    if input.len() < 4 {
        // blake2_f_run will fail anyway
        return Some(0);
    }
    Some(u32::from_be_bytes(*array_ref!(input, 0, 4)).into())
}

fn blake2_f_run(input: Bytes) -> Option<Bytes> {
    const BLAKE2_F_ARG_LEN: usize = 213;

    if input.len() != BLAKE2_F_ARG_LEN {
        return None;
    }

    let mut rounds_buf: [u8; 4] = [0; 4];
    rounds_buf.copy_from_slice(&input[0..4]);
    let rounds: u32 = u32::from_be_bytes(rounds_buf);

    // we use from_le_bytes below to effectively swap byte order to LE if architecture is BE

    let mut h_buf: [u8; 64] = [0; 64];
    h_buf.copy_from_slice(&input[4..68]);
    let mut h = [0u64; 8];
    let mut ctr = 0;
    for state_word in &mut h {
        let mut temp: [u8; 8] = Default::default();
        temp.copy_from_slice(&h_buf[(ctr * 8)..(ctr + 1) * 8]);
        *state_word = u64::from_le_bytes(temp);
        ctr += 1;
    }

    let mut m_buf: [u8; 128] = [0; 128];
    m_buf.copy_from_slice(&input[68..196]);
    let mut m = [0u64; 16];
    ctr = 0;
    for msg_word in &mut m {
        let mut temp: [u8; 8] = Default::default();
        temp.copy_from_slice(&m_buf[(ctr * 8)..(ctr + 1) * 8]);
        *msg_word = u64::from_le_bytes(temp);
        ctr += 1;
    }

    let mut t_0_buf: [u8; 8] = [0; 8];
    t_0_buf.copy_from_slice(&input[196..204]);
    let t_0 = u64::from_le_bytes(t_0_buf);

    let mut t_1_buf: [u8; 8] = [0; 8];
    t_1_buf.copy_from_slice(&input[204..212]);
    let t_1 = u64::from_le_bytes(t_1_buf);

    let f = if input[212] == 1 {
        true
    } else if input[212] == 0 {
        false
    } else {
        return None;
    };

    crate::crypto::blake2::compress(&mut h, m, [t_0, t_1], f, rounds as usize);

    let mut output_buf = [0u8; 8 * size_of::<u64>()];
    for (i, state_word) in h.iter().enumerate() {
        output_buf[i * 8..(i + 1) * 8].copy_from_slice(&state_word.to_le_bytes());
    }

    Some(output_buf.to_vec().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes_literal::bytes;
    use hex_literal::hex;

    #[test]
    fn ecrecover() {
        let input = hex!("18c547e4f7b0f325ad1e56f57e26c745b09a3e503d86e00e5255ff7f715d3d1c000000000000000000000000000000000000000000000000000000000000001c73b1693892219d736caba55bdb67216e485557ea6b6af75f37096c9aa6a5a75feeb940b1d03b21e36b0e47e79769f095fe2ab855bd91e3a38756b7d75a9c4549");
        assert_eq!(
            ecrecover_run(input.to_vec().into()),
            Some(
                hex!("000000000000000000000000a94f5374fce5edbc8e2a8697c15331677e6ebf0b")
                    .to_vec()
                    .into()
            )
        );

        // Unrecoverable key
        let input = hex!("a8b53bdf3306a35a7103ab5504a0c9b492295564b6202b1942a84ef300107281000000000000000000000000000000000000000000000000000000000000001b307835653165303366353363653138623737326363623030393366663731663366353366356337356237346463623331613835616138623838393262346538621122334455667788991011121314151617181920212223242526272829303132");
        assert_eq!(ecrecover_run(input.to_vec().into()), Some(Bytes::new()));
    }

    #[test]
    fn sha256() {
        let input = hex!("38d18acb67d25c8bb9942764b62f18e17054f66a817bd4295423adf9ed98873e000000000000000000000000000000000000000000000000000000000000001b38d18acb67d25c8bb9942764b62f18e17054f66a817bd4295423adf9ed98873e789d1dd423d25f0772d2748d60f7e4b81bb14d086eba8e8e8efb6dcff8a4ae02");

        assert_eq!(
            sha256_run(input.to_vec().into()),
            Some(
                hex!("811c7003375852fabd0d362e40e68607a12bdabae61a7d068fe5fdd1dbbf2a5d")
                    .to_vec()
                    .into()
            )
        );
    }

    #[test]
    fn ripemd160() {
        let input = hex!("38d18acb67d25c8bb9942764b62f18e17054f66a817bd4295423adf9ed98873e000000000000000000000000000000000000000000000000000000000000001b38d18acb67d25c8bb9942764b62f18e17054f66a817bd4295423adf9ed98873e789d1dd423d25f0772d2748d60f7e4b81bb14d086eba8e8e8efb6dcff8a4ae02");

        assert_eq!(
            ripemd160_run(input.to_vec().into()),
            Some(
                hex!("0000000000000000000000009215b8d9882ff46f0dfde6684d78e831467f65e6")
                    .to_vec()
                    .into()
            )
        )
    }

    #[test]
    fn modexp() {
        let input = hex!(
            "0000000000000000000000000000000000000000000000000000000000000001"
            "0000000000000000000000000000000000000000000000000000000000000020"
            "0000000000000000000000000000000000000000000000000000000000000020"
            "03"
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2e"
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f"
        );
        assert_eq!(
            expmod_gas(input.to_vec().into(), Revision::Byzantium),
            Some(13056)
        );

        assert_eq!(
            expmod_run(input.to_vec().into()).unwrap(),
            bytes!("0000000000000000000000000000000000000000000000000000000000000001")
        );

        let input = hex!(
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000020"
            "0000000000000000000000000000000000000000000000000000000000000020"
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2e"
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f"
        );

        assert_eq!(
            expmod_run(input.to_vec().into()).unwrap(),
            bytes!("0000000000000000000000000000000000000000000000000000000000000000")
        );

        let input = hex!(
            "0000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000000000000000000000000000000000000000020"
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffe"
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffd"
        );
        assert_eq!(expmod_gas(input.to_vec().into(), Revision::Byzantium), None);
        assert_eq!(expmod_gas(input.to_vec().into(), Revision::Berlin), None);

        let input = hex!(
            "0000000000000000000000000000000000000000000000000000000000000100"
            "0000000000000000000000000000000000000000000000000000000000000003"
            "0000000000000000000000000000000000000000000000000000000000000100"
            "94fff7dfe2f9c757463dab3aaa4103e9b820bed33aaa0f2b6c0ec056d338288dcd7c568aeb0a1c7bfdde436f4c69"
            "f242f79661df1d8c5b65836a41070f0b562002c67c5e6037b1e4d9e7c9e4e5faf6c9d3b46ed618b75dbf01c8f519"
            "ebd5afde96cf446a1cbd6fa58077592d22bdb661c16ebd9a207571f331d8e45eb0e3f58731eda925429d4e10d823"
            "fed0a6819ce94f68791bc90222b2f767e884858b5d054ac6fbfb0ec6dbdc88371bed2a85e13c2fd3f85963b7e8d0"
            "06373f9a7dd295ce1e87fdb28e3a9e1a3851169e24042bb401b872a0bdd55e8b36a01efed0d65fc3adf94dbf5eb3"
            "7365afa8add999aa5fcb772439f607c6127c32c7fe920efd7b74"
            "010001"
            "aa05b012cda6a5d91d80dc970a252e4b70aff168381da61bd7c655db438afe1322cc387442a8a801f974dbf4ffb1"
            "10e5b68c03202ca47470bda7cff40c50c2762a0e45222a4df1e6c6d69a1dccafd1535a1bb82d6c17dd2ac04b8d02"
            "6092d4189ab630d1348baac2ff5612faf07961f48482571f59e922c744dab8b9c7acf6295fcc72566626c6423776"
            "1c9d571616e1cbeef439413f348f9c6e89226a971b393fc8d45472951d68897eaf264acdbb5cd54b6c4ea520b45c"
            "3abbbd78fa27dd113921d3facbcc1d6040243c9761867c69a1dc13d9f71898121ff696561458d9d9f87536d6a84f"
            "b602c91f9b07e561fa2f54eb0f9f1984f3cbe728ec142cbed52f"
        );
        assert_eq!(
            expmod_gas(input.to_vec().into(), Revision::Byzantium),
            Some(30310)
        );
        assert_eq!(
            expmod_gas(input.to_vec().into(), Revision::Berlin),
            Some(5461)
        );
    }

    #[test]
    fn bn_add() {
        let input = hex!(
            "00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000"
            "00000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000"
            "000000010000000000000000000000000000000000000000000000000000000000000002"
        );
        assert_eq!(bn_add_run(input.to_vec().into()).unwrap(), bytes!("030644e72e131a029b85045b68181585d97816a916871ca8d3c208c16d87cfd315ed738c0e0a7c92e7845f96b2ae9c0a68a6a449e3538fc7ff3ebf7a5a18a2c4"));
    }

    #[test]
    fn bn_mul() {
        let input = hex!(
            "1a87b0584ce92f4593d161480614f2989035225609f08058ccfa3d0f940febe31a2f3c951f6dadcc7ee9007dff81504b0fcd6d7cf59996efdc33d92bf7f9f8f60000000000000000000000000000000000000000000000000000000000000009"
        );
        assert_eq!(bn_mul_run(input.to_vec().into()).unwrap(), bytes!("1dbad7d39dbc56379f78fac1bca147dc8e66de1b9d183c7b167351bfe0aeab742cd757d51289cd8dbd0acf9e673ad67d0f0a89f912af47ed1be53664f5692575"));
    }

    #[test]
    fn snarkv() {
        // empty input
        assert_eq!(
            snarkv_run(Bytes::new()).unwrap(),
            bytes!("0000000000000000000000000000000000000000000000000000000000000001")
        );

        // input size is not a multiple of 192
        assert_eq!(snarkv_run(hex!("ab").to_vec().into()), None);

        let input = hex!(
            "0f25929bcb43d5a57391564615c9e70a992b10eafa4db109709649cf48c50dd216da2f5cb6be7a0aa72c440c53c9"
            "bbdfec6c36c7d515536431b3a865468acbba2e89718ad33c8bed92e210e81d1853435399a271913a6520736a4729"
            "cf0d51eb01a9e2ffa2e92599b68e44de5bcf354fa2642bd4f26b259daa6f7ce3ed57aeb314a9a87b789a58af499b"
            "314e13c3d65bede56c07ea2d418d6874857b70763713178fb49a2d6cd347dc58973ff49613a20757d0fcc22079f9"
            "abd10c3baee245901b9e027bd5cfc2cb5db82d4dc9677ac795ec500ecd47deee3b5da006d6d049b811d7511c7815"
            "8de484232fc68daf8a45cf217d1c2fae693ff5871e8752d73b21198e9393920d483a7260bfb731fb5d25f1aa4933"
            "35a9e71297e485b7aef312c21800deef121f1e76426a00665e5c4479674322d4f75edadd46debd5cd992f6ed0906"
            "89d0585ff075ec9e99ad690c3395bc4b313370b38ef355acdadcd122975b12c85ea5db8c6deb4aab71808dcb408f"
            "e3d1e7690c43d37b4ce6cc0166fa7daa"
        );
        assert_eq!(
            snarkv_run(input.to_vec().into()).unwrap(),
            bytes!("0000000000000000000000000000000000000000000000000000000000000001")
        );

        let input = hex!(
            "00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000"
            "000000000000000000000000000000000002198e9393920d483a7260bfb731fb5d25f1aa493335a9e71297e485b7"
            "aef312c21800deef121f1e76426a00665e5c4479674322d4f75edadd46debd5cd992f6ed090689d0585ff075ec9e"
            "99ad690c3395bc4b313370b38ef355acdadcd122975b12c85ea5db8c6deb4aab71808dcb408fe3d1e7690c43d37b"
            "4ce6cc0166fa7daa"
        );
        assert_eq!(
            snarkv_run(input.to_vec().into()).unwrap(),
            bytes!("0000000000000000000000000000000000000000000000000000000000000000")
        );
    }

    // https://eips.ethereum.org/EIPS/eip-152#test-cases
    #[test]
    fn blake2() {
        let input = hex!(
            "00000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c3e"
            "2b8c68059b6bbd41fbabd9831f79217e1319cde05b61626300000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000300000000000000000000000000000001"
        );
        assert_eq!(blake2_f_run(input.to_vec().into()), None);

        let input = hex!(
            "000000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f"
            "6c3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b6162630000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "000000000000000000000000000300000000000000000000000000000001"
        );
        assert_eq!(blake2_f_run(input.to_vec().into()), None);

        let input = hex!(
            "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c"
            "3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b616263000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000300000000000000000000000000000002"
        );
        assert_eq!(blake2_f_run(input.to_vec().into()), None);

        let input = hex!(
            "0000000048c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c"
            "3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b616263000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000300000000000000000000000000000001"
        );
        assert_eq!(
            blake2_f_run(input.to_vec().into()).unwrap(),
            bytes!(
                "08c9bcf367e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d282e6ad7f520e511f6c3e2b8c68059b9442be0454267ce079217e1319cde05b"
            )
        );

        let input = hex!(
            "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c"
            "3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b616263000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000300000000000000000000000000000001"
        );
        assert_eq!(
            blake2_f_run(input.to_vec().into()).unwrap(),
            bytes!(
                "ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923"
            )
        );

        let input = hex!(
            "0000000c48c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c"
            "3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b616263000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000300000000000000000000000000000000"
        );
        assert_eq!(
            blake2_f_run(input.to_vec().into()).unwrap(),
            bytes!(
                "75ab69d3190a562c51aef8d88f1c2775876944407270c42c9844252c26d2875298743e7f6d5ea2f2d3e8d226039cd31b4e426ac4f2d3d666a610c2116fde4735"
            )
        );

        let input = hex!(
            "0000000148c9bdf267e6096a3ba7ca8485ae67bb2bf894fe72f36e3cf1361d5f3af54fa5d182e6ad7f520e511f6c"
            "3e2b8c68059b6bbd41fbabd9831f79217e1319cde05b616263000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            "0000000000000000000000000300000000000000000000000000000001"
        );
        assert_eq!(
            blake2_f_run(input.to_vec().into()).unwrap(),
            bytes!(
                "b63a380cb2897d521994a85234ee2c181b5f844d2c624c002677e9703449d2fba551b3a8333bcdf5f2f7e08993d53923de3d64fcc68c034e717b9293fed7a421"
            )
        );
    }
}
