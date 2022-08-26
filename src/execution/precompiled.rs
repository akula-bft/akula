use crate::{chain::protocol_param::param, crypto::*, models::*, util::*};
use arrayref::array_ref;
use bytes::{Buf, Bytes};
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
    collections::HashMap,
    cmp::min,
    convert::TryFrom,
    io::{repeat, Read},
};
use substrate_bn::*;
use tendermint::lite::{iavl_proof, light_client};
use parity_bytes::BytesRef;
use tracing::*;

pub type GasFunction = fn(Bytes, Revision) -> Option<u64>;
pub type RunFunction = fn(Bytes) -> Option<Bytes>;

#[derive(Clone)]
pub struct Contract {
    pub gas: GasFunction,
    pub run: RunFunction,
}

lazy_static! {
    pub static ref CONTRACTS: HashMap<u8, Contract> = {
        let mut m = HashMap::new();
        m.insert(1, Contract {
            gas: ecrecover_gas,
            run: ecrecover_run,
        });
        m.insert(2, Contract {
           gas: sha256_gas,
           run: sha256_run,
        });
        m.insert(3, Contract {
            gas: ripemd160_gas,
            run: ripemd160_run,
        });
        m.insert(4, Contract {
            gas: id_gas,
            run: id_run,
        });
        m.insert(5, Contract {
            gas: expmod_gas,
            run: expmod_run,
        });
        m.insert(6, Contract {
            gas: bn_add_gas,
            run: bn_add_run,
        });
        m.insert(7, Contract {
            gas: bn_mul_gas,
            run: bn_mul_run,
        });
        m.insert(8, Contract {
            gas: snarkv_gas,
            run: snarkv_run,
        });
        m.insert(9, Contract {
            gas: blake2_f_gas,
            run: blake2_f_run,
        });
        m.insert(100, Contract {
            gas: tendermint_header_verify_gas,
            run: tendermint_header_verify_run,
        });
        m.insert(101, Contract {
            gas: iavl_proof_verify_gas,
            run: iavl_proof_verify_run,
        });
        m
    };
}

pub const NUM_OF_FRONTIER_CONTRACTS: usize = 4;
pub const NUM_OF_BYZANTIUM_CONTRACTS: usize = 8;
pub const NUM_OF_ISTANBUL_CONTRACTS: usize = 11;
pub const MAX_NUM_OF_PRECOMPILED: usize = 127;

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

    let mut output_buf = [0u8; u64::BITS as usize];
    for (i, state_word) in h.iter().enumerate() {
        output_buf[i * 8..(i + 1) * 8].copy_from_slice(&state_word.to_le_bytes());
    }

    Some(output_buf.to_vec().into())
}

fn tendermint_header_verify_gas(_: Bytes, _: Revision) -> Option<u64> {
    Some(3_000)
}

fn tendermint_header_verify_run(input: Bytes) -> Option<Bytes> {
    let mut output = vec![0u8, 0, 0];
    let mut bytes = BytesRef::Flexible(&mut output);
    let res = light_client::TmHeaderVerifier::execute(input.as_ref(), &mut bytes);
    debug!("tendermint_header_verify_run input {} output {}", hex::encode(input.as_ref()), hex::encode(&output));
    return match res {
        Ok(()) => Some(Bytes::from(output)),
        Err(_) => None
    };
}

fn iavl_proof_verify_gas(_: Bytes, _: Revision) -> Option<u64> { Some(3_000) }

fn iavl_proof_verify_run(input: Bytes) -> Option<Bytes> {
    let mut output = [0u8; 32];
    let mut bytes = BytesRef::Fixed(&mut output);
    let res = iavl_proof::execute(input.as_ref(), &mut bytes);
    return match res {
        Ok(()) => Some(Bytes::copy_from_slice(&output[..])),
        Err(_) => None
    };
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

    #[test]
    fn tm_header_verify() {
        let input = hex::decode("0000000000000000000000000000000000000000000000000000000000001325000000000000000000000000000000000000000000000000000000000000022042696e616e63652d436861696e2d4e696c6500000000000000000000000000000000000003fc05e2b7029751d2a6581efc2f79712ec44d8b4981850325a7feadaa58ef4ddaa18a9380d9ab0fc10d18ca0e0832d5f4c063c5489ec1443dfb738252d038a82131b27ae17cbe9c20cdcfdf876b3b12978d3264a007fcaaa71c4cdb701d9ebc0323f44f000000174876e800184e7b103d34c41003f9b864d5f8c1adda9bd0436b253bb3c844bc739c1e77c9000000174876e8004d420aea843e92a0cfe69d89696dff6827769f9cb52a249af537ce89bf2a4b74000000174876e800bd03de9f8ab29e2800094e153fac6f696cfa512536c9c2f804dcb2c2c4e4aed6000000174876e8008f4a74a07351895ddf373057b98fae6dfaf2cd21f37a063e19601078fe470d53000000174876e8004a5d4753eb79f92e80efe22df7aca4f666a4f44bf81c536c4a09d4b9c5b654b5000000174876e800c80e9abef7ff439c10c68fe8f1303deddfc527718c3b37d8ba6807446e3c827a000000174876e8009142afcc691b7cc05d26c7b0be0c8b46418294171730e079f384fde2fa50bafc000000174876e80049b288e4ebbb3a281c2d546fc30253d5baf08993b6e5d295fb787a5b314a298e000000174876e80004224339688f012e649de48e241880092eaa8f6aa0f4f14bfcf9e0c76917c0b6000000174876e8004034b37ceda8a0bf13b1abaeee7a8f9383542099a554d219b93d0ce69e3970e8000000174876e800e3210a92130abb020a02080a121242696e616e63652d436861696e2d4e696c6518e38bf01f220c08e191aef20510f5f4e4c70230dae0c7173a480a20102b54820dd8fb5bc2c4e875ee573fa294d9b7b7ceb362aa8fd21b33dee41b1c12240801122082f341511f3e6b89d6177fd31f8a106013ba09d6e12ef40a7dec885d81b687634220b1b77e6977e0cd0177e3102a78833c9e152aa646ed4fb5a77e8af58c9867eec0522080d9ab0fc10d18ca0e0832d5f4c063c5489ec1443dfb738252d038a82131b27a5a2080d9ab0fc10d18ca0e0832d5f4c063c5489ec1443dfb738252d038a82131b27a6220294d8fbd0b94b767a7eba9840f299a3586da7fe6b5dead3b7eecba193c400f936a20a3e248bc209955054d880e4d89ff3c0419c0cd77681f4b4c6649ead5545054b982011462633d9db7ed78e951f79913fdc8231aa77ec12b12d1100a480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be212b601080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510cebfe23e321406fd60078eb4c2356137dd50036597db267cf61642409276f20ad4b152f91c344bd63ac691bad66e04e228a8b58dca293ff0bd10f8aef6dfbcecae49e32b09d89e10b771a6c01628628596a95e126b04763560c66c0f12b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510a4caa532321418e69cc672973992bb5f76d049a5b2c5ddf77436380142409ed2b74fa835296d552e68c439dd4ee3fa94fb197282edcc1cc815c863ca42a2c9a73475ff6be9064371a61655a3c31d2f0acc89c3a4489ad4c2671aef52360512b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510a69eca2f3214344c39bb8f4512d6cab1f6aafac1811ef9d8afdf38024240de2768ead90011bcbb1914abc1572749ab7b81382eb81cff3b41c56edc12470a7b8a4d61f8b4ca7b2cb7e24706edd219455796b4db74cd36965859f91dc8910312b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510dcdd833b321437ef19af29679b368d2b9e9de3f8769b357866763803424072ddfe0aeb13616b3f17eb60b19a923ec51fcc726625094aa069255c829c8cdd9e242080a1e559b0030fe9a0db19fd34e392bd78df12a9caff9f2b811bc1ac0a12b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510e9f2f859321462633d9db7ed78e951f79913fdc8231aa77ec12b38044240f5f61c640ab2402b44936de0d24e7b439df78bc3ef15467ecb29b92ece4aa0550790d5ce80761f2ac4b0e3283969725c42343749d9b44b179b2d4fced66c5d0412b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510ff90f55532147b343e041ca130000a8bc00c35152bd7e774003738054240df6e298b3efd42eb536e68a0210bc921e8b5dc145fe965f63f4d3490064f239f2a54a6db16c96086e4ae52280c04ad8b32b44f5ff3d41f0c364949ccb628c50312b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510cad7c931321491844d296bd8e591448efc65fd6ad51a888d58fa3806424030298627da1afd28229aac150f553724b594989e59136d6a175d84e45a4dee344ff9e0eeb69fdf29abb6d833adc3e1ccdc87b2a65019ef5fb627c44d9d132c0012b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510c8c296323214b3727172ce6473bc780298a2d66c12f1a14f5b2a38074240918491100730b4523f0c85409f6d1cca9ebc4b8ca6df8d55fe3d85158fa43286608693c50332953e1d3b93e3e78b24e158d6a2275ce8c6c7c07a7a646a19200312b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef2051086f1a2403214b6f20c7faa2b2f6f24518fa02b71cb5f4a09fba338084240ca59c9fc7f6ab660e9970fc03e5ed588ccb8be43fe5a3e8450287b726f29d039e53fe888438f178ac63c3d2ca969cd8c2fbc8606f067634339b6a94a7382960212b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef2051080efbb543214e0dd72609cc106210d1aa13936cb67b93a0aee2138094240e787a21f5cb7052624160759a9d379dd9db144f2b498bca026375c9ce8ecdc2a0936af1c309b3a0f686c92bf5578b595a4ca99036a19c9fc50d3718fd454b30012b801080210e38bf01f22480a207eaabf7df1081377e06e08efe7ad17974049380bdd65a9b053c099ef80ff6e6f122408011220d153cc308d9cb96ca43ffeceaae1ee85794c83d17408ff76cfee92f5e91d0be22a0b08e291aef20510ddf8d85a3214fc3108dc3814888f4187452182bc1baf83b71bc9380a4240d51ea31f6449eed71de22339722af1edbb0b21401037d85882b32a2ed8ae9127f2df4d1da2092729e582812856227ed6cdf98a3f60203d1ff80bd635fb03bb0912a4070a4f0a1406fd60078eb4c2356137dd50036597db267cf61612251624de6420e17cbe9c20cdcfdf876b3b12978d3264a007fcaaa71c4cdb701d9ebc0323f44f1880d0dbc3f4022080e0ebdaf2e2ffffff010a4b0a1418e69cc672973992bb5f76d049a5b2c5ddf7743612251624de6420184e7b103d34c41003f9b864d5f8c1adda9bd0436b253bb3c844bc739c1e77c91880d0dbc3f4022080d0dbc3f4020a4b0a14344c39bb8f4512d6cab1f6aafac1811ef9d8afdf12251624de64204d420aea843e92a0cfe69d89696dff6827769f9cb52a249af537ce89bf2a4b741880d0dbc3f4022080d0dbc3f4020a4b0a1437ef19af29679b368d2b9e9de3f8769b3578667612251624de6420bd03de9f8ab29e2800094e153fac6f696cfa512536c9c2f804dcb2c2c4e4aed61880d0dbc3f4022080d0dbc3f4020a4b0a1462633d9db7ed78e951f79913fdc8231aa77ec12b12251624de64208f4a74a07351895ddf373057b98fae6dfaf2cd21f37a063e19601078fe470d531880d0dbc3f4022080d0dbc3f4020a4b0a147b343e041ca130000a8bc00c35152bd7e774003712251624de64204a5d4753eb79f92e80efe22df7aca4f666a4f44bf81c536c4a09d4b9c5b654b51880d0dbc3f4022080d0dbc3f4020a4b0a1491844d296bd8e591448efc65fd6ad51a888d58fa12251624de6420c80e9abef7ff439c10c68fe8f1303deddfc527718c3b37d8ba6807446e3c827a1880d0dbc3f4022080d0dbc3f4020a4b0a14b3727172ce6473bc780298a2d66c12f1a14f5b2a12251624de64209142afcc691b7cc05d26c7b0be0c8b46418294171730e079f384fde2fa50bafc1880d0dbc3f4022080d0dbc3f4020a4b0a14b6f20c7faa2b2f6f24518fa02b71cb5f4a09fba312251624de642049b288e4ebbb3a281c2d546fc30253d5baf08993b6e5d295fb787a5b314a298e1880d0dbc3f4022080d0dbc3f4020a4b0a14e0dd72609cc106210d1aa13936cb67b93a0aee2112251624de642004224339688f012e649de48e241880092eaa8f6aa0f4f14bfcf9e0c76917c0b61880d0dbc3f4022080d0dbc3f4020a4b0a14fc3108dc3814888f4187452182bc1baf83b71bc912251624de64204034b37ceda8a0bf13b1abaeee7a8f9383542099a554d219b93d0ce69e3970e81880d0dbc3f4022080d0dbc3f402124f0a1406fd60078eb4c2356137dd50036597db267cf61612251624de6420e17cbe9c20cdcfdf876b3b12978d3264a007fcaaa71c4cdb701d9ebc0323f44f1880d0dbc3f4022080e0ebdaf2e2ffffff011aa4070a4f0a1406fd60078eb4c2356137dd50036597db267cf61612251624de6420e17cbe9c20cdcfdf876b3b12978d3264a007fcaaa71c4cdb701d9ebc0323f44f1880d0dbc3f4022080e0ebdaf2e2ffffff010a4b0a1418e69cc672973992bb5f76d049a5b2c5ddf7743612251624de6420184e7b103d34c41003f9b864d5f8c1adda9bd0436b253bb3c844bc739c1e77c91880d0dbc3f4022080d0dbc3f4020a4b0a14344c39bb8f4512d6cab1f6aafac1811ef9d8afdf12251624de64204d420aea843e92a0cfe69d89696dff6827769f9cb52a249af537ce89bf2a4b741880d0dbc3f4022080d0dbc3f4020a4b0a1437ef19af29679b368d2b9e9de3f8769b3578667612251624de6420bd03de9f8ab29e2800094e153fac6f696cfa512536c9c2f804dcb2c2c4e4aed61880d0dbc3f4022080d0dbc3f4020a4b0a1462633d9db7ed78e951f79913fdc8231aa77ec12b12251624de64208f4a74a07351895ddf373057b98fae6dfaf2cd21f37a063e19601078fe470d531880d0dbc3f4022080d0dbc3f4020a4b0a147b343e041ca130000a8bc00c35152bd7e774003712251624de64204a5d4753eb79f92e80efe22df7aca4f666a4f44bf81c536c4a09d4b9c5b654b51880d0dbc3f4022080d0dbc3f4020a4b0a1491844d296bd8e591448efc65fd6ad51a888d58fa12251624de6420c80e9abef7ff439c10c68fe8f1303deddfc527718c3b37d8ba6807446e3c827a1880d0dbc3f4022080d0dbc3f4020a4b0a14b3727172ce6473bc780298a2d66c12f1a14f5b2a12251624de64209142afcc691b7cc05d26c7b0be0c8b46418294171730e079f384fde2fa50bafc1880d0dbc3f4022080d0dbc3f4020a4b0a14b6f20c7faa2b2f6f24518fa02b71cb5f4a09fba312251624de642049b288e4ebbb3a281c2d546fc30253d5baf08993b6e5d295fb787a5b314a298e1880d0dbc3f4022080d0dbc3f4020a4b0a14e0dd72609cc106210d1aa13936cb67b93a0aee2112251624de642004224339688f012e649de48e241880092eaa8f6aa0f4f14bfcf9e0c76917c0b61880d0dbc3f4022080d0dbc3f4020a4b0a14fc3108dc3814888f4187452182bc1baf83b71bc912251624de64204034b37ceda8a0bf13b1abaeee7a8f9383542099a554d219b93d0ce69e3970e81880d0dbc3f4022080d0dbc3f402124f0a1406fd60078eb4c2356137dd50036597db267cf61612251624de6420e17cbe9c20cdcfdf876b3b12978d3264a007fcaaa71c4cdb701d9ebc0323f44f1880d0dbc3f4022080e0ebdaf2e2ffffff01").unwrap();
        let res = tendermint_header_verify_run(Bytes::from(input)).unwrap();
        let res = hex::encode(res);
        assert_eq!(res,"000000000000000000000000000000000000000000000000000000000000022042696e616e63652d436861696e2d4e696c6500000000000000000000000000000000000003fc05e3a3e248bc209955054d880e4d89ff3c0419c0cd77681f4b4c6649ead5545054b980d9ab0fc10d18ca0e0832d5f4c063c5489ec1443dfb738252d038a82131b27ae17cbe9c20cdcfdf876b3b12978d3264a007fcaaa71c4cdb701d9ebc0323f44f000000174876e800184e7b103d34c41003f9b864d5f8c1adda9bd0436b253bb3c844bc739c1e77c9000000174876e8004d420aea843e92a0cfe69d89696dff6827769f9cb52a249af537ce89bf2a4b74000000174876e800bd03de9f8ab29e2800094e153fac6f696cfa512536c9c2f804dcb2c2c4e4aed6000000174876e8008f4a74a07351895ddf373057b98fae6dfaf2cd21f37a063e19601078fe470d53000000174876e8004a5d4753eb79f92e80efe22df7aca4f666a4f44bf81c536c4a09d4b9c5b654b5000000174876e800c80e9abef7ff439c10c68fe8f1303deddfc527718c3b37d8ba6807446e3c827a000000174876e8009142afcc691b7cc05d26c7b0be0c8b46418294171730e079f384fde2fa50bafc000000174876e80049b288e4ebbb3a281c2d546fc30253d5baf08993b6e5d295fb787a5b314a298e000000174876e80004224339688f012e649de48e241880092eaa8f6aa0f4f14bfcf9e0c76917c0b6000000174876e8004034b37ceda8a0bf13b1abaeee7a8f9383542099a554d219b93d0ce69e3970e8000000174876e800");
    }

    #[test]
    fn iavl_proof_verify() {
        let input = hex::decode("00000000000000000000000000000000000000000000000000000000000007ae6962630000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e0000010038020000000000019ce2000000000000000000000000000000000000000000000000000000000000009300000000000000000000000000000000000000000000000000000e35fa931a0000f870a0424e420000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000889bef94293405280094ef81397266e8021d967c3099aa8b781f8c3f99f2948ba31c21685c7ffa3cbb69cd837672dd5254cadb86017713ec8491cb58b672e759d5c7d9b6ac2d39568153aaa730c488acaa3d6097d774df0976900a91070a066961766c3a76120e0000010038020000000000019ce21af606f4060af1060a2d08121083bc0618cbcf954222206473c3fc09d3e700e4b8121ebedb8defedb38a57a11fa54300c7f537318c9b190a2d0811108f960418cbcf954222208b061ab760b6341a915696265ab0841f0657f0d0ad75c5bc08b5a92df9f6e84a0a2d081010c7d20118cbcf9542222091759ca6146640e0a33de7be5b53db2c69abf3eaf4b483a0b86cc8893d8853ce0a2c080f10c77318cbcf95422220d0d5c5c95b4e1d15b8cf01dfa68b3af6a846d549d7fb61eaed3ae6a256bd0e350a2c080e10c74318cbcf954222207183ccf5b70efc3e4c849c86ded15118819a46a9fb7eea5034fd477d56bf3c490a2c080d10c72b18cbcf954222205b5a92812ee771649fa4a53464ae7070adfd3aaea01140384bd9bfc11fe1ec400a2c080c10c71318cbcf95422220dc4d735d7096527eda97f96047ac42bcd53eee22c67a8e2f4ed3f581cb11851a0a2c080b10c70718cbcf95422a20b5d530b424046e1269950724735a0da5c402d8640ad4a0e65499b2d05bf7b87b0a2c080a10e40518cbcf95422220a51a3db12a79f3c809f63df49061ad40b7276a10a1a6809d9e0281cc35534b3f0a2c080910e40318cbcf9542222015eb48e2a1dd37ad88276cb04935d4d3b39eb993b24ee20a18a6c3d3feabf7200a2c080810e40118cbcf954222204c1b127f2e7b9b063ef3111479426a3b7a8fdea03b566a6f0a0decc1ef4584b20a2b0807106418cbcf95422220d17128bc7133f1f1159d5c7c82748773260d9a9958aa03f39a380e6a506435000a2b0806102418cbcf95422220959951e4ac892925994b564b54f7dcdf96be49e6167497b7c34aac7d8b3e11ac0a2b0805101418cbcf95422220e047c1f1c58944a27af737dcee1313dc501c4e2006663e835bcca9662ffd84220a2b0804100c18cbcf95422220ddf4258a669d79d3c43411bdef4161d7fc299f0558e204f4eda40a7e112007300a2b0803100818cbcf95422220e2ecce5e132eebd9d01992c71a2d5eb5d579b10ab162fc8a35f41015ab08ac750a2b0802100418cbcf9542222078a11f6a79afcc1e2e4abf6c62d5c1683cfc3bd9789d5fd4828f88a9e36a3b230a2b0801100218cbcf954222206d61aa355d7607683ef2e3fafa19d85eca227e417d68a8fdc6166dde4930fece1a370a0e0000010038020000000000019ce2122086295bb11ac7cba0a6fc3b9cfd165ea6feb95c37b6a2f737436a5d138f29e23f18cbcf95420af6050a0a6d756c746973746f726512036962631ae205e0050add050a330a06746f6b656e7312290a2708d6cf95421220789d2c8eac364abf32a2200e1d924a0e255703a162ee0c3ac2c37b347ae3daff0a0e0a0376616c12070a0508d6cf95420a320a057374616b6512290a2708d6cf954212207ebe9250eeae08171b95b93a0e685e8f25b9e2cce0464d2101f3e5607b76869e0a320a05706169727312290a2708d6cf95421220fe5e73b53ccd86f727122d6ae81aeab35f1e5338c4bdeb90e30fae57b202e9360a300a0369626312290a2708d6cf95421220af249eb96336e7498ffc266165a9716feb3363fc9560980804e491e181d8b5760a330a0662726964676512290a2708d6cf95421220bd239e499785b20d4a4c61862145d1f8ddf96c8e7e046d6679e4dfd4d38f98300a0f0a046d61696e12070a0508d6cf95420a300a0361636312290a2708d6cf954212208450d84a94122dcbf3a60b59b5f03cc13d0fee2cfe4740928457b885e9637f070a380a0b61746f6d69635f7377617012290a2708d6cf954212208d76e0bb011e064ad1964c1b322a0df526d24158e1f3189efbf5197818e711cb0a2f0a02736312290a2708d6cf95421220aebdaccfd22b92af6a0d9357232b91b342f068386e1ddc610f433d9feeef18480a350a08736c617368696e6712290a2708d6cf95421220fb0f9a8cf22cca3c756f8fefed19516ea27b6793d23a68ee85873b92ffddfac20a360a0974696d655f6c6f636b12290a2708d6cf95421220b40e4164b954e829ee8918cb3310ba691ea8613dc810bf65e77379dca70bf6ae0a330a06706172616d7312290a2708d6cf9542122024a0aa2cea5a4fd1b5f375fcf1e1318e5f49a5ff89209f18c12505f2d7b6ecb40a300a03676f7612290a2708d6cf95421220939b333eb64a437d398da930435d6ca6b0b1c9db810698f1734c141013c08e350a300a0364657812290a2708d6cf954212204fb5c65140ef175a741c8603efe98fc04871717d978e7dfb80a4a48e66d21e960a110a066f7261636c6512070a0508d6cf9542").unwrap();
        let res = iavl_proof_verify_run(Bytes::from(input)).unwrap();
        let res = hex::encode(res);
        assert_eq!(res, "0000000000000000000000000000000000000000000000000000000000000001")
    }
}
