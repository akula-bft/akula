use arrayref::array_ref;
use bytes::Bytes;
use evmodin::Revision;

pub type GasFunction = fn(&[u8], Revision) -> u64;
pub type RunFunction = fn(Bytes<'static>) -> Option<Bytes<'static>>;

pub struct Contract {
    pub gas: GasFunction,
    pub run: RunFunction,
}

pub const CONTRACTS: [Contract; NUM_OF_ISTANBUL_CONTRACTS] = [
    Contract {
        gas: ecrec_gas,
        run: ecrec_run,
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

fn ecrec_gas(_: &[u8], _: Revision) -> u64 {
    3_000
}
fn ecrec_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

fn sha256_gas(input: &[u8], _: Revision) -> u64 {
    60 + 12 * ((input.len() as u64 + 31) / 32)
}
fn sha256_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

fn ripemd160_gas(input: &[u8], _: Revision) -> u64 {
    600 + 120 * ((input.len() as u64 + 31) / 32)
}
fn ripemd160_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

fn id_gas(input: &[u8], _: Revision) -> u64 {
    15 + 3 * ((input.len() as u64 + 31) / 32)
}
fn id_run(input: Bytes<'static>) -> Option<Bytes<'static>> {
    Some(input)
}

fn expmod_gas(_: &[u8], _: Revision) -> u64 {
    todo!()
}
fn expmod_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

fn bn_add_gas(_: &[u8], rev: Revision) -> u64 {
    if rev >= Revision::Istanbul {
        150
    } else {
        500
    }
}
fn bn_add_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

fn bn_mul_gas(_: &[u8], rev: Revision) -> u64 {
    if rev >= Revision::Istanbul {
        6_000
    } else {
        40_000
    }
}
fn bn_mul_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

const SNARKV_STRIDE: u8 = 192;

fn snarkv_gas(input: &[u8], rev: Revision) -> u64 {
    let k = input.len() as u64 / SNARKV_STRIDE as u64;
    if rev >= Revision::Istanbul {
        34_000 * k + 45_000
    } else {
        80_000 * k + 100_000
    }
}
fn snarkv_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}

fn blake2_f_gas(input: &[u8], _: Revision) -> u64 {
    if input.len() < 4 {
        // blake2_f_run will fail anyway
        return 0;
    }
    u32::from_be_bytes(*array_ref!(input, 0, 4)).into()
}
fn blake2_f_run(_: Bytes<'static>) -> Option<Bytes<'static>> {
    todo!()
}
