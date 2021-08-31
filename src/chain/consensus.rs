use crate::models::*;
use async_trait::*;
use std::fmt::Debug;

#[async_trait]
pub trait Consensus: Debug + Send + Sync {
    async fn verify_header(&self, header: &BlockHeader) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub struct NoProof;

#[async_trait]
impl Consensus for NoProof {
    async fn verify_header(&self, _: &BlockHeader) -> anyhow::Result<()> {
        Ok(())
    }
}

pub type Clique = NoProof;
pub type AuRa = NoProof;

#[derive(Debug)]
pub struct Ethash;

#[async_trait]
impl Consensus for Ethash {
    async fn verify_header(&self, header: &BlockHeader) -> anyhow::Result<()> {
        let _ = header;
        // TODO: port Ethash PoW verification
        // let epoch_number = {header.number / ethash::epoch_length};
        // auto epoch_context{ethash::create_epoch_context(static_cast<int>(epoch_number))};

        // auto boundary256{header.boundary()};
        // auto seal_hash(header.hash(/*for_sealing =*/true));
        // ethash::hash256 sealh256{*reinterpret_cast<ethash::hash256*>(seal_hash.bytes)};
        // ethash::hash256 mixh256{};
        // std::memcpy(mixh256.bytes, header.mix_hash.bytes, 32);

        // uint64_t nonce{endian::load_big_u64(header.nonce.data())};
        // return ethash::verify(*epoch_context, sealh256, mixh256, nonce, boundary256) ? ValidationError::Ok
        //                                                                              : ValidationError::InvalidSeal;
        Ok(())
    }
}
