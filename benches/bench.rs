use akula::{
    crypto::keccak256,
    etl::collector::{TableCollector, OPTIMAL_BUFFER_CAPACITY},
    kv::{new_mem_chaindata, tables},
    trie::{do_increment_intermediate_hashes, unpack_nibbles, DbTrieLoader, PrefixSet},
};
use criterion::*;
use ethereum_types::Address;
use primitive_types::H256;
use tempfile::tempdir;

fn generate_prefix_sets(address_from: u64, n_addresses: u64) -> (PrefixSet, PrefixSet) {
    let mut account_changes = PrefixSet::new();
    let mut storage_changes = PrefixSet::new();

    for i in 0..n_addresses {
        let hashed_address = keccak256(Address::from_low_u64_be(address_from + i));
        account_changes.insert(hashed_address.as_ref());
        if i % 2 == 0 {
            for j in 0..(i % 100) {
                let hashed_location = keccak256(H256::from_low_u64_be(j));
                let key = [
                    hashed_address.as_bytes(),
                    unpack_nibbles(hashed_location.as_bytes()).as_slice(),
                ]
                .concat();
                storage_changes.insert(key.as_slice());
            }
        }
    }

    (account_changes, storage_changes)
}

pub fn benchmark_trie(c: &mut Criterion) {
    let db = new_mem_chaindata().unwrap();
    let tx = db.begin_mutable().unwrap();

    let temp_dir = tempdir().unwrap();
    let mut account_collector =
        TableCollector::<tables::TrieAccount>::new(&temp_dir, OPTIMAL_BUFFER_CAPACITY);
    let mut storage_collector =
        TableCollector::<tables::TrieStorage>::new(&temp_dir, OPTIMAL_BUFFER_CAPACITY);

    {
        let mut loader = DbTrieLoader::new(&tx, &mut account_collector, &mut storage_collector);

        let (account_changes, storage_changes) = generate_prefix_sets(0, 20_000);

        c.bench_function("trie-loader-generate", |b| {
            b.iter_with_setup(
                || (account_changes.clone(), storage_changes.clone()),
                |(mut ac, mut sc)| loader.calculate_root(&mut ac, &mut sc).unwrap(),
            )
        });

        for i in 0..10 {
            let (mut account_changes, mut storage_changes) =
                generate_prefix_sets(10_000 + i * 100_000, 100_000);
            do_increment_intermediate_hashes(
                &tx,
                &temp_dir,
                None,
                &mut account_changes,
                &mut storage_changes,
            )
            .unwrap();
        }

        tx.commit().unwrap();
    }

    let tx = db.begin_mutable().unwrap();
    let mut loader = DbTrieLoader::new(&tx, &mut account_collector, &mut storage_collector);

    let (account_changes, storage_changes) = generate_prefix_sets(0, 20_000);

    c.bench_function("trie-loader-increment", |b| {
        b.iter_with_setup(
            || (account_changes.clone(), storage_changes.clone()),
            |(mut ac, mut sc)| loader.calculate_root(&mut ac, &mut sc).unwrap(),
        )
    });
}

criterion_group!(benches, benchmark_trie);
criterion_main!(benches);
