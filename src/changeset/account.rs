pub use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use ethereum_types::Address;

    #[test]
    fn account_encoding() {
        type Table = tables::PlainAccountChangeSet;

        let mut ch = ChangeSet::default();

        for (i, val) in vec![
            "f7f6db1eb17c6d582078e0ffdd0c".into(),
            "b1e9b5c16355eede662031dd621d08faf4ea".into(),
            "862cf52b74f1cea41ddd8ffa4b3e7c7790".into(),
        ]
        .into_iter()
        .enumerate()
        {
            let address = format!("0xBe828AD8B538D1D691891F6c725dEdc5989abBc{}", i)
                .parse::<Address>()
                .unwrap();

            ch.insert(Change::new(address.to_fixed_bytes(), val));
        }

        let mut ch2 = ChangeSet::default();

        for (k, v) in Table::encode(1, &ch) {
            let (_, k, v) = Table::decode(k, v);

            ch2.insert(Change::new(k, v));
        }

        assert_eq!(ch, ch2);
    }
}
