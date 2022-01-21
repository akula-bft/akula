use std::cmp;

pub(crate) fn has_prefix(s: &[u8], prefix: &[u8]) -> bool {
    &s[0..prefix.len()] == prefix
}

pub(crate) fn assert_subset(sub: u16, sup: u16) {
    assert_eq!(sub & sup, sub);
}

pub(crate) fn prefix_length(a: &[u8], b: &[u8]) -> usize {
    let len = cmp::min(a.len(), b.len());
    for i in 0..len {
        if a[i] != b[i] {
            return i;
        }
    }
    len
}
