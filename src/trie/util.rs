pub(crate) fn has_prefix(s: &[u8], prefix: &[u8]) -> bool {
    &s[0..prefix.len()] == prefix
}
