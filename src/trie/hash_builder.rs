pub(crate) struct HashBuilder;

pub(crate) fn pack_nibbles(nibbles: &[u8]) -> Vec<u8> {
    let n = (nibbles.len() + 1) / 2;
    let mut out = vec![0u8; n];
    if n == 0 {
        return out;
    }

    let mut i = 0;
    let mut j = 0;
    while j < nibbles.len() {
        out[i] = nibbles[j] << 4;
        j += 1;
        if j < nibbles.len() {
            out[i] += nibbles[j];
            j += 1;
            i += 1;
        }
    }

    out
}

fn unpack_nibbles(packed: &[u8]) -> Vec<u8> {
    todo!();
}
