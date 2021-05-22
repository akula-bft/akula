mod composite_keys;
mod history_index;
pub use self::{composite_keys::*, history_index::*};

pub const fn bytes_mask(fixed_bits: u64) -> (u64, u8) {
    let fixed_bytes = (fixed_bits + 7) / 8;
    let shift_bits = fixed_bits & 7;
    let mut mask = 0xff;
    if shift_bits != 0 {
        mask = 0xff << (8 - shift_bits);
    }
    (fixed_bytes, mask)
}
