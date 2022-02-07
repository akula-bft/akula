#[macro_use]
pub(crate) mod arithmetic;
#[macro_use]
pub(crate) mod bitwise;
#[macro_use]
pub(crate) mod boolean;
#[macro_use]
pub(crate) mod call;
#[macro_use]
pub(crate) mod control;
#[macro_use]
pub(crate) mod external;
#[macro_use]
pub mod instruction_table;
#[macro_use]
pub(crate) mod memory;
#[macro_use]
pub(crate) mod properties;
#[macro_use]
pub(crate) mod stack_manip;

pub use properties::PROPERTIES;
