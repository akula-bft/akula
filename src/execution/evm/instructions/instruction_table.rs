use crate::{execution::evm::instructions::properties, models::*};
use once_cell::sync::Lazy;

#[derive(Clone, Copy, Debug)]
pub struct InstructionTableEntry {
    pub gas_cost: u16,
    pub stack_height_required: u8,
    pub can_overflow_stack: bool,
}

pub type InstructionTable = [Option<InstructionTableEntry>; 256];
pub type InstructionTables = [InstructionTable; Revision::len()];

pub static INSTRUCTION_TABLES: Lazy<InstructionTables> = Lazy::new(|| {
    let mut table = [[None; 256]; Revision::len()];

    for revision in Revision::iter() {
        for (opcode, &cost) in properties::gas_costs(revision).iter().enumerate() {
            if let Some(cost) = cost {
                let stack_height_required = properties::PROPERTIES[opcode]
                    .unwrap()
                    .stack_height_required;

                // Because any instruction can increase stack height at most of 1,
                // stack overflow can only happen if stack height is already at the limit.
                debug_assert!(properties::PROPERTIES[opcode].unwrap().stack_height_change <= 1);

                table[revision as usize][opcode] = Some(InstructionTableEntry {
                    gas_cost: cost,
                    stack_height_required,
                    can_overflow_stack: properties::PROPERTIES[opcode].unwrap().stack_height_change
                        > 0,
                });
            }
        }
    }
    table
});

pub fn get_instruction_table(revision: Revision) -> &'static InstructionTable {
    &INSTRUCTION_TABLES[revision as usize]
}
