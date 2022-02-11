use super::properties::GAS_COSTS;
use crate::{execution::evm::instructions::properties, models::*};

#[derive(Clone, Copy, Debug)]
pub struct InstructionTableEntry {
    pub gas_cost: i16,
    pub stack_height_required: u8,
    pub can_overflow_stack: bool,
}

pub type InstructionTable = [InstructionTableEntry; 256];
pub type InstructionTables = [InstructionTable; Revision::len()];

const fn instruction_tables() -> InstructionTables {
    let mut table = [[InstructionTableEntry {
        gas_cost: -1,
        stack_height_required: 0,
        can_overflow_stack: false,
    }; 256]; Revision::len()];

    const LATEST: Revision = Revision::latest();

    let revtable = Revision::iter();
    let mut reviter = 0_usize;
    loop {
        let revision = revtable[reviter];

        let mut opcode = 0;
        loop {
            let (stack_height_required, can_overflow_stack) =
                if let Some(p) = &properties::PROPERTIES[opcode] {
                    (p.stack_height_required, p.stack_height_change > 0)
                } else {
                    (0, false)
                };

            table[revision as usize][opcode] = InstructionTableEntry {
                gas_cost: GAS_COSTS[revision as usize][opcode],
                stack_height_required,
                can_overflow_stack,
            };

            if opcode == u8::MAX as usize {
                break;
            } else {
                opcode += 1;
            }
        }

        if matches!(revision, LATEST) {
            break;
        } else {
            reviter += 1;
        }
    }
    table
}

pub const INSTRUCTION_TABLES: InstructionTables = instruction_tables();

#[inline]
pub fn get_instruction_table(revision: Revision) -> &'static InstructionTable {
    &INSTRUCTION_TABLES[revision as usize]
}
