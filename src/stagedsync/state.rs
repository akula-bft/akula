use super::unwind::PersistentUnwindStack;

pub struct State {
    unwind_stack: PersistentUnwindStack,
}
