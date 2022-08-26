use super::*;

#[derive(Debug)]
pub struct ParliaNewBlockState {
    next_validators: Option<Vec<Address>>
}

impl ParliaNewBlockState {

    pub fn new(next_validators: Option<Vec<Address>>) -> ParliaNewBlockState {
        ParliaNewBlockState {
            next_validators
        }
    }

    pub fn get_validators(&self) -> Option<&Vec<Address>> {
        self.next_validators.as_ref()
    }

    pub fn parsed_validators(&self) -> bool {
        self.next_validators.is_some()
    }
}