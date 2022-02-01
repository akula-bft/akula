use crate::models::*;
use bytes::Bytes;
use evmodin::{ExecutionState, OpCode, StatusCode};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, PartialEq)]
pub enum CodeKind {
    Precompile,
    Bytecode(Option<Bytes>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum CallKind {
    Call,
    CallCode,
    DelegateCall,
    StaticCall,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MessageKind {
    Create,
    Call {
        call_kind: CallKind,
        code_kind: CodeKind,
    },
}

#[allow(unused, clippy::too_many_arguments)]
pub trait Tracer: Send + 'static {
    fn capture_start(
        &mut self,
        depth: u16,
        from: Address,
        to: Address,
        call_type: MessageKind,
        input: Bytes,
        gas: u64,
        value: U256,
    ) {
    }
    fn capture_state(
        &mut self,
        env: &ExecutionState,
        pc: u64,
        op: OpCode,
        cost: u64,
        return_data: Bytes,
        depth: u16,
        err: StatusCode,
    ) {
    }
    fn capture_end(&mut self, depth: u16, output: Bytes, gas_left: u64, err: StatusCode) {}
    fn capture_self_destruct(&mut self, caller: Address, beneficiary: Address) {}
    fn capture_account_read(&mut self, account: Address) {}
    fn capture_account_write(&mut self, account: Address) {}
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CallTracerFlags {
    pub from: bool,
    pub to: bool,
}

#[derive(Debug, Default)]
pub struct CallTracer {
    addresses: HashMap<Address, CallTracerFlags>,
}

impl Tracer for CallTracer {
    fn capture_start(
        &mut self,
        _: u16,
        from: Address,
        to: Address,
        _: MessageKind,
        _: Bytes,
        _: u64,
        _: U256,
    ) {
        self.addresses.entry(from).or_default().from = true;
        self.addresses.entry(to).or_default().to = true;
    }

    fn capture_self_destruct(&mut self, caller: Address, beneficiary: Address) {
        self.addresses.entry(caller).or_default().from = true;
        self.addresses.entry(beneficiary).or_default().to = true;
    }
}

impl CallTracer {
    pub fn into_sorted_iter(&self) -> impl Iterator<Item = (Address, CallTracerFlags)> {
        self.addresses
            .iter()
            .map(|(&k, &v)| (k, v))
            .collect::<BTreeMap<_, _>>()
            .into_iter()
    }
}
