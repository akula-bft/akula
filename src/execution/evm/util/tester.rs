use crate::{
    execution::evm::{
        util::{mocked_host::*, *},
        *,
    },
    models::*,
};
use bytes::Bytes;
use educe::Educe;
use ethereum_types::Address;
use ethnum::{AsU256, U256};
use std::sync::Arc;

fn exec(
    host: &mut MockedHost,
    revision: Revision,
    message: InterpreterMessage,
    code: Vec<u8>,
) -> Output {
    // Add EIP-2929 tweak.
    if revision >= Revision::Berlin {
        host.access_account(message.sender);
        host.access_account(message.recipient);
    }
    let code = AnalyzedCode::analyze(&code);

    code.execute(host, &message, revision)
}

#[derive(Clone, Copy, Debug)]
enum GasCheck {
    Used(i64),
    Left(i64),
}

/// Tester that executes EVM bytecode with `MockedHost` context and runs set checks.
#[derive(Clone, Educe)]
#[educe(Debug)]
#[must_use]
pub struct EvmTester {
    host: MockedHost,
    #[educe(Debug(ignore))]
    apply_host_fns: Vec<Arc<dyn Fn(&mut MockedHost, &InterpreterMessage) + 'static>>,
    #[educe(Debug(ignore))]
    inspect_output_fn: Arc<dyn Fn(&[u8]) + 'static>,
    #[educe(Debug(ignore))]
    inspect_host_fn: Arc<dyn Fn(&MockedHost, &InterpreterMessage) + 'static>,
    #[educe(Debug(ignore))]
    inspect_fn: Arc<dyn Fn(&MockedHost, &InterpreterMessage, &[u8]) + 'static>,
    revision: Revision,
    message: InterpreterMessage,
    code: Vec<u8>,
    gas_check: Option<GasCheck>,
    expected_status_codes: Option<Vec<StatusCode>>,
    expected_output_data: Option<Vec<u8>>,
}

impl Default for EvmTester {
    fn default() -> Self {
        Self::new()
    }
}

impl EvmTester {
    /// Create new `EvmTester`.
    pub fn new() -> Self {
        Self {
            host: MockedHost::default(),
            apply_host_fns: vec![],
            inspect_output_fn: Arc::new(|_| ()),
            inspect_host_fn: Arc::new(|_, _| ()),
            inspect_fn: Arc::new(|_, _, _| ()),
            revision: Revision::Byzantium,
            message: InterpreterMessage {
                kind: CallKind::Call,
                is_static: false,
                depth: 0,
                gas: i64::MAX,
                recipient: Address::zero(),
                code_address: Address::zero(),
                sender: Address::zero(),
                input_data: Bytes::new(),
                value: U256::ZERO,
            },
            code: Vec::new(),
            gas_check: None,
            expected_status_codes: None,
            expected_output_data: None,
        }
    }

    /// Set code to be executed.
    pub fn code(mut self, code: impl Into<Bytecode>) -> Self {
        self.code = code.into().build();
        self
    }

    /// Queue function that will modify the host before execution.
    pub fn apply_host_fn(
        mut self,
        host_fn: impl Fn(&mut MockedHost, &InterpreterMessage) + 'static,
    ) -> Self {
        self.apply_host_fns.push(Arc::new(host_fn));
        self
    }

    /// Set EVM revision for this tester.
    pub fn revision(mut self, revision: Revision) -> Self {
        self.revision = revision;
        self
    }

    /// Set message depth.
    pub fn depth(mut self, depth: u16) -> Self {
        self.message.depth = depth.into();
        self
    }

    /// Set provided gas.
    pub fn gas(mut self, gas: i64) -> Self {
        self.message.gas = gas;
        self
    }

    /// Set static message flag.
    pub fn set_static(mut self, is_static: bool) -> Self {
        self.message.is_static = is_static;
        self
    }

    /// Set message destination.
    pub fn destination(mut self, destination: impl Into<Address>) -> Self {
        self.message.recipient = destination.into();
        self
    }

    /// Set message sender.
    pub fn sender(mut self, sender: impl Into<Address>) -> Self {
        self.message.sender = sender.into();
        self
    }

    /// Set message sender.
    pub fn value(mut self, value: impl AsU256) -> Self {
        self.message.value = value.as_u256();
        self
    }

    /// Check how much gas will be used. Mutually exclusive with `EvmTester::gas_left`.
    pub fn gas_used(mut self, expected_gas_used: i64) -> Self {
        self.gas_check = Some(GasCheck::Used(expected_gas_used));
        self
    }

    /// Check how much gas will be left after execution. Mutually exclusive with `EvmTester::gas_used`.
    pub fn gas_left(mut self, expected_gas_left: i64) -> Self {
        self.gas_check = Some(GasCheck::Left(expected_gas_left));
        self
    }

    /// Set provided input data.
    pub fn input(mut self, input: impl Into<Bytes>) -> Self {
        self.message.input_data = input.into();
        self
    }

    /// Check returned status.
    pub fn status(mut self, expected_status_code: StatusCode) -> Self {
        self.expected_status_codes = Some(vec![expected_status_code]);
        self
    }

    /// Check returned status to be one of these.
    pub fn status_one_of<const N: usize>(mut self, expected_status_code: [StatusCode; N]) -> Self {
        self.expected_status_codes = Some(expected_status_code.to_vec());
        self
    }

    /// Check output to be equal to provided integer.
    pub fn output_value(mut self, expected_output_data: impl AsU256) -> Self {
        self.expected_output_data = Some(expected_output_data.as_u256().to_be_bytes().to_vec());
        self
    }

    /// Check output data to be equal to provided byte string.
    pub fn output_data(mut self, expected_output_data: impl Into<Vec<u8>>) -> Self {
        self.expected_output_data = Some(expected_output_data.into());
        self
    }

    /// Inspect output with provided function.
    pub fn inspect_output(mut self, inspect_output_fn: impl Fn(&[u8]) + 'static) -> Self {
        self.inspect_output_fn = Arc::new(inspect_output_fn);
        self
    }

    /// Inspect host with provided function.
    pub fn inspect_host(mut self, f: impl Fn(&MockedHost, &InterpreterMessage) + 'static) -> Self {
        self.inspect_host_fn = Arc::new(f);
        self
    }

    /// Inspect host and output with provided function.
    pub fn inspect(
        mut self,
        f: impl Fn(&MockedHost, &InterpreterMessage, &[u8]) + 'static,
    ) -> Self {
        self.inspect_fn = Arc::new(f);
        self
    }

    /// Execute provided code, run checks and return bytecode returned by EVM.
    pub fn check_and_get_result(self) -> Output {
        let mut host = self.host;
        for f in self.apply_host_fns {
            (f)(&mut host, &self.message);
        }
        let output = exec(&mut host, self.revision, self.message.clone(), self.code);

        if let Some(status_codes) = self.expected_status_codes {
            assert!(
                status_codes.iter().any(|s| *s == output.status_code),
                "Status code mismatch: {}, but must be one of {:?}",
                output.status_code,
                status_codes
            );
        }

        if let Some(gas_check) = self.gas_check {
            match gas_check {
                GasCheck::Used(used) => assert_eq!(self.message.gas - output.gas_left, used),
                GasCheck::Left(left) => assert_eq!(output.gas_left, left),
            }
        }

        if let Some(expected_data) = &self.expected_output_data {
            assert_eq!(&*output.output_data, expected_data);
        }

        (self.inspect_output_fn)(&*output.output_data);
        (self.inspect_host_fn)(&host, &self.message);
        (self.inspect_fn)(&host, &self.message, &*output.output_data);

        output
    }

    /// Execute provided code and run checks.
    pub fn check(self) {
        self.check_and_get_result();
    }
}
