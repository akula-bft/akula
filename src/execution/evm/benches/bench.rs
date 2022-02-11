use akula::{
    execution::evm::{
        instructions::{instruction_table::get_instruction_table, PROPERTIES},
        util::{mocked_host::MockedHost, Bytecode},
        AnalyzedCode, CallKind, InterpreterMessage, OpCode, Output, StatusCode,
    },
    models::Revision,
};
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, profiler::Profiler, BatchSize, Criterion};
use ethereum_types::Address;
use ethnum::*;
use hex_literal::hex;
use pprof::{flamegraph::Options, ProfilerGuard};
use serde::Deserialize;
use std::{fmt::Display, fs::File, os::raw::c_int, path::Path};

/// Small custom profiler that can be used with Criterion to create a flamegraph for benchmarks.
/// Also see [the Criterion documentation on this][custom-profiler].
///
/// ## Example on how to enable the custom profiler:
///
/// ```
/// mod perf;
/// use perf::FlamegraphProfiler;
///
/// fn fibonacci_profiled(criterion: &mut Criterion) {
///     // Use the criterion struct as normal here.
/// }
///
/// fn custom() -> Criterion {
///     Criterion::default().with_profiler(FlamegraphProfiler::new())
/// }
///
/// criterion_group! {
///     name = benches;
///     config = custom();
///     targets = fibonacci_profiled
/// }
/// ```
///
/// The neat thing about this is that it will sample _only_ the benchmark, and not other stuff like
/// the setup process.
///
/// Further, it will only kick in if `--profile-time <time>` is passed to the benchmark binary.
/// A flamegraph will be created for each individual benchmark in its report directory under
/// `profile/flamegraph.svg`.
///
/// [custom-profiler]: https://bheisler.github.io/criterion.rs/book/user_guide/profiling.html#implementing-in-process-profiling-hooks
pub struct FlamegraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

impl<'a> FlamegraphProfiler<'a> {
    #[allow(dead_code)]
    pub fn new(frequency: c_int) -> Self {
        FlamegraphProfiler {
            frequency,
            active_profiler: None,
        }
    }
}

impl<'a> Profiler for FlamegraphProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();
        let flamegraph_path = benchmark_dir.join("flamegraph.svg");
        let flamegraph_file = File::create(&flamegraph_path)
            .expect("File system error while creating flamegraph.svg");
        if let Some(profiler) = self.active_profiler.take() {
            let mut options = Options::default();
            options.reverse_stack_order = true;
            profiler
                .report()
                .build()
                .unwrap()
                .flamegraph_with_options(flamegraph_file, &mut options)
                .expect("Error writing flamegraph");
        }
    }
}

/// Stack limit inside the EVM benchmark loop (one stack item is used for the loop counter).
const STACK_LIMIT: usize = 1023;

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// The code uses as minimal stack as possible.
    MinStack = 0,
    /// The code fills the stack up to its limit.
    FullStack = 1,
}

/// The instruction grouping by EVM stack requirements.
#[derive(Clone, Copy, Debug)]
enum InstructionCategory {
    /// No-op instruction.
    Nop,
    /// Nullary operator - produces a result without any stack input.
    Nullop,
    /// Unary operator.
    Unop,
    /// Binary operator.
    Binop,
    /// PUSH instruction.
    Push,
    /// DUP instruction.
    Dup,
    /// SWAP instruction.
    Swap,
    /// Not any of the categories above.
    Other,
}

impl InstructionCategory {
    fn from_opcode(opcode: OpCode) -> Self {
        let t = PROPERTIES[opcode.to_usize()].unwrap();
        if opcode.to_u8() >= OpCode::PUSH1.to_u8() && opcode.to_u8() <= OpCode::PUSH32.to_u8() {
            Self::Push
        } else if opcode.to_u8() >= OpCode::SWAP1.to_u8()
            && opcode.to_u8() <= OpCode::SWAP16.to_u8()
        {
            Self::Swap
        } else if opcode.to_u8() >= OpCode::DUP1.to_u8() && opcode.to_u8() <= OpCode::DUP16.to_u8()
        {
            Self::Dup
        } else if t.stack_height_required == 0 && t.stack_height_change == 0 {
            Self::Nop
        } else if t.stack_height_required == 0 && t.stack_height_change == 1 {
            Self::Nullop
        } else if t.stack_height_required == 1 && t.stack_height_change == 0 {
            Self::Unop
        } else if t.stack_height_required == 2 && t.stack_height_change == -1 {
            Self::Binop
        } else {
            Self::Other
        }
    }
}

impl Display for InstructionCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Nop => 'n',
                Self::Nullop => 'a',
                Self::Unop => 'u',
                Self::Binop => 'b',
                Self::Push => 'p',
                Self::Dup => 'd',
                Self::Swap => 's',
                Self::Other => 'X',
            }
        )
    }
}

struct CodeParams {
    opcode: OpCode,
    mode: Mode,
}

impl Display for CodeParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}{}",
            self.opcode.name(),
            self.opcode,
            self.mode as u8
        )
    }
}

fn generate_loop_inner_code(params: &CodeParams) -> Bytecode {
    let &CodeParams { opcode, mode } = params;

    let category = InstructionCategory::from_opcode(opcode);

    match (mode, category) {
        (Mode::MinStack, InstructionCategory::Nop) => {
            // JUMPDEST JUMPDEST ...
            STACK_LIMIT * Bytecode::new().opcode(opcode).opcode(opcode)
        }
        (Mode::MinStack, InstructionCategory::Nullop) => {
            // CALLER POP CALLER POP ...
            STACK_LIMIT * Bytecode::new().opcode(opcode).opcode(OpCode::POP)
        }
        (Mode::MinStack, InstructionCategory::Unop) => {
            // DUP1 NOT NOT ... POP
            Bytecode::new()
                .opcode(OpCode::DUP1)
                .append_bc(STACK_LIMIT * Bytecode::new().opcode(opcode).opcode(opcode))
                .opcode(OpCode::POP)
        }
        (Mode::MinStack, InstructionCategory::Binop) => {
            // DUP1 DUP1 ADD DUP1 ADD DUP1 ADD ... POP
            Bytecode::new()
                .opcode(OpCode::DUP1)
                .append_bc((STACK_LIMIT - 1) * Bytecode::new().opcode(OpCode::DUP1).opcode(opcode))
                .opcode(OpCode::POP)
        }
        (Mode::MinStack, InstructionCategory::Push) => {
            // PUSH1 POP PUSH1 POP ...
            STACK_LIMIT
                * (Bytecode::new()
                    .pushb(vec![0; (opcode.0 - OpCode::PUSH1.0) as usize + 1])
                    .opcode(OpCode::POP))
        }
        (Mode::MinStack, InstructionCategory::Dup) => {
            // The required n stack height for DUPn is provided by
            // duplicating the loop counter n-1 times with DUP1.
            let n = opcode.to_usize() - OpCode::DUP1.to_usize() + 1;
            // DUP1 ...  DUPn POP DUPn POP ...  POP ...
            // \ n-1  /                         \ n-1 /
            (n - 1) * Bytecode::from(OpCode::DUP1) +     // Required n stack height.
                (STACK_LIMIT - (n - 1)) *                //
                (Bytecode::from(opcode) + OpCode::POP) + // Multiple DUPn POP pairs.
                (n - 1) * Bytecode::from(OpCode::POP) // Pop initially duplicated values.
        }
        (Mode::MinStack, InstructionCategory::Swap) => {
            // The required n+1 stack height for SWAPn is provided by duplicating the loop counter
            // n times with DUP1. This also guarantees the loop counter remains unchanged because
            // it is always going to be swapped to the same value.
            let n = opcode.to_usize() - OpCode::SWAP1.to_usize() + 1;
            // DUP1 ...  SWAPn SWAPn ...  POP ...
            // \  n   /                   \  n  /
            n * Bytecode::from(OpCode::DUP1) +                // Required n+1 stack height.
                   STACK_LIMIT * 2 * Bytecode::from(opcode) + // Multiple SWAPns.
                   n * Bytecode::from(OpCode::POP) // Pop initially duplicated values.
        }
        (Mode::FullStack, InstructionCategory::Nullop) => {
            // CALLER CALLER ... POP POP ...
            STACK_LIMIT * Bytecode::from(opcode) + STACK_LIMIT * Bytecode::from(OpCode::POP)
        }
        (Mode::FullStack, InstructionCategory::Binop) => {
            // DUP1 DUP1 DUP1 ... ADD ADD ADD ... POP
            STACK_LIMIT * Bytecode::from(OpCode::DUP1)
                + (STACK_LIMIT - 1) * Bytecode::new().opcode(opcode).opcode(OpCode::POP)
        }
        (Mode::FullStack, InstructionCategory::Push) => {
            // PUSH1 PUSH1 PUSH1 ... POP POP POP ...
            STACK_LIMIT * Bytecode::new().pushb(vec![0; (opcode.0 - OpCode::PUSH1.0) as usize + 1])
                + STACK_LIMIT * Bytecode::from(OpCode::POP)
        }
        (Mode::FullStack, InstructionCategory::Dup) => {
            // The required initial n stack height for DUPn is provided by
            // duplicating the loop counter n-1 times with DUP1.
            let n = opcode.to_usize() - OpCode::DUP1.to_usize() + 1;
            // DUP1 ...  DUPn DUPn ...  POP POP ...
            // \ n-1  /  \  S-(n-1)  /  \    S    /
            (n - 1) * Bytecode::from(OpCode::DUP1) +                   // Required n stack height.
                   (STACK_LIMIT - (n - 1)) * Bytecode::from(opcode) +  // Fill the stack with DUPn.
                   STACK_LIMIT * Bytecode::from(OpCode::POP) // Clear whole stack.
        }
        _ => Bytecode::new(),
    }
}

/// Generates a benchmark loop with given inner code.
///
/// This generates do-while loop with 255 iterations and it starts with PUSH1 of 255 as the loop
/// counter. The while check is done as `(counter += -1) != 0`. The SUB is avoided because it
/// consumes arguments in unnatural order and additional SWAP would be required.
///
/// The loop counter stays on the stack top. The inner code is allowed to duplicate it, but must not
/// modify it.
fn generate_loop_v1(inner_code: Bytecode) -> Bytecode {
    let counter = Bytecode::new().pushv(255);
    let jumpdest_offset = counter.len();
    Bytecode::new()
        .append_bc(counter)
        // loop label + inner code
        .opcode(OpCode::JUMPDEST)
        .append_bc(inner_code)
        // -1
        .pushb(hex!(
            "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
        ))
        // counter += (-1)
        .opcode(OpCode::ADD)
        .opcode(OpCode::DUP1)
        .pushv(jumpdest_offset)
        // jump to jumpdest_offset if counter != 0
        .opcode(OpCode::JUMPI)
}

/// Generates a benchmark loop with given inner code.
///
/// This is improved variant of v1. It has exactly the same instructions and consumes the same
/// amount of gas, but according to performed benchmarks (see "loop_v1" and "loop_v2") it runs
/// faster. And we want the lowest possible loop overhead.
/// The change is to set the loop counter to -255 and check `(counter += 1) != 0`.
fn generate_loop_v2(inner_code: Bytecode) -> Bytecode {
    // -255
    let counter = Bytecode::new().pushb(hex!(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff01"
    ));
    let jumpdest_offset = counter.len();
    Bytecode::new()
        .append_bc(counter)
        // loop label + inner code
        .opcode(OpCode::JUMPDEST)
        .append_bc(inner_code)
        // counter += 1
        .pushv(1)
        .opcode(OpCode::ADD)
        .opcode(OpCode::DUP1)
        // jump to jumpdest_offset if counter != 0
        .pushv(jumpdest_offset)
        .opcode(OpCode::JUMPI)
}

fn generate_code(params: &CodeParams) -> Bytecode {
    generate_loop_v2(generate_loop_inner_code(params))
}

#[inline]
fn execute(
    (code, mut host, msg, rev): (AnalyzedCode, MockedHost, InterpreterMessage, Revision),
) -> Output {
    code.execute(&mut host, &msg, rev)
}

fn synthetic_benchmarks(c: &mut Criterion) {
    let mut params_list = vec![];

    // Nops & unops.
    for opcode in [OpCode::JUMPDEST, OpCode::ISZERO, OpCode::NOT] {
        params_list.push(CodeParams {
            opcode,
            mode: Mode::MinStack,
        });
    }

    // Binops.
    for opcode in [
        OpCode::ADD,
        OpCode::MUL,
        OpCode::SUB,
        OpCode::SIGNEXTEND,
        OpCode::LT,
        OpCode::GT,
        OpCode::SLT,
        OpCode::SGT,
        OpCode::EQ,
        OpCode::AND,
        OpCode::OR,
        OpCode::XOR,
        OpCode::BYTE,
        OpCode::SHL,
        OpCode::SHR,
        OpCode::SAR,
    ] {
        for mode in [Mode::MinStack, Mode::FullStack] {
            params_list.push(CodeParams { opcode, mode });
        }
    }

    // Nullops.
    for opcode in [
        OpCode::ADDRESS,
        OpCode::CALLER,
        OpCode::CALLVALUE,
        OpCode::CALLDATASIZE,
        OpCode::CODESIZE,
        OpCode::RETURNDATASIZE,
        OpCode::PC,
        OpCode::MSIZE,
        OpCode::GAS,
    ] {
        for mode in [Mode::MinStack, Mode::FullStack] {
            params_list.push(CodeParams { opcode, mode });
        }
    }

    // PUSH.
    for opcode in OpCode::PUSH1.to_u8()..OpCode::PUSH32.to_u8() {
        for mode in [Mode::MinStack, Mode::FullStack] {
            params_list.push(CodeParams {
                opcode: OpCode(opcode),
                mode,
            });
        }
    }

    // SWAP.
    for opcode in OpCode::SWAP1.to_u8()..OpCode::SWAP16.to_u8() {
        params_list.push(CodeParams {
            opcode: OpCode(opcode),
            mode: Mode::MinStack,
        });
    }

    // DUP.
    for opcode in OpCode::DUP1.to_u8()..OpCode::DUP16.to_u8() {
        for mode in [Mode::MinStack, Mode::FullStack] {
            params_list.push(CodeParams {
                opcode: OpCode(opcode),
                mode,
            });
        }
    }

    c.bench_function("/total/synth/loop_v1", |b| {
        b.iter_batched(
            || {
                prepare(
                    AnalyzedCode::analyze(&generate_loop_v1(Bytecode::new()).build()),
                    Bytes::new(),
                )
            },
            execute,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("/total/synth/loop_v2", |b| {
        b.iter_batched(
            || {
                prepare(
                    AnalyzedCode::analyze(&generate_loop_v2(Bytecode::new()).build()),
                    Bytes::new(),
                )
            },
            execute,
            BatchSize::SmallInput,
        )
    });

    for params in params_list {
        c.bench_function(
            &format!(
                "/total/synth/{}/{}{}",
                params.opcode,
                InstructionCategory::from_opcode(params.opcode),
                params.mode as usize
            ),
            |b| {
                b.iter_batched(
                    || {
                        prepare(
                            AnalyzedCode::analyze(&generate_code(&params).build()),
                            Bytes::new(),
                        )
                    },
                    execute,
                    BatchSize::SmallInput,
                )
            },
        );
    }
}

fn prepare(
    code: AnalyzedCode,
    input_data: Bytes,
) -> (AnalyzedCode, MockedHost, InterpreterMessage, Revision) {
    get_instruction_table(Revision::Istanbul);
    (
        code,
        MockedHost::default(),
        InterpreterMessage {
            kind: CallKind::Call,
            gas: i64::MAX,
            is_static: false,
            depth: 0,
            recipient: Address::zero(),
            code_address: Address::zero(),
            sender: Address::zero(),
            input_data,
            value: U256::ZERO,
        },
        Revision::Istanbul,
    )
}

fn main_benchmarks(c: &mut Criterion) {
    #[derive(Deserialize)]
    struct Input {
        x: Option<usize>,
        c: String,
    }

    #[derive(Deserialize)]
    struct BenchParams {
        args: Vec<Input>,
        out: String,
    }

    for (name, code, args) in [
        (
            "snailtracer",
            include_str!("inputs/benchmarks/main/snailtracer.evm"),
            include_str!("inputs/benchmarks/main/snailtracer.toml"),
        ),
        (
            "blake2b_huff",
            include_str!("inputs/benchmarks/main/blake2b_huff.evm"),
            include_str!("inputs/benchmarks/main/blake2b_huff.toml"),
        ),
        (
            "blake2b_shifts",
            include_str!("inputs/benchmarks/main/blake2b_shifts.evm"),
            include_str!("inputs/benchmarks/main/blake2b_shifts.toml"),
        ),
        (
            "sha1_divs",
            include_str!("inputs/benchmarks/main/sha1_divs.evm"),
            include_str!("inputs/benchmarks/main/sha1_divs.toml"),
        ),
        (
            "sha1_shifts",
            include_str!("inputs/benchmarks/main/sha1_shifts.evm"),
            include_str!("inputs/benchmarks/main/sha1_shifts.toml"),
        ),
        (
            "weierstrudel",
            include_str!("inputs/benchmarks/main/weierstrudel.evm"),
            include_str!("inputs/benchmarks/main/weierstrudel.toml"),
        ),
    ] {
        let code = hex::decode(code).unwrap();

        let bench_vars = toml::from_str::<toml::value::Table>(args).unwrap();
        for (bench, params) in bench_vars {
            let params = params.try_into::<BenchParams>().unwrap();
            let mut input_data = Vec::new();
            for arg in params.args {
                let b = hex::decode(arg.c).unwrap();
                for _ in 0..arg.x.unwrap_or(1) {
                    input_data.extend_from_slice(&b);
                }
            }

            let expected_output = hex::decode(&params.out).unwrap();
            let analyzed_code = AnalyzedCode::analyze(&*code);
            let input_data = Bytes::from(input_data);

            let res = execute(prepare(analyzed_code.clone(), input_data.clone()));

            assert_eq!(res.status_code, StatusCode::Success);
            assert_eq!(res.output_data, expected_output);

            c.bench_function(&format!("{}/{}", name, bench), move |b| {
                let analyzed_code = analyzed_code.clone();
                let input_data = input_data.clone();
                b.iter_batched(
                    move || prepare(analyzed_code.clone(), input_data.clone()),
                    execute,
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default().with_profiler(FlamegraphProfiler::new(100));
    targets = main_benchmarks, synthetic_benchmarks
);

criterion_main!(benches);
