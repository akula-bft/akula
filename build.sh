#!/usr/bin/env bash

export RUST_BACKTRACE=1
#export CARGO_HTTP_DEBUG=1

ARGS="--all --profile=production"
if [ "$1" == "tracing" ] || [ "$1" == "trace" ]
then
	shift
	ARGS="$ARGS --features=console"
	RUSTFLAGS="--cfg tokio_unstable $RUSTFLAGS"
	export RUST_LOG=trace
else
	export RUST_LOG=debug
fi

echo "RUST_LOG $RUST_LOG"
echo "RUST_BACKTRACE $RUST_BACKTRACE"
echo "RUSTFLAGS=$RUSTFLAGS"

echo "*** Running: cargo build $ARGS"
cargo build $ARGS
echo "cargo returned $?"
