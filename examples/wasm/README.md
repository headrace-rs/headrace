# wasm transform example

A minimal `wasm` transform authored with `headrace-wasm-guest`: it doubles every record's `value`.
The built module doubles as the host's test fixture.

This crate is excluded from the workspace because it targets `wasm32-unknown-unknown`, not the host.

## Build and refresh the fixture

```sh
rustup target add wasm32-unknown-unknown   # once
cargo build --release --target wasm32-unknown-unknown --manifest-path examples/wasm/Cargo.toml
cp examples/wasm/target/wasm32-unknown-unknown/release/headrace_wasm_example_double.wasm \
   crates/headrace-core/tests/fixtures/double.wasm
```

The host test `transform::wasm::tests::sdk_built_module_doubles_value` loads that `.wasm`, so
`cargo test` needs no wasm toolchain - only a refresh does.
