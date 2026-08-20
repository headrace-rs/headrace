//! The `#[transform]` attribute for `headrace-wasm-guest`. It wraps an `fn(Record) -> Vec<Record>`
//! and emits the wasm ABI exports (ADR-0018); see that crate for usage.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Export the annotated `fn(Record) -> Vec<Record>` as a Headrace wasm transform, emitting the
/// `alloc`/`dealloc`/`transform` ABI the host calls. Apply it once in a `cdylib` module.
#[proc_macro_attribute]
pub fn transform(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let name = &func.sig.ident;
    quote! {
        #func

        #[unsafe(no_mangle)]
        pub extern "C" fn alloc(len: u32) -> u32 {
            ::headrace_wasm_guest::__alloc(len)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn dealloc(ptr: u32, len: u32) {
            ::headrace_wasm_guest::__dealloc(ptr, len);
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn transform(ptr: u32, len: u32) -> u64 {
            ::headrace_wasm_guest::__run(#name, ptr, len)
        }

        #[unsafe(no_mangle)]
        pub extern "C" fn __headrace_abi_version() -> u32 {
            ::headrace_wasm_guest::ABI_VERSION
        }
    }
    .into()
}
