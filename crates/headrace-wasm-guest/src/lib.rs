//! Author a Headrace `wasm` transform in Rust. Write an `fn(Record) -> Vec<Record>` and annotate
//! it with [`macro@transform`]; the macro emits the ABI (ADR-0018) - `alloc`/`dealloc`/`transform`
//! over linear memory, with a `Record` crossing as MessagePack. Build the crate as a `cdylib` for
//! `wasm32-unknown-unknown` and point a `wasm` node at the resulting `.wasm`.
//!
//! ```ignore
//! use headrace_wasm_guest::{transform, Record};
//!
//! #[transform]
//! fn double(mut rec: Record) -> Vec<Record> {
//!     rec.value *= 2.0;
//!     vec![rec]
//! }
//! ```
//!
//! Your function is ordinary safe Rust; the one `unsafe` block the ABI needs (raw pointers at the
//! host boundary) lives in this crate's [`__run`], not in your module.

pub use headrace_record::{ABI_VERSION, AttrValue, Attrs, Record};

/// Annotate an `fn(Record) -> Vec<Record>` to export it as the module's transform.
pub use headrace_wasm_macro::transform;

// The codec is part of the ABI; re-export it so the generated code always matches the SDK version.
#[doc(hidden)]
pub use rmp_serde;

/// ABI-internal: allocate `len` bytes for the host to write a record into. Call only via
/// [`macro@transform`].
#[doc(hidden)]
pub fn __alloc(len: u32) -> u32 {
    let layout = std::alloc::Layout::from_size_align(len.max(1) as usize, 1).expect("valid layout");
    // SAFETY: `layout` has a non-zero size (`max(1)`); the raw pointer is handed straight back to
    // the host, which writes exactly `len` bytes and later frees it via `__dealloc`.
    (unsafe { std::alloc::alloc(layout) }) as u32
}

/// ABI-internal: free a buffer previously returned by [`__alloc`].
#[doc(hidden)]
pub fn __dealloc(ptr: u32, len: u32) {
    // Guard the error sentinel (a zero pointer): it was never allocated.
    if ptr == 0 {
        return;
    }
    let layout = std::alloc::Layout::from_size_align(len.max(1) as usize, 1).expect("valid layout");
    // SAFETY: `ptr`/`len` come straight from a prior `__alloc`, so the layout matches its allocation.
    unsafe { std::alloc::dealloc(ptr as *mut u8, layout) };
}

/// ABI-internal: decode the input record, run `f`, and return the encoded output's `(ptr, len)`
/// packed into a `u64` (`ptr << 32 | len`). Returns 0 on a codec error, which the host meters as
/// an invalid record.
#[doc(hidden)]
pub fn __run(f: fn(Record) -> Vec<Record>, ptr: u32, len: u32) -> u64 {
    // SAFETY: the host wrote `len` bytes at `ptr` via a matching `__alloc` before calling us, and
    // does not touch them again until `transform` returns, so this slice is valid and exclusive.
    let input = unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) };
    let Ok(rec) = rmp_serde::from_slice::<Record>(input) else {
        return 0;
    };
    let Ok(bytes) = rmp_serde::to_vec(&f(rec)) else {
        return 0;
    };
    let out_len = bytes.len() as u32;
    let out_ptr = __alloc(out_len);
    // SAFETY: `__alloc(out_len)` just reserved `out_len` bytes at `out_ptr`, which does not overlap
    // `bytes`, so copying `bytes` into it is sound.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr as *mut u8, bytes.len()) };
    (u64::from(out_ptr) << 32) | u64::from(out_len)
}
