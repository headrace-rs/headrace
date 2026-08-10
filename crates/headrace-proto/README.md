# headrace-proto

[![crates.io](https://img.shields.io/crates/v/headrace-proto?style=flat-square)](https://crates.io/crates/headrace-proto)
[![docs.rs](https://img.shields.io/docsrs/headrace-proto?style=flat-square)](https://docs.rs/headrace-proto)

Checked-in gRPC/protobuf stubs for [Headrace](https://headrace.rs)'s admin services (the
state-inspection API). A support crate for the Headrace workspace; you probably want the
[`headrace`](https://crates.io/crates/headrace) binary or
[`headrace-core`](https://crates.io/crates/headrace-core).

The stubs are committed so building Headrace needs no `protoc`; regenerate them with the
`headrace-proto-gen` dev tool.

## License

Apache-2.0.
