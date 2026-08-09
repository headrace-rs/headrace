# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately** through GitHub's
[private vulnerability reporting](https://github.com/headrace-rs/headrace/security/advisories/new)
(the repository's **Security -> Report a vulnerability** tab). We aim to acknowledge a report
within a few days and will keep you updated as we work on a fix. Please do not open a public
issue for a security problem.

## Supported versions

Headrace is pre-1.0 and under active development. Security fixes land on the latest release;
pin a version and upgrade to pick them up.

## Scope: the inspection API

`headrace run --inspect-addr <addr>` serves a read-only gRPC state API that is **unauthenticated
by design**, for local debugging. It is off by default and must be bound to a trusted network
(localhost or a debug sidecar), never the public data path - see
[State inspection](https://headrace.rs/docs/state-inspection). Exposing it publicly is a
deployment mistake, not a Headrace vulnerability.
