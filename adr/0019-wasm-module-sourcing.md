# 0019. WASM module sourcing: oci:// and file:// references

- Status: Accepted
- Date: 2026-08-19

## Context

ADR-0018 loads a `wasm` module from a local file path and defers remote sourcing. That path is
awkward to deploy: a Helm chart has to bake the `.wasm` into the image or mount it through a
ConfigMap or volume, separately from the pipeline config it belongs with. We want a module
reference a chart can set inline and that pulls the artifact at runtime.

## Decision

- **`module` is a URI with a scheme.**
  - `file://` (RFC 8089) for a local path; a bare path stays valid as a `file://` shorthand, so
    existing configs keep working.
  - `oci://<registry>/<repository>@sha256:<digest>` for a registry, following the OCI distribution
    spec's reference grammar (the same `oci://` convention Helm and ORAS use).
- **OCI references are digest-pinned.** An `oci://` reference must carry an `@sha256:` digest; the
  digest is the integrity check, so the node's `sha256:` field is redundant for `oci://` (it stays
  optional for `file://`). A bare tag is rejected - it is mutable, and running fetched code demands
  an immutable reference.
- **Behind a `wasm-oci` cargo feature.** The OCI client and its transitive dependencies compile
  only when a deployment needs registry pulls. The host pulls once at node start, verifies the
  digest, and caches by digest on disk (content-addressed), so a restart or a co-located node does
  not refetch.
- **Registry trust is explicit.** Pulls authenticate from the ambient credential chain and are
  restricted to an operator-configured allowlist of registries.

## Consequences

- Helm sets `module: oci://ghcr.io/org/mod@sha256:...` in the pipeline config - no image rebuild,
  no ConfigMap-mounted binary. This is the deployment ergonomics ADR-0018 left open.
- Running a fetched module is a supply-chain surface. It is bounded by the immutable digest pin, the
  registry allowlist, and authenticated pulls; the ADR-0018 sandbox still contains what runs.
- The `wasm-oci` feature adds an OCI client to the build; being opt-in keeps the default binary and
  the `file://` path lean.
- This extends ADR-0018's sourcing (local only); the sandbox, failure policy, and scope there are
  unchanged.
