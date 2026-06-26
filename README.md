# AXTP TypeScript Runtime

AXTP TypeScript Runtime provides protocol runtime and SDK utilities for AXTP
clients and tools.

It is written in TypeScript and can support multiple JavaScript environments,
including:

- Browser WebSocket clients
- Node.js adapters
- CLI tools
- Mock clients
- Electron or web management applications

This repository is not the AXTP Spec source of truth. Protocol documents,
registries, schemas, domain specs, flow specs, and conformance cases are
maintained in the AXTP main specification repository.

## Architecture

The repository is layered in the same direction as the C++ runtime: protocol
facts come from the locked AXTP spec, low-level byte and wire helpers sit below
the runtime core, and SDK/transports stay above the core contracts.

```text
devtools/generators -> src/core/protocol/generated
src/core/support/io -> src/core/protocol -> src/core/runtime -> src/sdk
src/core/runtime/transport -> src/transports
src/core/runtime -> src/json_rpc
```

## Repository Layout

| Path                           | Purpose                                                                                      |
| ------------------------------ | -------------------------------------------------------------------------------------------- |
| `src/index.ts`                 | Stable package entry that re-exports the public runtime, protocol, SDK, and generated types. |
| `src/node.ts`                  | Node-specific package entry for optional Node transports.                                    |
| `src/core/support/io/`         | Byte helpers, readers/writers, sinks, and CRC utilities.                                     |
| `src/core/protocol/model/`     | Protocol value types, payload helpers, frame/message models, and constants.                  |
| `src/core/protocol/generated/` | Generated AXTP IDs, registries, and generated version constants. Do not edit by hand.        |
| `src/core/protocol/wire/`      | Framed binary and WebSocket JSON-RPC payload codecs plus inbound/outbound processors.        |
| `src/core/runtime/core/`       | `AxtpCore`, core events, session state, and request/response coordination.                   |
| `src/core/runtime/broker/`     | `BasicBroker`, business routing, task/result queues, and handler contracts.                  |
| `src/core/runtime/endpoint/`   | Endpoint glue between core, broker, and transports.                                          |
| `src/core/runtime/transport/`  | Transport profile and `ITransport` contracts plus mock transport.                            |
| `src/sdk/`                     | Higher-level client/server SDK APIs.                                                         |
| `src/json_rpc/`                | Optional WebSocket JSON-RPC adapter layer above the runtime.                                 |
| `src/transports/`              | Optional concrete transports, currently Node TCP.                                            |
| `tests/`                       | Unit and integration tests for the runtime layers.                                           |
| `devtools/generators/`         | TypeScript generator that consumes the AXTP spec and emits runtime artifacts.                |
| `devtools/scripts/`            | Spec lock, generation, versioning, conformance, and release helper scripts.                  |
| `devtools/conformance/`        | Runtime conformance profile and Vitest conformance runner.                                   |

## AXTP Spec Compatibility

This runtime implements AXTP Spec from the AXTP main specification repository.

See `AXTP_SPEC.lock.yaml` for:

- AXTP Spec repository
- Spec tag
- Spec version
- Source commit
- Compatibility range

Runtime code must not redefine AXTP protocol semantics. Protocol documents,
registries, schemas, business domains, business flows, and conformance cases are
maintained in the AXTP spec repository.

## AXTP Spec Dependency

Use `AXTP_SPEC_PATH` to point local tooling to a checked out AXTP spec
repository:

```bash
export AXTP_SPEC_PATH=/path/to/axtp
```

The checkout should match the tag and commit recorded in
`AXTP_SPEC.lock.yaml`. Do not depend on the `main` branch for reproducible
runtime builds.

If a package dependency is added later, pin it to a released `spec/vX.Y.Z` tag
or explicit commit, not to `main`.

## Build And Test

```bash
pnpm install
pnpm build
pnpm test
```

## Spec Lock Checks

```bash
devtools/scripts/check-axtp-spec-lock.sh
```

## AXTP Spec Upgrade

This runtime follows AXTP Spec via `AXTP_SPEC.lock.yaml`.

To upgrade:

```bash
devtools/scripts/upgrade-axtp-spec.sh spec/v0.3.0
devtools/scripts/check-axtp-spec-lock.sh
```

After upgrading, run generator checks, TypeScript build/tests, and the
conformance runner before merging.

## Conformance

Conformance cases are owned by the AXTP spec repository. Point the runner at the
locked spec checkout and run:

```bash
AXTP_SPEC_PATH=/path/to/axtp devtools/scripts/run-conformance.sh
```

The runner writes `conformance-results/result.json`. Required failures exit
nonzero. Optional cases are reported as skipped or passed unless
`CONFORMANCE_STRICT_OPTIONAL=true`; upgrade PR workflows may temporarily use
`CONFORMANCE_ALLOW_INCOMPLETE=true`.

## Automated AXTP Spec Upgrade

This repository is automatically upgraded when the AXTP Spec repository publishes a tag like `spec/vX.Y.Z`.

Automation flow:

1. Receive `axtp_spec_released` repository dispatch.
2. Update `AXTP_SPEC.lock.yaml`.
3. Set runtime/tool release version to `X.Y.Z.0`.
4. Generate code and `generated/axtp_generated_manifest.json`.
5. Open an Upgrade PR.
6. Auto-merge the PR after checks pass.
7. Create tag `vX.Y.Z.0`.
8. Create a GitHub Release.

AXTP Spec tag: `spec/vX.Y.Z`

Runtime/tool tag: `vX.Y.Z.0`

Repository settings must allow GitHub Actions to create PRs, enable auto-merge, create tags, and create releases. Configure `AXTP_RUNTIME_AUTOMATION_TOKEN` when PR-created-by-actions workflows must trigger downstream pull_request checks.

## Local Generator

This repository maintains its own generator under `devtools/generators/`.

```bash
export AXTP_SPEC_PATH=/path/to/axtp
pnpm --dir devtools/generators install
pnpm --dir devtools/generators build
pnpm --dir devtools/generators test
pnpm --dir devtools/generators generate:runtime
```

Generated TypeScript artifacts are written to `src/core/protocol/generated/`.

To move to a later released spec tag:

```bash
devtools/scripts/upgrade-axtp-spec.sh spec/v0.1.0
```

## Versioning

This repository keeps AXTP Spec, runtime, and generated artifact versions
separate:

- AXTP Spec tags use `spec/vX.Y.Z` and are recorded in `AXTP_SPEC.lock.yaml`.
- Runtime releases use `vX.Y.Z.R`, with `R=0` for the first release from a spec tag.
- Generated artifact metadata is recorded in `generated/axtp_generated_manifest.json`.

Use `devtools/scripts/check-generated-version.sh` to verify that the lock file,
generated manifest, runtime version, and generated constants are aligned.

See `docs/generator/GENERATED_VERSIONING.md` for generator versioning details.

## Release

Runtime releases are created from runtime tags:

- Runtime tags: `vX.Y.Z.R`
- AXTP Spec tags: `spec/vX.Y.Z`

AXTP Spec updates create automated upgrade PRs. After checks pass, the PR is auto-merged; the main branch workflow then creates the matching `vX.Y.Z.0` runtime/tool tag, and that tag triggers the GitHub Release.

Each release records runtime version, AXTP Spec tag, AXTP Spec commit, generator
version, and the generated manifest.
