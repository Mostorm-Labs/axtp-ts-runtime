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

The runtime is layered from the locked AXTP spec downward: generated protocol
facts feed the codec/model layer, byte helpers sit below the protocol layer, and
the connection / session / SDK layers build on top. Transport implementations sit
behind a single `ITransport` contract, so Node TCP/WS, mock, and custom transports
are interchangeable.

```text
devtools/generators -> src/protocol/generated
src/io -> src/protocol -> src/connection -> src/session -> src/sdk
src/transport  <-  src/connection              (transport contract + implementations)
src/connection -> src/protocol/codec/jsonRpc   (WebSocket unframed-JSON path)
```

The package exposes several entry points (see `package.json` `exports`). The main
entry `@axtp/ts-sdk` re-exports everything for backward compatibility; the
subpaths `./node`, `./protocol`, `./transport`, `./mock`, and `./io` let consumers
import only what they need and keep browser builds free of `node:net` / `ws`.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `src/index.ts` | Main entry: stable SDK API plus re-export of every subpath (backward compatible). |
| `src/node.ts` | `./node` subpath: Node TCP + WebSocket transports. |
| `src/protocol.ts` | `./protocol` subpath: payload model, constants, and factories. |
| `src/transport.ts` | `./transport` subpath: custom `ITransport` contracts and capability factories. |
| `src/mock.ts` | `./mock` subpath: mock transports for tests. |
| `src/io.ts` | `./io` subpath: `Bytes` type and byte helpers. |
| `src/io/` | `Bytes` type, byte helpers, `ByteReader` / `ByteWriter`, CRC16 utilities. |
| `src/protocol/` | Protocol layer: `model.ts`, `codec/` (frame / control / stream / payload / jsonRpc), and `generated/` (AXTP IDs, registries, version — do not edit by hand). |
| `src/connection/` | Connection layer: `Connection`, `Heartbeat`, `reconnect/`, and `codec/` (`CodecPipeline`, `ControlSession` link state machine). |
| `src/session/` | Session layer: `AxtpSession`, `handshake/`, `rpc/`, `stream/`, `handler/`. |
| `src/transport/` | Transport implementations: `tcp/`, `ws/`, `mock/`. |
| `src/sdk/` | Higher-level `AxtpClient` and `AxtpServer` SDK APIs. |
| `src/types/` | Shared types: `registry` (method/event single source of truth), `error`, `events`. |
| `tests/` | Unit, integration, and public-export snapshot tests (Vitest). |
| `devtools/conformance/` | AXTP conformance cases run against the locked spec; emits `conformance-results/result.json` via `run-conformance.sh` / `pnpm test:conformance`. |
| `devtools/generators/` | TypeScript generator (`axtp-gen`) that consumes the AXTP spec and emits runtime artifacts. |
| `devtools/scripts/` | Spec lock, generation, versioning, conformance, and release helper scripts. |

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

Generated TypeScript artifacts are written to `src/protocol/generated/`.

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

## Installation

`@axtp/ts-sdk` is published to a private Verdaccio registry. Consumers must
configure registry access before installing. Add the following to a project-level
or user-level `.npmrc`:

```ini
@axtp:registry=https://<verdaccio-host>/
//<verdaccio-host>/:_authToken=<token>
```

Then install:

```bash
pnpm add @axtp/ts-sdk
```

The published version follows the runtime release version derived from the tag
`vX.Y.Z.R`: a spec upgrade publishes `X.Y.Z` (revision `0`), while a runtime-only
revision publishes the semver pre-release `X.Y.Z-runtime.R`. The package exposes
the subpath entry points listed under `exports` (`@axtp/ts-sdk`,
`@axtp/ts-sdk/node`, `/protocol`, `/transport`, `/mock`, `/io`).
