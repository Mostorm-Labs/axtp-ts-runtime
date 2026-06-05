# AXTP Node Runtime

This repository contains the AXTP Node.js and TypeScript runtime extracted from
the AXTP specification repository.

The runtime layout is intentionally kept as copied:

```text
src/
package.json
pnpm-lock.yaml
pnpm-workspace.yaml
tsconfig.json
vitest.config.ts
```

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
scripts/check-axtp-spec-lock.sh
```

## Local Generator

This repository maintains its own generator under `generators/`.

```bash
export AXTP_SPEC_PATH=/path/to/axtp
pnpm --dir generators install
pnpm --dir generators build
pnpm --dir generators test
pnpm --dir generators generate:runtime
```

Generated TypeScript artifacts are written to `src/generated/`.

To move to a later released spec tag:

```bash
scripts/upgrade-axtp-spec.sh spec/v0.1.0
```
