#!/usr/bin/env node
// sync-docs.mjs：把 generator 的 protocol 快照复制到发布包 docs/ 下。
// 由根 `pnpm build` 调用。快照（devtools/generators/src/__snapshots__/protocol.generated.md）
// 是唯一来源，docs/protocol.md 为构建产物（gitignored，靠 package.json 的 files 白名单进 npm 包）。

import { copyFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDir, "../..");

const source = path.join(
  root,
  "devtools/generators/src/__snapshots__/protocol.generated.md"
);
const dest = path.join(root, "docs/protocol.md");

try {
  await mkdir(path.dirname(dest), { recursive: true });
  await copyFile(source, dest);
  console.log(
    `[sync-docs] copied protocol reference -> ${path.relative(root, dest)}`
  );
} catch (err) {
  console.error(
    `[sync-docs] failed to copy ${path.relative(root, source)} -> ${path.relative(root, dest)}: ${
      err instanceof Error ? err.message : err
    }`
  );
  console.error(
    "[sync-docs] hint: the snapshot is committed under devtools/generators/src/__snapshots__; run `pnpm --dir devtools/generators test` to regenerate it."
  );
  process.exit(1);
}
