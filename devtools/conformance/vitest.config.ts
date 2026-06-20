import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["devtools/conformance/**/*.test.ts"]
  }
});
