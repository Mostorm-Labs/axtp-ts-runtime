// ESLint flat config（ESM）。项目为 "type": "module"，须用 flat config 而非 .eslintrc。
// 组合：@eslint/js recommended + typescript-eslint recommended（类型感知）+ prettier（关闭冲突规则）。
// ESLint 只管代码质量，prettier 管格式，二者零冲突。

import js from "@eslint/js";
import tseslint from "typescript-eslint";
import prettierConfig from "eslint-config-prettier";

export default tseslint.config(
  // 全局忽略（等价旧 .eslintignore，与 .prettierignore 保持一致）
  {
    ignores: [
      "dist/**",
      "node_modules/**",
      "third_party/**",
      "devtools/**",
      "src/protocol/generated/**",
      "generated/**"
    ]
  },

  // 基础推荐规则
  js.configs.recommended,

  // TS 推荐（非类型感知基线；类型感知在下方项目覆盖里按需开启）
  ...tseslint.configs.recommended,

  // 关闭与 prettier 冲突的格式化规则（最后加载，确保覆盖）
  prettierConfig,

  // 项目覆盖：src + tests 都用 TS 规则，开启类型感知
  {
    files: ["src/**/*.ts", "tests/**/*.ts"],
    languageOptions: {
      ecmaVersion: 2022,
      sourceType: "module",
      parserOptions: {
        // 用 tsconfig.test.json：它 include 了 src + tests，单一 project 覆盖所有被 lint 文件，
        // 避免 projectService 在 src（tsconfig.json）与 tests（无匹配 project）间分裂。
        project: "tsconfig.test.json",
        tsconfigRootDir: import.meta.dirname
      }
    },
    rules: {
      // 边界/生成产物周边用 any 合理，提示但不阻断
      "@typescript-eslint/no-explicit-any": "warn",
      // 未使用变量报错，但允许下划线前缀（_ctx / _params 等占位）
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" }
      ],
      // 强制 import type，匹配现有大量 import type 用法
      "@typescript-eslint/consistent-type-imports": "error",
      // SDK 内部有合理 console（日志）
      "no-console": "off",
      // 现状测试/边界有少量 !，先 warn 不阻断
      "@typescript-eslint/no-non-null-assertion": "warn"
    }
  }
);
