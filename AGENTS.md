# AGENTS.md

## Project Overview

Type-safe QuestDB client library for TypeScript. Bun monorepo managed by Turborepo.

**Structure:**
- `packages/questdb-typesafe-client/` — main library (source, tests, build)
- `samples/basic-usage/` — example usage of the library

**Runtime:** Bun (v1.3.9+)
**Package manager:** Bun (lockfile: `bun.lock`)
**Monorepo orchestrator:** Turborepo

---

## Build Commands

```bash
# Install dependencies
bun install

# Build all packages (via Turborepo)
bun run build

# Build only the library
bun run build              # from packages/questdb-typesafe-client/
bun run build:js           # bundle JS only (bun build → ESM)
bun run build:types        # emit .d.ts only (tsc)
```

## Lint Commands

```bash
# Lint all packages (via Turborepo)
bun run lint

# Lint library only — runs tsc --noEmit
bun run lint               # from packages/questdb-typesafe-client/
```

There is no ESLint, Biome, or Prettier. Type-checking via `tsc --noEmit` is the lint step.

## Formatting

The project uses **oxfmt** (oxc-based formatter) with default settings.

```bash
bun oxfmt <file>
```

## Test Commands

Tests use **Bun's built-in test runner** (`bun:test`). No separate config file.

```bash
# Run ALL tests (via Turborepo)
bun run test

# Run library tests only
bun test                   # from packages/questdb-typesafe-client/

# Run a SINGLE test file
bun test packages/questdb-typesafe-client/__tests__/escape.test.ts

# Run tests matching a file name pattern
bun test --filter "escape"

# Run a specific test by name
bun test --filter "escapeIdentifier"
```

Test files live in `packages/questdb-typesafe-client/__tests__/*.test.ts` and
`samples/basic-usage/__tests__/*.test.ts`.

---

## Code Style Guidelines

### File Naming

- **Source files:** `kebab-case.ts` (e.g., `column-builder.ts`, `query-builder.test.ts`)
- **Test files:** `<name>.test.ts` inside `__tests__/` directories

### Naming Conventions

| Element            | Convention         | Examples                                      |
| ------------------ | ------------------ | --------------------------------------------- |
| Classes            | PascalCase         | `QuestDBClient`, `SelectBuilder`, `Table`     |
| Interfaces / Types | PascalCase         | `QColumn`, `SqlExpr`, `ColumnMeta`, `InferRow`|
| Type parameters    | `T`-prefixed       | `TDef`, `TResult`, `TType`, `TRightAlias`    |
| Functions          | camelCase          | `defineTable`, `escapeIdentifier`             |
| Constants          | camelCase          | `const q = { ... }`, `const fn = { ... }`    |
| Internal fields    | `_` prefix         | `_brand`, `_node`, `_client`, `_def`          |

### Imports

- **Explicit `.ts` extensions** on all relative imports (required by `verbatimModuleSyntax`):
  ```typescript
  import { escapeIdentifier } from "../sql/escape.ts";
  ```
- **`import type` is mandatory** for type-only imports (enforced by `verbatimModuleSyntax`):
  ```typescript
  import type { QuestDBClientConfig } from "./config.ts";
  ```
- External packages use bare specifiers: `import { z } from "zod/v4";`
- Tests import from relative `../src/` paths, not the package name.

### Formatting Rules

- **Semicolons:** always
- **Quotes:** double quotes
- **Indentation:** 2 spaces
- **Trailing commas:** yes
- **Nullish coalescing:** prefer `??` over `||`
- **Numeric separators:** use underscores for readability (`30_000`, `100_000`)

### TypeScript Patterns

- **Strict mode** is enabled with these additional checks:
  - `noFallthroughCasesInSwitch`, `noUncheckedIndexedAccess`, `noImplicitOverride`
- **Branded types** for domain objects (`_brand: "TableDef"`, `_brand: "QColumn"`)
- **Const type parameters** for literal inference (`const TName extends string`)
- **Discriminated unions** for SQL AST nodes (`kind: "column" | "literal" | ...`)
- **Zod v4** schemas on column definitions for runtime validation (import from `"zod/v4"`)
- **`as const` assertions** for branding and literal types

### Error Handling

- **Custom error classes** extend `Error`:
  - `QuestDBError` — HTTP errors from QuestDB (includes `status`, `questdbMessage`, `position`, `sql`)
  - `QuestDBConnectionError` — network/timeout failures (includes `cause`)
- **Retry logic:** exponential backoff on 5xx/network errors; no retry on 4xx
- **Validation errors:** thrown as plain `Error` with descriptive messages
- Cast unknown errors with `error as Error`

### Code Organization

- **Section banners** to separate logical blocks:
  ```typescript
  // ---------------------------------------------------------------------------
  // Section Name
  // ---------------------------------------------------------------------------
  ```
- **JSDoc comments** on all public APIs, with usage examples in code blocks
- **`/** @internal */`** on internal members (paired with `_` prefix)
- **Fluent builder pattern** — query builders return `this` or new instances for chaining
- **Barrel exports** — single `index.ts` re-exports everything by category
- **Immutable vs mutable builders:**
  - `SelectBuilder` uses `clone()` (immutable)
  - `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder` mutate `this`

### Testing Patterns

- Use `describe` / `test` / `expect` from `bun:test`
- **Mocking:** inject a fake `fetch` via `QuestDBClientConfig.fetch`
- **Type-level tests:** use `assertType<T>(value)` helper and `@ts-expect-error` for negative cases
- All test data is inline (no fixture files)

### Dependencies

- **Runtime:** `zod` (v4, imported as `"zod/v4"`)
- **Peer:** `zod ^4.0.0`, `typescript` (beta)
- **Dev:** `@types/bun`
