<p align="center">
  <img src="media/library-icon.png" width="200" alt="questdb-typesafe-client" />
</p>

<h1 align="center">questdb-typesafe-client</h1>

<p align="center">
  Type-safe QuestDB client for TypeScript - schema definitions, query builders, and DDL with full type inference.
</p>

---

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [Documentation](#documentation)
- [License](#license)

## Features

- **Type-safe schema** — define tables with `defineTable` and get full TypeScript inference for rows, inserts, and updates
- **Query builders** — fluent, chainable SELECT / INSERT / UPDATE / DELETE with typed results
- **QuestDB-native** — first-class support for `SAMPLE BY`, `LATEST ON`, `ASOF JOIN`, `LT JOIN`, `SPLICE JOIN`, designated timestamps, partitioning, WAL, and dedup keys
- **DDL builders** — CREATE TABLE, ALTER TABLE, DROP, TRUNCATE, DESCRIBE with all QuestDB options
- **20+ column types** — every QuestDB type including `SYMBOL`, `GEOHASH`, `UUID`, `IPV4`, `TIMESTAMP (ns)`, and `ARRAY`
- **Runtime validation** — Zod v4 schemas on every column for insert-time validation
- **Aggregate helpers** — `count`, `sum`, `avg`, `min`, `max`, `first`, `last`, `countDistinct`, `ksum`, `nsum`
- **Zero config** — works over QuestDB's HTTP REST API, no native drivers needed

## Installation

```bash
bun add @fcannizzaro/questdb-typesafe-client zod
# or
# pnpm add @fcannizzaro/questdb-typesafe-client zod
# npm install @fcannizzaro/questdb-typesafe-client zod
```


> Requires `zod >= 4.0.0`.

## Configuration

```typescript
import { QuestDBClient } from "@fcannizzaro/questdb-typesafe-client";

const db = new QuestDBClient({
  host: "localhost",       // default: "localhost"
  port: 9000,              // default: 9000
  https: false,            // default: false
  username: "admin",       // optional — Basic auth
  password: "quest",       // optional — Basic auth
  timeout: 30_000,         // request timeout in ms (default: 30000)
  retries: 3,              // retry count for 5xx / network errors (default: 0)
  fetch: customFetch,      // optional — custom fetch implementation (useful for testing)
});
```

| Option | Type | Default | Description |
|---|---|---|---|
| `host` | `string` | `"localhost"` | QuestDB host |
| `port` | `number` | `9000` | HTTP port |
| `https` | `boolean` | `false` | Use HTTPS |
| `username` | `string` | — | Basic auth username |
| `password` | `string` | — | Basic auth password |
| `timeout` | `number` | `30000` | Request timeout (ms) |
| `retries` | `number` | `0` | Retry count for transient errors |
| `fetch` | `typeof fetch` | — | Custom fetch implementation |

Retries use exponential backoff on 5xx and network errors. 4xx errors are never retried.

## Quick Start

```typescript
import { QuestDBClient, defineTable, q, and } from "@fcannizzaro/questdb-typesafe-client";

// 1. Define your table schema
const energyReadings = defineTable({
  name: "energy_readings",
  columns: {
    ts: q.timestamp.designated(),
    source: q.symbol(),
    power_kw: q.double(),
    energy_kwh: q.double(),
    meter_active: q.boolean(),
  },
  partitionBy: "DAY",
  wal: true,
});

// 2. Connect and bind the table
const db = new QuestDBClient({ host: "localhost", port: 9000 });
const t = db.table(energyReadings);

// 3. Create the table
await t.ddl()
       .create()
       .ifNotExists()
       .execute();

// 4. Insert rows
await t.insert({ meter_active: true, source: "solar", power_kw: 48.7, energy_kwh: 312.5 })
       .execute();

// 5. Query with full type safety
const rows = await t
  .select("source", "power_kw")
  .where((c) => and(c.source.eq("solar"), c.power_kw.gt(40)))
  .orderBy("power_kw", "DESC")
  .limit(10)
  .execute();
  // ^-- { source: string | null; power_kw: number | null }[]
```

## Documentation

For detailed guides on schema definitions, query builders, DDL operations, QuestDB-specific features, and more, visit the full documentation.

[View Documentation](https://fcannizzaro.github.io/questdb-typesafe-client)

## License

MIT
