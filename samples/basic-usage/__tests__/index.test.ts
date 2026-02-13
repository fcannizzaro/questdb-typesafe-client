import { test, expect, describe } from "bun:test";
import { defineTable, q, Table, and } from "@fcannizzaro/questdb-typesafe-client";
import type { QuestDBClient } from "@fcannizzaro/questdb-typesafe-client";

// ---------------------------------------------------------------------------
// Mock client — mirrors the pattern from packages/questdb-typesafe-client/__tests__/
// ---------------------------------------------------------------------------

const mockClient = {
  exec: async () => ({
    columns: [
      { name: "source", type: "SYMBOL" },
      { name: "power_kw", type: "DOUBLE" },
      { name: "energy_kwh", type: "DOUBLE" },
    ],
    dataset: [
      ["solar", 48.7, 312.5],
      ["wind", 120.3, 890.1],
    ],
  }),
} as unknown as QuestDBClient;

// ---------------------------------------------------------------------------
// Table definition — same schema as samples/basic-usage/index.ts
// ---------------------------------------------------------------------------

const energyReadings = defineTable({
  name: "energy_readings",
  columns: {
    ts: q.timestamp.designated(),
    source: q.symbol(),
    reading: q.symbol(),
    power_kw: q.double(),
    energy_kwh: q.double(),
    meter_active: q.boolean(),
  },
  partitionBy: "DAY",
  wal: true,
});

const table = new Table(energyReadings, mockClient);

// ---------------------------------------------------------------------------
// Table definition
// ---------------------------------------------------------------------------

describe("Table definition", () => {
  test("has correct table name", () => {
    expect(energyReadings.name).toBe("energy_readings");
  });

  test("is partitioned by DAY", () => {
    expect(energyReadings.partitionBy).toBe("DAY");
  });

  test("has WAL enabled", () => {
    expect(energyReadings.wal).toBe(true);
  });

  test("has designated timestamp column", () => {
    expect(energyReadings.columns.ts.meta.designated).toBe(true);
  });

  test("preserves all column definitions", () => {
    const cols = Object.keys(energyReadings.columns);
    expect(cols).toEqual(["ts", "source", "reading", "power_kw", "energy_kwh", "meter_active"]);
  });
});

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

describe("SelectBuilder", () => {
  test("SELECT * generates correct SQL", () => {
    const sql = table.select().toSQL();
    expect(sql).toContain("SELECT *");
    expect(sql).toContain("FROM energy_readings");
  });

  test("SELECT specific columns", () => {
    const sql = table.select("source", "power_kw", "energy_kwh").toSQL();
    expect(sql).toContain("SELECT source, power_kw, energy_kwh");
  });

  test("WHERE with single condition", () => {
    const sql = table
      .select()
      .where((c) => c.source.eq("solar"))
      .toSQL();
    expect(sql).toContain("WHERE (source = 'solar')");
  });

  test("WHERE with AND", () => {
    const sql = table
      .select("source", "power_kw", "energy_kwh")
      .where((c) => and(c.source.eq("solar"), c.power_kw.gt(50)))
      .orderBy("power_kw", "DESC")
      .limit(10)
      .toSQL();
    expect(sql).toContain("AND");
    expect(sql).toContain("source = 'solar'");
    expect(sql).toContain("power_kw > 50");
    expect(sql).toContain("ORDER BY power_kw DESC");
    expect(sql).toContain("LIMIT 10");
  });

  test("ORDER BY + LIMIT", () => {
    const sql = table.select().orderBy("power_kw", "DESC").limit(10).toSQL();
    expect(sql).toContain("ORDER BY power_kw DESC");
    expect(sql).toContain("LIMIT 10");
  });

  test("LATEST ON partitioned by source", () => {
    const sql = table.select().latestOn("source").toSQL();
    expect(sql).toContain("LATEST ON ts PARTITION BY source");
  });

  test("SAMPLE BY with FILL", () => {
    const sql = table.select("power_kw").sampleBy("1h", "PREV").toSQL();
    expect(sql).toContain("SAMPLE BY 1h");
    expect(sql).toContain("FILL(PREV)");
  });

  test("execute() returns parsed mock results", async () => {
    const results = await table.select("source", "power_kw", "energy_kwh").execute();
    expect(results).toHaveLength(2);
    expect(results[0]).toEqual({
      source: "solar",
      power_kw: 48.7,
      energy_kwh: 312.5,
    });
    expect(results[1]).toEqual({
      source: "wind",
      power_kw: 120.3,
      energy_kwh: 890.1,
    });
  });

  test("first() returns first mock result", async () => {
    const result = await table.select("source", "power_kw", "energy_kwh").first();
    expect(result).toEqual({
      source: "solar",
      power_kw: 48.7,
      energy_kwh: 312.5,
    });
  });
});

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

describe("InsertBuilder", () => {
  test("single row INSERT", () => {
    const sql = table
      .insert({
        meter_active: true,
        source: "solar",
        reading: "produced",
        power_kw: 48.7,
        energy_kwh: 312.5,
      })
      .toSQL();
    expect(sql).toContain("INSERT INTO energy_readings");
    expect(sql).toContain("source");
    expect(sql).toContain("reading");
    expect(sql).toContain("power_kw");
    expect(sql).toContain("energy_kwh");
    expect(sql).toContain("meter_active");
    expect(sql).toContain("'solar'");
    expect(sql).toContain("'produced'");
    expect(sql).toContain("48.7");
    expect(sql).toContain("312.5");
    expect(sql).toContain("true");
  });

  test("batch INSERT", () => {
    const sql = table
      .insert([
        {
          meter_active: true,
          source: "wind",
          reading: "produced",
          power_kw: 120.3,
          energy_kwh: 890.1,
        },
        {
          meter_active: true,
          source: "solar",
          reading: "consumed",
          power_kw: 5.2,
          energy_kwh: 41.6,
        },
        {
          meter_active: false,
          source: "grid",
          reading: "consumed",
          power_kw: 0.0,
          energy_kwh: 0.0,
        },
      ])
      .toSQL();
    expect(sql).toContain("INSERT INTO energy_readings");
    expect(sql).toContain("'wind'");
    expect(sql).toContain("'solar'");
    expect(sql).toContain("'grid'");
  });

  test("INSERT with only required columns", () => {
    const sql = table.insert({ meter_active: false }).toSQL();
    expect(sql).toContain("meter_active");
    expect(sql).toContain("false");
  });

  test("single row INSERT executes against mock", async () => {
    await expect(
      table
        .insert({
          meter_active: true,
          source: "solar",
          reading: "produced",
          power_kw: 48.7,
          energy_kwh: 312.5,
        })
        .execute(),
    ).resolves.toBeDefined();
  });
});

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

describe("UpdateBuilder", () => {
  test("basic UPDATE", () => {
    const sql = table
      .update()
      .set({ power_kw: 52.3 })
      .where((c) => c.source.eq("solar"))
      .toSQL();
    expect(sql).toContain("UPDATE energy_readings");
    expect(sql).toContain("SET power_kw = 52.3");
    expect(sql).toContain("WHERE (source = 'solar')");
  });

  test("UPDATE multiple columns", () => {
    const sql = table
      .update()
      .set({ power_kw: 0.0, meter_active: false })
      .where((c) => c.source.eq("grid"))
      .toSQL();
    expect(sql).toContain("power_kw =");
    expect(sql).toContain("meter_active =");
  });
});

// ---------------------------------------------------------------------------
// DELETE PARTITION
// ---------------------------------------------------------------------------

describe("deletePartition", () => {
  test("single partition key", async () => {
    let executedSql = "";
    const spyClient = {
      exec: async (sql: string) => {
        executedSql = sql;
        return {};
      },
    } as unknown as QuestDBClient;
    const spyTable = new Table(energyReadings, spyClient);
    await spyTable.deletePartition("2026-01-15");
    expect(executedSql).toBe("ALTER TABLE energy_readings DROP PARTITION LIST '2026-01-15'");
  });

  test("multiple partition keys", async () => {
    let executedSql = "";
    const spyClient = {
      exec: async (sql: string) => {
        executedSql = sql;
        return {};
      },
    } as unknown as QuestDBClient;
    const spyTable = new Table(energyReadings, spyClient);
    await spyTable.deletePartition(["2026-01-15", "2026-01-16"]);
    expect(executedSql).toBe(
      "ALTER TABLE energy_readings DROP PARTITION LIST '2026-01-15', '2026-01-16'",
    );
  });
});

// ---------------------------------------------------------------------------
// SQL preview (.toSQL())
// ---------------------------------------------------------------------------

describe("SQL preview", () => {
  test("generates complete query for SELECT with WHERE, ORDER BY, LIMIT", () => {
    const sql = table
      .select("source", "power_kw")
      .where((c) => c.power_kw.gt(100))
      .orderBy("power_kw", "DESC")
      .limit(5)
      .toSQL();
    expect(sql).toContain("SELECT source, power_kw");
    expect(sql).toContain("FROM energy_readings");
    expect(sql).toContain("WHERE (power_kw > 100)");
    expect(sql).toContain("ORDER BY power_kw DESC");
    expect(sql).toContain("LIMIT 5");
  });
});
