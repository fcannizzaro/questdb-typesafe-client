import { test, expect, describe } from "bun:test";
import { defineTable } from "../src/schema/define.ts";
import { q } from "../src/schema/column-builder.ts";
import { Table } from "../src/schema/table.ts";
import type { QuestDBClient } from "../src/client/connection.ts";
import { and, or, not, fn } from "../src/query/expression.ts";

// Mock client
const mockClient = {
  exec: async () => ({
    columns: [
      { name: "source", type: "SYMBOL" },
      { name: "power_kw", type: "DOUBLE" },
    ],
    dataset: [
      ["solar", 48.7],
      ["wind", 120.3],
    ],
  }),
} as unknown as QuestDBClient;

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
});

const readings = new Table(energyReadings, mockClient);

describe("SelectBuilder", () => {
  test("SELECT *", () => {
    const sql = readings.select().toSQL();
    expect(sql).toContain("SELECT *");
    expect(sql).toContain("FROM energy_readings");
  });

  test("SELECT specific columns", () => {
    const sql = readings.select("source", "power_kw").toSQL();
    expect(sql).toContain("SELECT source, power_kw");
  });

  test("WHERE with column expression", () => {
    const sql = readings
      .select()
      .where((c) => c.source.eq("solar"))
      .toSQL();
    expect(sql).toContain("WHERE (source = 'solar')");
  });

  test("WHERE with AND", () => {
    const sql = readings
      .select()
      .where((c) => and(c.source.eq("solar"), c.power_kw.gt(50)))
      .toSQL();
    expect(sql).toContain("AND");
    expect(sql).toContain("source = 'solar'");
    expect(sql).toContain("power_kw > 50");
  });

  test("WHERE with OR", () => {
    const sql = readings
      .select()
      .where((c) => or(c.source.eq("solar"), c.source.eq("wind")))
      .toSQL();
    expect(sql).toContain("OR");
  });

  test("WHERE with NOT", () => {
    const sql = readings
      .select()
      .where((c) => not(c.meter_active.eq(false)))
      .toSQL();
    expect(sql).toContain("NOT");
  });

  test("andWhere appends with AND", () => {
    const sql = readings
      .select()
      .where((c) => c.source.eq("solar"))
      .andWhere((c) => c.power_kw.gt(50))
      .toSQL();
    expect(sql).toContain("AND");
  });

  test("whereTimestamp", () => {
    const sql = readings.select().whereTimestamp("ts", "2026").toSQL();
    expect(sql).toContain("WHERE ts IN '2026'");
  });

  test("whereTimestamp with existing WHERE", () => {
    const sql = readings
      .select()
      .where((c) => c.source.eq("solar"))
      .whereTimestamp("ts", "2026-01;3M")
      .toSQL();
    expect(sql).toContain("AND");
    expect(sql).toContain("ts IN '2026-01;3M'");
  });

  test("ORDER BY", () => {
    const sql = readings.select().orderBy("ts", "DESC").toSQL();
    expect(sql).toContain("ORDER BY ts DESC");
  });

  test("LIMIT", () => {
    const sql = readings.select().limit(100).toSQL();
    expect(sql).toContain("LIMIT 100");
  });

  test("LIMIT with offset", () => {
    const sql = readings.select().limit(100, 50).toSQL();
    expect(sql).toContain("LIMIT 100, 50");
  });

  test("DISTINCT", () => {
    const sql = readings.select().distinct().toSQL();
    expect(sql).toContain("SELECT DISTINCT *");
  });

  test("LATEST ON", () => {
    const sql = readings.select().latestOn("source").toSQL();
    expect(sql).toContain("LATEST ON ts PARTITION BY source");
  });

  test("SAMPLE BY", () => {
    const sql = readings
      .select("ts", "source")
      .addExpr(fn.avg("power_kw", "avg_power"))
      .sampleBy("1h")
      .toSQL();
    expect(sql).toContain("SAMPLE BY 1h");
  });

  test("SAMPLE BY with FILL", () => {
    const sql = readings
      .select("ts", "source")
      .addExpr(fn.avg("power_kw", "avg_power"))
      .sampleBy("1h", "PREV")
      .toSQL();
    expect(sql).toContain("SAMPLE BY 1h");
    expect(sql).toContain("FILL(PREV)");
  });

  test("SAMPLE BY with multiple FILL strategies", () => {
    const sql = readings
      .select("ts", "source")
      .addExpr(fn.avg("power_kw", "avg_power"))
      .sampleBy("1h", ["NULL", "PREV", "LINEAR"])
      .toSQL();
    expect(sql).toContain("FILL(NULL, PREV, LINEAR)");
  });

  test("SAMPLE BY with ALIGN", () => {
    const sql = readings
      .select("ts", "source")
      .addExpr(fn.avg("power_kw", "avg_power"))
      .sampleBy("1d", "NULL", "FIRST OBSERVATION")
      .toSQL();
    expect(sql).toContain("ALIGN TO FIRST OBSERVATION");
  });

  test("SAMPLE BY with SELECT * throws", () => {
    expect(() => readings.select().sampleBy("1h", "PREV").toSQL()).toThrow(
      "SAMPLE BY requires explicit columns",
    );
  });

  test("GROUP BY", () => {
    const sql = readings.select().groupBy("source").toSQL();
    expect(sql).toContain("GROUP BY source");
  });

  test("column operators: isNull, isNotNull", () => {
    const sql1 = readings
      .select()
      .where((c) => c.power_kw.isNull())
      .toSQL();
    expect(sql1).toContain("power_kw IS NULL");

    const sql2 = readings
      .select()
      .where((c) => c.power_kw.isNotNull())
      .toSQL();
    expect(sql2).toContain("power_kw IS NOT NULL");
  });

  test("column operators: in, between, like, matches", () => {
    const sql1 = readings
      .select()
      .where((c) => c.source.in(["solar", "wind"]))
      .toSQL();
    expect(sql1).toContain("source IN ('solar', 'wind')");

    const sql2 = readings
      .select()
      .where((c) => c.power_kw.between(100, 200))
      .toSQL();
    expect(sql2).toContain("power_kw BETWEEN 100 AND 200");

    const sql3 = readings
      .select()
      .where((c) => c.source.like("sol%"))
      .toSQL();
    expect(sql3).toContain("LIKE 'sol%'");

    const sql4 = readings
      .select()
      .where((c) => c.source.matches("^sol.*"))
      .toSQL();
    expect(sql4).toContain("~ '^sol.*'");
  });

  test("chaining produces independent builders (immutable)", () => {
    const base = readings.select().where((c) => c.source.eq("solar"));
    const withLimit = base.limit(10);
    const withOrder = base.orderBy("ts", "DESC");

    expect(withLimit.toSQL()).toContain("LIMIT 10");
    expect(withLimit.toSQL()).not.toContain("ORDER BY");
    expect(withOrder.toSQL()).toContain("ORDER BY");
    expect(withOrder.toSQL()).not.toContain("LIMIT");
  });

  test("execute returns parsed results", async () => {
    const results = await readings.select("source", "power_kw").execute();
    expect(results).toHaveLength(2);
    expect(results[0]).toEqual({ source: "solar", power_kw: 48.7 });
    expect(results[1]).toEqual({ source: "wind", power_kw: 120.3 });
  });

  test("first returns first result", async () => {
    const result = await readings.select("source", "power_kw").first();
    expect(result).toEqual({ source: "solar", power_kw: 48.7 });
  });
});

describe("InsertBuilder", () => {
  test("single row INSERT", () => {
    const sql = readings.insert({ meter_active: true, source: "solar", power_kw: 48.7 }).toSQL();
    expect(sql).toContain("INSERT INTO energy_readings");
    expect(sql).toContain("source");
    expect(sql).toContain("power_kw");
    expect(sql).toContain("meter_active");
    expect(sql).toContain("'solar'");
    expect(sql).toContain("48.7");
    expect(sql).toContain("true");
  });

  test("batch INSERT", () => {
    const sql = readings
      .insert([
        { meter_active: true, source: "solar", power_kw: 49 },
        { meter_active: true, source: "wind", power_kw: 120 },
      ])
      .toSQL();
    expect(sql).toContain("INSERT INTO energy_readings");
    expect(sql).toContain("'solar'");
    expect(sql).toContain("'wind'");
  });

  test("INSERT with optional columns omitted", () => {
    // Only `meter_active` is required (non-nullable), rest are optional
    const sql = readings.insert({ meter_active: false }).toSQL();
    expect(sql).toContain("meter_active");
    expect(sql).toContain("false");
  });

  test("INSERT auto-injects now() for omitted designated timestamp", () => {
    const sql = readings.insert({ meter_active: true, source: "solar", power_kw: 48.7 }).toSQL();
    expect(sql).toContain("ts");
    expect(sql).toContain("now()");
  });

  test("INSERT preserves explicit designated timestamp", () => {
    const sql = readings
      .insert({
        ts: new Date("2026-01-15T00:00:00Z"),
        meter_active: true,
        source: "solar",
        power_kw: 48.7,
      })
      .toSQL();
    expect(sql).toContain("ts");
    expect(sql).toContain("2026-01-15");
    expect(sql).not.toContain("now()");
  });
});

describe("UpdateBuilder", () => {
  test("basic UPDATE", () => {
    const sql = readings
      .update()
      .set({ power_kw: 52.3 })
      .where((c) => c.source.eq("solar"))
      .toSQL();
    expect(sql).toContain("UPDATE energy_readings");
    expect(sql).toContain("SET power_kw = 52.3");
    expect(sql).toContain("WHERE (source = 'solar')");
  });

  test("UPDATE multiple columns", () => {
    const sql = readings
      .update()
      .set({ power_kw: 52.3, meter_active: false })
      .where((c) => c.source.eq("solar"))
      .toSQL();
    expect(sql).toContain("power_kw =");
    expect(sql).toContain("meter_active =");
  });
});

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

describe("JOIN builders", () => {
  const energyForecasts = defineTable({
    name: "energy_forecasts",
    columns: {
      ts: q.timestamp.designated(),
      source: q.symbol(),
      forecast_kw: q.double(),
      actual_kw: q.double(),
    },
    partitionBy: "DAY",
  });

  test("ASOF JOIN", () => {
    const sql = readings
      .select()
      .asofJoin(energyForecasts, "f", (t, f) => and(t.source.ref(), f.source.ref()))
      .toSQL();
    expect(sql).toContain("ASOF JOIN energy_forecasts f ON");
  });

  test("INNER JOIN", () => {
    const sql = readings
      .select()
      .innerJoin(energyForecasts, "f", (t, f) => and(t.source.ref(), f.source.ref()))
      .toSQL();
    expect(sql).toContain("INNER JOIN energy_forecasts f ON");
  });

  test("LEFT JOIN", () => {
    const sql = readings
      .select()
      .leftJoin(energyForecasts, "f", (t, f) => and(t.source.ref(), f.source.ref()))
      .toSQL();
    expect(sql).toContain("LEFT JOIN energy_forecasts f ON");
  });

  test("CROSS JOIN", () => {
    const sql = readings.select().crossJoin(energyForecasts, "f").toSQL();
    expect(sql).toContain("CROSS JOIN energy_forecasts f");
  });
});
