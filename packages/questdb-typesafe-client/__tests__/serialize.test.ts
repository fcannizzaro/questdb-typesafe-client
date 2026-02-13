import { test, expect, describe } from "bun:test";
import { serializeSelect, serializeInsert, serializeUpdate } from "../src/sql/serialize.ts";
import type { SelectNode, InsertNode, UpdateNode } from "../src/types/sql.ts";

function baseSelect(overrides: Partial<SelectNode> = {}): SelectNode {
  return {
    kind: "select",
    distinct: false,
    columns: [],
    from: { kind: "table", name: "energy_readings" },
    joins: [],
    where: null,
    groupBy: [],
    orderBy: [],
    limit: null,
    sampleBy: null,
    latestOn: null,
    ...overrides,
  };
}

describe("serializeSelect", () => {
  test("basic SELECT *", () => {
    const sql = serializeSelect(baseSelect());
    expect(sql).toBe("SELECT *\nFROM energy_readings");
  });

  test("SELECT DISTINCT", () => {
    const sql = serializeSelect(baseSelect({ distinct: true }));
    expect(sql).toContain("SELECT DISTINCT *");
  });

  test("SELECT specific columns", () => {
    const sql = serializeSelect(
      baseSelect({
        columns: [
          { expr: { kind: "column", name: "source" } },
          { expr: { kind: "column", name: "power_kw" } },
        ],
      }),
    );
    expect(sql).toContain("SELECT source, power_kw");
  });

  test("SELECT with alias", () => {
    const sql = serializeSelect(
      baseSelect({
        columns: [{ expr: { kind: "column", name: "power_kw" }, alias: "p" }],
      }),
    );
    expect(sql).toContain("SELECT power_kw AS p");
  });

  test("WHERE with binary expression", () => {
    const sql = serializeSelect(
      baseSelect({
        where: {
          kind: "binary",
          op: "=",
          left: { kind: "column", name: "source" },
          right: { kind: "literal", value: "solar", type: "varchar" },
        },
      }),
    );
    expect(sql).toContain("WHERE (source = 'solar')");
  });

  test("WHERE with timestamp IN", () => {
    const sql = serializeSelect(
      baseSelect({
        where: { kind: "timestamp_in", column: "ts", interval: "2026" },
      }),
    );
    expect(sql).toContain("WHERE ts IN '2026'");
  });

  test("WHERE IS NULL", () => {
    const sql = serializeSelect(
      baseSelect({
        where: { kind: "is_null", column: "power_kw", negated: false },
      }),
    );
    expect(sql).toContain("WHERE power_kw IS NULL");
  });

  test("WHERE IS NOT NULL", () => {
    const sql = serializeSelect(
      baseSelect({
        where: { kind: "is_null", column: "power_kw", negated: true },
      }),
    );
    expect(sql).toContain("WHERE power_kw IS NOT NULL");
  });

  test("WHERE IN list", () => {
    const sql = serializeSelect(
      baseSelect({
        where: {
          kind: "in_list",
          column: "source",
          values: [
            { kind: "literal", value: "solar", type: "varchar" },
            { kind: "literal", value: "wind", type: "varchar" },
          ],
        },
      }),
    );
    expect(sql).toContain("WHERE source IN ('solar', 'wind')");
  });

  test("WHERE BETWEEN", () => {
    const sql = serializeSelect(
      baseSelect({
        where: {
          kind: "between",
          column: "power_kw",
          low: { kind: "literal", value: 100, type: "double" },
          high: { kind: "literal", value: 200, type: "double" },
        },
      }),
    );
    expect(sql).toContain("WHERE power_kw BETWEEN 100 AND 200");
  });

  test("ORDER BY", () => {
    const sql = serializeSelect(
      baseSelect({
        orderBy: [{ expr: { kind: "column", name: "ts" }, direction: "DESC" }],
      }),
    );
    expect(sql).toContain("ORDER BY ts DESC");
  });

  test("LIMIT", () => {
    const sql = serializeSelect(baseSelect({ limit: { count: 100 } }));
    expect(sql).toContain("LIMIT 100");
  });

  test("LIMIT with offset", () => {
    const sql = serializeSelect(baseSelect({ limit: { count: 100, offset: 50 } }));
    expect(sql).toContain("LIMIT 100, 50");
  });

  test("LATEST ON", () => {
    const sql = serializeSelect(
      baseSelect({
        latestOn: { timestamp: "ts", partitionBy: ["source"] },
      }),
    );
    expect(sql).toContain("LATEST ON ts PARTITION BY source");
  });

  test("SAMPLE BY", () => {
    const sql = serializeSelect(
      baseSelect({
        sampleBy: { interval: "1h", fill: [] },
      }),
    );
    expect(sql).toContain("SAMPLE BY 1h");
  });

  test("SAMPLE BY with FILL", () => {
    const sql = serializeSelect(
      baseSelect({
        sampleBy: { interval: "1h", fill: ["PREV", "LINEAR"] },
      }),
    );
    expect(sql).toContain("SAMPLE BY 1h");
    expect(sql).toContain("FILL(PREV, LINEAR)");
  });

  test("SAMPLE BY with ALIGN", () => {
    const sql = serializeSelect(
      baseSelect({
        sampleBy: { interval: "1d", fill: [], align: "FIRST OBSERVATION" },
      }),
    );
    expect(sql).toContain("ALIGN TO FIRST OBSERVATION");
  });

  test("SAMPLE BY with constant fill", () => {
    const sql = serializeSelect(
      baseSelect({
        sampleBy: { interval: "1h", fill: [{ constant: 0 }] },
      }),
    );
    expect(sql).toContain("FILL(0)");
  });

  test("GROUP BY", () => {
    const sql = serializeSelect(
      baseSelect({
        groupBy: [{ kind: "column", name: "source" }],
      }),
    );
    expect(sql).toContain("GROUP BY source");
  });

  test("JOIN", () => {
    const sql = serializeSelect(
      baseSelect({
        joins: [
          {
            type: "ASOF",
            table: { kind: "table", name: "energy_forecasts", alias: "f" },
            on: {
              kind: "binary",
              op: "=",
              left: { kind: "column", table: "energy_readings", name: "source" },
              right: { kind: "column", table: "f", name: "source" },
            },
          },
        ],
      }),
    );
    expect(sql).toContain("ASOF JOIN energy_forecasts f ON (energy_readings.source = f.source)");
  });

  test("subquery in FROM", () => {
    const inner = baseSelect({ limit: { count: 10 } });
    const sql = serializeSelect({
      ...baseSelect(),
      from: { kind: "subquery", select: inner, alias: "sub" },
    });
    expect(sql).toContain("FROM (SELECT *\nFROM energy_readings\nLIMIT 10) sub");
  });

  test("aggregate function", () => {
    const sql = serializeSelect(
      baseSelect({
        columns: [
          {
            expr: {
              kind: "aggregate",
              name: "count",
              args: [{ kind: "raw", sql: "*" }],
              alias: "total",
            },
          },
        ],
      }),
    );
    expect(sql).toContain("SELECT count(*) AS total");
  });

  test("raw expression", () => {
    const sql = serializeSelect(
      baseSelect({
        where: { kind: "raw", sql: "custom_fn(x) > 10" },
      }),
    );
    expect(sql).toContain("WHERE custom_fn(x) > 10");
  });
});

describe("serializeInsert", () => {
  test("single row", () => {
    const node: InsertNode = {
      kind: "insert",
      table: "energy_readings",
      columns: ["source", "power_kw"],
      values: [["solar", 48.7]],
      columnTypes: ["symbol", "double"],
    };
    const sql = serializeInsert(node);
    expect(sql).toContain("INSERT INTO energy_readings");
    expect(sql).toContain("(source, power_kw)");
    expect(sql).toContain("('solar', 48.7)");
  });

  test("multiple rows", () => {
    const node: InsertNode = {
      kind: "insert",
      table: "energy_readings",
      columns: ["source", "power_kw"],
      values: [
        ["solar", 49],
        ["wind", 120],
      ],
      columnTypes: ["symbol", "double"],
    };
    const sql = serializeInsert(node);
    expect(sql).toContain("('solar', 49)");
    expect(sql).toContain("('wind', 120)");
  });

  test("with null values", () => {
    const node: InsertNode = {
      kind: "insert",
      table: "energy_readings",
      columns: ["source", "power_kw"],
      values: [["solar", null]],
      columnTypes: ["symbol", "double"],
    };
    const sql = serializeInsert(node);
    expect(sql).toContain("('solar', NULL)");
  });

  test("throws on empty values", () => {
    const node: InsertNode = {
      kind: "insert",
      table: "energy_readings",
      columns: ["source"],
      values: [],
      columnTypes: ["symbol"],
    };
    expect(() => serializeInsert(node)).toThrow("at least one row");
  });
});

describe("serializeUpdate", () => {
  test("basic UPDATE", () => {
    const node: UpdateNode = {
      kind: "update",
      table: "energy_readings",
      set: [
        {
          column: "power_kw",
          value: { kind: "literal", value: 52.3, type: "double" },
        },
      ],
      where: {
        kind: "binary",
        op: "=",
        left: { kind: "column", name: "source" },
        right: { kind: "literal", value: "solar", type: "varchar" },
      },
    };
    const sql = serializeUpdate(node);
    expect(sql).toContain("UPDATE energy_readings");
    expect(sql).toContain("SET power_kw = 52.3");
    expect(sql).toContain("WHERE (source = 'solar')");
  });

  test("UPDATE with FROM", () => {
    const node: UpdateNode = {
      kind: "update",
      table: "energy_readings",
      set: [
        {
          column: "power_kw",
          value: { kind: "literal", value: 0, type: "double" },
        },
      ],
      from: { kind: "table", name: "corrections", alias: "c" },
      where: null,
    };
    const sql = serializeUpdate(node);
    expect(sql).toContain("FROM corrections c");
  });
});
