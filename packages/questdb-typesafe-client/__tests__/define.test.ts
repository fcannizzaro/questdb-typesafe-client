import { test, expect, describe } from "bun:test";
import { z } from "zod/v4";
import { defineTable } from "../src/schema/define.ts";
import { q } from "../src/schema/column-builder.ts";

describe("defineTable", () => {
  test("creates a basic table definition", () => {
    const def = defineTable({
      name: "energy_readings",
      columns: {
        ts: q.timestamp.designated(),
        source: q.symbol(),
        power_kw: q.double(),
        meter_active: q.boolean(),
      },
      partitionBy: "DAY",
    });

    expect(def._brand).toBe("TableDef");
    expect(def.name).toBe("energy_readings");
    expect(def.partitionBy).toBe("DAY");
    expect(def.wal).toBe(true);
    expect(def.dedupKeys).toEqual([]);
    expect(def.ttl).toBeUndefined();
  });

  test("defaults partitionBy to DAY", () => {
    const def = defineTable({
      name: "test",
      columns: {
        ts: q.timestamp.designated(),
      },
    });
    expect(def.partitionBy).toBe("DAY");
  });

  test("allows NONE partition without designated timestamp", () => {
    const def = defineTable({
      name: "lookup",
      columns: {
        id: q.int(),
        name: q.varchar(),
      },
      partitionBy: "NONE",
    });
    expect(def.partitionBy).toBe("NONE");
  });

  test("supports dedup keys", () => {
    const def = defineTable({
      name: "energy_readings",
      columns: {
        ts: q.timestamp.designated(),
        meterId: q.uuid(),
        power_kw: q.double(),
      },
      partitionBy: "DAY",
      dedupKeys: ["ts", "meterId"],
    });
    expect(def.dedupKeys).toEqual(["ts", "meterId"]);
  });

  test("supports WAL disabled", () => {
    const def = defineTable({
      name: "test",
      columns: {
        ts: q.timestamp.designated(),
      },
      partitionBy: "DAY",
      wal: false,
    });
    expect(def.wal).toBe(false);
  });

  test("supports TTL", () => {
    const def = defineTable({
      name: "test",
      columns: {
        ts: q.timestamp.designated(),
      },
      partitionBy: "DAY",
      ttl: "90d",
    });
    expect(def.ttl).toBe("90d");
  });

  test("supports maxUncommittedRows and o3MaxLag", () => {
    const def = defineTable({
      name: "test",
      columns: {
        ts: q.timestamp.designated(),
      },
      partitionBy: "DAY",
      maxUncommittedRows: 10000,
      o3MaxLag: "1s",
    });
    expect(def.maxUncommittedRows).toBe(10000);
    expect(def.o3MaxLag).toBe("1s");
  });

  test("preserves all column definitions", () => {
    const def = defineTable({
      name: "test",
      columns: {
        ts: q.timestamp.designated(),
        src: q.symbol.options().capacity(256).cache().index().build(),
        power_kw: q.double(),
        meter_active: q.boolean(),
      },
      partitionBy: "DAY",
    });

    expect(Object.keys(def.columns)).toEqual(["ts", "src", "power_kw", "meter_active"]);
    expect(def.columns.ts.qdbType).toBe("timestamp");
    expect(def.columns.ts.meta.designated).toBe(true);
    expect(def.columns.src.qdbType).toBe("symbol");
    expect(def.columns.src.meta.symbolCapacity).toBe(256);
  });
});

// ---------------------------------------------------------------------------
// Schema-based defineTable
// ---------------------------------------------------------------------------

describe("defineTable with schema", () => {
  test("creates a table definition from a Zod schema", () => {
    const def = defineTable({
      name: "energy_readings",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        source: z.string().meta({ symbol: true }),
        reading: z.string().meta({ symbol: true }),
        power_kw: z.number(),
        energy_kwh: z.number(),
        meter_active: z.boolean(),
      }),
      partitionBy: "DAY",
      wal: true,
    });

    expect(def._brand).toBe("TableDef");
    expect(def.name).toBe("energy_readings");
    expect(def.partitionBy).toBe("DAY");
    expect(def.wal).toBe(true);
    expect(Object.keys(def.columns)).toEqual([
      "ts",
      "source",
      "reading",
      "power_kw",
      "energy_kwh",
      "meter_active",
    ]);
  });

  test("maps z.date().meta({ designated: true }) to designated timestamp", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
      }),
    });

    expect(def.columns.ts.qdbType).toBe("timestamp");
    expect(def.columns.ts.meta.designated).toBe(true);
  });

  test("maps z.date() to regular timestamp", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        created_at: z.date(),
      }),
      partitionBy: "NONE",
    });

    expect(def.columns.created_at.qdbType).toBe("timestamp");
    expect(def.columns.created_at.meta.designated).toBeUndefined();
  });

  test("maps z.string().meta({ symbol: true }) to symbol column", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        source: z.string().meta({ symbol: true }),
      }),
    });

    // Runtime type is "symbol" but type-level defaults to "varchar" (meta is invisible at compile time)
    expect(def.columns.source.qdbType as string).toBe("symbol");
  });

  test("maps z.string() to varchar by default", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        label: z.string(),
      }),
    });

    expect(def.columns.label.qdbType).toBe("varchar");
  });

  test("maps z.number() to double by default", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        power_kw: z.number(),
      }),
    });

    expect(def.columns.power_kw.qdbType).toBe("double");
  });

  test("maps z.number().meta({ int: true }) to int", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        count: z.number().meta({ int: true }),
      }),
    });

    // Runtime type is "int" but type-level defaults to "double" (meta is invisible at compile time)
    expect(def.columns.count.qdbType as string).toBe("int");
  });

  test("maps z.number().meta({ float: true }) to float", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        temperature: z.number().meta({ float: true }),
      }),
    });

    // Runtime type is "float" but type-level defaults to "double" (meta is invisible at compile time)
    expect(def.columns.temperature.qdbType as string).toBe("float");
  });

  test("maps z.boolean() to boolean", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        active: z.boolean(),
      }),
    });

    expect(def.columns.active.qdbType).toBe("boolean");
  });

  test("maps z.bigint() to long", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        big_id: z.bigint(),
      }),
    });

    expect(def.columns.big_id.qdbType).toBe("long");
  });

  test("unwraps z.optional() wrappers", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        label: z.string().optional(),
      }),
    });

    expect(def.columns.label.qdbType).toBe("varchar");
  });

  test("unwraps z.nullable() wrappers", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        value: z.number().nullable(),
      }),
    });

    expect(def.columns.value.qdbType).toBe("double");
  });

  test("unwraps z.nullable().meta() with symbol annotation on wrapper", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        source: z.string().nullable().meta({ symbol: true }),
      }),
    });

    // Runtime type is "symbol" but type-level sees nullable<varchar> (meta is invisible at compile time)
    expect(def.columns.source.qdbType as string).toBe("symbol");
  });

  test("throws for unsupported Zod types", () => {
    expect(() => {
      defineTable({
        name: "test",
        schema: z.object({
          ts: z.date().meta({ designated: true }),
          tags: z.array(z.string()),
        }),
      } as any);
    }).toThrow("Unsupported Zod type");
  });

  test("defaults partitionBy to DAY", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
      }),
    });

    expect(def.partitionBy).toBe("DAY");
  });

  test("supports dedupKeys", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        source: z.string().meta({ symbol: true }),
      }),
      dedupKeys: ["ts", "source"],
    });

    expect(def.dedupKeys).toEqual(["ts", "source"]);
  });

  test("supports all table options", () => {
    const def = defineTable({
      name: "test",
      schema: z.object({
        ts: z.date().meta({ designated: true }),
        value: z.number(),
      }),
      partitionBy: "HOUR",
      wal: false,
      ttl: "30d",
      maxUncommittedRows: 100_000,
      o3MaxLag: "500ms",
    });

    expect(def.partitionBy).toBe("HOUR");
    expect(def.wal).toBe(false);
    expect(def.ttl).toBe("30d");
    expect(def.maxUncommittedRows).toBe(100_000);
    expect(def.o3MaxLag).toBe("500ms");
  });
});
