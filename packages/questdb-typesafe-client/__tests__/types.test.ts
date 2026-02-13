import { test, expect, describe } from "bun:test";
import { defineTable } from "../src/schema/define.ts";
import { q } from "../src/schema/column-builder.ts";
import type { InferRow, InferInsertRow, InferUpdateRow } from "../src/types/infer.ts";

/**
 * Type-level assertion helper.
 * If this compiles, the type assertion is valid.
 */
function assertType<T>(_value: T): void {}

// Test schema
const energyReadings = defineTable({
  name: "energy_readings",
  columns: {
    ts: q.timestamp.designated(),
    source: q.symbol(),
    power_kw: q.double(),
    energy_kwh: q.long(),
    meter_active: q.boolean(),
    count: q.byte(),
    level: q.short(),
  },
  partitionBy: "DAY",
});

describe("Type inference", () => {
  test("InferRow produces correct types", () => {
    type Row = InferRow<typeof energyReadings>;

    // These assertions are checked at compile time
    assertType<Row>({
      ts: new Date(),
      source: "solar",
      power_kw: 48.7,
      energy_kwh: BigInt(100),
      meter_active: true,
      count: 5,
      level: 10,
    });

    // Nullable columns accept null
    assertType<Row>({
      ts: null,
      source: null,
      power_kw: null,
      energy_kwh: null,
      meter_active: true, // non-nullable
      count: 5, // non-nullable
      level: 10, // non-nullable
    });
  });

  test("InferInsertRow makes designated optional", () => {
    type Insert = InferInsertRow<typeof energyReadings>;

    // Required fields: meter_active, count, level (non-nullable)
    // Optional fields: ts (designated), source, power_kw, energy_kwh (nullable)
    assertType<Insert>({
      meter_active: true,
      count: 1,
      level: 1,
    });

    // With optional fields
    assertType<Insert>({
      meter_active: true,
      count: 1,
      level: 1,
      ts: new Date(),
      source: "solar",
      power_kw: 48.7,
      energy_kwh: BigInt(100),
    });

    // Nullable fields accept null
    assertType<Insert>({
      meter_active: true,
      count: 1,
      level: 1,
      source: null,
      power_kw: null,
    });
  });

  test("InferUpdateRow excludes designated timestamp", () => {
    type Update = InferUpdateRow<typeof energyReadings>;

    // All fields are optional in update
    assertType<Update>({});
    assertType<Update>({ power_kw: 48.7 });
    assertType<Update>({ source: "wind", meter_active: true });

    // ts should not be in UpdateRow (designated timestamp)
    // @ts-expect-error — ts should not be assignable
    const _bad: Update = { ts: new Date() };
    void _bad;
  });

  test("NONE partition does not require designated timestamp", () => {
    // This should compile without error
    const lookup = defineTable({
      name: "lookup",
      columns: {
        id: q.int(),
        name: q.varchar(),
      },
      partitionBy: "NONE",
    });

    type Row = InferRow<typeof lookup>;
    assertType<Row>({ id: 1, name: "test" });
    assertType<Row>({ id: null, name: null });
  });

  test("Non-nullable types: boolean, byte, short", () => {
    const schema = defineTable({
      name: "test",
      columns: {
        ts: q.timestamp.designated(),
        flag: q.boolean(),
        b: q.byte(),
        s: q.short(),
      },
      partitionBy: "DAY",
    });

    type Row = InferRow<typeof schema>;

    // These are never null
    const row: Row = {
      ts: null,
      flag: false,
      b: 0,
      s: 0,
    };

    expect(row.flag).toBe(false);
    expect(row.b).toBe(0);
  });
});
