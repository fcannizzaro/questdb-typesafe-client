import { test, expect, describe } from "bun:test";
import { parseResultSet, coerceValue } from "../src/types/result.ts";
import type { QuestDBExecResponse } from "../src/client/connection.ts";

describe("coerceValue", () => {
  test("null stays null", () => {
    expect(coerceValue(null, "DOUBLE")).toBeNull();
    expect(coerceValue(undefined, "INT")).toBeNull();
  });

  test("TIMESTAMP → Date", () => {
    const result = coerceValue("2026-01-15T10:30:00.000000Z", "TIMESTAMP");
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).getFullYear()).toBe(2026);
  });

  test("DATE → Date", () => {
    const result = coerceValue("2026-01-15", "DATE");
    expect(result).toBeInstanceOf(Date);
  });

  test("LONG → BigInt", () => {
    const result = coerceValue("9223372036854775807", "LONG");
    expect(result).toBe(BigInt("9223372036854775807"));
  });

  test("LONG from number → BigInt", () => {
    const result = coerceValue(42, "LONG");
    expect(result).toBe(BigInt(42));
  });

  test("LONG256 → BigInt", () => {
    const result = coerceValue("12345", "LONG256");
    expect(result).toBe(BigInt(12345));
  });

  test("TIMESTAMP_NS → BigInt", () => {
    const result = coerceValue("1705312200000000000", "TIMESTAMP_NS");
    expect(result).toBe(BigInt("1705312200000000000"));
  });

  test("BOOLEAN → boolean", () => {
    expect(coerceValue(true, "BOOLEAN")).toBe(true);
    expect(coerceValue(false, "BOOLEAN")).toBe(false);
  });

  test("INT → number", () => {
    expect(coerceValue(42, "INT")).toBe(42);
  });

  test("DOUBLE → number", () => {
    expect(coerceValue(3.14, "DOUBLE")).toBe(3.14);
  });

  test("SYMBOL → string (passthrough)", () => {
    expect(coerceValue("solar", "SYMBOL")).toBe("solar");
  });

  test("VARCHAR → string (passthrough)", () => {
    expect(coerceValue("hello", "VARCHAR")).toBe("hello");
  });

  test("UUID → string (passthrough)", () => {
    const uuid = "550e8400-e29b-41d4-a716-446655440000";
    expect(coerceValue(uuid, "UUID")).toBe(uuid);
  });
});

describe("parseResultSet", () => {
  test("parses columnar response to row objects", () => {
    const response: QuestDBExecResponse = {
      columns: [
        { name: "source", type: "SYMBOL" },
        { name: "power_kw", type: "DOUBLE" },
        { name: "meter_active", type: "BOOLEAN" },
      ],
      dataset: [
        ["solar", 48.7, true],
        ["wind", 120.3, false],
      ],
    };

    const rows = parseResultSet<{
      source: string;
      power_kw: number;
      meter_active: boolean;
    }>(response);

    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({
      source: "solar",
      power_kw: 48.7,
      meter_active: true,
    });
    expect(rows[1]).toEqual({
      source: "wind",
      power_kw: 120.3,
      meter_active: false,
    });
  });

  test("coerces timestamps to Date", () => {
    const response: QuestDBExecResponse = {
      columns: [{ name: "ts", type: "TIMESTAMP" }],
      dataset: [["2026-01-15T10:30:00.000000Z"]],
    };

    const rows = parseResultSet<{ ts: Date }>(response);
    expect(rows[0]!.ts).toBeInstanceOf(Date);
  });

  test("coerces longs to BigInt", () => {
    const response: QuestDBExecResponse = {
      columns: [{ name: "id", type: "LONG" }],
      dataset: [["9223372036854775807"]],
    };

    const rows = parseResultSet<{ id: bigint }>(response);
    expect(rows[0]!.id).toBe(BigInt("9223372036854775807"));
  });

  test("handles null values", () => {
    const response: QuestDBExecResponse = {
      columns: [
        { name: "source", type: "SYMBOL" },
        { name: "power_kw", type: "DOUBLE" },
      ],
      dataset: [["solar", null]],
    };

    const rows = parseResultSet<{ source: string; power_kw: number | null }>(response);
    expect(rows[0]!.power_kw).toBeNull();
  });

  test("handles empty response", () => {
    const response: QuestDBExecResponse = {
      ddl: "OK",
    };

    const rows = parseResultSet(response);
    expect(rows).toHaveLength(0);
  });

  test("handles empty dataset", () => {
    const response: QuestDBExecResponse = {
      columns: [{ name: "source", type: "SYMBOL" }],
      dataset: [],
    };

    const rows = parseResultSet(response);
    expect(rows).toHaveLength(0);
  });
});
