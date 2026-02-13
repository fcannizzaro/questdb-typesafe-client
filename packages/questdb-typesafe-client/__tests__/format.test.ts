import { test, expect, describe } from "bun:test";
import { formatValue } from "../src/sql/format.ts";

describe("formatValue", () => {
  test("null/undefined → NULL", () => {
    expect(formatValue(null, "varchar")).toBe("NULL");
    expect(formatValue(undefined, "int")).toBe("NULL");
  });

  test("boolean", () => {
    expect(formatValue(true, "boolean")).toBe("true");
    expect(formatValue(false, "boolean")).toBe("false");
  });

  test("numeric types", () => {
    expect(formatValue(42, "int")).toBe("42");
    expect(formatValue(3.14, "double")).toBe("3.14");
    expect(formatValue(0, "float")).toBe("0");
    expect(formatValue(-128, "byte")).toBe("-128");
    expect(formatValue(32767, "short")).toBe("32767");
  });

  test("long", () => {
    expect(formatValue(BigInt("9223372036854775807"), "long")).toBe("9223372036854775807");
    expect(formatValue(42, "long")).toBe("42");
  });

  test("string types", () => {
    expect(formatValue("hello", "varchar")).toBe("'hello'");
    expect(formatValue("hello", "string")).toBe("'hello'");
    expect(formatValue("hello", "symbol")).toBe("'hello'");
    expect(formatValue("it's", "varchar")).toBe("'it''s'");
  });

  test("uuid", () => {
    expect(formatValue("550e8400-e29b-41d4-a716-446655440000", "uuid")).toBe(
      "'550e8400-e29b-41d4-a716-446655440000'",
    );
  });

  test("ipv4", () => {
    expect(formatValue("192.168.1.1", "ipv4")).toBe("'192.168.1.1'");
  });

  test("char", () => {
    expect(formatValue("A", "char")).toBe("'A'");
  });

  test("decimal", () => {
    expect(formatValue("123.456", "decimal")).toBe("'123.456'");
  });

  test("timestamp from Date", () => {
    const d = new Date("2026-01-15T10:30:00.000Z");
    expect(formatValue(d, "timestamp")).toBe("'2026-01-15T10:30:00.000Z'");
  });

  test("timestamp from number (epoch micros)", () => {
    expect(formatValue(1705312200000000, "timestamp")).toBe("1705312200000000");
  });

  test("date from Date", () => {
    const d = new Date("2026-01-15");
    expect(formatValue(d, "date")).toContain("2026-01-15");
  });

  test("timestamp_ns from bigint", () => {
    expect(formatValue(BigInt("1705312200000000000"), "timestamp_ns")).toBe("1705312200000000000");
  });

  test("geohash", () => {
    expect(formatValue("u33dc", "geohash")).toBe("##u33dc");
  });

  test("long256", () => {
    expect(formatValue("0xabc", "long256")).toBe("'0xabc'");
  });

  test("binary throws", () => {
    expect(() => formatValue(new Uint8Array([1, 2, 3]), "binary")).toThrow(
      "Binary values cannot be inserted via SQL",
    );
  });

  test("array", () => {
    expect(formatValue([1.0, 2.0, 3.0], "array")).toBe("{1,2,3}");
  });

  test("array throws for non-array", () => {
    expect(() => formatValue("not array", "array")).toThrow("Expected array value");
  });
});
