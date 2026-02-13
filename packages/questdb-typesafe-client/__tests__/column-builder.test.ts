import { test, expect, describe } from "bun:test";
import { q } from "../src/schema/column-builder.ts";

describe("q column builders", () => {
  test("q.boolean() creates a boolean column", () => {
    const col = q.boolean();
    expect(col._brand).toBe("QColumn");
    expect(col.qdbType).toBe("boolean");
    expect(col.meta).toEqual({});
  });

  test("q.byte() creates a byte column", () => {
    const col = q.byte();
    expect(col.qdbType).toBe("byte");
  });

  test("q.short() creates a short column", () => {
    const col = q.short();
    expect(col.qdbType).toBe("short");
  });

  test("q.int() creates an int column", () => {
    const col = q.int();
    expect(col.qdbType).toBe("int");
  });

  test("q.long() creates a long column", () => {
    const col = q.long();
    expect(col.qdbType).toBe("long");
  });

  test("q.float() creates a float column", () => {
    const col = q.float();
    expect(col.qdbType).toBe("float");
  });

  test("q.double() creates a double column", () => {
    const col = q.double();
    expect(col.qdbType).toBe("double");
  });

  test("q.decimal() creates a decimal column", () => {
    const col = q.decimal();
    expect(col.qdbType).toBe("decimal");
  });

  test("q.char() creates a char column", () => {
    const col = q.char();
    expect(col.qdbType).toBe("char");
  });

  test("q.varchar() creates a varchar column", () => {
    const col = q.varchar();
    expect(col.qdbType).toBe("varchar");
  });

  test("q.string() creates a string column", () => {
    const col = q.string();
    expect(col.qdbType).toBe("string");
  });

  test("q.date() creates a date column", () => {
    const col = q.date();
    expect(col.qdbType).toBe("date");
  });

  test("q.timestamp() creates a timestamp column", () => {
    const col = q.timestamp();
    expect(col.qdbType).toBe("timestamp");
    expect(col.meta).toEqual({});
  });

  test("q.timestamp.designated() creates a designated timestamp", () => {
    const col = q.timestamp.designated();
    expect(col.qdbType).toBe("timestamp");
    expect(col.meta).toEqual({ designated: true });
  });

  test("q.timestamp.ns() creates a nanosecond timestamp", () => {
    const col = q.timestamp.ns();
    expect(col.qdbType).toBe("timestamp_ns");
  });

  test("q.symbol() creates a symbol column", () => {
    const col = q.symbol();
    expect(col.qdbType).toBe("symbol");
    expect(col.meta).toEqual({});
  });

  test("q.symbol.options() with full chain", () => {
    const col = q.symbol.options().capacity(256).cache().index(1024).build();
    expect(col.qdbType).toBe("symbol");
    expect(col.meta.symbolCapacity).toBe(256);
    expect(col.meta.symbolCache).toBe(true);
    expect(col.meta.symbolIndex).toBe(true);
    expect(col.meta.symbolIndexCapacity).toBe(1024);
  });

  test("q.symbol.options() with nocache", () => {
    const col = q.symbol.options().nocache().build();
    expect(col.meta.symbolCache).toBe(false);
  });

  test("q.uuid() creates a uuid column", () => {
    const col = q.uuid();
    expect(col.qdbType).toBe("uuid");
  });

  test("q.ipv4() creates an ipv4 column", () => {
    const col = q.ipv4();
    expect(col.qdbType).toBe("ipv4");
  });

  test("q.binary() creates a binary column", () => {
    const col = q.binary();
    expect(col.qdbType).toBe("binary");
  });

  test("q.long256() creates a long256 column", () => {
    const col = q.long256();
    expect(col.qdbType).toBe("long256");
  });

  test("q.geohash() creates a geohash column with bits", () => {
    const col = q.geohash(30);
    expect(col.qdbType).toBe("geohash");
    expect(col.meta.geohashBits).toBe(30);
  });

  test("q.array() creates an array column", () => {
    const col = q.array("double");
    expect(col.qdbType).toBe("array");
    expect(col.meta.arrayType).toBe("double");
  });
});
