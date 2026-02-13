import { test, expect, describe } from "bun:test";
import { escapeIdentifier, escapeString } from "../src/sql/escape.ts";

describe("escapeIdentifier", () => {
  test("passes through simple names", () => {
    expect(escapeIdentifier("energy_readings")).toBe("energy_readings");
    expect(escapeIdentifier("my_table")).toBe("my_table");
    expect(escapeIdentifier("col1")).toBe("col1");
    expect(escapeIdentifier("_private")).toBe("_private");
  });

  test("quotes names with special characters", () => {
    expect(escapeIdentifier("my table")).toBe('"my table"');
    expect(escapeIdentifier("my-table")).toBe('"my-table"');
    expect(escapeIdentifier("123abc")).toBe('"123abc"');
    expect(escapeIdentifier("my.col")).toBe('"my.col"');
  });

  test("escapes double quotes inside names", () => {
    expect(escapeIdentifier('my"table')).toBe('"my""table"');
  });
});

describe("escapeString", () => {
  test("passes through normal strings", () => {
    expect(escapeString("hello")).toBe("hello");
    expect(escapeString("world 123")).toBe("world 123");
  });

  test("escapes single quotes", () => {
    expect(escapeString("it's")).toBe("it''s");
    expect(escapeString("'quoted'")).toBe("''quoted''");
    expect(escapeString("a''b")).toBe("a''''b");
  });
});
