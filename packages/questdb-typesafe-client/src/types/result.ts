import type { QuestDBExecResponse } from "../client/connection.ts";

/**
 * Parse QuestDB's columnar response format into typed row objects.
 *
 * QuestDB returns: { columns: [{name, type}], dataset: [[v1, v2], ...] }
 * We transform to: [{ col1: v1, col2: v2 }, ...]
 */
export function parseResultSet<T>(response: QuestDBExecResponse): T[] {
  const columns = response.columns;
  const dataset = response.dataset;
  if (!columns || !dataset) return [];

  return dataset.map((row) => {
    const obj: Record<string, unknown> = {};
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i]!;
      const raw = row[i];
      obj[col.name] = coerceValue(raw, col.type);
    }
    return obj as T;
  });
}

/**
 * Coerce a raw QuestDB REST API value to the appropriate JS type.
 *
 * QuestDB returns:
 * - timestamps as ISO 8601 strings (e.g., "2026-01-15T10:30:00.000000Z")
 * - longs as strings when quoteLargeNum=true
 * - booleans as true/false
 * - numbers as numbers
 * - nulls as null
 */
export function coerceValue(value: unknown, questdbType: string): unknown {
  if (value === null || value === undefined) return null;

  switch (questdbType.toUpperCase()) {
    case "TIMESTAMP":
    case "DATE":
      return new Date(value as string);

    case "LONG":
    case "TIMESTAMP_NS":
    case "LONG256":
      if (typeof value === "string") return BigInt(value);
      if (typeof value === "number") return BigInt(value);
      return value;

    case "BOOLEAN":
      return Boolean(value);

    case "BYTE":
    case "SHORT":
    case "INT":
    case "FLOAT":
    case "DOUBLE":
      return Number(value);

    case "BINARY":
      // REST API returns base64 for binary
      if (typeof value === "string") {
        return Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
      }
      return value;

    default:
      // strings, symbols, uuid, ipv4, geohash, decimal, char, varchar, array
      return value;
  }
}
