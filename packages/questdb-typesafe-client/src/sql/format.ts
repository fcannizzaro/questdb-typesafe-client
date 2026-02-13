import type { QuestDBType } from "../types/column.ts";
import { escapeString } from "./escape.ts";

/**
 * Format a JavaScript value to a SQL literal string for the given QuestDB type.
 */
export function formatValue(value: unknown, type: QuestDBType): string {
  if (value === null || value === undefined) {
    return "NULL";
  }

  switch (type) {
    case "boolean":
      return value ? "true" : "false";

    case "byte":
    case "short":
    case "int":
    case "float":
    case "double":
      return String(value);

    case "long":
      return `${value}`;

    case "decimal":
      return `'${escapeString(String(value))}'`;

    case "char":
      return `'${escapeString(String(value))}'`;

    case "varchar":
    case "string":
    case "symbol":
    case "uuid":
    case "ipv4":
    case "long256":
      return `'${escapeString(String(value))}'`;

    case "timestamp":
    case "date": {
      if (value instanceof Date) {
        return `'${value.toISOString()}'`;
      }
      if (typeof value === "number" || typeof value === "bigint") {
        return `${value}`;
      }
      return `'${escapeString(String(value))}'`;
    }

    case "timestamp_ns": {
      return `${value}`;
    }

    case "geohash":
      return `##${String(value)}`;

    case "binary":
      throw new Error("Binary values cannot be inserted via SQL; use the /imp endpoint");

    case "array": {
      if (Array.isArray(value)) {
        return `{${value.join(",")}}`;
      }
      throw new Error(`Expected array value, got ${typeof value}`);
    }

    default:
      return `'${escapeString(String(value))}'`;
  }
}
