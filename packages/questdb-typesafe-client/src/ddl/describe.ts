import type { QuestDBClient } from "../client/connection.ts";
import { parseResultSet } from "../types/result.ts";
import { escapeString } from "../sql/escape.ts";

/**
 * Column information returned by table introspection.
 */
export interface ColumnInfo {
  column: string;
  type: string;
  indexed: boolean;
  indexBlockCapacity: number;
  symbolCached: boolean;
  symbolCapacity: number;
  designated: boolean;
  upsertKey: boolean;
}

/**
 * Describe a table's columns.
 */
export async function describeTable(client: QuestDBClient, name: string): Promise<ColumnInfo[]> {
  const response = await client.exec(`SELECT * FROM table_columns('${escapeString(name)}')`);
  return parseResultSet<ColumnInfo>(response);
}

/**
 * Check if a table exists.
 */
export async function tableExists(client: QuestDBClient, name: string): Promise<boolean> {
  try {
    await client.exec(`SELECT * FROM table_columns('${escapeString(name)}') LIMIT 1`);
    return true;
  } catch {
    return false;
  }
}
