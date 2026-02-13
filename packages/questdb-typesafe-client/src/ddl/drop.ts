import type { QuestDBClient } from "../client/connection.ts";
import { escapeIdentifier } from "../sql/escape.ts";

/**
 * Drop a table.
 */
export async function dropTable(
  client: QuestDBClient,
  name: string,
  ifExists = false,
): Promise<void> {
  const sql = `DROP TABLE${ifExists ? " IF EXISTS" : ""} ${escapeIdentifier(name)}`;
  await client.exec(sql);
}

/**
 * Truncate a table (fast deletion of all rows).
 */
export async function truncateTable(client: QuestDBClient, name: string): Promise<void> {
  await client.exec(`TRUNCATE TABLE ${escapeIdentifier(name)}`);
}
