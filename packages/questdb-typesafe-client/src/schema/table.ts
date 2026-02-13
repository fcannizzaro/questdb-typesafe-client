import type { QuestDBClient } from "../client/connection.ts";
import { AlterBuilder } from "../ddl/alter.ts";
import { CreateBuilder } from "../ddl/create.ts";
import { describeTable, tableExists } from "../ddl/describe.ts";
import { dropTable, truncateTable } from "../ddl/drop.ts";
import { escapeIdentifier } from "../sql/escape.ts";
import { InsertBuilder } from "../query/insert.ts";
import { SelectBuilder } from "../query/select.ts";
import { UpdateBuilder } from "../query/update.ts";
import type { InferInsertRow, InferRow, TableDefLike } from "../types/infer.ts";
import type { Prettify } from "../util/types.ts";

/**
 * Runtime representation of a defined table.
 * Entry point for building queries and DDL operations.
 *
 * ```ts
 * const db = new QuestDBClient();
 * const sensorsTable = db.table(sensors);
 *
 * // SELECT
 * const rows = await sensorsTable.select("power_kw", "source").where(...).execute();
 *
 * // INSERT
 * await sensorsTable.insert({ meter_active: true, power_kw: 100 }).execute();
 * ```
 */
export class Table<TDef extends TableDefLike> {
  readonly def: TDef;
  private readonly client: QuestDBClient;

  constructor(def: TDef, client: QuestDBClient) {
    this.def = def;
    this.client = client;
  }

  // ---------------------------------------------------------------------------
  // SELECT
  // ---------------------------------------------------------------------------

  /** Start building a SELECT * query. */
  select(): SelectBuilder<TDef>;
  /** Start building a SELECT with specific columns. */
  select<K extends keyof InferRow<TDef> & string>(
    ...columns: K[]
  ): SelectBuilder<TDef, Prettify<Pick<InferRow<TDef>, K>>>;
  select(...columns: string[]): SelectBuilder<TDef, any> {
    const builder = new SelectBuilder(this.client, this.def);
    if (columns.length > 0) {
      return builder.columns(...(columns as any));
    }
    return builder;
  }

  // ---------------------------------------------------------------------------
  // INSERT
  // ---------------------------------------------------------------------------

  /** Insert a single row. */
  insert(row: InferInsertRow<TDef>): InsertBuilder<TDef>;
  /** Insert multiple rows. */
  insert(rows: InferInsertRow<TDef>[]): InsertBuilder<TDef>;
  insert(rowOrRows: InferInsertRow<TDef> | InferInsertRow<TDef>[]): InsertBuilder<TDef> {
    const builder = new InsertBuilder(this.client, this.def);
    if (Array.isArray(rowOrRows)) {
      return builder.valuesMany(rowOrRows);
    }
    return builder.values(rowOrRows);
  }

  // ---------------------------------------------------------------------------
  // UPDATE
  // ---------------------------------------------------------------------------

  /** Start building an UPDATE query. */
  update(): UpdateBuilder<TDef> {
    return new UpdateBuilder(this.client, this.def);
  }

  // ---------------------------------------------------------------------------
  // DELETE PARTITION
  // ---------------------------------------------------------------------------

  /**
   * Drop one or more partitions from this table.
   *
   * QuestDB does not support row-level DELETE — use this to remove entire
   * partitions instead.
   *
   * ```ts
   * await sensorsTable.deletePartition("2026-01-15");
   * await sensorsTable.deletePartition(["2026-01-15", "2026-01-16"]);
   * ```
   */
  async deletePartition(partition: string | string[]): Promise<void> {
    const partitions = Array.isArray(partition) ? partition : [partition];
    const list = partitions.map((p) => `'${p}'`).join(", ");
    const sql = `ALTER TABLE ${escapeIdentifier(this.def.name)} DROP PARTITION LIST ${list}`;
    await this.client.exec(sql);
  }

  // ---------------------------------------------------------------------------
  // DDL
  // ---------------------------------------------------------------------------

  /** DDL operations for this table. */
  ddl() {
    const client = this.client;
    const def = this.def;
    return {
      /** Create the table. */
      create: () => new CreateBuilder(client, def),
      /** Alter the table. */
      alter: () => new AlterBuilder(client, def),
      /** Drop the table. */
      drop: (ifExists = false) => dropTable(client, def.name, ifExists),
      /** Truncate the table (remove all rows). */
      truncate: () => truncateTable(client, def.name),
      /** Describe table columns. */
      describe: () => describeTable(client, def.name),
      /** Check if the table exists. */
      exists: () => tableExists(client, def.name),
    };
  }

  // ---------------------------------------------------------------------------
  // Raw SQL
  // ---------------------------------------------------------------------------

  /** Execute raw SQL against this table's client (escape hatch). */
  raw(sql: string) {
    return this.client.exec(sql);
  }
}
