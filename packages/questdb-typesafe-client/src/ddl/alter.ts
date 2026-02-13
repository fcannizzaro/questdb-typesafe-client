import type { QColumn, QuestDBType, ColumnMeta } from "../types/column.ts";
import type { TableDef } from "../schema/define.ts";
import type { InferRow } from "../types/infer.ts";
import type { QuestDBClient } from "../client/connection.ts";
import { escapeIdentifier } from "../sql/escape.ts";
import { mapQDBType } from "./create.ts";

/**
 * Builder for ALTER TABLE operations.
 *
 * Accumulates operations, then executes them sequentially.
 * QuestDB doesn't support compound ALTER statements.
 */
export class AlterBuilder<TDef extends TableDef> {
  private readonly client: QuestDBClient;
  private readonly def: TDef;
  private readonly operations: string[] = [];

  constructor(client: QuestDBClient, def: TDef) {
    this.client = client;
    this.def = def;
  }

  private get tableName(): string {
    return escapeIdentifier(this.def.name);
  }

  addColumn(name: string, col: QColumn<QuestDBType, unknown, ColumnMeta>): this {
    this.operations.push(
      `ALTER TABLE ${this.tableName} ADD COLUMN ${escapeIdentifier(name)} ${mapQDBType(col)}`,
    );
    return this;
  }

  dropColumn(name: keyof InferRow<TDef> & string): this {
    this.operations.push(`ALTER TABLE ${this.tableName} DROP COLUMN ${escapeIdentifier(name)}`);
    return this;
  }

  renameColumn(from: keyof InferRow<TDef> & string, to: string): this {
    this.operations.push(
      `ALTER TABLE ${this.tableName} RENAME COLUMN ${escapeIdentifier(from)} TO ${escapeIdentifier(to)}`,
    );
    return this;
  }

  setTTL(ttl: string): this {
    this.operations.push(`ALTER TABLE ${this.tableName} SET PARAM ttl = '${ttl}'`);
    return this;
  }

  setMaxUncommittedRows(n: number): this {
    this.operations.push(`ALTER TABLE ${this.tableName} SET PARAM maxUncommittedRows = ${n}`);
    return this;
  }

  setO3MaxLag(lag: string): this {
    this.operations.push(`ALTER TABLE ${this.tableName} SET PARAM o3MaxLag = '${lag}'`);
    return this;
  }

  dropPartition(partition: string): this {
    this.operations.push(`ALTER TABLE ${this.tableName} DROP PARTITION LIST '${partition}'`);
    return this;
  }

  detachPartition(partition: string): this {
    this.operations.push(`ALTER TABLE ${this.tableName} DETACH PARTITION LIST '${partition}'`);
    return this;
  }

  attachPartition(partition: string): this {
    this.operations.push(`ALTER TABLE ${this.tableName} ATTACH PARTITION LIST '${partition}'`);
    return this;
  }

  squashPartitions(): this {
    this.operations.push(`ALTER TABLE ${this.tableName} SQUASH PARTITIONS`);
    return this;
  }

  resumeWal(): this {
    this.operations.push(`ALTER TABLE ${this.tableName} RESUME WAL`);
    return this;
  }

  toSQL(): string[] {
    return [...this.operations];
  }

  /** Execute all ALTER statements sequentially. */
  async execute(): Promise<void> {
    for (const sql of this.operations) {
      await this.client.exec(sql);
    }
  }
}
