import type { TableDefLike, InferInsertRow } from "../types/infer.ts";
import type { QColumn, QuestDBType, ColumnMeta } from "../types/column.ts";
import type { QuestDBClient } from "../client/connection.ts";
import type { InsertNode } from "../types/sql.ts";
import { serializeInsert } from "../sql/serialize.ts";

/**
 * Type-safe INSERT query builder.
 */
export class InsertBuilder<TDef extends TableDefLike> {
  /** @internal */ readonly _client: QuestDBClient;
  /** @internal */ readonly _def: TDef;
  /** @internal */ readonly _rows: Record<string, unknown>[] = [];

  constructor(client: QuestDBClient, def: TDef) {
    this._client = client;
    this._def = def;
  }

  /** Add one row. */
  values(row: InferInsertRow<TDef>): this {
    this._rows.push(row as Record<string, unknown>);
    return this;
  }

  /** Add multiple rows. */
  valuesMany(rows: InferInsertRow<TDef>[]): this {
    for (const row of rows) {
      this._rows.push(row as Record<string, unknown>);
    }
    return this;
  }

  /** Generate the SQL string. */
  toSQL(): string {
    const colEntries = Object.entries(this._def.columns) as [
      string,
      QColumn<QuestDBType, unknown, ColumnMeta>,
    ][];

    // Determine which columns appear in any row
    const allKeys = new Set<string>();
    for (const row of this._rows) {
      for (const key of Object.keys(row)) {
        if (row[key] !== undefined) {
          allKeys.add(key);
        }
      }
    }

    // Auto-inject now() for omitted designated timestamp columns
    let designatedCol: string | undefined;
    for (const [name, col] of colEntries) {
      if (col.meta.designated && !allKeys.has(name)) {
        designatedCol = name;
        break;
      }
    }

    const colNames: string[] = [];
    const colTypes: QuestDBType[] = [];

    for (const [name, col] of colEntries) {
      if (allKeys.has(name) || name === designatedCol) {
        colNames.push(name);
        colTypes.push(col.qdbType);
      }
    }

    const values = this._rows.map((row) =>
      colNames.map((name) => {
        if (name === designatedCol && row[name] === undefined) {
          return { _rawSQL: "now()" };
        }
        return row[name] ?? null;
      }),
    );

    const node: InsertNode = {
      kind: "insert",
      table: this._def.name,
      columns: colNames,
      values,
      columnTypes: colTypes,
    };

    return serializeInsert(node);
  }

  /** Execute the INSERT. */
  async execute(): Promise<{ count: number }> {
    if (this._rows.length === 0) {
      return { count: 0 };
    }
    const sql = this.toSQL();
    await this._client.exec(sql);
    return { count: this._rows.length };
  }
}
