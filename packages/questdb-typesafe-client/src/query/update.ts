import type { QuestDBClient } from "../client/connection.ts";
import { serializeUpdate } from "../sql/serialize.ts";
import type { InferUpdateRow, TableDefLike } from "../types/infer.ts";
import type { SqlExpr, UpdateNode } from "../types/sql.ts";
import { type ColumnExprs, buildColumnExprs } from "./expression.ts";

/**
 * Type-safe UPDATE query builder.
 */
export class UpdateBuilder<TDef extends TableDefLike> {
  /** @internal */ readonly _client: QuestDBClient;
  /** @internal */ readonly _def: TDef;
  /** @internal */ _node: UpdateNode;

  constructor(client: QuestDBClient, def: TDef) {
    this._client = client;
    this._def = def;
    this._node = {
      kind: "update",
      table: def.name,
      set: [],
      where: null,
    };
  }

  /**
   * Set column values.
   * Designated timestamp is excluded from the type.
   */
  set(values: InferUpdateRow<TDef>): this {
    const valuesObj = values as Record<string, unknown>;
    for (const [key, value] of Object.entries(valuesObj)) {
      if (value !== undefined) {
        // Determine the QDB type for this column
        const col = this._def.columns[key];
        const qdbType = col ? col.qdbType : "varchar";
        this._node.set.push({
          column: key,
          value: { kind: "literal", value, type: qdbType },
        });
      }
    }
    return this;
  }

  /**
   * UPDATE ... FROM — QuestDB supports UPDATE with a FROM clause.
   */
  from<TFrom extends TableDefLike>(table: TFrom, alias?: string): this {
    this._node.from = { kind: "table", name: table.name, alias };
    return this;
  }

  /**
   * WHERE clause (should be set for safety).
   */
  where(fn: (cols: ColumnExprs<TDef>) => SqlExpr): this {
    const exprs = buildColumnExprs(this._def);
    this._node.where = fn(exprs);
    return this;
  }

  /** Generate the SQL string. */
  toSQL(): string {
    return serializeUpdate(this._node);
  }

  /** Execute the UPDATE. */
  async execute(): Promise<{ updated: number }> {
    const sql = this.toSQL();
    const response = await this._client.exec(sql);
    return { updated: response.updated ?? 0 };
  }
}
