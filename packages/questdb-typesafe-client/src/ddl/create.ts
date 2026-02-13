import type { QColumn, QuestDBType, ColumnMeta } from "../types/column.ts";
import type { TableDef, ColumnsDefinition } from "../schema/define.ts";
import type { QuestDBClient } from "../client/connection.ts";
import { escapeIdentifier } from "../sql/escape.ts";

/**
 * Map a QColumn to its SQL type string, including symbol options.
 */
export function mapQDBType(col: QColumn<QuestDBType, unknown, ColumnMeta>): string {
  switch (col.qdbType) {
    case "symbol": {
      let sql = "SYMBOL";
      if (col.meta.symbolCapacity !== undefined) {
        sql += ` CAPACITY ${col.meta.symbolCapacity}`;
      }
      if (col.meta.symbolCache === true) sql += " CACHE";
      if (col.meta.symbolCache === false) sql += " NOCACHE";
      if (col.meta.symbolIndex) {
        sql += " INDEX";
        if (col.meta.symbolIndexCapacity !== undefined) {
          sql += ` CAPACITY ${col.meta.symbolIndexCapacity}`;
        }
      }
      return sql;
    }
    case "geohash": {
      const bits = col.meta.geohashBits ?? 20;
      return `GEOHASH(${bits}b)`;
    }
    case "array": {
      const elem = col.meta.arrayType ?? "double";
      return `${elem.toUpperCase()}[]`;
    }
    case "timestamp_ns":
      return "TIMESTAMP";
    default:
      return col.qdbType.toUpperCase();
  }
}

/**
 * Find the designated timestamp column name from a columns definition.
 */
export function findDesignatedColumn(columns: ColumnsDefinition): string | null {
  for (const [name, col] of Object.entries(columns)) {
    if (col.meta.designated) return name;
  }
  return null;
}

/**
 * Builder for CREATE TABLE statements.
 */
export class CreateBuilder<TDef extends TableDef> {
  private readonly client: QuestDBClient;
  private readonly def: TDef;
  private ifNotExistsFlag = false;

  constructor(client: QuestDBClient, def: TDef) {
    this.client = client;
    this.def = def;
  }

  ifNotExists(): this {
    this.ifNotExistsFlag = true;
    return this;
  }

  toSQL(): string {
    const cols = Object.entries(this.def.columns).map(([name, col]) => {
      return `  ${escapeIdentifier(name)} ${mapQDBType(col)}`;
    });

    let sql = `CREATE TABLE`;
    if (this.ifNotExistsFlag) sql += ` IF NOT EXISTS`;
    sql += ` ${escapeIdentifier(this.def.name)} (\n`;
    sql += cols.join(",\n");
    sql += "\n)";

    // Designated timestamp
    const designated = findDesignatedColumn(this.def.columns);
    if (designated) {
      sql += ` timestamp(${escapeIdentifier(designated)})`;
    }

    // Partition
    sql += ` PARTITION BY ${this.def.partitionBy}`;

    // WAL
    sql += this.def.wal ? " WAL" : " BYPASS WAL";

    // Dedup
    if (this.def.dedupKeys.length > 0) {
      sql += ` DEDUP UPSERT KEYS(${this.def.dedupKeys.map(escapeIdentifier).join(", ")})`;
    }

    // WITH parameters
    const withParams: string[] = [];
    if (this.def.maxUncommittedRows !== undefined) {
      withParams.push(`maxUncommittedRows=${this.def.maxUncommittedRows}`);
    }
    if (this.def.o3MaxLag !== undefined) {
      withParams.push(`o3MaxLag=${this.def.o3MaxLag}`);
    }
    if (withParams.length > 0) {
      sql += `\nWITH ${withParams.join(", ")}`;
    }

    return sql;
  }

  async execute(): Promise<void> {
    const sql = this.toSQL();
    await this.client.exec(sql);

    // TTL is set via ALTER TABLE after creation
    if (this.def.ttl) {
      await this.client.exec(
        `ALTER TABLE ${escapeIdentifier(this.def.name)} SET PARAM ttl = '${this.def.ttl}'`,
      );
    }
  }
}
