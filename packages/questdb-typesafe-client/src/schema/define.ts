import type { z } from "zod/v4";
import type { QColumn, QuestDBType, ColumnMeta } from "../types/column.ts";
import type { StringKeyOf } from "../util/types.ts";
import { zodToColumns } from "./column-builder.ts";

// ---------------------------------------------------------------------------
// Partition & table config
// ---------------------------------------------------------------------------

export type PartitionBy = "NONE" | "HOUR" | "DAY" | "WEEK" | "MONTH" | "YEAR";

/**
 * A plain object mapping column names to QColumn definitions.
 */
export type ColumnsDefinition = Record<string, QColumn<QuestDBType, any, ColumnMeta>>;

// ---------------------------------------------------------------------------
// Zod → QColumn type-level mapping
// ---------------------------------------------------------------------------

/**
 * Map a single Zod type to its default QColumn representation at the type level.
 *
 * > `.meta()` annotations (designated, symbol, int, float) are only applied at
 * > runtime — they do not alter the compile-time column type.
 */
type ZodToQColumn<T extends z.ZodType> =
  T extends z.ZodBoolean
    ? QColumn<"boolean", boolean, ColumnMeta>
    : T extends z.ZodNumber
      ? QColumn<"double", number, ColumnMeta>
      : T extends z.ZodString
        ? QColumn<"varchar", string, ColumnMeta>
        : T extends z.ZodDate
          ? QColumn<"timestamp", Date, ColumnMeta>
          : T extends z.ZodBigInt
            ? QColumn<"long", bigint, ColumnMeta>
            : T extends z.ZodOptional<infer U extends z.ZodType>
              ? ZodToQColumn<U>
              : T extends z.ZodNullable<infer U extends z.ZodType>
                ? ZodToQColumn<U>
                : QColumn<QuestDBType, z.output<T>, ColumnMeta>;

/**
 * Convert an entire Zod object shape to a ColumnsDefinition at the type level.
 */
type ZodShapeToColumns<TShape extends Record<string, z.ZodType>> = {
  [K in keyof TShape]: ZodToQColumn<TShape[K]>;
};

// ---------------------------------------------------------------------------
// Table configs
// ---------------------------------------------------------------------------

/**
 * Table configuration passed to defineTable() — columns-based variant.
 */
export interface TableConfig<TCols extends ColumnsDefinition> {
  /** Table name in QuestDB */
  name: string;
  /** Column definitions using q.* builders */
  columns: TCols;
  /** Not allowed when using columns */
  schema?: never;
  /** Partition strategy (immutable after creation). Default: DAY */
  partitionBy?: PartitionBy;
  /** Enable Write-Ahead Log. Default: true */
  wal?: boolean;
  /** Deduplication upsert keys (column names). Requires WAL. */
  dedupKeys?: Array<StringKeyOf<TCols>>;
  /** TTL expiration interval string, e.g. '30d', '12h' */
  ttl?: string;
  /** Max uncommitted rows for ingestion performance */
  maxUncommittedRows?: number;
  /** Out-of-order commit lag, e.g. '1s', '500ms' */
  o3MaxLag?: string;
}

/**
 * Table configuration passed to defineTable() — Zod schema-based variant.
 *
 * Use `.meta()` on individual Zod fields for QuestDB-specific overrides:
 * - `z.date().meta({ designated: true })` → designated timestamp
 * - `z.string().meta({ symbol: true })` → SYMBOL column
 * - `z.number().meta({ int: true })` → INT column
 * - `z.number().meta({ float: true })` → FLOAT column
 */
export interface SchemaTableConfig<TSchema extends z.ZodObject<any>> {
  /** Table name in QuestDB */
  name: string;
  /** Not allowed when using schema */
  columns?: never;
  /** Zod v4 object schema defining the table columns */
  schema: TSchema;
  /** Partition strategy (immutable after creation). Default: DAY */
  partitionBy?: PartitionBy;
  /** Enable Write-Ahead Log. Default: true */
  wal?: boolean;
  /** Deduplication upsert keys (column names). Requires WAL. */
  dedupKeys?: Array<StringKeyOf<ZodShapeToColumns<TSchema["shape"]>>>;
  /** TTL expiration interval string, e.g. '30d', '12h' */
  ttl?: string;
  /** Max uncommitted rows for ingestion performance */
  maxUncommittedRows?: number;
  /** Out-of-order commit lag, e.g. '1s', '500ms' */
  o3MaxLag?: string;
}

// ---------------------------------------------------------------------------
// Designated timestamp validation
// ---------------------------------------------------------------------------

/** Extract column names where meta.designated is true */
type DesignatedColumns<T extends ColumnsDefinition> = {
  [K in keyof T]: T[K] extends QColumn<"timestamp", any, infer M>
    ? M extends { designated: true }
      ? K
      : never
    : never;
}[keyof T];

/** Enforce that a table with partitioning has a designated timestamp */
type ValidateDesignated<T extends ColumnsDefinition, P extends string> = P extends "NONE"
  ? unknown
  : DesignatedColumns<T> extends never
    ? {
        __error: "Tables with partitioning require a designated timestamp column (use q.timestamp.designated())";
      }
    : unknown;

// ---------------------------------------------------------------------------
// TableDef — return type of defineTable()
// ---------------------------------------------------------------------------

export interface TableDef<
  TName extends string = string,
  TCols extends ColumnsDefinition = ColumnsDefinition,
  TPartition extends string = string,
> {
  readonly _brand: "TableDef";
  readonly name: TName;
  readonly columns: TCols;
  readonly partitionBy: TPartition;
  readonly wal: boolean;
  readonly dedupKeys: string[];
  readonly ttl: string | undefined;
  readonly maxUncommittedRows: number | undefined;
  readonly o3MaxLag: string | undefined;
}

// ---------------------------------------------------------------------------
// defineTable() — the entry point
// ---------------------------------------------------------------------------

/**
 * Define a typed table schema using `q.*` column builders.
 */
export function defineTable<
  const TName extends string,
  const TCols extends ColumnsDefinition,
  const TPartition extends PartitionBy = "DAY",
>(
  config: TableConfig<TCols> & {
    name: TName;
    partitionBy?: TPartition;
  } & ValidateDesignated<TCols, TPartition>,
): TableDef<TName, TCols, TPartition>;

/**
 * Define a typed table schema from a Zod v4 object schema.
 *
 * Use `.meta()` on Zod fields for QuestDB-specific overrides:
 * ```ts
 * defineTable({
 *   name: "readings",
 *   schema: z.object({
 *     ts: z.date().meta({ designated: true }),
 *     source: z.string().meta({ symbol: true }),
 *     value: z.number(),
 *   }),
 * });
 * ```
 */
export function defineTable<
  const TName extends string,
  const TSchema extends z.ZodObject<any>,
  const TPartition extends PartitionBy = "DAY",
>(
  config: SchemaTableConfig<TSchema> & {
    name: TName;
    partitionBy?: TPartition;
  },
): TableDef<TName, ZodShapeToColumns<TSchema["shape"]>, TPartition>;

// Implementation
export function defineTable(
  config: TableConfig<any> | SchemaTableConfig<any>,
): TableDef<string, ColumnsDefinition, string> {
  const columns = "schema" in config && config.schema ? zodToColumns(config.schema) : (config as any).columns;
  return {
    _brand: "TableDef" as const,
    name: config.name,
    columns,
    partitionBy: (config.partitionBy ?? "DAY") as string,
    wal: config.wal ?? true,
    dedupKeys: (config.dedupKeys ?? []) as string[],
    ttl: config.ttl,
    maxUncommittedRows: config.maxUncommittedRows,
    o3MaxLag: config.o3MaxLag,
  };
}
