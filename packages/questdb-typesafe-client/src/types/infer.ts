import type { QColumn, QuestDBType, NonNullableQDBType, ColumnMeta } from "./column.ts";
import type { Prettify } from "../util/types.ts";

// ---------------------------------------------------------------------------
// Re-export ColumnsDefinition locally to avoid circular import
// ---------------------------------------------------------------------------

type AnyQColumn = QColumn<QuestDBType, any, ColumnMeta>;
type AnyColumnsDefinition = Record<string, AnyQColumn>;

/** Generic TableDef shape for inference (avoids circular import) */
export interface TableDefLike {
  readonly _brand: "TableDef";
  readonly name: string;
  readonly columns: AnyColumnsDefinition;
  readonly partitionBy: string;
  readonly wal: boolean;
  readonly dedupKeys: string[];
  readonly ttl: string | undefined;
  readonly maxUncommittedRows: number | undefined;
  readonly o3MaxLag: string | undefined;
}

// ---------------------------------------------------------------------------
// Single column → TypeScript type
// ---------------------------------------------------------------------------

/**
 * Infer the TS type from a single QColumn for read (SELECT) operations.
 *
 * Non-nullable QDB types (boolean, byte, short) return the raw type.
 * All other types return T | null (QuestDB has no NOT NULL constraint).
 */
export type InferColumnType<C> =
  C extends QColumn<infer TType, infer TTS, any>
    ? TType extends NonNullableQDBType
      ? TTS
      : TTS | null
    : never;

/**
 * Infer the insert type for a single column.
 *
 * - Designated timestamp is optional (server can auto-assign)
 * - Non-nullable types are required
 * - All nullable columns accept undefined (omit from insert)
 */
export type InferColumnInsertType<C> =
  C extends QColumn<infer TType, infer TTS, infer TMeta>
    ? TMeta extends { designated: true }
      ? TTS | undefined
      : TType extends NonNullableQDBType
        ? TTS
        : TTS | null | undefined
    : never;

// ---------------------------------------------------------------------------
// Full row types
// ---------------------------------------------------------------------------

/**
 * Row — the type returned by SELECT * (all columns, with nullability).
 */
export type InferRow<TDef extends TableDefLike> = Prettify<{
  [K in keyof TDef["columns"]]: InferColumnType<TDef["columns"][K]>;
}>;

/**
 * InsertRow — the type accepted by INSERT.
 * Designated timestamp and nullable columns are optional.
 */
export type InferInsertRow<TDef extends TableDefLike> = Prettify<
  // Required keys: non-nullable, non-designated
  {
    [K in keyof TDef["columns"] as undefined extends InferColumnInsertType<TDef["columns"][K]>
      ? never
      : K]: InferColumnInsertType<TDef["columns"][K]>;
  } &
    // Optional keys: nullable or designated
    {
      [K in keyof TDef["columns"] as undefined extends InferColumnInsertType<TDef["columns"][K]>
        ? K
        : never]?: Exclude<InferColumnInsertType<TDef["columns"][K]>, undefined>;
    }
>;

/**
 * UpdateRow — Partial of all non-designated columns.
 * You cannot UPDATE a designated timestamp in QuestDB.
 */
export type InferUpdateRow<TDef extends TableDefLike> = Prettify<
  Partial<{
    [K in keyof TDef["columns"] as TDef["columns"][K] extends QColumn<
      "timestamp",
      any,
      { designated: true }
    >
      ? never
      : K]: InferColumnType<TDef["columns"][K]>;
  }>
>;

// ---------------------------------------------------------------------------
// Partial row (column subset selection)
// ---------------------------------------------------------------------------

/**
 * Pick specific columns from a Row type, preserving nullability.
 */
export type PickColumns<TDef extends TableDefLike, TKeys extends keyof InferRow<TDef>> = Prettify<
  Pick<InferRow<TDef>, TKeys>
>;

// ---------------------------------------------------------------------------
// Join result type
// ---------------------------------------------------------------------------

/**
 * Merge two row types with table-qualified prefixed keys for JOINs.
 * For ASOF/LT/LEFT joins, the right-side columns become nullable.
 */
export type JoinRow<
  TLeft extends Record<string, unknown>,
  TRight extends Record<string, unknown>,
  TRightAlias extends string,
  TNullableRight extends boolean = false,
> = Prettify<
  TLeft & {
    [K in keyof TRight as `${TRightAlias}.${K & string}`]: TNullableRight extends true
      ? TRight[K] | null
      : TRight[K];
  }
>;
