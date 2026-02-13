import type { z } from "zod/v4";

/**
 * All QuestDB native types as a string literal union.
 */
export type QuestDBType =
  | "boolean"
  | "byte"
  | "short"
  | "char"
  | "int"
  | "float"
  | "long"
  | "double"
  | "decimal"
  | "date"
  | "timestamp"
  | "timestamp_ns"
  | "symbol"
  | "varchar"
  | "string"
  | "uuid"
  | "ipv4"
  | "binary"
  | "long256"
  | "geohash"
  | "array";

/**
 * Map from QuestDB type to TypeScript representation.
 * This is the canonical mapping used for type inference.
 */
export interface TSTypeMap {
  boolean: boolean;
  byte: number;
  short: number;
  char: string;
  int: number;
  float: number;
  long: bigint;
  double: number;
  decimal: string;
  date: Date;
  timestamp: Date;
  timestamp_ns: bigint;
  symbol: string;
  varchar: string;
  string: string;
  uuid: string;
  ipv4: string;
  binary: Uint8Array;
  long256: string;
  geohash: string;
  array: number[];
}

/**
 * Types that are NOT nullable in QuestDB.
 * boolean defaults to false, byte/short default to 0.
 */
export type NonNullableQDBType = "boolean" | "byte" | "short";

/**
 * QuestDB-specific metadata for Zod `.meta()` annotations.
 *
 * Use these on individual Zod fields when defining a table via `schema`:
 * ```ts
 * z.date().meta({ designated: true })
 * z.string().meta({ symbol: true })
 * z.number().meta({ int: true })
 * ```
 */
export interface QuestDBColumnMeta {
  /** Mark a `z.date()` field as the designated timestamp */
  designated?: boolean;
  /** Map a `z.string()` field to QuestDB SYMBOL instead of VARCHAR */
  symbol?: boolean;
  /** Map a `z.number()` field to QuestDB INT instead of DOUBLE */
  int?: boolean;
  /** Map a `z.number()` field to QuestDB FLOAT instead of DOUBLE */
  float?: boolean;
}

/**
 * Metadata that can be attached to a column definition.
 * All fields optional — only relevant ones are set by builders.
 */
export interface ColumnMeta {
  designated?: boolean;
  symbolCapacity?: number;
  symbolCache?: boolean;
  symbolIndex?: boolean;
  symbolIndexCapacity?: number;
  geohashBits?: number;
  geohashUnit?: "b" | "c";
  arrayType?: QuestDBType;
}

/**
 * The core column definition wrapper.
 *
 * TType: the QuestDB type literal (e.g. "timestamp", "symbol")
 * TTS: the inferred TypeScript type (e.g. Date, string)
 * TMeta: additional metadata (designated, symbol options, etc.)
 */
export interface QColumn<
  TType extends QuestDBType = QuestDBType,
  TTS = unknown,
  TMeta extends ColumnMeta = ColumnMeta,
> {
  readonly _brand: "QColumn";
  readonly qdbType: TType;
  readonly schema: z.ZodType<TTS>;
  readonly meta: TMeta;
}
