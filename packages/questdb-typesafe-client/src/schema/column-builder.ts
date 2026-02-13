import { z } from "zod/v4";
import type { QColumn, QuestDBType, ColumnMeta, QuestDBColumnMeta } from "../types/column.ts";

// ---------------------------------------------------------------------------
// Internal helper
// ---------------------------------------------------------------------------

function col<T extends QuestDBType, TTS, TMeta extends ColumnMeta = ColumnMeta>(
  qdbType: T,
  schema: z.ZodType<TTS>,
  meta: TMeta = {} as TMeta,
): QColumn<T, TTS, TMeta> {
  return {
    _brand: "QColumn" as const,
    qdbType,
    schema,
    meta,
  };
}

// ---------------------------------------------------------------------------
// Symbol builder with chained options
// ---------------------------------------------------------------------------

export interface SymbolColumnBuilder {
  capacity(n: number): SymbolColumnBuilder;
  cache(): SymbolColumnBuilder;
  nocache(): SymbolColumnBuilder;
  index(capacity?: number): SymbolColumnBuilder;
  build(): QColumn<"symbol", string, ColumnMeta>;
}

function symbolBuilder(initial: ColumnMeta = {}): SymbolColumnBuilder {
  let meta = { ...initial };
  const builder: SymbolColumnBuilder = {
    capacity(n) {
      meta = { ...meta, symbolCapacity: n };
      return builder;
    },
    cache() {
      meta = { ...meta, symbolCache: true };
      return builder;
    },
    nocache() {
      meta = { ...meta, symbolCache: false };
      return builder;
    },
    index(cap?) {
      meta = { ...meta, symbolIndex: true, symbolIndexCapacity: cap };
      return builder;
    },
    build() {
      return col("symbol", z.string(), meta);
    },
  };
  return builder;
}

// ---------------------------------------------------------------------------
// Public q namespace — column type builders
// ---------------------------------------------------------------------------

/**
 * Column type builders for QuestDB schema definitions.
 *
 * Usage:
 * ```ts
 * const schema = defineTable({
 *   name: "sensors",
 *   columns: {
 *     ts:       q.timestamp.designated(),
 *     source:   q.symbol(),
 *     power_kw: q.double(),
 *   },
 *   partitionBy: "DAY",
 * });
 * ```
 */
export const q = {
  // --- Non-nullable types (boolean, byte, short) ---
  boolean: () => col("boolean", z.boolean()),
  byte: () => col("byte", z.number().int().min(-128).max(127)),
  short: () => col("short", z.number().int().min(-32768).max(32767)),

  // --- Nullable numeric types ---
  char: () => col("char", z.string().max(1)),
  int: () => col("int", z.number().int()),
  float: () => col("float", z.number()),
  long: () => col("long", z.bigint()),
  double: () => col("double", z.number()),
  decimal: () => col("decimal", z.string()),

  // --- Temporal types ---
  date: () => col("date", z.date()),

  /**
   * Timestamp column (microsecond precision).
   *
   * - `q.timestamp()` — regular timestamp column
   * - `q.timestamp.designated()` — designated timestamp (determines sort order)
   * - `q.timestamp.ns()` — nanosecond precision timestamp
   */
  timestamp: Object.assign(() => col("timestamp", z.date()), {
    designated: () => col("timestamp", z.date(), { designated: true } as const),
    ns: () => col("timestamp_ns", z.bigint()),
  }),

  // --- String types ---

  /**
   * Symbol column (dictionary-encoded, optimized for low-cardinality strings).
   *
   * - `q.symbol()` — simple symbol column
   * - `q.symbol.options().capacity(256).cache().index().build()` — with options
   */
  symbol: Object.assign(() => col("symbol", z.string()), {
    options: () => symbolBuilder(),
  }),

  varchar: () => col("varchar", z.string()),
  /** @deprecated Use varchar instead — string type uses UTF-16 in QuestDB */
  string: () => col("string", z.string()),

  // --- Special types ---
  uuid: () => col("uuid", z.uuid()),
  ipv4: () => col("ipv4", z.ipv4()),
  binary: () => col("binary", z.instanceof(Uint8Array)),
  long256: () => col("long256", z.string()),

  /**
   * Geohash column with precision in bits.
   * @param bits Number of bits (1-60)
   */
  geohash: (bits: number) => col("geohash", z.string(), { geohashBits: bits }),

  /**
   * Array column (QuestDB only supports numeric element types).
   * @param elementType The element type ("int" | "float" | "double" | "long" | "short")
   */
  array: (elementType: "int" | "float" | "double" | "long" | "short") =>
    col("array", z.array(z.number()), { arrayType: elementType }),
} as const;

// ---------------------------------------------------------------------------
// Zod schema → QColumn conversion
// ---------------------------------------------------------------------------

/**
 * Unwrap Zod wrapper types (optional, nullable, readonly, default, catch)
 * to find the base schema type, preserving metadata from the outermost layer.
 * @internal
 */
function unwrapZod(schema: z.ZodType): z.ZodType {
  const t = (schema as any).type as string;
  if (t === "optional" || t === "nullable" || t === "readonly" || t === "default" || t === "catch") {
    return unwrapZod((schema as any).unwrap());
  }
  return schema;
}

/**
 * Convert a single Zod field schema to a QColumn.
 * Uses `schema.type` for the base Zod kind and `.meta()` for QuestDB overrides.
 * @internal
 */
function zodFieldToQColumn(schema: z.ZodType): QColumn<QuestDBType, unknown, ColumnMeta> {
  // Read meta from the original (possibly wrapped) schema first
  const qdbMeta: QuestDBColumnMeta = (schema.meta() as QuestDBColumnMeta) ?? {};

  // Unwrap optional/nullable wrappers to get the base type
  const base = unwrapZod(schema);
  // If meta was on the wrapper, also check the base
  const baseMeta: QuestDBColumnMeta = (base.meta() as QuestDBColumnMeta) ?? {};
  const meta = { ...baseMeta, ...qdbMeta };

  const baseType = (base as any).type as string;

  switch (baseType) {
    case "boolean":
      return col("boolean", base);

    case "number":
      if (meta.int) return col("int", base);
      if (meta.float) return col("float", base);
      return col("double", base);

    case "string":
      if (meta.symbol) return col("symbol", base);
      return col("varchar", base);

    case "date":
      if (meta.designated) return col("timestamp", base, { designated: true } as const);
      return col("timestamp", base);

    case "bigint":
      return col("long", base);

    default:
      throw new Error(
        `Unsupported Zod type "${baseType}" for QuestDB column mapping. ` +
          `Supported types: boolean, number, string, date, bigint.`,
      );
  }
}

/**
 * Convert a Zod v4 object schema to a ColumnsDefinition.
 *
 * Each field in the Zod object is mapped to a QColumn using default rules,
 * with optional `.meta()` annotations for QuestDB-specific overrides:
 *
 * - `z.date().meta({ designated: true })` → designated timestamp
 * - `z.string().meta({ symbol: true })` → SYMBOL column
 * - `z.number().meta({ int: true })` → INT column
 * - `z.number().meta({ float: true })` → FLOAT column
 *
 * @param schema A `z.object(...)` schema
 * @returns A ColumnsDefinition suitable for TableDef
 */
export function zodToColumns(schema: z.ZodObject<any>): Record<string, QColumn<QuestDBType, unknown, ColumnMeta>> {
  const shape = schema.shape as Record<string, z.ZodType>;
  const columns: Record<string, QColumn<QuestDBType, unknown, ColumnMeta>> = {};
  for (const [key, fieldSchema] of Object.entries(shape)) {
    columns[key] = zodFieldToQColumn(fieldSchema);
  }
  return columns;
}
