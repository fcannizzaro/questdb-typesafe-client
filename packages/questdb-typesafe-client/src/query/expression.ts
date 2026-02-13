import type { SqlExpr } from "../types/sql.ts";
import type { QuestDBType } from "../types/column.ts";
import type { TableDefLike, InferRow } from "../types/infer.ts";

// ---------------------------------------------------------------------------
// Column expression — typed comparison operators for WHERE/ON
// ---------------------------------------------------------------------------

/**
 * A typed column expression providing comparison operators.
 * Used in WHERE/ON callbacks.
 */
export interface ColumnExpr<T = unknown> {
  /** Equal: column = value */
  eq(value: NonNullable<T>): SqlExpr;
  /** Not equal: column != value */
  neq(value: NonNullable<T>): SqlExpr;
  /** Greater than: column > value */
  gt(value: NonNullable<T>): SqlExpr;
  /** Greater than or equal: column >= value */
  gte(value: NonNullable<T>): SqlExpr;
  /** Less than: column < value */
  lt(value: NonNullable<T>): SqlExpr;
  /** Less than or equal: column <= value */
  lte(value: NonNullable<T>): SqlExpr;
  /** IN list: column IN (values) */
  in(values: NonNullable<T>[]): SqlExpr;
  /** IS NULL */
  isNull(): SqlExpr;
  /** IS NOT NULL */
  isNotNull(): SqlExpr;
  /** BETWEEN: column BETWEEN low AND high */
  between(low: NonNullable<T>, high: NonNullable<T>): SqlExpr;
  /** LIKE pattern match (string columns) */
  like(pattern: string): SqlExpr;
  /** Regex match: column ~ pattern (QuestDB Java regex) */
  matches(pattern: string): SqlExpr;
  /** Raw column reference (for JOIN ON clauses) */
  ref(): SqlExpr;
}

/**
 * Mapped type: column names → ColumnExpr with the column's inferred type.
 */
export type ColumnExprs<TDef extends TableDefLike> = {
  [K in keyof InferRow<TDef> & string]: ColumnExpr<InferRow<TDef>[K]>;
};

// ---------------------------------------------------------------------------
// Build column expr at runtime
// ---------------------------------------------------------------------------

function inferQdbType(value: unknown): QuestDBType {
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "bigint") return "long";
  if (typeof value === "number") return "double";
  if (value instanceof Date) return "timestamp";
  return "varchar";
}

/**
 * Create a ColumnExpr for a single column.
 */
export function buildColumnExpr(name: string, table?: string): ColumnExpr {
  const colRef: SqlExpr = { kind: "column", name, table };

  return {
    eq(value) {
      return {
        kind: "binary",
        op: "=",
        left: colRef,
        right: { kind: "literal", value, type: inferQdbType(value) },
      };
    },
    neq(value) {
      return {
        kind: "binary",
        op: "!=",
        left: colRef,
        right: { kind: "literal", value, type: inferQdbType(value) },
      };
    },
    gt(value) {
      return {
        kind: "binary",
        op: ">",
        left: colRef,
        right: { kind: "literal", value, type: inferQdbType(value) },
      };
    },
    gte(value) {
      return {
        kind: "binary",
        op: ">=",
        left: colRef,
        right: { kind: "literal", value, type: inferQdbType(value) },
      };
    },
    lt(value) {
      return {
        kind: "binary",
        op: "<",
        left: colRef,
        right: { kind: "literal", value, type: inferQdbType(value) },
      };
    },
    lte(value) {
      return {
        kind: "binary",
        op: "<=",
        left: colRef,
        right: { kind: "literal", value, type: inferQdbType(value) },
      };
    },
    in(values) {
      return {
        kind: "in_list",
        column: name,
        values: values.map((v) => ({
          kind: "literal" as const,
          value: v,
          type: inferQdbType(v),
        })),
      };
    },
    isNull() {
      return { kind: "is_null", column: name, negated: false };
    },
    isNotNull() {
      return { kind: "is_null", column: name, negated: true };
    },
    between(low, high) {
      return {
        kind: "between",
        column: name,
        low: { kind: "literal", value: low, type: inferQdbType(low) },
        high: { kind: "literal", value: high, type: inferQdbType(high) },
      };
    },
    like(pattern) {
      return {
        kind: "binary",
        op: "LIKE",
        left: colRef,
        right: { kind: "literal", value: pattern, type: "varchar" },
      };
    },
    matches(pattern) {
      return {
        kind: "binary",
        op: "~",
        left: colRef,
        right: { kind: "literal", value: pattern, type: "varchar" },
      };
    },
    ref() {
      return colRef;
    },
  };
}

/**
 * Build ColumnExprs object for all columns in a table definition.
 */
export function buildColumnExprs<TDef extends TableDefLike>(
  def: TDef,
  tableAlias?: string,
): ColumnExprs<TDef> {
  const exprs: Record<string, ColumnExpr> = {};
  for (const name of Object.keys(def.columns)) {
    exprs[name] = buildColumnExpr(name, tableAlias);
  }
  return exprs as ColumnExprs<TDef>;
}

// ---------------------------------------------------------------------------
// Logical combinators
// ---------------------------------------------------------------------------

/** Combine expressions with AND */
export function and(...exprs: SqlExpr[]): SqlExpr {
  if (exprs.length === 0) throw new Error("and() requires at least one expression");
  if (exprs.length === 1) return exprs[0]!;
  return exprs.reduce((acc, expr) => ({
    kind: "binary",
    op: "AND",
    left: acc,
    right: expr,
  }));
}

/** Combine expressions with OR */
export function or(...exprs: SqlExpr[]): SqlExpr {
  if (exprs.length === 0) throw new Error("or() requires at least one expression");
  if (exprs.length === 1) return exprs[0]!;
  return exprs.reduce((acc, expr) => ({
    kind: "binary",
    op: "OR",
    left: acc,
    right: expr,
  }));
}

/** Negate an expression with NOT */
export function not(expr: SqlExpr): SqlExpr {
  return { kind: "unary", op: "NOT", operand: expr };
}

// ---------------------------------------------------------------------------
// Aggregate function helpers
// ---------------------------------------------------------------------------

function agg(name: string, col?: string, alias?: string): SqlExpr {
  const args: SqlExpr[] = col ? [{ kind: "column", name: col }] : [];
  return { kind: "aggregate", name, args, alias };
}

/** SQL aggregate function builders */
export const fn = {
  /** COUNT(*) or COUNT(column) */
  count: (col?: string, alias?: string) =>
    col
      ? agg("count", col, alias)
      : {
          kind: "aggregate" as const,
          name: "count",
          args: [{ kind: "raw" as const, sql: "*" }],
          alias,
        },

  /** SUM(column) */
  sum: (col: string, alias?: string) => agg("sum", col, alias),

  /** AVG(column) */
  avg: (col: string, alias?: string) => agg("avg", col, alias),

  /** MIN(column) */
  min: (col: string, alias?: string) => agg("min", col, alias),

  /** MAX(column) */
  max: (col: string, alias?: string) => agg("max", col, alias),

  /** FIRST(column) — QuestDB specific */
  first: (col: string, alias?: string) => agg("first", col, alias),

  /** LAST(column) — QuestDB specific */
  last: (col: string, alias?: string) => agg("last", col, alias),

  /** COUNT_DISTINCT(column) */
  countDistinct: (col: string, alias?: string) => agg("count_distinct", col, alias),

  /** KSUM(column) — Kahan summation */
  ksum: (col: string, alias?: string) => agg("ksum", col, alias),

  /** NSUM(column) — Neumaier summation */
  nsum: (col: string, alias?: string) => agg("nsum", col, alias),
} as const;

// ---------------------------------------------------------------------------
// Raw SQL expression
// ---------------------------------------------------------------------------

/** Create a raw SQL expression (escape hatch) */
export function raw(sql: string): SqlExpr {
  return { kind: "raw", sql };
}
