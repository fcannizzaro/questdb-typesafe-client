import type { QuestDBType } from "./column.ts";

// ---------------------------------------------------------------------------
// SQL Expressions (AST)
// ---------------------------------------------------------------------------

export type SqlExpr =
  | { kind: "column"; table?: string; name: string }
  | { kind: "literal"; value: unknown; type: QuestDBType }
  | { kind: "binary"; op: string; left: SqlExpr; right: SqlExpr }
  | { kind: "unary"; op: string; operand: SqlExpr }
  | { kind: "function"; name: string; args: SqlExpr[] }
  | { kind: "aggregate"; name: string; args: SqlExpr[]; alias?: string }
  | { kind: "subquery"; sql: SelectNode }
  | { kind: "timestamp_in"; column: string; interval: string }
  | { kind: "in_list"; column: string; values: SqlExpr[] }
  | { kind: "between"; column: string; low: SqlExpr; high: SqlExpr }
  | { kind: "is_null"; column: string; negated: boolean }
  | { kind: "raw"; sql: string };

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

export interface SelectNode {
  kind: "select";
  distinct: boolean;
  columns: Array<{ expr: SqlExpr; alias?: string }>;
  from: FromClause;
  joins: JoinClause[];
  where: SqlExpr | null;
  groupBy: SqlExpr[];
  orderBy: Array<{ expr: SqlExpr; direction: "ASC" | "DESC" }>;
  limit: { count: number; offset?: number } | null;
  sampleBy: SampleByClause | null;
  latestOn: LatestOnClause | null;
}

export interface SampleByClause {
  interval: string;
  fill: FillStrategy[];
  align?: "CALENDAR" | "FIRST OBSERVATION";
}

export type FillStrategy = "NONE" | "NULL" | "PREV" | "LINEAR" | { constant: unknown };

export interface LatestOnClause {
  timestamp: string;
  partitionBy: string[];
}

// ---------------------------------------------------------------------------
// JOIN
// ---------------------------------------------------------------------------

export type JoinType = "INNER" | "LEFT" | "CROSS" | "ASOF" | "LT" | "SPLICE";

export interface JoinClause {
  type: JoinType;
  table: FromClause;
  on: SqlExpr | null;
  tolerance?: string;
}

// ---------------------------------------------------------------------------
// FROM
// ---------------------------------------------------------------------------

export type FromClause =
  | { kind: "table"; name: string; alias?: string }
  | { kind: "subquery"; select: SelectNode; alias: string };

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

export interface InsertNode {
  kind: "insert";
  table: string;
  columns: string[];
  values: unknown[][];
  columnTypes: QuestDBType[];
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

export interface UpdateNode {
  kind: "update";
  table: string;
  set: Array<{ column: string; value: SqlExpr }>;
  from?: FromClause;
  where: SqlExpr | null;
}
