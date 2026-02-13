// ---------------------------------------------------------------------------
// Schema definition
// ---------------------------------------------------------------------------
export { q, zodToColumns } from "./schema/column-builder";
export { defineTable } from "./schema/define";
export { Table } from "./schema/table";

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------
export { QuestDBClient } from "./client/connection";
export { QuestDBError, QuestDBConnectionError } from "./client/error";

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------
export { fn, and, or, not, raw } from "./query/expression";

// ---------------------------------------------------------------------------
// Builders (for advanced usage)
// ---------------------------------------------------------------------------
export { SelectBuilder } from "./query/select";
export { InsertBuilder } from "./query/insert";
export { UpdateBuilder } from "./query/update";
export { CreateBuilder } from "./ddl/create";
export { AlterBuilder } from "./ddl/alter";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
export type { QuestDBClientConfig, ImportOptions } from "./client/config";
export type { QuestDBExecResponse, QuestDBImportResponse } from "./client/connection";
export type {
  QColumn,
  QuestDBType,
  ColumnMeta,
  QuestDBColumnMeta,
  TSTypeMap,
  NonNullableQDBType,
} from "./types/column";
export type { TableDef, TableConfig, SchemaTableConfig, PartitionBy, ColumnsDefinition } from "./schema/define";
export type {
  TableDefLike,
  InferRow,
  InferInsertRow,
  InferUpdateRow,
  InferColumnType,
  InferColumnInsertType,
  PickColumns,
  JoinRow,
} from "./types/infer";
export type {
  SqlExpr,
  SelectNode,
  InsertNode,
  UpdateNode,
  FillStrategy,
  JoinType,
  LatestOnClause,
  SampleByClause,
  FromClause,
  JoinClause,
} from "./types/sql";
export type { ColumnExpr, ColumnExprs } from "./query/expression";
export type { ColumnInfo } from "./ddl/describe";
export type { SymbolColumnBuilder } from "./schema/column-builder";
