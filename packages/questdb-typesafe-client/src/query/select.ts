import type { QuestDBClient } from "../client/connection.ts";
import { findDesignatedColumn } from "../ddl/create.ts";
import { serializeSelect } from "../sql/serialize.ts";
import type { QColumn } from "../types/column.ts";
import type { InferRow, TableDefLike } from "../types/infer.ts";
import { parseResultSet } from "../types/result.ts";
import type { FillStrategy, JoinType, SelectNode, SqlExpr } from "../types/sql.ts";
import type { Prettify } from "../util/types.ts";
import { type ColumnExprs, buildColumnExprs } from "./expression.ts";

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

/** Extract the designated timestamp column name from a TableDef */
type DesignatedTimestampColumn<TDef extends TableDefLike> = {
  [K in keyof TDef["columns"]]: TDef["columns"][K] extends QColumn<
    "timestamp",
    any,
    { designated: true }
  >
    ? K & string
    : never;
}[keyof TDef["columns"]];

// ---------------------------------------------------------------------------
// SelectBuilder
// ---------------------------------------------------------------------------

/**
 * Type-safe SELECT query builder.
 *
 * TDef — the source table definition (for column name checking)
 * TResult — the current result row type (narrows as you select columns)
 */
export class SelectBuilder<TDef extends TableDefLike, TResult = InferRow<TDef>> {
  /** @internal */ readonly _node: SelectNode;
  /** @internal */ readonly _client: QuestDBClient;
  /** @internal */ readonly _def: TDef;

  constructor(client: QuestDBClient, def: TDef, node?: Partial<SelectNode>) {
    this._client = client;
    this._def = def;
    this._node = {
      kind: "select",
      distinct: false,
      columns: [],
      from: { kind: "table", name: def.name },
      joins: [],
      where: null,
      groupBy: [],
      orderBy: [],
      limit: null,
      sampleBy: null,
      latestOn: null,
      ...node,
    };
  }

  private clone(patch: Partial<SelectNode>): SelectBuilder<TDef, TResult> {
    return new SelectBuilder<TDef, TResult>(this._client, this._def, {
      ...this._node,
      ...patch,
    });
  }

  // ---------------------------------------------------------------------------
  // DISTINCT
  // ---------------------------------------------------------------------------

  distinct(): SelectBuilder<TDef, TResult> {
    return this.clone({ distinct: true });
  }

  // ---------------------------------------------------------------------------
  // Column selection — narrows TResult
  // ---------------------------------------------------------------------------

  /**
   * Select specific columns. Narrows the result type.
   *
   * ```ts
   * table.select().columns("power_kw", "source")
   * // TResult narrows to { power_kw: number | null; source: string | null }
   * ```
   */
  columns<TKeys extends keyof InferRow<TDef> & string>(
    ...cols: TKeys[]
  ): SelectBuilder<TDef, Prettify<Pick<InferRow<TDef>, TKeys>>> {
    return new SelectBuilder(this._client, this._def, {
      ...this._node,
      columns: cols.map((c) => ({
        expr: { kind: "column" as const, name: c },
      })),
    }) as unknown as SelectBuilder<TDef, Prettify<Pick<InferRow<TDef>, TKeys>>>;
  }

  /**
   * Add a raw expression to the SELECT list (for aggregates, functions, etc.)
   */
  addExpr(expr: SqlExpr, alias?: string): SelectBuilder<TDef, TResult> {
    return this.clone({
      columns: [...this._node.columns, { expr, alias }],
    });
  }

  // ---------------------------------------------------------------------------
  // WHERE
  // ---------------------------------------------------------------------------

  /**
   * Add a WHERE clause using typed column expressions.
   *
   * ```ts
   * .where(c => c.source.eq("solar"))
   * .where(c => and(c.power_kw.gt(100), c.source.eq("solar")))
   * ```
   */
  where(fn: (cols: ColumnExprs<TDef>) => SqlExpr): SelectBuilder<TDef, TResult> {
    const exprs = buildColumnExprs(this._def);
    return this.clone({ where: fn(exprs) });
  }

  /**
   * Add an AND condition to an existing WHERE clause.
   * If no WHERE exists, this becomes the WHERE clause.
   */
  andWhere(fn: (cols: ColumnExprs<TDef>) => SqlExpr): SelectBuilder<TDef, TResult> {
    const exprs = buildColumnExprs(this._def);
    const newExpr = fn(exprs);
    const where = this._node.where
      ? { kind: "binary" as const, op: "AND", left: this._node.where, right: newExpr }
      : newExpr;
    return this.clone({ where });
  }

  /**
   * QuestDB timestamp WHERE shorthand.
   *
   * ```ts
   * .whereTimestamp("ts", "2026")            // WHERE ts IN '2026'
   * .whereTimestamp("ts", "2026-01;3M")      // WHERE ts IN '2026-01;3M'
   * ```
   */
  whereTimestamp(
    column: DesignatedTimestampColumn<TDef>,
    interval: string,
  ): SelectBuilder<TDef, TResult> {
    const expr: SqlExpr = { kind: "timestamp_in", column, interval };
    const where = this._node.where
      ? { kind: "binary" as const, op: "AND", left: this._node.where, right: expr }
      : expr;
    return this.clone({ where });
  }

  // ---------------------------------------------------------------------------
  // SAMPLE BY — time-series downsampling
  // ---------------------------------------------------------------------------

  /**
   * Time-series downsampling.
   *
   * ```ts
   * .sampleBy("1h")
   * .sampleBy("5m", "PREV")
   * .sampleBy("1d", ["NULL", "PREV", "LINEAR"], "CALENDAR")
   * ```
   */
  sampleBy(
    interval: string,
    fill?: FillStrategy | FillStrategy[],
    align?: "CALENDAR" | "FIRST OBSERVATION",
  ): SelectBuilder<TDef, TResult> {
    const fillArr = fill ? (Array.isArray(fill) ? fill : [fill]) : [];
    return this.clone({
      sampleBy: { interval, fill: fillArr, align },
    });
  }

  // ---------------------------------------------------------------------------
  // LATEST ON — latest record per partition key
  // ---------------------------------------------------------------------------

  /**
   * Get the latest record per partition key.
   *
   * ```ts
   * .latestOn("source")  // LATEST ON ts PARTITION BY source
   * ```
   */
  latestOn<TPartitionKeys extends keyof InferRow<TDef> & string>(
    ...partitionBy: TPartitionKeys[]
  ): SelectBuilder<TDef, TResult> {
    const designated = findDesignatedColumn(this._def.columns);
    if (!designated) {
      throw new Error("LATEST ON requires a designated timestamp column");
    }
    return this.clone({
      latestOn: {
        timestamp: designated,
        partitionBy,
      },
    });
  }

  // ---------------------------------------------------------------------------
  // JOINs
  // ---------------------------------------------------------------------------

  private joinWith<TRight extends TableDefLike, TRightAlias extends string>(
    type: JoinType,
    right: TRight,
    alias: TRightAlias,
    on?: (left: ColumnExprs<TDef>, right: ColumnExprs<TRight>) => SqlExpr,
    tolerance?: string,
  ) {
    const leftExprs = buildColumnExprs(this._def);
    const rightExprs = buildColumnExprs(right, alias);
    const onExpr = on ? on(leftExprs, rightExprs) : null;

    return new SelectBuilder(this._client, this._def, {
      ...this._node,
      joins: [
        ...this._node.joins,
        {
          type,
          table: { kind: "table", name: right.name, alias },
          on: onExpr,
          tolerance,
        },
      ],
    });
  }

  /**
   * ASOF JOIN — for each left row, find the closest preceding right row.
   * Right-side columns become nullable.
   */
  asofJoin<TRight extends TableDefLike, TRightAlias extends string>(
    right: TRight,
    alias: TRightAlias,
    on?: (left: ColumnExprs<TDef>, right: ColumnExprs<TRight>) => SqlExpr,
    tolerance?: string,
  ): SelectBuilder<
    TDef,
    Prettify<
      TResult & {
        [K in keyof InferRow<TRight> as `${TRightAlias}.${K & string}`]: InferRow<TRight>[K] | null;
      }
    >
  > {
    return this.joinWith("ASOF", right, alias, on, tolerance) as any;
  }

  /**
   * LT JOIN — like ASOF but strictly less than (no equal timestamps).
   * Right-side columns become nullable.
   */
  ltJoin<TRight extends TableDefLike, TRightAlias extends string>(
    right: TRight,
    alias: TRightAlias,
    on?: (left: ColumnExprs<TDef>, right: ColumnExprs<TRight>) => SqlExpr,
    tolerance?: string,
  ): SelectBuilder<
    TDef,
    Prettify<
      TResult & {
        [K in keyof InferRow<TRight> as `${TRightAlias}.${K & string}`]: InferRow<TRight>[K] | null;
      }
    >
  > {
    return this.joinWith("LT", right, alias, on, tolerance) as any;
  }

  /**
   * SPLICE JOIN — full ASOF join, returns all records from both tables.
   */
  spliceJoin<TRight extends TableDefLike, TRightAlias extends string>(
    right: TRight,
    alias: TRightAlias,
    on?: (left: ColumnExprs<TDef>, right: ColumnExprs<TRight>) => SqlExpr,
  ): SelectBuilder<
    TDef,
    Prettify<
      TResult & {
        [K in keyof InferRow<TRight> as `${TRightAlias}.${K & string}`]: InferRow<TRight>[K] | null;
      }
    >
  > {
    return this.joinWith("SPLICE", right, alias, on) as any;
  }

  /**
   * INNER JOIN — matching rows from both tables.
   */
  innerJoin<TRight extends TableDefLike, TRightAlias extends string>(
    right: TRight,
    alias: TRightAlias,
    on: (left: ColumnExprs<TDef>, right: ColumnExprs<TRight>) => SqlExpr,
  ): SelectBuilder<
    TDef,
    Prettify<
      TResult & {
        [K in keyof InferRow<TRight> as `${TRightAlias}.${K & string}`]: InferRow<TRight>[K];
      }
    >
  > {
    return this.joinWith("INNER", right, alias, on) as any;
  }

  /**
   * LEFT JOIN — all left rows, NULLs for unmatched right.
   */
  leftJoin<TRight extends TableDefLike, TRightAlias extends string>(
    right: TRight,
    alias: TRightAlias,
    on: (left: ColumnExprs<TDef>, right: ColumnExprs<TRight>) => SqlExpr,
  ): SelectBuilder<
    TDef,
    Prettify<
      TResult & {
        [K in keyof InferRow<TRight> as `${TRightAlias}.${K & string}`]: InferRow<TRight>[K] | null;
      }
    >
  > {
    return this.joinWith("LEFT", right, alias, on) as any;
  }

  /**
   * CROSS JOIN — Cartesian product.
   */
  crossJoin<TRight extends TableDefLike, TRightAlias extends string>(
    right: TRight,
    alias: TRightAlias,
  ): SelectBuilder<
    TDef,
    Prettify<
      TResult & {
        [K in keyof InferRow<TRight> as `${TRightAlias}.${K & string}`]: InferRow<TRight>[K];
      }
    >
  > {
    return this.joinWith("CROSS", right, alias) as any;
  }

  // ---------------------------------------------------------------------------
  // ORDER BY, LIMIT, GROUP BY
  // ---------------------------------------------------------------------------

  orderBy(
    column: keyof TResult & string,
    direction: "ASC" | "DESC" = "ASC",
  ): SelectBuilder<TDef, TResult> {
    return this.clone({
      orderBy: [...this._node.orderBy, { expr: { kind: "column", name: column }, direction }],
    });
  }

  limit(count: number, offset?: number): SelectBuilder<TDef, TResult> {
    return this.clone({ limit: { count, offset } });
  }

  groupBy(...columns: (keyof InferRow<TDef> & string)[]): SelectBuilder<TDef, TResult> {
    return this.clone({
      groupBy: columns.map((c) => ({ kind: "column" as const, name: c })),
    });
  }

  // ---------------------------------------------------------------------------
  // Terminal methods
  // ---------------------------------------------------------------------------

  /** Generate the SQL string without executing. */
  toSQL(): string {
    if (this._node.sampleBy && this._node.columns.length === 0) {
      throw new Error(
        "SAMPLE BY requires explicit columns with at least one aggregation function. " +
          "Use .columns() and .addExpr(fn.avg(...)) instead of SELECT *.",
      );
    }
    return serializeSelect(this._node);
  }

  /** Execute and return typed results. */
  async execute(): Promise<TResult[]> {
    const sql = this.toSQL();
    const response = await this._client.exec(sql);
    return parseResultSet<TResult>(response);
  }

  /** Execute and return first result or null. */
  async first(): Promise<TResult | null> {
    const limited = this.limit(1);
    const results = await limited.execute();
    return results[0] ?? null;
  }
}
