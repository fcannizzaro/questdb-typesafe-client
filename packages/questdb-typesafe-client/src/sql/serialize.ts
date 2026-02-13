import type { SqlExpr, SelectNode, InsertNode, UpdateNode, FromClause } from "../types/sql.ts";
import type { QuestDBType } from "../types/column.ts";
import { escapeIdentifier, escapeString } from "./escape.ts";
import { formatValue } from "./format.ts";

// ---------------------------------------------------------------------------
// Expression serialization
// ---------------------------------------------------------------------------

export function serializeExpr(expr: SqlExpr): string {
  switch (expr.kind) {
    case "column":
      return expr.table
        ? `${escapeIdentifier(expr.table)}.${escapeIdentifier(expr.name)}`
        : escapeIdentifier(expr.name);

    case "literal":
      return formatValue(expr.value, expr.type);

    case "binary":
      return `(${serializeExpr(expr.left)} ${expr.op} ${serializeExpr(expr.right)})`;

    case "unary":
      return `(${expr.op} ${serializeExpr(expr.operand)})`;

    case "function":
      return `${expr.name}(${expr.args.map(serializeExpr).join(", ")})`;

    case "aggregate": {
      const fn = `${expr.name}(${expr.args.map(serializeExpr).join(", ")})`;
      return expr.alias ? `${fn} AS ${escapeIdentifier(expr.alias)}` : fn;
    }

    case "subquery":
      return `(${serializeSelect(expr.sql)})`;

    case "timestamp_in":
      return `${escapeIdentifier(expr.column)} IN '${escapeString(expr.interval)}'`;

    case "in_list":
      return `${escapeIdentifier(expr.column)} IN (${expr.values.map(serializeExpr).join(", ")})`;

    case "between":
      return `${escapeIdentifier(expr.column)} BETWEEN ${serializeExpr(expr.low)} AND ${serializeExpr(expr.high)}`;

    case "is_null":
      return expr.negated
        ? `${escapeIdentifier(expr.column)} IS NOT NULL`
        : `${escapeIdentifier(expr.column)} IS NULL`;

    case "raw":
      return expr.sql;
  }
}

// ---------------------------------------------------------------------------
// SELECT serialization
// ---------------------------------------------------------------------------

export function serializeSelect(node: SelectNode): string {
  const parts: string[] = [];

  // SELECT columns
  const selectKw = node.distinct ? "SELECT DISTINCT" : "SELECT";
  if (node.columns.length === 0) {
    parts.push(`${selectKw} *`);
  } else {
    const cols = node.columns.map((c) => {
      const expr = serializeExpr(c.expr);
      return c.alias ? `${expr} AS ${escapeIdentifier(c.alias)}` : expr;
    });
    parts.push(`${selectKw} ${cols.join(", ")}`);
  }

  // FROM
  parts.push(`FROM ${serializeFrom(node.from)}`);

  // JOINs
  for (const join of node.joins) {
    let joinStr = `${join.type} JOIN ${serializeFrom(join.table)}`;
    if (join.on) {
      joinStr += ` ON ${serializeExpr(join.on)}`;
    }
    parts.push(joinStr);
  }

  // LATEST ON (before WHERE in QuestDB syntax)
  if (node.latestOn) {
    parts.push(
      `LATEST ON ${escapeIdentifier(node.latestOn.timestamp)} PARTITION BY ${node.latestOn.partitionBy.map(escapeIdentifier).join(", ")}`,
    );
  }

  // WHERE
  if (node.where) {
    parts.push(`WHERE ${serializeExpr(node.where)}`);
  }

  // SAMPLE BY
  if (node.sampleBy) {
    parts.push(`SAMPLE BY ${node.sampleBy.interval}`);

    if (node.sampleBy.fill.length > 0) {
      const fills = node.sampleBy.fill.map((f) => {
        if (typeof f === "object" && "constant" in f) {
          return String(f.constant);
        }
        return f;
      });
      parts.push(`FILL(${fills.join(", ")})`);
    }

    if (node.sampleBy.align) {
      parts.push(`ALIGN TO ${node.sampleBy.align}`);
    }
  }

  // GROUP BY
  if (node.groupBy.length > 0) {
    parts.push(`GROUP BY ${node.groupBy.map(serializeExpr).join(", ")}`);
  }

  // ORDER BY
  if (node.orderBy.length > 0) {
    const orders = node.orderBy.map((o) => `${serializeExpr(o.expr)} ${o.direction}`);
    parts.push(`ORDER BY ${orders.join(", ")}`);
  }

  // LIMIT
  if (node.limit) {
    let limitStr = `LIMIT ${node.limit.count}`;
    if (node.limit.offset !== undefined) {
      limitStr += `, ${node.limit.offset}`;
    }
    parts.push(limitStr);
  }

  return parts.join("\n");
}

function serializeFrom(from: FromClause): string {
  if (from.kind === "table") {
    const name = escapeIdentifier(from.name);
    return from.alias ? `${name} ${escapeIdentifier(from.alias)}` : name;
  }
  return `(${serializeSelect(from.select)}) ${escapeIdentifier(from.alias)}`;
}

// ---------------------------------------------------------------------------
// INSERT serialization
// ---------------------------------------------------------------------------

export function serializeInsert(node: InsertNode): string {
  if (node.values.length === 0) {
    throw new Error("INSERT requires at least one row");
  }

  const valueRows = node.values.map((row) =>
    row
      .map((val, i) => {
        if (val !== null && typeof val === "object" && "_rawSQL" in val) {
          return (val as { _rawSQL: string })._rawSQL;
        }
        return formatValue(val, node.columnTypes[i] ?? ("varchar" as QuestDBType));
      })
      .join(", "),
  );

  return [
    `INSERT INTO ${escapeIdentifier(node.table)}`,
    `(${node.columns.map(escapeIdentifier).join(", ")})`,
    "VALUES",
    valueRows.map((v) => `(${v})`).join(",\n"),
  ].join("\n");
}

// ---------------------------------------------------------------------------
// UPDATE serialization
// ---------------------------------------------------------------------------

export function serializeUpdate(node: UpdateNode): string {
  const parts: string[] = [];
  parts.push(`UPDATE ${escapeIdentifier(node.table)}`);

  const sets = node.set.map((s) => `${escapeIdentifier(s.column)} = ${serializeExpr(s.value)}`);
  parts.push(`SET ${sets.join(", ")}`);

  if (node.from) {
    parts.push(`FROM ${serializeFrom(node.from)}`);
  }

  if (node.where) {
    parts.push(`WHERE ${serializeExpr(node.where)}`);
  }

  return parts.join("\n");
}
