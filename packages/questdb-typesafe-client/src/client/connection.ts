import type { QuestDBClientConfig, ImportOptions } from "./config.ts";
import type { TableDefLike } from "../types/infer.ts";
import { QuestDBError, QuestDBConnectionError } from "./error.ts";
import { Table } from "../schema/table.ts";

/**
 * Raw response shape from QuestDB /exec endpoint (SELECT).
 */
export interface QuestDBExecResponse {
  query?: string;
  columns?: Array<{ name: string; type: string }>;
  dataset?: unknown[][];
  timestamp?: number;
  count?: number;
  ddl?: string;
  updated?: number;
}

/**
 * Response from /imp (CSV import).
 */
export interface QuestDBImportResponse {
  status: string;
  location: string;
  rowsRejected: number;
  rowsImported: number;
  header: boolean;
  columns: Array<{ name: string; type: string; size: number; errors: number }>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * QuestDB REST API client.
 *
 * Uses the `/exec` endpoint for queries and DDL,
 * and `/imp` for CSV import.
 */
export class QuestDBClient {
  private readonly baseUrl: string;
  private readonly headers: Headers;
  private readonly timeout: number;
  private readonly retries: number;
  private readonly fetchFn: typeof globalThis.fetch;

  constructor(config: QuestDBClientConfig = {}) {
    const {
      host = "localhost",
      port = 9000,
      https: useHttps = false,
      username,
      password,
      timeout = 30_000,
      retries = 0,
      fetch: customFetch,
    } = config;

    const protocol = useHttps ? "https" : "http";
    this.baseUrl = `${protocol}://${host}:${port}`;
    this.timeout = timeout;
    this.retries = retries;
    this.fetchFn = customFetch ?? globalThis.fetch;

    this.headers = new Headers({
      Accept: "application/json",
    });

    if (username && password) {
      const encoded = btoa(`${username}:${password}`);
      this.headers.set("Authorization", `Basic ${encoded}`);
    }
  }

  /**
   * Execute a SQL query via /exec.
   */
  async exec(sql: string): Promise<QuestDBExecResponse> {
    return this.execWithRetry(sql, this.retries);
  }

  private async execWithRetry(sql: string, retriesLeft: number): Promise<QuestDBExecResponse> {
    const url = new URL(`${this.baseUrl}/exec`);
    url.searchParams.set("query", sql);
    url.searchParams.set("quoteLargeNum", "true");

    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), this.timeout);

      const response = await this.fetchFn(url.toString(), {
        headers: this.headers,
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        const text = await response.text();
        let parsed: Record<string, unknown> = {};
        try {
          parsed = JSON.parse(text) as Record<string, unknown>;
        } catch {
          // ignore parse error
        }

        throw new QuestDBError({
          status: response.status,
          message: `QuestDB error (${response.status}): ${(parsed["error"] as string) ?? text}`,
          questdbMessage: (parsed["error"] as string) ?? text,
          position: parsed["position"] as number | undefined,
          sql,
        });
      }

      return (await response.json()) as QuestDBExecResponse;
    } catch (error) {
      if (error instanceof QuestDBError) {
        // Don't retry client errors (4xx)
        if (error.status >= 400 && error.status < 500) throw error;
        // Retry server errors (5xx)
        if (retriesLeft > 0) {
          await sleep(100 * (this.retries - retriesLeft + 1));
          return this.execWithRetry(sql, retriesLeft - 1);
        }
        throw error;
      }

      // Network / timeout errors
      if (retriesLeft > 0) {
        await sleep(100 * (this.retries - retriesLeft + 1));
        return this.execWithRetry(sql, retriesLeft - 1);
      }

      throw new QuestDBConnectionError(
        `Failed to connect to QuestDB at ${this.baseUrl}: ${(error as Error).message}`,
        error as Error,
      );
    }
  }

  /**
   * Import CSV data via /imp.
   */
  async import(
    tableName: string,
    csvData: string | Uint8Array,
    options?: ImportOptions,
  ): Promise<QuestDBImportResponse> {
    const url = new URL(`${this.baseUrl}/imp`);
    url.searchParams.set("name", tableName);

    if (options?.overwrite) url.searchParams.set("overwrite", "true");
    if (options?.durable) url.searchParams.set("durable", "true");
    if (options?.atomicity) url.searchParams.set("atomicity", options.atomicity);
    if (options?.timestamp) url.searchParams.set("timestamp", options.timestamp);
    if (options?.partitionBy) url.searchParams.set("partitionBy", options.partitionBy);

    const formData = new FormData();
    const blob = new Blob([csvData as BlobPart], { type: "text/csv" });
    formData.append("data", blob, `${tableName}.csv`);

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    const response = await this.fetchFn(url.toString(), {
      method: "POST",
      headers: this.headers,
      body: formData,
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      const text = await response.text();
      throw new QuestDBError({
        status: response.status,
        message: `QuestDB import error: ${text}`,
        questdbMessage: text,
        sql: `[CSV import to ${tableName}]`,
      });
    }

    return (await response.json()) as QuestDBImportResponse;
  }

  /**
   * Health check — ping the server.
   */
  async ping(): Promise<boolean> {
    try {
      const response = await this.fetchFn(
        `${this.baseUrl}/exec?query=${encodeURIComponent("SELECT 1")}`,
        {
          headers: this.headers,
          signal: AbortSignal.timeout(5000),
        },
      );
      return response.ok;
    } catch {
      return false;
    }
  }

  /**
   * Create a typed Table instance from a TableDef.
   *
   * ```ts
   * const db = new QuestDBClient();
   * const sensorsTable = db.table(sensors);
   * ```
   */
  table<TDef extends TableDefLike>(def: TDef): Table<TDef> {
    return new Table(def, this);
  }
}
