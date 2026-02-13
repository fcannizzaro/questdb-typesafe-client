import type { PartitionBy } from "../schema/define.ts";

/**
 * Configuration for the QuestDB REST client.
 */
export interface QuestDBClientConfig {
  /** Host. Default: "localhost" */
  host?: string;
  /** HTTP port. Default: 9000 */
  port?: number;
  /** Use HTTPS. Default: false */
  https?: boolean;
  /** Basic auth username */
  username?: string;
  /** Basic auth password */
  password?: string;
  /** Request timeout in ms. Default: 30000 */
  timeout?: number;
  /** Default retry count for transient errors. Default: 0 */
  retries?: number;
  /** Custom fetch implementation (for testing) */
  fetch?: typeof globalThis.fetch;
}

/**
 * Options for CSV import via /imp endpoint.
 */
export interface ImportOptions {
  overwrite?: boolean;
  durable?: boolean;
  atomicity?: "relaxed" | "strict";
  timestamp?: string;
  partitionBy?: PartitionBy;
}
