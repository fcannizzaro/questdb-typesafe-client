/**
 * Error thrown when QuestDB returns an error response (4xx/5xx).
 */
export class QuestDBError extends Error {
  /** HTTP status code */
  readonly status: number;
  /** QuestDB error message from response body */
  readonly questdbMessage: string;
  /** Position in SQL where error occurred (if available) */
  readonly position?: number;
  /** The SQL that caused the error */
  readonly sql?: string;

  constructor(opts: {
    status: number;
    message: string;
    questdbMessage: string;
    position?: number;
    sql?: string;
  }) {
    super(opts.message);
    this.name = "QuestDBError";
    this.status = opts.status;
    this.questdbMessage = opts.questdbMessage;
    this.position = opts.position;
    this.sql = opts.sql;
  }
}

/**
 * Error thrown when a connection to QuestDB fails (network error, timeout, etc.).
 */
export class QuestDBConnectionError extends Error {
  override readonly cause: Error;

  constructor(message: string, cause: Error) {
    super(message);
    this.name = "QuestDBConnectionError";
    this.cause = cause;
  }
}
