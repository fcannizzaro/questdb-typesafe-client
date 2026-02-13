import { describe, expect, test } from "bun:test";
import { QuestDBClient } from "../src/client/connection.ts";
import { QuestDBError } from "../src/client/error.ts";

function mockFetch(responseBody: unknown, status = 200): typeof globalThis.fetch {
  return (async () => ({
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(responseBody),
    json: async () => responseBody,
    headers: new Headers(),
  })) as unknown as typeof globalThis.fetch;
}

describe("QuestDBClient", () => {
  test("exec sends GET to /exec", async () => {
    let capturedUrl: string | undefined;

    const client = new QuestDBClient({
      host: "localhost",
      port: 9000,
      fetch: (async (url: string) => {
        capturedUrl = url;
        return {
          ok: true,
          status: 200,
          json: async () => ({ columns: [], dataset: [] }),
        };
      }) as unknown as typeof globalThis.fetch,
    });

    await client.exec("SELECT * FROM energy_readings");
    expect(capturedUrl).toContain("http://localhost:9000/exec?");
    expect(capturedUrl).toContain("query=SELECT");
    expect(capturedUrl).toContain("quoteLargeNum=true");
  });

  test("exec returns response data", async () => {
    const mockResponse = {
      columns: [
        { name: "source", type: "SYMBOL" },
        { name: "power_kw", type: "DOUBLE" },
      ],
      dataset: [["solar", 48.7]],
      timestamp: 0,
      count: 1,
    };

    const client = new QuestDBClient({
      fetch: mockFetch(mockResponse),
    });

    const result = await client.exec("SELECT * FROM energy_readings");
    expect(result.columns).toHaveLength(2);
    expect(result.dataset).toHaveLength(1);
  });

  test("exec throws QuestDBError on 400", async () => {
    const client = new QuestDBClient({
      fetch: mockFetch({ error: "unexpected token", position: 5 }, 400),
    });

    expect(client.exec("SELCT bad")).rejects.toBeInstanceOf(QuestDBError);
  });

  test("exec throws QuestDBError with details", async () => {
    const client = new QuestDBClient({
      fetch: mockFetch({ error: "unexpected token: SELCT", position: 0 }, 400),
    });

    try {
      await client.exec("SELCT bad");
    } catch (e) {
      const err = e as QuestDBError;
      expect(err.status).toBe(400);
      expect(err.questdbMessage).toContain("unexpected token");
      expect(err.position).toBe(0);
      expect(err.sql).toBe("SELCT bad");
    }
  });

  test("exec retries on 500", async () => {
    let callCount = 0;
    const client = new QuestDBClient({
      retries: 2,
      fetch: (async () => {
        callCount++;
        if (callCount < 3) {
          return {
            ok: false,
            status: 500,
            text: async () => JSON.stringify({ error: "internal error" }),
            json: async () => ({ error: "internal error" }),
          };
        }
        return {
          ok: true,
          status: 200,
          json: async () => ({ columns: [], dataset: [] }),
        };
      }) as unknown as typeof globalThis.fetch,
    });

    const result = await client.exec("SELECT 1");
    expect(callCount).toBe(3);
    expect(result).toBeDefined();
  });

  test("exec does NOT retry on 400", async () => {
    let callCount = 0;
    const client = new QuestDBClient({
      retries: 3,
      fetch: (async () => {
        callCount++;
        return {
          ok: false,
          status: 400,
          text: async () => JSON.stringify({ error: "bad query" }),
        };
      }) as unknown as typeof globalThis.fetch,
    });

    try {
      await client.exec("BAD");
    } catch {
      // expected
    }
    expect(callCount).toBe(1);
  });

  test("ping returns true when server responds", async () => {
    const client = new QuestDBClient({
      fetch: mockFetch({ columns: [], dataset: [] }),
    });
    const result = await client.ping();
    expect(result).toBe(true);
  });

  test("ping returns false on failure", async () => {
    const client = new QuestDBClient({
      fetch: (() => {
        throw new Error("connection refused");
      }) as unknown as typeof globalThis.fetch,
    });
    const result = await client.ping();
    expect(result).toBe(false);
  });

  test("uses basic auth when configured", async () => {
    let capturedHeaders: Headers | undefined;

    const client = new QuestDBClient({
      username: "admin",
      password: "quest",
      fetch: (async (_url: string, init: RequestInit) => {
        capturedHeaders = init.headers as Headers;
        return {
          ok: true,
          status: 200,
          json: async () => ({ columns: [], dataset: [] }),
        };
      }) as unknown as typeof globalThis.fetch,
    });

    await client.exec("SELECT 1");
    expect(capturedHeaders?.get("Authorization")).toContain("Basic");
  });

  test("table() creates a Table instance", () => {
    const client = new QuestDBClient({
      fetch: mockFetch({}),
    });

    const { defineTable } = require("../src/schema/define.ts");
    const { q } = require("../src/schema/column-builder.ts");

    const energyReadings = defineTable({
      name: "energy_readings",
      columns: {
        ts: q.timestamp.designated(),
        power_kw: q.double(),
        meter_active: q.boolean(),
      },
      partitionBy: "DAY",
    });

    const t = client.table(energyReadings);
    expect(t).toBeDefined();
    expect(t.def.name).toBe("energy_readings");
  });
});
