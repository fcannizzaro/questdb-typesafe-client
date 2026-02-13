import { test, expect, describe } from "bun:test";
import { defineTable } from "../src/schema/define.ts";
import { q } from "../src/schema/column-builder.ts";
import { CreateBuilder } from "../src/ddl/create.ts";
import { AlterBuilder } from "../src/ddl/alter.ts";

// Mock client
const mockClient = { exec: async () => ({}) } as any;

const energyReadings = defineTable({
  name: "energy_readings",
  columns: {
    ts: q.timestamp.designated(),
    source: q.symbol.options().capacity(256).cache().index().build(),
    reading: q.symbol(),
    power_kw: q.double(),
    energy_kwh: q.double(),
    meterId: q.uuid(),
    meter_active: q.boolean(),
  },
  partitionBy: "DAY",
  wal: true,
  dedupKeys: ["ts", "meterId"],
});

describe("CreateBuilder", () => {
  test("generates basic CREATE TABLE", () => {
    const sql = new CreateBuilder(mockClient, energyReadings).toSQL();
    expect(sql).toContain("CREATE TABLE energy_readings");
    expect(sql).toContain("ts TIMESTAMP");
    expect(sql).toContain("source SYMBOL CAPACITY 256 CACHE INDEX");
    expect(sql).toContain("reading SYMBOL");
    expect(sql).toContain("power_kw DOUBLE");
    expect(sql).toContain("energy_kwh DOUBLE");
    expect(sql).toContain("meterId UUID");
    expect(sql).toContain("meter_active BOOLEAN");
    expect(sql).toContain("timestamp(ts)");
    expect(sql).toContain("PARTITION BY DAY");
    expect(sql).toContain("WAL");
    expect(sql).toContain("DEDUP UPSERT KEYS(ts, meterId)");
  });

  test("generates CREATE TABLE IF NOT EXISTS", () => {
    const sql = new CreateBuilder(mockClient, energyReadings).ifNotExists().toSQL();
    expect(sql).toContain("CREATE TABLE IF NOT EXISTS energy_readings");
  });

  test("handles BYPASS WAL", () => {
    const def = defineTable({
      name: "test",
      columns: { ts: q.timestamp.designated() },
      partitionBy: "DAY",
      wal: false,
    });
    const sql = new CreateBuilder(mockClient, def).toSQL();
    expect(sql).toContain("BYPASS WAL");
  });

  test("handles WITH parameters", () => {
    const def = defineTable({
      name: "test",
      columns: { ts: q.timestamp.designated() },
      partitionBy: "DAY",
      maxUncommittedRows: 50000,
      o3MaxLag: "1s",
    });
    const sql = new CreateBuilder(mockClient, def).toSQL();
    expect(sql).toContain("WITH maxUncommittedRows=50000, o3MaxLag=1s");
  });

  test("handles NONE partition", () => {
    const def = defineTable({
      name: "lookup",
      columns: { id: q.int(), name: q.varchar() },
      partitionBy: "NONE",
    });
    const sql = new CreateBuilder(mockClient, def).toSQL();
    expect(sql).toContain("PARTITION BY NONE");
    expect(sql).not.toContain("timestamp(");
  });

  test("handles geohash column", () => {
    const def = defineTable({
      name: "geo",
      columns: {
        ts: q.timestamp.designated(),
        loc: q.geohash(30),
      },
      partitionBy: "DAY",
    });
    const sql = new CreateBuilder(mockClient, def).toSQL();
    expect(sql).toContain("loc GEOHASH(30b)");
  });

  test("handles array column", () => {
    const def = defineTable({
      name: "vectors",
      columns: {
        ts: q.timestamp.designated(),
        vec: q.array("double"),
      },
      partitionBy: "DAY",
    });
    const sql = new CreateBuilder(mockClient, def).toSQL();
    expect(sql).toContain("vec DOUBLE[]");
  });
});

describe("AlterBuilder", () => {
  test("generates ADD COLUMN", () => {
    const sqls = new AlterBuilder(mockClient, energyReadings)
      .addColumn("phone", q.varchar())
      .toSQL();
    expect(sqls).toEqual(["ALTER TABLE energy_readings ADD COLUMN phone VARCHAR"]);
  });

  test("generates DROP COLUMN", () => {
    const sqls = new AlterBuilder(mockClient, energyReadings).dropColumn("energy_kwh").toSQL();
    expect(sqls).toEqual(["ALTER TABLE energy_readings DROP COLUMN energy_kwh"]);
  });

  test("generates RENAME COLUMN", () => {
    const sqls = new AlterBuilder(mockClient, energyReadings)
      .renameColumn("energy_kwh", "total_energy_kwh")
      .toSQL();
    expect(sqls).toEqual([
      "ALTER TABLE energy_readings RENAME COLUMN energy_kwh TO total_energy_kwh",
    ]);
  });

  test("generates SET PARAM ttl", () => {
    const sqls = new AlterBuilder(mockClient, energyReadings).setTTL("30d").toSQL();
    expect(sqls).toEqual(["ALTER TABLE energy_readings SET PARAM ttl = '30d'"]);
  });

  test("accumulates multiple operations", () => {
    const sqls = new AlterBuilder(mockClient, energyReadings)
      .addColumn("phone", q.varchar())
      .dropColumn("reading")
      .setTTL("90d")
      .toSQL();
    expect(sqls).toHaveLength(3);
  });

  test("generates partition operations", () => {
    const builder = new AlterBuilder(mockClient, energyReadings);
    builder.dropPartition("2026-01-01");
    builder.detachPartition("2026-01-02");
    builder.attachPartition("2026-01-03");
    builder.squashPartitions();
    builder.resumeWal();
    const sqls = builder.toSQL();
    expect(sqls).toHaveLength(5);
    expect(sqls[0]).toContain("DROP PARTITION LIST '2026-01-01'");
    expect(sqls[1]).toContain("DETACH PARTITION LIST '2026-01-02'");
    expect(sqls[2]).toContain("ATTACH PARTITION LIST '2026-01-03'");
    expect(sqls[3]).toContain("SQUASH PARTITIONS");
    expect(sqls[4]).toContain("RESUME WAL");
  });
});
