import { QuestDBClient, defineTable, q, and, fn } from "@fcannizzaro/questdb-typesafe-client";

// ---------------------------------------------------------------------------
// 1. Define the table schema
// ---------------------------------------------------------------------------

const energyReadings = defineTable({
  name: "energy_readings",
  columns: {
    ts: q.timestamp.designated(),
    source: q.symbol(),
    reading: q.symbol(),
    power_kw: q.double(),
    energy_kwh: q.double(),
    meter_active: q.boolean(),
  },
  partitionBy: "DAY",
  wal: true,
});

// ---------------------------------------------------------------------------
// 2. Connect to QuestDB and bind the table
// ---------------------------------------------------------------------------

const db = new QuestDBClient({ host: "localhost", port: 9000 });
const readings = db.table(energyReadings);

async function main() {
  // -----------------------------------------------------------------------
  // 3. CREATE TABLE
  // -----------------------------------------------------------------------
  console.log("--- CREATE TABLE ---");
  await readings.ddl().create().ifNotExists().execute();
  console.log("Table created (if not exists)\n");

  // -----------------------------------------------------------------------
  // 4. INSERT — single row
  // -----------------------------------------------------------------------
  console.log("--- INSERT (single) ---");
  await readings
    .insert({
      meter_active: true,
      source: "solar",
      reading: "produced",
      power_kw: 48.7,
      energy_kwh: 312.5,
    })
    .execute();
  console.log("Inserted 1 row\n");

  // -----------------------------------------------------------------------
  // 4b. INSERT — batch
  // -----------------------------------------------------------------------
  console.log("--- INSERT (batch) ---");
  await readings
    .insert([
      {
        meter_active: true,
        source: "wind",
        reading: "produced",
        power_kw: 120.3,
        energy_kwh: 890.1,
      },
      { meter_active: true, source: "solar", reading: "consumed", power_kw: 5.2, energy_kwh: 41.6 },
      { meter_active: false, source: "grid", reading: "consumed", power_kw: 0.0, energy_kwh: 0.0 },
    ])
    .execute();
  console.log("Inserted 3 rows\n");

  // -----------------------------------------------------------------------
  // 5. SELECT — filtered query (should be empty)
  // -----------------------------------------------------------------------
  console.log("--- SELECT (filtered) ---");
  const rows = await readings
    .select("source", "power_kw", "energy_kwh")
    .where((c) => and(c.source.eq("solar"), c.power_kw.gt(50)))
    .orderBy("power_kw", "DESC")
    .limit(10)
    .execute();
  console.log("Results:", rows, "\n");

  // -----------------------------------------------------------------------
  // 6. SELECT — QuestDB-specific: LATEST ON
  // -----------------------------------------------------------------------
  console.log("--- SELECT (LATEST ON) ---");
  const latest = await readings.select().latestOn("source").execute();
  console.log("Latest per source:", latest, "\n");

  // -----------------------------------------------------------------------
  // 7. SELECT — QuestDB-specific: SAMPLE BY
  // -----------------------------------------------------------------------
  console.log("--- SELECT (SAMPLE BY) ---");
  const sampled = await readings
    .select("ts", "source")
    .addExpr(fn.avg("power_kw", "avg_power"))
    .addExpr(fn.avg("energy_kwh", "avg_energy"))
    .sampleBy("1h", "PREV")
    .execute();
  console.log("Sampled:", sampled, "\n");

  // -----------------------------------------------------------------------
  // 8. UPDATE
  // -----------------------------------------------------------------------
  console.log("--- UPDATE ---");
  await readings
    .update()
    .set({ power_kw: 100 })
    .where((c) => c.source.eq("solar"))
    .execute();
  console.log("Updated solar power reading\n");

  // -----------------------------------------------------------------------
  // 9. DELETE PARTITION
  // -----------------------------------------------------------------------
  console.log("--- DELETE PARTITION ---");
  await readings.deletePartition("2026-01-15");
  console.log("Deleted partition 2026-01-15\n");

  // -----------------------------------------------------------------------
  // 10. SQL preview with .toSQL()
  // -----------------------------------------------------------------------
  console.log("--- SQL PREVIEW ---");
  const sql = readings
    .select("source", "power_kw")
    .where((c) => c.power_kw.gt(100))
    .orderBy("power_kw", "DESC")
    .limit(5)
    .toSQL();
  console.log(sql, "\n");

  // -----------------------------------------------------------------------
  // 11. Cleanup — DROP TABLE
  // -----------------------------------------------------------------------
  console.log("--- DROP TABLE ---");
  await readings.ddl().drop(true);
  console.log("Table dropped\n");
}

main().catch(console.error);
