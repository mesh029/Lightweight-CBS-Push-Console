import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";

type EtlRunResult = {
  ok: boolean;
  log: string[];
};

// Runs the same ETL stored procedures KenyaEMR uses, against the *selected*
// uploaded OpenMRS database, then syncs facility MFL into the ETL facility tables.
export async function runEtlAndSyncMfl(openmrsDbName: string): Promise<EtlRunResult> {
  const log: string[] = [];
  const openmrsDb = openmrsDbName.trim();

  if (!openmrsDb) {
    return { ok: false, log: ["Missing openmrsDbName"] };
  }

  const cfg = loadDbConfig();

  log.push(`ETL runner: connecting to OpenMRS DB '${openmrsDb}'...`);
  const conn = await mysql.createConnection({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: openmrsDb
  });

  let allProcsOk = true;

  // Procedures live in schema `openmrs`, but reference unqualified tables like `person`,
  // so they will read from the *currently-selected* database (database: openmrsDb).
  const procedures = [
    "CALL openmrs.create_etl_tables();",
    "CALL openmrs.sp_first_time_setup();",
    "CALL openmrs.create_dwapi_tables();",
    "CALL openmrs.sp_dwapi_etl_refresh();"
  ];

  for (const stmt of procedures) {
    try {
      log.push(`ETL runner: executing ${stmt}`);
      await conn.query(stmt);
      log.push(`ETL runner: success ${stmt}`);
    } catch (err) {
      allProcsOk = false;
      log.push(`ETL runner: failed ${stmt}: ${String(err)}`);
    }
  }

  await conn.end();

  // Sync facility mfl_code into ETL facility info tables.
  // This is required because payload builders depend on ETL facility defaults.
  log.push(`ETL runner: reading facility.mflcode from '${openmrsDb}'...`);
  let facilityMfl: string | null = null;

  const isValidMfl = (mfl: string | null | undefined) => {
    const v = (mfl ?? "").trim();
    if (!v) return false;
    if (v.toUpperCase() === "UNKNOWN") return false;
    if (v === "12345") return false; // placeholder value from many dumps
    return /^[0-9]+$/.test(v);
  };

  try {
    const gpConn = await mysql.createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: openmrsDb
    });
    const [gpRows] = await gpConn.query(
      "SELECT property_value FROM global_property WHERE property = 'facility.mflcode' LIMIT 1;"
    );
    if (Array.isArray(gpRows) && gpRows.length > 0) {
      const r = gpRows[0] as { property_value?: string | null };
      facilityMfl = r.property_value ?? null;
    }
    await gpConn.end();
  } catch (err) {
    allProcsOk = false;
    log.push(`ETL runner: failed reading facility.mflcode: ${String(err)}`);
  }

  // Important: do not overwrite ETL facility siteCode with placeholder values.
  // KenyaEMR dumps often contain facility.mflcode=12345 but the ETL stored procedures
  // still compute the correct siteCode in `kenyaemr_etl.etl_default_facility_info`.
  // Our previous behavior overwrote that correct value with the placeholder.
  if (facilityMfl && isValidMfl(facilityMfl)) {
    // kenyaemr_etl.etl_default_facility_info
    try {
      log.push(`ETL runner: syncing kenyaemr_etl.etl_default_facility_info.siteCode=${facilityMfl}`);
      const etlConn = await mysql.createConnection({
        host: cfg.host,
        port: cfg.port,
        user: cfg.user,
        password: cfg.password,
        database: "kenyaemr_etl"
      });
      const [tables] = await etlConn.query(
        "SHOW TABLES LIKE 'etl_default_facility_info';"
      );
      if (Array.isArray(tables) && tables.length > 0) {
        await etlConn.query(
          "UPDATE etl_default_facility_info SET siteCode = ?",
          [facilityMfl]
        );
      } else {
        log.push("ETL runner: etl_default_facility_info missing; skipping ETL facility sync");
      }
      await etlConn.end();
    } catch (err) {
      allProcsOk = false;
      log.push(`ETL runner: failed syncing kenyaemr_etl: ${String(err)}`);
    }

    // kenyaemr_datatools.default_facility_info (optional; ETL might recreate kenyaemr_datatools but not always)
    try {
      log.push(`ETL runner: syncing kenyaemr_datatools.default_facility_info.siteCode=${facilityMfl} (optional)`);
      const dtConn = await mysql.createConnection({
        host: cfg.host,
        port: cfg.port,
        user: cfg.user,
        password: cfg.password,
        database: "kenyaemr_datatools"
      });
      const [dtTables] = await dtConn.query(
        "SHOW TABLES LIKE 'default_facility_info';"
      );
      if (Array.isArray(dtTables) && dtTables.length > 0) {
        await dtConn.query(
          "UPDATE default_facility_info SET siteCode = ?",
          [facilityMfl]
        );
      } else {
        log.push("ETL runner: kenyaemr_datatools.default_facility_info missing; skipping datatools sync");
      }
      await dtConn.end();
    } catch (err) {
      // optional
      log.push(`ETL runner: optional datatools sync failed: ${String(err)}`);
    }
  } else if (facilityMfl) {
    // Present but invalid/placeholder: keep whatever ETL procedures computed.
    log.push(
      `ETL runner: facility.mflcode='${facilityMfl}' is invalid/placeholder; skipping kenyaemr_etl etl_default_facility_info sync`
    );
  } else {
    allProcsOk = false;
    log.push("ETL runner: facility.mflcode not found; cannot sync facility");
  }

  return { ok: allProcsOk, log };
}

