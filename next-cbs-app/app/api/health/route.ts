import { NextResponse } from "next/server";
import os from "os";
import mysql from "mysql2/promise";
import { getDbPool } from "@lib/db";
import { loadDbConfig, loadCbsConfig } from "@lib/config";
import { runEtlAndSyncMfl } from "@lib/etlRunner";

type HealthOptions = {
  openmrsDbNameOverride?: string;
  autoRefreshEtl?: boolean;
  // Backward/forward compatible alias: if true, we skip ETL auto-refresh.
  skipEtlRefresh?: boolean;
};

async function runHealth(options: HealthOptions = {}) {
  const configIssues: string[] = [];
  const log: string[] = [];

  log.push("Starting health check...");

  let dbConfig;
  try {
    dbConfig = loadDbConfig();
    log.push(
      `Loaded DB config: host=${dbConfig.host}, db=${dbConfig.name}, user=${dbConfig.user}`
    );
  } catch (err) {
    const msg = String(err);
    configIssues.push(msg);
    log.push(`DB config error: ${msg}`);
  }

  const cbsConfig = loadCbsConfig();
  log.push(
    `Loaded CBS config: endpoint=${cbsConfig.endpointUrl || "none"}, facilityCode=${
      cbsConfig.facilityCode || "unknown"
    }`
  );
  if (!cbsConfig.endpointUrl) {
    configIssues.push("Missing CBS_ENDPOINT_URL");
  }

  let dbStatus: "ok" | "error" = "ok";
  let etlCount: number | null = null;
  let dbError: string | null = null;
  let facilityMfl: string | null = null;
  let facilityName: string | null = null;
  let emrMflFromGp: string | null = null;
  let emrVersion: string | null = null;
  const openmrsDbName = options.openmrsDbNameOverride || "openmrs";
  const hostInfo = {
    nodeVersion: process.version,
    platform: os.platform(),
    release: os.release(),
    arch: os.arch()
  };

  const shouldAutoRefreshEtl =
    options.autoRefreshEtl ??
    (typeof options.skipEtlRefresh === "boolean"
      ? !options.skipEtlRefresh
      : Boolean(options.openmrsDbNameOverride));

  if (shouldAutoRefreshEtl) {
    log.push(
      `Auto-refresh enabled: running ETL + facility sync for OpenMRS DB '${openmrsDbName}'...`
    );
    try {
      const etlRes = await runEtlAndSyncMfl(openmrsDbName);
      log.push(...etlRes.log);
      if (!etlRes.ok) {
        configIssues.push("ETL auto-refresh failed");
      }
    } catch (err) {
      configIssues.push(`ETL auto-refresh exception: ${String(err)}`);
    }
  }

  try {
    log.push("Connecting to ETL database...");
    const pool = getDbPool();

    // Simple probe: count rows from a common ETL table if it exists
    const [rows] = await pool.query("SHOW TABLES LIKE 'etl_current_in_care';");

    if (Array.isArray(rows) && rows.length > 0) {
      log.push("Found etl_current_in_care table, counting rows...");
      const [countRows] = await pool.query("SELECT COUNT(*) as c FROM etl_current_in_care;");
      if (Array.isArray(countRows) && countRows.length > 0 && "c" in countRows[0]) {
        // @ts-expect-error dynamic row
        etlCount = Number(countRows[0].c);
        log.push(`etl_current_in_care row count: ${etlCount}`);
      }
    } else {
      log.push("etl_current_in_care table NOT found in ETL database");
    }

    // Try fetch default facility info from ETL if table exists
    const [facTables] = await pool.query("SHOW TABLES LIKE 'etl_default_facility_info';");
    if (Array.isArray(facTables) && facTables.length > 0) {
      log.push("Found etl_default_facility_info table, reading facility info...");
      const [facRows] = await pool.query(
        "SELECT siteCode AS mflCode, FacilityName AS facilityName FROM etl_default_facility_info LIMIT 1;"
      );
      if (Array.isArray(facRows) && facRows.length > 0) {
        const row = facRows[0] as { mflCode?: string; facilityName?: string };
        facilityMfl = row.mflCode ?? null;
        facilityName = row.facilityName ?? null;
        log.push(
          `ETL facility detected: MFL=${facilityMfl || "unknown"}, name=${
            facilityName || "unknown"
          }`
        );
      }
    } else {
      log.push("etl_default_facility_info table NOT found in ETL database");
    }

    // Additionally, read facility.mflcode and kenyaemr.version from openmrs.global_property
    try {
      log.push(
        `Connecting to OpenMRS database '${openmrsDbName}' to read global properties...`
      );
      const base = dbConfig ?? loadDbConfig();
      const conn = await mysql.createConnection({
        host: base.host,
        port: base.port,
        user: base.user,
        password: base.password,
        database: openmrsDbName
      });
      const [gpRows] = await conn.query(
        "SELECT property, property_value FROM global_property WHERE property IN ('facility.mflcode','kenyaemr.version');"
      );
      if (Array.isArray(gpRows)) {
        for (const r of gpRows as { property: string; property_value: string | null }[]) {
          if (r.property === "facility.mflcode") {
            emrMflFromGp = r.property_value ?? null;
          }
          if (r.property === "kenyaemr.version") {
            emrVersion = r.property_value ?? null;
          }
        }
      }
      await conn.end();
      log.push(
        `OpenMRS global properties: facility.mflcode=${emrMflFromGp || "none"}, kenyaemr.version=${
          emrVersion || "unknown"
        }`
      );
    } catch (gpErr) {
      log.push(`Error reading OpenMRS global properties: ${String(gpErr)}`);
    }
  } catch (err) {
    dbStatus = "error";
    dbError = String(err);
    log.push(`DB error: ${dbError}`);
  }

  let cbsStatus: "ok" | "skip" | "error" = "skip";
  let cbsHttpStatus: number | null = null;
  let cbsError: string | null = null;
  let cbsReachable = false;

  if (cbsConfig.endpointUrl) {
    try {
      log.push(`Probing CBS endpoint: ${cbsConfig.endpointUrl} ...`);
      // Lightweight reachability check – method GET to avoid mutating state.
      const res = await fetch(cbsConfig.endpointUrl, {
        method: "GET",
        headers: {
          ...(cbsConfig.token ? { Authorization: `Bearer ${cbsConfig.token}` } : {})
        }
      });
      cbsStatus = res.ok ? "ok" : "error";
      cbsHttpStatus = res.status;
      cbsReachable = true; // network and DNS resolved, server responded
      log.push(`CBS endpoint responded with HTTP ${res.status}`);
      if (!res.ok) {
        cbsError = `CBS endpoint responded with status ${res.status}`;
      }
    } catch (err) {
      cbsStatus = "error";
      cbsError = String(err);
      log.push(`CBS probe error: ${cbsError}`);
    }
  } else {
    log.push("CBS endpoint URL not configured");
  }

  const overallOk = configIssues.length === 0 && dbStatus === "ok" && (cbsStatus === "ok" || cbsStatus === "skip");

  return NextResponse.json(
    {
      ok: overallOk,
      message: overallOk ? "Health check OK" : "Health check has issues",
      configIssues,
      emrVersion,
      log,
      host: hostInfo,
      openmrsDbNameUsed: openmrsDbName,
      db: {
        env: dbConfig ?? null,
        status: dbStatus,
        etlCurrentInCareCount: etlCount,
        facility: facilityMfl || facilityName || emrMflFromGp
          ? { mflCode: facilityMfl ?? emrMflFromGp, name: facilityName }
          : null,
        openmrsMflFromGlobalProperty: emrMflFromGp,
        error: dbError
      },
      cbs: {
        env: cbsConfig,
        status: cbsStatus,
        reachable: cbsReachable,
        httpStatus: cbsHttpStatus,
        error: cbsError
      }
    },
    { status: overallOk ? 200 : 207 }
  );
}

export async function GET() {
  return runHealth();
}

export async function POST(req: Request) {
  const body = (await req.json().catch(() => ({}))) as HealthOptions;
  return runHealth(body);
}

