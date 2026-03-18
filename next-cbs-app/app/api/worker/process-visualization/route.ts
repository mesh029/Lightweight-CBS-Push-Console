import { NextResponse } from "next/server";
import mysql from "mysql2/promise";
import { loadCbsConfig, loadDbConfig } from "@lib/config";
import { runEtlAndSyncMfl } from "@lib/etlRunner";
import { randomUUID } from "crypto";

function formatTimestamp(date: Date) {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}${pad(
    date.getHours()
  )}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

async function resolveVisualizationEndpointFromOpenmrs(
  openmrsDbName: string
): Promise<string | null> {
  const dbCfg = loadDbConfig();
  const conn = await mysql.createConnection({
    host: dbCfg.host,
    port: dbCfg.port,
    user: dbCfg.user,
    password: dbCfg.password,
    database: openmrsDbName
  });

  try {
    const [rows] = await conn.query(
      "SELECT property_value FROM global_property WHERE property = 'visualization.metrics.post.api' LIMIT 1;"
    );
    if (Array.isArray(rows) && rows.length > 0) {
      const r = rows[0] as { property_value?: string | null };
      return r.property_value ?? null;
    }
    return null;
  } finally {
    await conn.end();
  }
}

export async function POST(req: Request) {
  const log: string[] = [];
  try {
    const correlationId = randomUUID();
    const body = (await req.json().catch(() => ({}))) as {
      openmrsDbNameOverride?: string;
      facilityCodeOverride?: string;
      skipEtlRefresh?: boolean;
    };

    const openmrsDbName = (body.openmrsDbNameOverride || "").trim() || "openmrs";
    const facilityCodeOverride = body.facilityCodeOverride?.trim() || null;
    const shouldRefresh = body.skipEtlRefresh ? false : true;

    if (shouldRefresh) {
      log.push(`Worker: running ETL + facility sync for '${openmrsDbName}'...`);
      const etlRes = await runEtlAndSyncMfl(openmrsDbName);
      log.push(...etlRes.log);
      if (!etlRes.ok) throw new Error("ETL auto-refresh failed");
    }

    const cbsCfg = loadCbsConfig();
    let endpointUrl = cbsCfg.endpointUrl;
    if (!endpointUrl) {
      endpointUrl = await resolveVisualizationEndpointFromOpenmrs(openmrsDbName);
    }
    if (!endpointUrl) throw new Error("visualization.metrics.post.api endpointUrl missing");

    // Read version and facility.mflcode from selected OpenMRS DB
    const dbCfg = loadDbConfig();
    const conn = await mysql.createConnection({
      host: dbCfg.host,
      port: dbCfg.port,
      user: dbCfg.user,
      password: dbCfg.password,
      database: openmrsDbName
    });

    let emrVersion: string | null = null;
    let facilityMflFromDb: string | null = null;
    let staffCount: Array<{ staff: string; staff_count: number }> = [];
    let workload: Array<{ department: string; total: string }> = [];
    let waitTime: Array<{
      queue: string;
      total_wait_time: string;
      patient_count: string;
    }> = [];
    try {
      const [rows] = await conn.query(
        "SELECT property, property_value FROM global_property WHERE property IN ('kenyaemr.version','facility.mflcode');"
      );
      if (Array.isArray(rows)) {
        for (const r of rows as { property: string; property_value: string | null }[]) {
          if (r.property === "kenyaemr.version") emrVersion = r.property_value;
          if (r.property === "facility.mflcode") facilityMflFromDb = r.property_value;
        }
      }

      const [staffRows] = await conn.query(
        "SELECT role AS staff, COUNT(DISTINCT user_id) AS staff_count FROM user_role GROUP BY role ORDER BY staff_count DESC;"
      );
      if (Array.isArray(staffRows)) {
        staffCount = (staffRows as any[]).map((r) => ({
          staff: String(r.staff ?? ""),
          staff_count: Number(r.staff_count ?? 0)
        }));
      }

      const [workloadRows] = await conn.query(
        `
        WITH relevant_visits AS (
          SELECT visit_id
          FROM visit
          WHERE voided = 0
        )
        SELECT department, total
        FROM (
          SELECT 'Registration' AS department, COUNT(*) AS total
          FROM relevant_visits
          UNION ALL
          SELECT et.name AS department, COUNT(*) AS total
          FROM encounter e
          INNER JOIN relevant_visits v ON e.visit_id = v.visit_id
          INNER JOIN encounter_type et ON e.encounter_type = et.encounter_type_id
          WHERE e.voided = 0
            AND et.name IS NOT NULL
          GROUP BY et.name
        ) x
        ORDER BY total DESC
        LIMIT 12;
        `
      );
      if (Array.isArray(workloadRows)) {
        workload = (workloadRows as any[]).map((r) => ({
          department: String(r.department ?? ""),
          total: String(Number(r.total ?? 0))
        }));
      }

      const [waitRows] = await conn.query(
        `
        SELECT
          q.name AS queue,
          ROUND(SUM(TIMESTAMPDIFF(SECOND, qe.started_at, qe.ended_at)) / 60, 2) AS total_wait_time,
          COUNT(DISTINCT qe.patient_id) AS patient_count
        FROM queue_entry qe
        INNER JOIN queue q ON q.queue_id = qe.queue_id
        WHERE qe.voided = 0
          AND qe.started_at IS NOT NULL
          AND qe.ended_at IS NOT NULL
        GROUP BY q.name
        ORDER BY patient_count DESC
        LIMIT 12;
        `
      );
      if (Array.isArray(waitRows)) {
        waitTime = (waitRows as any[]).map((r) => ({
          queue: String(r.queue ?? ""),
          total_wait_time: String(Number(r.total_wait_time ?? 0)),
          patient_count: String(Number(r.patient_count ?? 0))
        }));
      }
    } finally {
      await conn.end();
    }

    const mflCodeFinal = facilityCodeOverride || facilityMflFromDb || "UNKNOWN";
    const versionFinal = emrVersion || "UNKNOWN";
    const timestamp = formatTimestamp(new Date());

    // Phase 4: visits grouped by type_of_visit from kenyaemr_etl.
    let visits: Array<{ category: string; details: Array<{ visit_type: string; total: string }> }> =
      [];
    try {
      const etlConn = await mysql.createConnection({
        host: dbCfg.host,
        port: dbCfg.port,
        user: dbCfg.user,
        password: dbCfg.password,
        database: "kenyaemr_etl"
      });
      const [visitRows] = await etlConn.query(
        `
        SELECT 'HEI Follow-up' AS visit_type, COUNT(*) AS total FROM etl_hei_follow_up_visit
        UNION ALL
        SELECT 'MCH Antenatal' AS visit_type, COUNT(*) AS total FROM etl_mch_antenatal_visit
        UNION ALL
        SELECT 'MCH Postnatal' AS visit_type, COUNT(*) AS total FROM etl_mch_postnatal_visit
        UNION ALL
        SELECT 'TB Follow-up' AS visit_type, COUNT(*) AS total FROM etl_tb_follow_up_visit;
        `
      );
      if (Array.isArray(visitRows)) {
        const details = (visitRows as any[]).filter(
          (r) => r.visit_type != null && String(r.visit_type).trim() !== ""
        );
        visits = [
          {
            category: "visit_type",
            details: details.map((r) => ({
              visit_type: String(r.visit_type ?? ""),
              total: String(Number(r.total ?? 0))
            }))
          }
        ];
      }
      await etlConn.end();
    } catch (err) {
      log.push(
        `Worker: could not build visits dataset from ETL (visit tables): ${String(err)}`
      );
    }

    // Phase 4: admissions by age (Child vs Adult) to populate `admissions`.
    let admissions: Array<{ age: string; no_of_patients: string }> = [];
    try {
      const etlConn = await mysql.createConnection({
        host: dbCfg.host,
        port: dbCfg.port,
        user: dbCfg.user,
        password: dbCfg.password,
        database: "kenyaemr_etl"
      });

      const [admRows] = await etlConn.query(
        `
        SELECT
          CASE
            WHEN TIMESTAMPDIFF(YEAR, dob, CURRENT_DATE) < 5 THEN 'Child'
            ELSE 'Adult'
          END AS age,
          COUNT(*) AS no_of_patients
        FROM etl_current_in_care
        WHERE dob IS NOT NULL
        GROUP BY age
        ORDER BY no_of_patients DESC
        LIMIT 2;
        `
      );

      if (Array.isArray(admRows)) {
        admissions = (admRows as any[]).map((r) => ({
          age: String(r.age ?? ""),
          no_of_patients: String(Number(r.no_of_patients ?? 0))
        }));
      }
      await etlConn.end();
    } catch (err) {
      log.push(`Worker: could not build admissions dataset from ETL: ${String(err)}`);
    }

    // Phase 4: bed_management derived from admissions age distribution.
    let bedManagement:
      | Array<{ visit_type: string; total: string; age_details: Array<{ age: string; total: string }> }>
      | [] = [];
    if (admissions.length > 0) {
      const total = admissions.reduce(
        (sum, a) => sum + Number(a.no_of_patients ?? 0),
        0
      );
      bedManagement = [
        {
          visit_type: "Inpatient",
          total: String(total),
          age_details: admissions.map((a) => ({
            age: a.age,
            total: a.no_of_patients
          }))
        }
      ];
    }

    const payload = {
      sha_enrollments: "",
      Immunization: [],
      wait_time: waitTime,
      waivers: [{ waivers: "" }],
      payments: [],
      mfl_code: mflCodeFinal,
      diagnosis: [],
      workload,
      admissions,
      inventory: [],
      version: versionFinal,
      billing: [],
      visits,
      bed_management: bedManagement,
      mortality: [],
      staff_count: staffCount,
      timestamp
    };

    log.push(
      `Worker: POST ${endpointUrl} with mfl_code=${mflCodeFinal}, version=${versionFinal}, timestamp=${timestamp}`
    );

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      ...(cbsCfg.token ? { Authorization: `Bearer ${cbsCfg.token}` } : {})
    };

    let ok = false;
    let lastStatus = 0;
    let lastBody = "";
    const maxAttempts = 3;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      log.push(
        `Worker: attempt ${attempt}/${maxAttempts} (correlationId=${correlationId})`
      );
      try {
        const res = await fetch(endpointUrl, {
          method: "POST",
          headers,
          body: JSON.stringify(payload)
        });
        lastStatus = res.status;
        lastBody = await res.text();
        if (res.ok) {
          ok = true;
          break;
        }
      } catch (err) {
        lastBody = String(err);
      }

      await new Promise((r) => setTimeout(r, 500 * attempt));
    }

    log.push(`Worker: CBS/Interop responded HTTP ${lastStatus} (ok=${ok})`);

    return NextResponse.json(
      {
        ok,
        message: ok ? "Worker visualization push OK" : "Worker visualization push failed",
        log,
        payload,
        cbsStatus: lastStatus,
        cbsBody: lastBody
      },
      { status: ok ? 200 : 502 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Worker failed",
        error: String(err),
        log
      },
      { status: 500 }
    );
  }
}

