import { NextResponse } from "next/server";
import mysql from "mysql2/promise";
import { loadCbsConfig, loadDbConfig } from "@lib/config";
import { runEtlAndSyncMfl } from "@lib/etlRunner";

function formatTimestamp(date: Date) {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}${pad(
    date.getHours()
  )}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

export async function POST(req: Request) {
  try {
    const body = (await req.json().catch(() => ({}))) as {
      facilityCodeOverride?: string;
      openmrsDbNameOverride?: string; // reserved for future if we want to tailor ETL per DB
      skipEtlRefresh?: boolean;
    };
    const log: string[] = [];

    const sourceDbName = (body.openmrsDbNameOverride || "").trim() || "openmrs";
    const facilityCodeEffective = body.facilityCodeOverride?.trim() || null;
    const shouldRefresh = body.skipEtlRefresh ? false : true;

    if (shouldRefresh) {
      log.push(`Preview: auto-refresh ETL for '${sourceDbName}'...`);
      const etlRes = await runEtlAndSyncMfl(sourceDbName);
      log.push(...etlRes.log);
      if (!etlRes.ok) {
        throw new Error("ETL auto-refresh failed; cannot generate preview");
      }
    }

    const cbsCfg = loadCbsConfig();
    const dbCfg = loadDbConfig();

    log.push(
      `Preparing Visualization/Superset payload preview from OpenMRS DB '${sourceDbName}'...`
    );

    // Read version + facility MFL code and staffing from the selected OpenMRS DB
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
      const conn = await mysql.createConnection({
        host: dbCfg.host,
        port: dbCfg.port,
        user: dbCfg.user,
        password: dbCfg.password,
        database: sourceDbName
      });
      const [rows] = await conn.query(
        "SELECT property, property_value FROM global_property WHERE property IN ('kenyaemr.version','facility.mflcode');"
      );
      if (Array.isArray(rows)) {
        for (const r of rows as { property: string; property_value: string | null }[]) {
          if (r.property === "kenyaemr.version") emrVersion = r.property_value;
          if (r.property === "facility.mflcode") facilityMflFromDb = r.property_value;
        }
      }

      // Staff counts are derived from user_role.
      // IL Visualization payload uses `staff_count: [{staff, staff_count}, ...]`.
      const [staffRows] = await conn.query(
        "SELECT role AS staff, COUNT(DISTINCT user_id) AS staff_count FROM user_role GROUP BY role ORDER BY staff_count DESC;"
      );
      if (Array.isArray(staffRows)) {
        staffCount = (staffRows as any[]).map((r) => ({
          staff: String(r.staff ?? ""),
          staff_count: Number(r.staff_count ?? 0)
        }));
      }

      // Phase 4 workload: IL builds it from OpenMRS Visits + Encounter types.
      // We approximate within the same spirit:
      //   - department="Registration" -> count of relevant visits
      //   - department=<encounter_type.name> -> count of encounters in those visits
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

      // Phase 4 wait_time: derived from OpenMRS queue_entry + queue.
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
      await conn.end();
    } catch (err) {
      log.push(`Error reading OpenMRS global_property for version/MFL: ${String(err)}`);
    }

    const mflCodeFinal = facilityCodeEffective || facilityMflFromDb || "UNKNOWN";
    const versionFinal = emrVersion || "UNKNOWN";
    const timestamp = formatTimestamp(new Date());

    log.push(
      `Previewing payload for mfl_code=${mflCodeFinal}, version=${versionFinal}, timestamp=${timestamp} targeting ${cbsCfg.endpointUrl || "N/A"}`
    );

    // Payload wrapper based on kenyaemrIL VisualizationDataExchange keys seen in logs.
    // Phase 4 connectivity/schema test: we populate `staff_count` from `user_role` to avoid placeholders.
    // Derive some lightweight datasets from kenyaemr_etl (ETL output).
    // Phase 4: visits grouped by type_of_visit.
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
        const details = (visitRows as any[])
          .filter((r) => r.visit_type != null && String(r.visit_type).trim() !== "")
          .map((r) => ({
            visit_type: String(r.visit_type ?? ""),
            total: String(Number(r.total ?? 0))
          }));
        visits = [{ category: "visit_type", details }];
      }
      await etlConn.end();
    } catch (err) {
      // Keep payload valid even if ETL tables are missing or stale.
      log.push(
        `Preview: could not build visits dataset from etl (visit tables): ${String(err)}`
      );
    }

    // Phase 4: admissions by age (Child vs Adult) based on ETL inpatient admissions.
    // Matches legacy `VisualizationDataExchange` structure:
    //   admissions: [{ age: 'Child'|'Adult', no_of_patients: '...' }, ...]
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
      log.push(`Preview: could not build admissions dataset from ETL: ${String(err)}`);
    }

    // Phase 4: bed_management is derived from age distribution.
    // IL attaches `age_details` where each entry is { age, total }.
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

    return NextResponse.json(
      {
        ok: true,
        message: "Visualization payload preview generated",
        log,
        payload
      },
      { status: 200 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Error generating preview",
        error: String(err)
      },
      { status: 500 }
    );
  }
}

