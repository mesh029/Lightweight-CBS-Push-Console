import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";
import { runEtlAndSyncMfl } from "@lib/etlRunner";
import { randomUUID } from "crypto";

export const runtime = "nodejs";

function formatTimestamp(date: Date) {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}${pad(date.getMonth() + 1)}${pad(date.getDate())}${pad(
    date.getHours()
  )}${pad(date.getMinutes())}${pad(date.getSeconds())}`;
}

function normalizeSex(sex: unknown): string {
  if (sex == null) return "";
  const s = String(sex).trim().toUpperCase();
  if (!s) return "";
  if (s === "M" || s === "MALE") return "MALE";
  if (s === "F" || s === "FEMALE") return "FEMALE";
  if (s.startsWith("M")) return "MALE";
  if (s.startsWith("F")) return "FEMALE";
  return "";
}

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function updateOpenmrsFacilityMflAndEtlsiteCode(
  openmrsDbName: string,
  newMfl: string,
  log: (line: string) => void
) {
  const raw = newMfl.trim();
  if (!/^[0-9]+$/.test(raw)) {
    throw new Error(
      `updateOpenmrsFacilityMflAndEtlsiteCode: newMfl must be numeric, got '${newMfl}'`
    );
  }

  const cfg = loadDbConfig();

  // 1) Update OpenMRS global_property.facility.mflcode
  log(`Facility sync: setting openmrs.global_property.facility.mflcode='${raw}'...`);
  const openmrsConn = await mysql.createConnection({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: openmrsDbName
  });

  await openmrsConn.query(
    "INSERT INTO global_property (property, property_value, uuid) VALUES ('facility.mflcode', ?, UUID()) ON DUPLICATE KEY UPDATE property_value = VALUES(property_value);",
    [raw]
  );
  await openmrsConn.end();

  // 2) Update ETL default facility info tables if present
  const etlConn = await mysql.createConnection({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: "kenyaemr_etl"
  });

  const [etlTables] = await etlConn.query(
    "SHOW TABLES LIKE 'etl_default_facility_info';"
  );
  if (Array.isArray(etlTables) && etlTables.length > 0) {
    await etlConn.query(
      "UPDATE etl_default_facility_info SET siteCode = ?;",
      [raw]
    );
    log("Facility sync: updated kenyaemr_etl.etl_default_facility_info.siteCode");
  } else {
    log("Facility sync: etl_default_facility_info not found in kenyaemr_etl (skipped)");
  }
  await etlConn.end();

  // 3) Update datatools default facility info if present
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
    await dtConn.query("UPDATE default_facility_info SET siteCode = ?;", [raw]);
    log("Facility sync: updated kenyaemr_datatools.default_facility_info.siteCode");
  } else {
    log("Facility sync: default_facility_info not found in kenyaemr_datatools (skipped)");
  }
  await dtConn.end();
}

async function verifyFacilityMflMatchesEtlSiteCode(
  openmrsDbName: string,
  expectedMfl: string,
  log: (line: string) => void
) {
  const rawExpected = expectedMfl.trim();
  const cfg = loadDbConfig();

  const openmrsConn = await mysql.createConnection({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: openmrsDbName
  });

  const [gpRows] = await openmrsConn.query(
    "SELECT property_value FROM global_property WHERE property = 'facility.mflcode' LIMIT 1;"
  );
  await openmrsConn.end();

  const openmrsValue =
    Array.isArray(gpRows) && gpRows.length
      ? String((gpRows[0] as any)?.property_value ?? "").trim()
      : "";
  if (!openmrsValue) {
    throw new Error(
      "verifyFacilityMflMatchesEtlSiteCode: openmrs global_property.facility.mflcode is empty/missing"
    );
  }
  if (openmrsValue !== rawExpected) {
    throw new Error(
      `verifyFacilityMflMatchesEtlSiteCode: openmrs facility.mflcode mismatch expected=${rawExpected} actual=${openmrsValue}`
    );
  }

  const etlConn = await mysql.createConnection({
    host: cfg.host,
    port: cfg.port,
    user: cfg.user,
    password: cfg.password,
    database: "kenyaemr_etl"
  });

  const [etlRows] = await etlConn.query(
    "SELECT siteCode FROM etl_default_facility_info LIMIT 1;"
  );
  await etlConn.end();

  const etlValue =
    Array.isArray(etlRows) && etlRows.length
      ? String((etlRows[0] as any)?.siteCode ?? "").trim()
      : "";
  if (!etlValue) {
    throw new Error(
      "verifyFacilityMflMatchesEtlSiteCode: kenyaemr_etl.etl_default_facility_info.siteCode is empty/missing"
    );
  }
  if (etlValue !== rawExpected) {
    throw new Error(
      `verifyFacilityMflMatchesEtlSiteCode: etl siteCode mismatch expected=${rawExpected} actual=${etlValue}`
    );
  }

  log(
    `Facility verification OK: openmrs.facility.mflcode=${rawExpected}, etl.siteCode=${etlValue}`
  );
}

async function getCaseSurveillanceGlobals(openmrsDbName: string) {
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
      "SELECT property, property_value FROM global_property WHERE property IN (" +
        "'case.surveillance.base.url.api'," +
        "'case.surveillance.token.url'," +
        "'case.surveillance.client.id'," +
        "'case.surveillance.client.secret'," +
        "'kenyaemr.version'," +
        "'facility.mflcode'" +
        ");"
    );

    const map: Record<string, string | null> = {};
    if (Array.isArray(rows)) {
      for (const r of rows as {
        property: string;
        property_value: string | null;
      }[]) {
        map[r.property] = r.property_value ?? null;
      }
    }
    return map;
  } finally {
    await conn.end();
  }
}

export async function POST(req: Request) {
  const encoder = new TextEncoder();
  const correlationId = randomUUID();

  const body = (await req.json().catch(() => ({}))) as {
    openmrsDbNameOverride?: string;
    facilityCodeOverride?: string;
    skipEtlRefresh?: boolean;
    versionOverride?: string;
    maxNewCases?: number;
    maxLinkedCases?: number;
    maxEligibleForVl?: number;
    maxHeiAt6to8Weeks?: number;
    includeEventTypes?: string[];
  };

  const openmrsDbName = (body.openmrsDbNameOverride || "").trim() || "openmrs";
  const facilityCodeOverride = body.facilityCodeOverride?.trim() || null;
  const versionOverride = body.versionOverride?.trim() || null;

  // Critical safety: always refresh ETL before pushing.
  const shouldRefresh = true;

  const maxNewCases =
    typeof body.maxNewCases === "number" && body.maxNewCases > 0 ? body.maxNewCases : 0;
  const maxLinkedCases =
    typeof body.maxLinkedCases === "number" && body.maxLinkedCases > 0 ? body.maxLinkedCases : 0;
  const maxEligibleForVl =
    typeof body.maxEligibleForVl === "number" && body.maxEligibleForVl > 0 ? body.maxEligibleForVl : 0;
  const maxHeiAt6to8Weeks =
    typeof body.maxHeiAt6to8Weeks === "number" && body.maxHeiAt6to8Weeks > 0
      ? body.maxHeiAt6to8Weeks
      : 0;

  const allowedEventTypes = new Set([
    "roll_call",
    "new_case",
    "linked_case",
    "eligible_for_vl",
    "hei_at_6_to_8_weeks"
  ]);

  const requestedTypes = Array.isArray(body.includeEventTypes)
    ? body.includeEventTypes
        .map((t) => String(t).trim())
        .filter((t) => allowedEventTypes.has(t))
    : [];

  const includeEventTypes = new Set(
    requestedTypes.length > 0
      ? requestedTypes
      : ["roll_call", "new_case", "linked_case", "eligible_for_vl", "hei_at_6_to_8_weeks"]
  );

  if (includeEventTypes.size === 0) {
    const result = {
      ok: false,
      message: "No valid event types selected for case surveillance push",
      correlationId,
      log: [],
      payload: null,
      eventList: [],
      pushSummary: null,
      cbsStatus: null,
      cbsBody: null
    };
    return new Response(
      JSON.stringify(result),
      { status: 400, headers: { "Content-Type": "application/json" } }
    );
  }

  const stream = new ReadableStream({
    async start(controller) {
      const emitLog = (line: string) => {
        controller.enqueue(
          encoder.encode(JSON.stringify({ type: "log", line }) + "\n")
        );
      };

      const emitDone = (result: any) => {
        controller.enqueue(encoder.encode(JSON.stringify({ type: "done", result }) + "\n"));
      };

      const log: string[] = [];
      let finalResult: any = null;

      const safePushLog = (line: string) => {
        log.push(line);
        emitLog(line);
      };

      try {
        safePushLog(
          `Case surveillance push (correlationId=${correlationId}) started for OpenMRS DB '${openmrsDbName}'`
        );

        if (shouldRefresh) {
          if (facilityCodeOverride) {
            await updateOpenmrsFacilityMflAndEtlsiteCode(
              openmrsDbName,
              facilityCodeOverride,
              safePushLog
            );
          }

          safePushLog(`Case-surveillance push: running ETL + facility sync for '${openmrsDbName}'...`);
          const etlRes = await runEtlAndSyncMfl(openmrsDbName);
          safePushLog("ETL runner: completed.");
          for (const l of etlRes.log) safePushLog(`ETL runner: ${l}`);
          if (!etlRes.ok) {
            throw new Error("ETL auto-refresh failed; cannot push payload");
          }
          safePushLog("ETL refresh forced=true (completed) for requested facility.");
        }

        safePushLog(`Reading case-surveillance globals from '${openmrsDbName}'...`);
        let globals = await getCaseSurveillanceGlobals(openmrsDbName);

        let endpointUrl = globals["case.surveillance.base.url.api"];
        let tokenUrl = globals["case.surveillance.token.url"];
        let clientId = globals["case.surveillance.client.id"];
        let clientSecret = globals["case.surveillance.client.secret"];
        let emrVersion = globals["kenyaemr.version"];
        let facilityMflFromDb = globals["facility.mflcode"];

        if (facilityCodeOverride) {
          const requested = facilityCodeOverride.trim();
          const current = String(facilityMflFromDb ?? "").trim();
          if (requested && current !== requested) {
            safePushLog(
              `Facility mismatch detected: requested=${requested} but openmrs.facility.mflcode=${current || "empty"}. Updating + forcing ETL rerun...`
            );
            await updateOpenmrsFacilityMflAndEtlsiteCode(openmrsDbName, requested, safePushLog);
            const etlRes = await runEtlAndSyncMfl(openmrsDbName);
            for (const l of etlRes.log) safePushLog(`ETL runner: ${l}`);
            if (!etlRes.ok) {
              throw new Error(
                "ETL auto-refresh failed after facility mismatch correction"
              );
            }

            globals = await getCaseSurveillanceGlobals(openmrsDbName);
            endpointUrl = globals["case.surveillance.base.url.api"];
            tokenUrl = globals["case.surveillance.token.url"];
            clientId = globals["case.surveillance.client.id"];
            clientSecret = globals["case.surveillance.client.secret"];
            emrVersion = globals["kenyaemr.version"];
            facilityMflFromDb = globals["facility.mflcode"];
          }
        }

        const facilityCodeFinal = facilityCodeOverride || facilityMflFromDb || "UNKNOWN";
        const versionFinal = versionOverride || emrVersion || "UNKNOWN";
        const timestamp = formatTimestamp(new Date());

        if (!endpointUrl) throw new Error("Missing case.surveillance.base.url.api");
        if (!tokenUrl) throw new Error("Missing case.surveillance.token.url");
        if (!clientId) throw new Error("Missing case.surveillance.client.id");
        if (!clientSecret) throw new Error("Missing case.surveillance.client.secret");

        await verifyFacilityMflMatchesEtlSiteCode(
          openmrsDbName,
          facilityCodeFinal,
          safePushLog
        );

        const payload = {
          mfl_code: facilityCodeFinal,
          version: versionFinal,
          timestamp
        };

        const dbCfg = loadDbConfig();
        const etlConn = await mysql.createConnection({
          host: dbCfg.host,
          port: dbCfg.port,
          user: dbCfg.user,
          password: dbCfg.password,
          database: "kenyaemr_etl"
        });

        const eventList: any[] = [];
        let builtEventTypeCounts: Record<string, number> = {};
        let builtMinCreatedAt: string | null = null;
        let builtMaxCreatedAt: string | null = null;

        if (includeEventTypes.has("roll_call")) {
          eventList.push({
            eventType: "roll_call",
            event: { mflCode: facilityCodeFinal, emrVersion: versionFinal }
          });
        }

        try {
          safePushLog("Building case-surveillance events from kenyaemr_etl (ETL outputs)...");

          if (includeEventTypes.has("new_case")) {
            const [newCaseRows] = await etlConn.query(
              `
        SELECT
          t.patient_id AS patientPk,
          addr.county AS county,
          addr.sub_county AS subCounty,
          addr.ward AS ward,
          demo.Gender AS sex,
          DATE_FORMAT(demo.DOB, '%Y-%m-%d') AS dob,
          DATE_FORMAT(t.date_created, '%Y-%m-%d %H:%i:%s') AS createdAt,
          DATE_FORMAT(COALESCE(t.date_last_modified, t.date_created), '%Y-%m-%d %H:%i:%s') AS updatedAt,
          DATE_FORMAT(t.date_created, '%Y-%m-%d %H:%i:%s') AS positiveHivTestDate
        FROM (
          SELECT patient_id, MAX(date_created) AS date_created
          FROM etl_hts_test
          WHERE (voided = 0 OR voided IS NULL)
            AND final_test_result = 'Positive'
          GROUP BY patient_id
        ) pos
        INNER JOIN etl_hts_test t
          ON t.patient_id = pos.patient_id AND t.date_created = pos.date_created
        INNER JOIN etl_patient_demographics demo
          ON demo.patient_id = t.patient_id
        LEFT JOIN etl_person_address addr
          ON addr.patient_id = t.patient_id
          AND (addr.voided = 0 OR addr.voided IS NULL)
        `
            );

            const newCaseArr = (Array.isArray(newCaseRows) ? newCaseRows : []) as any[];
            const newCaseArrLimited =
              maxNewCases > 0 ? newCaseArr.slice(0, maxNewCases) : newCaseArr;
            safePushLog(
              `Case event building: new_case candidates=${newCaseArr.length}, sent=${newCaseArrLimited.length} (maxNewCases=${maxNewCases})`
            );

            for (const r of newCaseArrLimited) {
              eventList.push({
                eventType: "new_case",
                client: {
                  county: r.county ?? "",
                  subCounty: r.subCounty ?? "",
                  ward: r.ward ?? "",
                  patientPk: String(r.patientPk ?? ""),
                  sex: normalizeSex(r.sex),
                  dob: r.dob ?? ""
                },
                event: {
                  mflCode: facilityCodeFinal,
                  createdAt: r.createdAt ?? null,
                  updatedAt: r.updatedAt ?? null,
                  positiveHivTestDate: r.positiveHivTestDate ?? null
                }
              });
            }
          } else {
            safePushLog("Case event building: new_case skipped by selection.");
          }

          if (includeEventTypes.has("linked_case")) {
            const [linkedCaseRows] = await etlConn.query(
              `
        SELECT
          e.patient_id AS patientPk,
          addr.county AS county,
          addr.sub_county AS subCounty,
          addr.ward AS ward,
          demo.Gender AS sex,
          DATE_FORMAT(demo.DOB, '%Y-%m-%d') AS dob,
          DATE_FORMAT(COALESCE(e.date_created, e.date_started_art_at_transferring_facility), '%Y-%m-%d %H:%i:%s') AS createdAt,
          DATE_FORMAT(COALESCE(e.date_last_modified, e.date_started_art_at_transferring_facility), '%Y-%m-%d %H:%i:%s') AS updatedAt,
          DATE_FORMAT(e.date_started_art_at_transferring_facility, '%Y-%m-%d') AS artStartDate
        FROM etl_hiv_enrollment e
        INNER JOIN etl_patient_demographics demo
          ON demo.patient_id = e.patient_id
        LEFT JOIN etl_person_address addr
          ON addr.patient_id = e.patient_id
          AND (addr.voided = 0 OR addr.voided IS NULL)
        WHERE (e.voided = 0 OR e.voided IS NULL)
          AND e.date_started_art_at_transferring_facility IS NOT NULL
        `
            );

            const linkedCaseArr = (Array.isArray(linkedCaseRows) ? linkedCaseRows : []) as any[];
            const linkedCaseArrLimited =
              maxLinkedCases > 0 ? linkedCaseArr.slice(0, maxLinkedCases) : linkedCaseArr;

            safePushLog(
              `Case event building: linked_case candidates=${linkedCaseArr.length}, sent=${linkedCaseArrLimited.length} (maxLinkedCases=${maxLinkedCases})`
            );

            for (const r of linkedCaseArrLimited) {
              eventList.push({
                eventType: "linked_case",
                client: {
                  county: r.county ?? "",
                  subCounty: r.subCounty ?? "",
                  ward: r.ward ?? "",
                  patientPk: String(r.patientPk ?? ""),
                  sex: normalizeSex(r.sex),
                  dob: r.dob ?? ""
                },
                event: {
                  mflCode: facilityCodeFinal,
                  createdAt: r.createdAt ?? null,
                  updatedAt: r.updatedAt ?? null,
                  artStartDate: r.artStartDate ?? null,
                  positiveHivTestDate: null
                }
              });
            }
          } else {
            safePushLog("Case event building: linked_case skipped by selection.");
          }

          // eligible_for_vl
          if (includeEventTypes.has("eligible_for_vl")) {
            const [eligibleVlRows] = await etlConn.query(
              `
      SELECT
        v.patient_id AS patientPk,
        addr.county AS county,
        addr.sub_county AS subCounty,
        addr.ward AS ward,
        demo.Gender AS sex,
        DATE_FORMAT(demo.DOB, '%Y-%m-%d') AS dob,
        DATE_FORMAT(v.date_created, '%Y-%m-%d %H:%i:%s') AS createdAt,
        DATE_FORMAT(v.date_created, '%Y-%m-%d %H:%i:%s') AS updatedAt,
        DATE_FORMAT(v.date_confirmed_hiv_positive, '%Y-%m-%d %H:%i:%s') AS positiveHivTestDate,
        DATE_FORMAT(v.latest_hiv_followup_visit, '%Y-%m-%d %H:%i:%s') AS visitDate,
        DATE_FORMAT(v.date_started_art, '%Y-%m-%d %H:%i:%s') AS artStartDate,
        DATE_FORMAT(v.date_test_requested, '%Y-%m-%d %H:%i:%s') AS lastVlOrderDate,
        v.vl_result AS lastVlResults,
        DATE_FORMAT(v.date_test_result_received, '%Y-%m-%d %H:%i:%s') AS lastVlResultsDate,
        v.order_reason AS vlOrderReason,
        CASE
          WHEN v.pregnancy_status = '1065' THEN 'YES'
          WHEN v.pregnancy_status = '1066' THEN 'NO'
          ELSE null
        END AS pregnancyStatus,
        CASE
          WHEN v.breastfeeding_status = '1065' THEN 'YES'
          WHEN v.breastfeeding_status = '1066' THEN 'NO'
          ELSE null
        END AS breastFeedingStatus
      FROM etl_viral_load_validity_tracker v
      INNER JOIN etl_patient_demographics demo
        ON demo.patient_id = v.patient_id
      LEFT JOIN etl_person_address addr
        ON addr.patient_id = v.patient_id
        AND (addr.voided = 0 OR addr.voided IS NULL)
      WHERE v.date_started_art IS NOT NULL
        AND (
          (TIMESTAMPDIFF(MONTH, v.date_started_art, CURRENT_DATE()) >= 3
            AND v.base_viral_load_test_result IS NULL)
          OR
          ((v.pregnancy_status = '1065' OR v.breastfeeding_status = '1065')
            AND TIMESTAMPDIFF(MONTH, v.date_started_art, CURRENT_DATE()) >= 3
            AND v.vl_result IS NOT NULL
            AND v.date_test_requested < CURRENT_DATE()
            AND v.order_reason NOT IN ('159882','1434','2001237','163718'))
          OR
          (v.lab_test = 856
            AND CAST(v.vl_result AS UNSIGNED) >= 200
            AND TIMESTAMPDIFF(MONTH, v.date_test_requested, CURRENT_DATE()) >= 3)
          OR
          (((v.lab_test = 1305 AND CAST(v.vl_result AS UNSIGNED) = 1302) OR CAST(v.vl_result AS UNSIGNED) < 200)
            AND TIMESTAMPDIFF(MONTH, v.date_test_requested, CURRENT_DATE()) >= 6
            AND TIMESTAMPDIFF(YEAR, demo.DOB, v.date_test_requested) BETWEEN 0 AND 24)
          OR
          (((v.lab_test = 1305 AND CAST(v.vl_result AS UNSIGNED) = 1302) OR CAST(v.vl_result AS UNSIGNED) < 200)
            AND TIMESTAMPDIFF(MONTH, v.date_test_requested, CURRENT_DATE()) >= 12
            AND TIMESTAMPDIFF(YEAR, demo.DOB, v.date_test_requested) > 24)
          OR
          ((v.pregnancy_status = '1065' OR v.breastfeeding_status = '1065')
            AND TIMESTAMPDIFF(MONTH, v.date_started_art, CURRENT_DATE()) >= 3
            AND v.order_reason IN ('159882','1434','2001237','163718')
            AND TIMESTAMPDIFF(MONTH, v.date_test_requested, CURRENT_DATE()) >= 6
            AND ((v.lab_test = 1305 AND CAST(v.vl_result AS UNSIGNED) = 1302) OR CAST(v.vl_result AS UNSIGNED) < 200))
        );
      `
            );

            const eligibleVlArr = (Array.isArray(eligibleVlRows) ? eligibleVlRows : []) as any[];
            const eligibleVlArrLimited =
              maxEligibleForVl > 0 ? eligibleVlArr.slice(0, maxEligibleForVl) : eligibleVlArr;

            safePushLog(
              `Case event building: eligible_for_vl candidates=${eligibleVlArr.length}, sent=${eligibleVlArrLimited.length} (maxEligibleForVl=${maxEligibleForVl})`
            );

            for (const r of eligibleVlArrLimited) {
              eventList.push({
                eventType: "eligible_for_vl",
                client: {
                  county: r.county ?? "",
                  subCounty: r.subCounty ?? "",
                  ward: r.ward ?? "",
                  patientPk: String(r.patientPk ?? ""),
                  sex: normalizeSex(r.sex),
                  dob: r.dob ?? ""
                },
                event: {
                  mflCode: facilityCodeFinal,
                  createdAt: r.createdAt ?? null,
                  updatedAt: r.updatedAt ?? null,
                  positiveHivTestDate: r.positiveHivTestDate ?? null,
                  artStartDate: r.artStartDate ?? null,
                  pregnancyStatus: r.pregnancyStatus ?? null,
                  breastFeedingStatus: r.breastFeedingStatus ?? null,
                  lastVlOrderDate: r.lastVlOrderDate ?? null,
                  lastVlResults: r.lastVlResults ?? null,
                  lastVlResultsDate: r.lastVlResultsDate ?? null,
                  visitDate: r.visitDate ?? null,
                  vlOrderReason: r.vlOrderReason ?? null
                }
              });
            }
          } else {
            safePushLog("Case event building: eligible_for_vl skipped by selection.");
          }

          // hei_at_6_to_8_weeks
          if (includeEventTypes.has("hei_at_6_to_8_weeks")) {
            const [heiAt6to8Rows] = await etlConn.query(
              `
      SELECT
        h.patient_id AS patientPk,
        addr.county AS county,
        addr.sub_county AS subCounty,
        addr.ward AS ward,
        demo.Gender AS sex,
        DATE_FORMAT(demo.DOB, '%Y-%m-%d') AS dob,
        DATE_FORMAT(h.date_created, '%Y-%m-%d %H:%i:%s') AS createdAt,
        DATE_FORMAT(COALESCE(h.date_last_modified, h.date_created), '%Y-%m-%d %H:%i:%s') AS updatedAt,
        demo.hei_no AS heiId
      FROM etl_hei_follow_up_visit h
      INNER JOIN etl_patient_demographics demo
        ON demo.patient_id = h.patient_id
      LEFT JOIN etl_person_address addr
        ON addr.patient_id = h.patient_id
        AND (addr.voided = 0 OR addr.voided IS NULL)
      WHERE h.followup_type = 5622;
      `
            );

            const heiAt6to8Arr = (Array.isArray(heiAt6to8Rows) ? heiAt6to8Rows : []) as any[];
            const heiAt6to8ArrLimited =
              maxHeiAt6to8Weeks > 0 ? heiAt6to8Arr.slice(0, maxHeiAt6to8Weeks) : heiAt6to8Arr;

            safePushLog(
              `Case event building: hei_at_6_to_8_weeks candidates=${heiAt6to8Arr.length}, sent=${heiAt6to8ArrLimited.length} (maxHeiAt6to8Weeks=${maxHeiAt6to8Weeks})`
            );

            for (const r of heiAt6to8ArrLimited) {
              eventList.push({
                eventType: "hei_at_6_to_8_weeks",
                client: {
                  county: r.county ?? "",
                  subCounty: r.subCounty ?? "",
                  ward: r.ward ?? "",
                  patientPk: String(r.patientPk ?? ""),
                  sex: normalizeSex(r.sex),
                  dob: r.dob ?? ""
                },
                event: {
                  mflCode: facilityCodeFinal,
                  createdAt: r.createdAt ?? null,
                  updatedAt: r.updatedAt ?? null,
                  heiId: r.heiId ?? null
                }
              });
            }
          } else {
            safePushLog("Case event building: hei_at_6_to_8_weeks skipped by selection.");
          }

          safePushLog(`Built case-surveillance eventList size=${eventList.length}.`);

          builtEventTypeCounts = {};
          builtMinCreatedAt = null;
          builtMaxCreatedAt = null;

          for (const ev of eventList) {
            const t = String(ev?.eventType ?? "");
            if (!t) continue;

            builtEventTypeCounts[t] = (builtEventTypeCounts[t] ?? 0) + 1;

            const createdAt = String(ev?.event?.createdAt ?? "");
            if (createdAt) {
              if (builtMinCreatedAt == null || createdAt < builtMinCreatedAt) builtMinCreatedAt = createdAt;
              if (builtMaxCreatedAt == null || createdAt > builtMaxCreatedAt) builtMaxCreatedAt = createdAt;
            }

            if ((ev?.event?.mflCode ?? null) !== facilityCodeFinal) {
              throw new Error(
                `payload facility label mismatch detected: facilityCodeFinal=${facilityCodeFinal} eventType=${t} event.mflCode=${String(
                  ev?.event?.mflCode ?? ""
                )}`
              );
            }
          }

          safePushLog(
            `Event fingerprint BEFORE PUT: facilityCodeFinal=${facilityCodeFinal} eventTypeCounts=${JSON.stringify(
              builtEventTypeCounts
            )} createdAtRange=${builtMinCreatedAt ?? "n/a"}..${builtMaxCreatedAt ?? "n/a"}`
          );
          safePushLog(
            `pushSummary BEFORE PUT: ${JSON.stringify({
              etlRefreshForced: true,
              facilityCodeFinal,
              versionFinal,
              includeEventTypes: Array.from(includeEventTypes),
              eventTypeCounts: builtEventTypeCounts,
              createdAtRange: { min: builtMinCreatedAt, max: builtMaxCreatedAt }
            })}`
          );
        } finally {
          await etlConn.end();
        }

        safePushLog(`Requesting bearer token (correlationId=${correlationId})...`);
        const tokenRes = await fetch(tokenUrl, {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            grant_type: "client_credentials",
            client_id: clientId,
            client_secret: clientSecret
          }).toString()
        });
        const tokenText = await tokenRes.text();

        if (!tokenRes.ok) {
          throw new Error(
            `Failed to retrieve bearer token: HTTP ${tokenRes.status} body=${tokenText}`
          );
        }

        let tokenJson: any = null;
        try {
          tokenJson = JSON.parse(tokenText);
        } catch {
          // token endpoint sometimes returns non-JSON; we keep the raw text for logs.
        }

        const bearerToken =
          tokenJson?.access_token || tokenJson?.token || tokenJson?.accessToken;
        if (!bearerToken) {
          throw new Error("Bearer token not found in token response JSON.");
        }

        safePushLog(
          `Pushing case surveillance eventList via HTTP PUT to ${endpointUrl} (selected event types: ${Array.from(
            includeEventTypes
          ).join(", ")})...`
        );

        const headers: Record<string, string> = {
          "Content-Type": "application/json",
          Authorization: `Bearer ${bearerToken}`
        };

        let ok = false;
        let lastStatus = 0;
        let lastBody = "";
        const maxAttempts = 3;

        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          safePushLog(
            `Case-surveillance push attempt ${attempt}/${maxAttempts} (correlationId=${correlationId})`
          );
          try {
            const res = await fetch(endpointUrl, {
              method: "PUT",
              headers,
              body: JSON.stringify(eventList)
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

          await sleep(500 * attempt);
        }

        safePushLog(`Case-surveillance responded HTTP ${lastStatus} (ok=${ok})`);

        finalResult = {
          ok,
          message: ok
            ? "Case surveillance payload pushed successfully"
            : "Case surveillance push failed",
          correlationId,
          log,
          payload,
          eventList,
          pushSummary: {
            etlRefreshForced: true,
            facilityCodeFinal,
            versionFinal,
            includeEventTypes: Array.from(includeEventTypes),
            eventTypeCounts: builtEventTypeCounts,
            createdAtRange: { min: builtMinCreatedAt, max: builtMaxCreatedAt }
          },
          cbsStatus: lastStatus,
          cbsBody: lastBody
        };
        emitDone(finalResult);
        controller.close();
      } catch (err) {
        finalResult = {
          ok: false,
          message: "Case surveillance push failed",
          error: String(err),
          correlationId,
          log,
          payload: null,
          eventList: [],
          pushSummary: null,
          cbsStatus: null,
          cbsBody: null
        };
        emitDone(finalResult);
        controller.close();
      }
    }
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "application/x-ndjson",
      "Cache-Control": "no-cache",
      Connection: "keep-alive"
    }
  });
}

