import { NextResponse } from "next/server";
import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";
import { runEtlAndSyncMfl } from "@lib/etlRunner";
import { randomUUID } from "crypto";

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
  // Sometimes ETL may store "Male"/"Female" or 1/0; keep strict output expected by server.
  if (s.startsWith("M")) return "MALE";
  if (s.startsWith("F")) return "FEMALE";
  return "";
}

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
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
      for (const r of rows as { property: string; property_value: string | null }[]) {
        map[r.property] = r.property_value ?? null;
      }
    }
    return map;
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
      versionOverride?: string;
      // Optional testing controls. 0/undefined means "no hard cap besides what DB returns".
      maxNewCases?: number;
      maxLinkedCases?: number;
      maxEligibleForVl?: number;
      maxHeiAt6to8Weeks?: number;
    };

    const openmrsDbName = (body.openmrsDbNameOverride || "").trim() || "openmrs";
    const facilityCodeOverride = body.facilityCodeOverride?.trim() || null;
    const versionOverride = body.versionOverride?.trim() || null;
    const shouldRefresh = body.skipEtlRefresh ? false : true;

    const maxNewCases =
      typeof body.maxNewCases === "number" && body.maxNewCases > 0 ? body.maxNewCases : 0;
    const maxLinkedCases =
      typeof body.maxLinkedCases === "number" && body.maxLinkedCases > 0 ? body.maxLinkedCases : 0;
    const maxEligibleForVl =
      typeof body.maxEligibleForVl === "number" && body.maxEligibleForVl > 0
        ? body.maxEligibleForVl
        : 0;
    const maxHeiAt6to8Weeks =
      typeof body.maxHeiAt6to8Weeks === "number" && body.maxHeiAt6to8Weeks > 0
        ? body.maxHeiAt6to8Weeks
        : 0;

    if (shouldRefresh) {
      log.push(
        `Case-surveillance push: running ETL + facility sync for '${openmrsDbName}'...`
      );
      const etlRes = await runEtlAndSyncMfl(openmrsDbName);
      log.push(...etlRes.log);
      if (!etlRes.ok) throw new Error("ETL auto-refresh failed");
    }

    log.push(`Reading case-surveillance globals from '${openmrsDbName}'...`);
    const globals = await getCaseSurveillanceGlobals(openmrsDbName);

    const endpointUrl = globals["case.surveillance.base.url.api"];
    const tokenUrl = globals["case.surveillance.token.url"];
    const clientId = globals["case.surveillance.client.id"];
    const clientSecret = globals["case.surveillance.client.secret"];
    const emrVersion = globals["kenyaemr.version"];
    const facilityMflFromDb = globals["facility.mflcode"];

    if (!endpointUrl) throw new Error("Missing case.surveillance.base.url.api");
    if (!tokenUrl) throw new Error("Missing case.surveillance.token.url");
    if (!clientId) throw new Error("Missing case.surveillance.client.id");
    if (!clientSecret) throw new Error("Missing case.surveillance.client.secret");

    const facilityCodeFinal = facilityCodeOverride || facilityMflFromDb || "UNKNOWN";
    const versionFinal = versionOverride || emrVersion || "UNKNOWN";
    const timestamp = formatTimestamp(new Date());

    // Case surveillance expects a JSON array of EventBase objects created by IL
    // (see `mapToDatasetStructure` and `generateCaseSurveillancePayload`).
    // Phase 4: populate at least `roll_call`, `new_case`, and `linked_case` from ETL tables.
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

    const eventList: any[] = [
      {
        eventType: "roll_call",
        event: { mflCode: facilityCodeFinal, emrVersion: versionFinal }
      }
    ];

    try {
      log.push("Building case-surveillance events from kenyaemr_etl (ETL outputs)...");

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
    const newCaseArrLimited = maxNewCases > 0 ? newCaseArr.slice(0, maxNewCases) : newCaseArr;
    log.push(
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
    log.push(
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

    // Phase 4 expansion: include eligible_for_vl and HEI-at-6-to-8-weeks events
    // so we stop “only linked_case” payload behavior on the CS dashboards.
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
    log.push(
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
    log.push(
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

      log.push(`Built case-surveillance eventList size=${eventList.length}.`);
    } finally {
      await etlConn.end();
    }

    log.push(`Requesting bearer token (correlationId=${correlationId})...`);
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

    log.push(
      `Pushing case surveillance eventList via HTTP PUT to ${endpointUrl} (includes roll_call + new_case + linked_case + eligible_for_vl + hei_at_6_to_8_weeks)...`
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
      log.push(
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

    log.push(`Case-surveillance responded HTTP ${lastStatus} (ok=${ok})`);

    return NextResponse.json(
      {
        ok,
        message: ok
          ? "Case surveillance payload pushed successfully"
          : "Case surveillance push failed",
        correlationId,
        log,
        payload,
        eventList,
        cbsStatus: lastStatus,
        cbsBody: lastBody
      },
      { status: ok ? 200 : 502 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Case surveillance push failed",
        error: String(err),
        log
      },
      { status: 500 }
    );
  }
}

