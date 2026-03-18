import { NextResponse } from "next/server";
import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";

type DetectRequest = {
  openmrsDbNameOverride?: string;
};

type DetectResponse = {
  ok: boolean;
  message?: string;
  facilityName: string | null;
  facilityMflCode: string | null;
  needsMflInput: boolean;
  mflSource: "global_property" | "etl_default_facility_info" | "unknown";
  log?: string[];
};

function isValidMfl(mfl: string | null | undefined) {
  const v = (mfl ?? "").trim();
  if (!v) return false;
  if (v.toUpperCase() === "UNKNOWN") return false;
  if (v === "12345") return false; // placeholder value from dumps
  return /^[0-9]+$/.test(v);
}

function safeParseFacilityInformation(raw: string): string | null {
  try {
    const parsed = JSON.parse(raw);
    const name = parsed?.facilityName;
    if (typeof name === "string" && name.trim()) return name.trim();
    return null;
  } catch {
    return null;
  }
}

export async function POST(req: Request) {
  const log: string[] = [];
  try {
    const body = (await req.json().catch(() => ({}))) as DetectRequest;
    const openmrsDbName = (body.openmrsDbNameOverride || "").trim();
    if (!openmrsDbName) {
      return NextResponse.json(
        {
          ok: false,
          message: "openmrsDbNameOverride is required"
        },
        { status: 400 }
      );
    }

    // Basic guard: prevent weird identifiers being injected into `database: ...`.
    if (!/^[a-zA-Z0-9_]+$/.test(openmrsDbName)) {
      return NextResponse.json(
        { ok: false, message: "Invalid openmrsDbNameOverride format" },
        { status: 400 }
      );
    }

    const cfg = loadDbConfig();
    const conn = await mysql.createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: openmrsDbName
    });

    try {
      log.push(`Detect: reading facility.mflcode + facility identifiers from '${openmrsDbName}'...`);

      // Keep the query tight: we only need a few globals.
      const [rows] = await conn.query(
        `
        SELECT property, property_value
        FROM global_property
        WHERE property IN (
          'facility.mflcode',
          'facility.name',
          'facilityName',
          'facility.reporting.name',
          'kenyaemr.defaultLocation',
          'kenyaemr.cashier.receipt.facilityInformation'
        )
        LIMIT 50;
        `
      );

      let mfl: string | null = null;
      let facilityNameFromGp: string | null = null;
      let defaultLocationId: number | null = null;
      let cashierInfoRaw: string | null = null;

      if (Array.isArray(rows)) {
        for (const r of rows as { property: string; property_value: string | null }[]) {
          if (r.property === "facility.mflcode") mfl = r.property_value ?? null;
          if (r.property === "facility.name") facilityNameFromGp = r.property_value ?? null;
          if (r.property === "facilityName") facilityNameFromGp = r.property_value ?? null;
          if (r.property === "facility.reporting.name") facilityNameFromGp = r.property_value ?? null;
          if (r.property === "kenyaemr.defaultLocation") {
            const n = Number(r.property_value);
            defaultLocationId = Number.isFinite(n) && n > 0 ? n : null;
          }
          if (r.property === "kenyaemr.cashier.receipt.facilityInformation") {
            cashierInfoRaw = r.property_value ?? null;
          }
        }
      }

      let facilityName: string | null = facilityNameFromGp;
      if (!facilityName && cashierInfoRaw) {
        facilityName = safeParseFacilityInformation(cashierInfoRaw);
      }

      // Fallback: resolve facility name from the OpenMRS location table using kenyaemr.defaultLocation.
      if (!facilityName && defaultLocationId) {
        log.push(`Detect: resolving facility name from location.location_id=${defaultLocationId}...`);
        try {
          const [locRows] = await conn.query(
            "SELECT name FROM location WHERE location_id = ? LIMIT 1;",
            [defaultLocationId]
          );
          if (Array.isArray(locRows) && locRows.length > 0) {
            const row = locRows[0] as { name?: string | null };
            if (row?.name && String(row.name).trim()) facilityName = String(row.name).trim();
          }
        } catch (e) {
          log.push(`Detect: location lookup failed: ${String(e)}`);
        }
      }

      let mflTrim = (mfl ?? "").trim();
      let needsMflInput = !isValidMfl(mflTrim);
      let mflSource: DetectResponse["mflSource"] = mfl ? "global_property" : "unknown";

      // Fallback: some dumps have placeholder facility.mflcode but still contain a valid
      // siteCode in ETL-like tables.
      if (needsMflInput) {
        try {
          log.push(
            `Detect: facility.mflcode is missing/placeholder (${mflTrim || "empty"}); trying etl_default_facility_info.siteCode...`
          );
          const [etlTables] = await conn.query(
            "SHOW TABLES LIKE 'etl_default_facility_info';"
          );
          const hasTable = Array.isArray(etlTables) && etlTables.length > 0;
          if (hasTable) {
            const [etlRows] = await conn.query(
              "SELECT siteCode, FacilityName FROM etl_default_facility_info LIMIT 1;"
            );
            if (Array.isArray(etlRows) && etlRows.length > 0) {
              const row = etlRows[0] as {
                siteCode?: string | null;
                FacilityName?: string | null;
              };
              const candidate = String(row.siteCode ?? "").trim();
              const candidateFacilityName = String(row.FacilityName ?? "").trim();

              if (isValidMfl(candidate)) {
                // Guard against stale ETL facility defaults inside the dump.
                // If the candidate ETL facility name doesn't match what we detected
                // for this dump, DO NOT auto-accept the MFL.
                const detectedName = (facilityName ?? "").trim().toLowerCase();
                const candidateNameLower = candidateFacilityName.toLowerCase();

                const namesMatch =
                  !!detectedName &&
                  !!candidateFacilityName &&
                  (candidateNameLower.includes(detectedName) ||
                    detectedName.includes(candidateNameLower) ||
                    detectedName.split(/\\s+/).some((t) => t.length >= 4 && candidateNameLower.includes(t)));

                if (namesMatch) {
                  mflTrim = candidate;
                  needsMflInput = false;
                  mflSource = "etl_default_facility_info";
                  log.push(
                    `Detect: resolved facility siteCode from etl_default_facility_info = ${candidate} (facilityName match ok)`
                  );
                } else {
                  log.push(
                    `Detect: etl_default_facility_info siteCode candidate=${candidate} but facilityName mismatch. detected='${facilityName}' candidate='${candidateFacilityName}'. Prompting user for MFL.`
                  );
                  needsMflInput = true;
                }
              }
            }
          }
        } catch (e) {
          log.push(`Detect: etl_default_facility_info fallback failed: ${String(e)}`);
        }
      }

      return NextResponse.json(
        {
          ok: true,
          facilityName: facilityName ?? null,
          facilityMflCode: isValidMfl(mflTrim) ? mflTrim : mflTrim || null,
          needsMflInput,
          mflSource,
          log
        } satisfies DetectResponse,
        { status: 200 }
      );
    } finally {
      await conn.end();
    }
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Facility detect failed",
        facilityName: null,
        facilityMflCode: null,
        needsMflInput: true,
        mflSource: "unknown",
        log
      },
      { status: 500 }
    );
  }
}

