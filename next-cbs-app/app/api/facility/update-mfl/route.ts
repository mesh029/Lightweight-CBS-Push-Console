import { NextResponse } from "next/server";
import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";

export async function POST(req: Request) {
  try {
    const body = (await req.json()) as { newMfl?: string; openmrsDbNameOverride?: string };
    const raw = (body.newMfl ?? "").trim();

    if (!raw) {
      return NextResponse.json(
        { ok: false, message: "newMfl is required" },
        { status: 400 }
      );
    }

    // Basic sanity check: numeric code
    if (!/^[0-9]+$/.test(raw)) {
      return NextResponse.json(
        { ok: false, message: "newMfl must be numeric (MFL code)" },
        { status: 400 }
      );
    }

    const cfg = loadDbConfig();
    const log: string[] = [];
    log.push(`Updating facility MFL code to ${raw}...`);

    // 1. Update/insert OpenMRS global_property.facility.mflcode
    // global_property.property is the PK, so INSERT..ON DUPLICATE KEY UPDATE is safe.
    const openmrsConn = await mysql.createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: body.openmrsDbNameOverride || "openmrs"
    });

    const [gpResult] = await openmrsConn.query(
      "INSERT INTO global_property (property, property_value, uuid) VALUES ('facility.mflcode', ?, UUID()) ON DUPLICATE KEY UPDATE property_value = VALUES(property_value);",
      [raw]
    );
    log.push("Updated openmrs.global_property.facility.mflcode");
    await openmrsConn.end();

    // 2. Update ETL facility tables if present
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
      log.push("Updated kenyaemr_etl.etl_default_facility_info.siteCode");
    } else {
      log.push("etl_default_facility_info not found in kenyaemr_etl (skipped)");
    }
    await etlConn.end();

    // 3. Update datatools default_facility_info if present
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
        "UPDATE default_facility_info SET siteCode = ?;",
        [raw]
      );
      log.push("Updated kenyaemr_datatools.default_facility_info.siteCode");
    } else {
      log.push("default_facility_info not found in kenyaemr_datatools (skipped)");
    }
    await dtConn.end();

    return NextResponse.json(
      {
        ok: true,
        message: "Facility MFL code updated in OpenMRS + ETL/datools (where present)",
        newMfl: raw,
        log
      },
      { status: 200 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Error updating facility MFL code",
        error: String(err)
      },
      { status: 500 }
    );
  }
}

