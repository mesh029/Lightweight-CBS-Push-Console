import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";

export const runtime = "nodejs";

type DeleteLoadedRequest = {
  openmrsDbNameOverride?: string;
  deleteDumpFile?: boolean;
};

export async function POST(req: Request) {
  const log: string[] = [];
  try {
    const body = (await req.json().catch(() => ({}))) as DeleteLoadedRequest;
    const uploadsDir = path.join(process.cwd(), "uploaded_dbs");
    const statePath = path.join(uploadsDir, ".lcp_current_openmrs.json");

    // Prefer the server-side "current loaded" marker so we don't depend on browser cache.
    let marker: any = null;
    try {
      const raw = await fs.readFile(statePath, "utf8");
      marker = JSON.parse(raw);
    } catch {
      marker = null;
    }

    const dbNameFromOverride = (body.openmrsDbNameOverride || "").trim();
    const dbName = (marker?.dbName || dbNameFromOverride || "").trim();

    if (!dbName) {
      return NextResponse.json(
        {
          ok: false,
          message: `Cannot determine which DB to delete. ${marker?.dbName ? `Marker present but invalid dbName: ${String(marker.dbName)}` : "No server marker found. Upload/import must have run at least once, or provide openmrsDbNameOverride."}`,
          log
        },
        { status: 400 }
      );
    }
    if (!/^[a-zA-Z0-9_]+$/.test(dbName)) {
      return NextResponse.json(
        { ok: false, message: "Invalid openmrsDbNameOverride format", log },
        { status: 400 }
      );
    }
    if (dbName === "openmrs") {
      return NextResponse.json(
        { ok: false, message: "Refusing to delete base 'openmrs' database", log },
        { status: 400 }
      );
    }

    const cfg = loadDbConfig();
    const conn = await mysql.createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password
    });

    try {
      log.push(`Dropping database '${dbName}' if it exists...`);
      await conn.query(`DROP DATABASE IF EXISTS \`${dbName}\`;`);
      log.push(`DROP DATABASE completed for '${dbName}'.`);
    } finally {
      await conn.end();
    }

    let deletedDumpFiles: string[] = [];
    if (body.deleteDumpFile) {
      // Imported DB names are openmrs_<YYYYMMDDHHMMSS>, while uploaded file names are
      // <YYYYMMDDHHMMSS>-<original>.sql. Use that shared timestamp for cleanup.
      const ts = dbName.startsWith("openmrs_") ? dbName.slice("openmrs_".length) : "";

      try {
        log.push(`Dump directory: ${uploadsDir}`);
        const names = await fs.readdir(uploadsDir);
        const matches = names.filter((n) => (ts ? n.startsWith(`${ts}-`) : false));
        for (const fileName of matches) {
          const fullPath = path.join(uploadsDir, fileName);
          await fs.unlink(fullPath);
          deletedDumpFiles.push(fileName);
        }
        if (matches.length === 0) {
          log.push("No matching dump file found for this DB timestamp.");
        } else {
          log.push(`Deleted ${matches.length} matching dump file(s).`);
        }
      } catch (e) {
        log.push(`Dump file cleanup skipped/failed: ${String(e)}`);
      }
    }

    // Clear marker so future "delete current loaded" calls don't re-delete the same DB.
    try {
      await fs.unlink(statePath);
      log.push("Cleared server-side current-loaded marker.");
    } catch {
      // ignore
    }

    return NextResponse.json(
      {
        ok: true,
        message: `Deleted loaded DB '${dbName}'`,
        dbName,
        deletedDumpFiles,
        uploadsDir,
        statePath,
        markerUsed: marker?.dbName ? { dbName: marker.dbName, uploadedDumpFileName: marker.uploadedDumpFileName } : null,
        log
      },
      { status: 200 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Failed to delete loaded DB",
        error: String(err),
        log
      },
      { status: 500 }
    );
  }
}

