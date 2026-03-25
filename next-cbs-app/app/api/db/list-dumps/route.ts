import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import mysql from "mysql2/promise";
import { loadDbConfig } from "@lib/config";
import { exec } from "child_process";

export const runtime = "nodejs";

function execCommand(cmd: string): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    exec(cmd, (error, stdout, stderr) => {
      resolve({
        code: error ? (error as any).code ?? 1 : 0,
        stdout,
        stderr
      });
    });
  });
}

function extractDumpTimestamp(fileName: string): string | null {
  // Our naming pattern is: <YYYYMMDDHHMMSS>-<original>.sql or .sql.gz
  const m = fileName.match(/^(\d{14})-/);
  return m?.[1] ?? null;
}

export async function GET() {
  const log: string[] = [];
  try {
    const uploadsDir = path.join(process.cwd(), "uploaded_dbs");
    const statePath = path.join(uploadsDir, ".lcp_current_openmrs.json");

    // 1) Get disk free space for /
    const dfRes = await execCommand("df -B1 / | tail -n 1");
    const dfLine = (dfRes.stdout ?? "").trim();
    let rootAvailBytes: number | null = null;
    if (dfLine) {
      // Filesystem Size Used Avail Use% Mounted on
      const parts = dfLine.split(/\s+/);
      // Avail is typically index 3
      const avail = Number(parts[3]);
      if (Number.isFinite(avail)) rootAvailBytes = avail;
    }

    // 2) Read current marker (if present)
    let marker: any = null;
    try {
      const raw = await fs.readFile(statePath, "utf8");
      marker = JSON.parse(raw);
    } catch {
      marker = null;
    }

    const markerDbName: string | null = typeof marker?.dbName === "string" ? marker.dbName : null;
    const markerTs = markerDbName?.startsWith("openmrs_") ? markerDbName.slice("openmrs_".length) : null;

    // 3) List dump files + sizes
    let fileNames: string[] = [];
    try {
      fileNames = await fs.readdir(uploadsDir);
    } catch (e) {
      return NextResponse.json(
        { ok: false, message: `Cannot read uploads dir: ${uploadsDir}`, log: [String(e)] },
        { status: 500 }
      );
    }

    const dumpFiles = fileNames.filter((n) => n.endsWith(".sql") || n.endsWith(".sql.gz"));

    // 4) Query existing openmrs_<timestamp> schemas
    const cfg = loadDbConfig();
    const conn = await mysql.createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password
    });

    let openmrsTimestamps = new Set<string>();
    try {
      const [rows] = await conn.query("SHOW DATABASES;");
      const lines = Array.isArray(rows) ? (rows as any[]).map((r) => Object.values(r)[0]) : [];
      for (const line of lines) {
        const dbName = String(line ?? "");
        if (!dbName.startsWith("openmrs_")) continue;
        const ts = dbName.slice("openmrs_".length);
        if (/^\d{14}$/.test(ts)) openmrsTimestamps.add(ts);
      }
    } finally {
      await conn.end();
    }

    const dumps = [];
    let uploadedDbsBytes = 0;
    for (const fileName of dumpFiles) {
      const ts = extractDumpTimestamp(fileName);
      if (!ts) continue;
      const fullPath = path.join(uploadsDir, fileName);
      let sizeBytes = 0;
      try {
        const st = await fs.stat(fullPath);
        sizeBytes = st.size;
      } catch {
        sizeBytes = 0;
      }
      uploadedDbsBytes += sizeBytes;

      const openmrsDbExists = openmrsTimestamps.has(ts);
      const isOrphan = !openmrsDbExists;
      const isCurrent = markerTs && markerTs === ts;

      dumps.push({
        fileName,
        fullPath,
        timestamp: ts,
        sizeBytes,
        openmrsDbExists,
        isOrphan,
        isCurrent
      });
    }

    dumps.sort((a, b) => (b.timestamp ?? "").localeCompare(a.timestamp ?? ""));

    const orphanCount = dumps.filter((d) => d.isOrphan).length;
    return NextResponse.json(
      {
        ok: true,
        uploadsDir,
        statePath,
        marker: marker
          ? {
              dbName: markerDbName,
              uploadedDumpFileName: marker?.uploadedDumpFileName ?? null,
              createdAt: marker?.createdAt ?? null
            }
          : null,
        rootAvailBytes,
        uploadedDbsBytes,
        orphanCount,
        dumps
      },
      { status: 200 }
    );
  } catch (err) {
    log.push(`Error listing dumps: ${String(err)}`);
    return NextResponse.json(
      { ok: false, message: "Error listing dumps", error: String(err), log },
      { status: 500 }
    );
  }
}

