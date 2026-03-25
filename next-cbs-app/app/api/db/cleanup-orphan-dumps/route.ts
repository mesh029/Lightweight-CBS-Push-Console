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
  const m = fileName.match(/^(\d{14})-/);
  return m?.[1] ?? null;
}

export async function POST(req: Request) {
  const log: string[] = [];
  try {
    const body = (await req.json().catch(() => ({}))) as { dryRun?: boolean };
    const dryRun = Boolean(body?.dryRun);

    const uploadsDir = path.join(process.cwd(), "uploaded_dbs");
    const statePath = path.join(uploadsDir, ".lcp_current_openmrs.json");

    // Read marker; we still delete only "orphans" but might want to preserve marker-related dumps later.
    let marker: any = null;
    try {
      const raw = await fs.readFile(statePath, "utf8");
      marker = JSON.parse(raw);
    } catch {
      marker = null;
    }
    const markerDbName: string | null = typeof marker?.dbName === "string" ? marker.dbName : null;
    const markerTs = markerDbName?.startsWith("openmrs_") ? markerDbName.slice("openmrs_".length) : null;

    // Get existing openmrs_<timestamp> DBs
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

    // List dump files
    const fileNames = await fs.readdir(uploadsDir);
    const dumpFiles = fileNames.filter((n) => n.endsWith(".sql") || n.endsWith(".sql.gz"));

    const orphans: Array<{ fileName: string; ts: string; sizeBytes: number }> = [];
    for (const fileName of dumpFiles) {
      const ts = extractDumpTimestamp(fileName);
      if (!ts) continue;
      const openmrsDbExists = openmrsTimestamps.has(ts);
      const isOrphan = !openmrsDbExists;
      if (!isOrphan) continue;
      const fullPath = path.join(uploadsDir, fileName);
      let sizeBytes = 0;
      try {
        const st = await fs.stat(fullPath);
        sizeBytes = st.size;
      } catch {
        sizeBytes = 0;
      }
      // Defensive: never delete the file that corresponds to current marker timestamp.
      if (markerTs && markerTs === ts) {
        continue;
      }
      orphans.push({ fileName, ts, sizeBytes });
    }

    orphans.sort((a, b) => (a.ts ?? "").localeCompare(b.ts ?? ""));

    const totalBytes = orphans.reduce((s, o) => s + (o.sizeBytes ?? 0), 0);

    if (orphans.length === 0) {
      return NextResponse.json(
        {
          ok: true,
          message: "No orphan dump files found to delete",
          uploadsDir,
          orphanCount: 0,
          deletedFiles: [],
          freedBytes: 0,
          dryRun
        },
        { status: 200 }
      );
    }

    if (dryRun) {
      return NextResponse.json(
        {
          ok: true,
          message: `Dry run: would delete ${orphans.length} orphan dump file(s)`,
          uploadsDir,
          orphanCount: orphans.length,
          deletedFiles: [],
          freedBytes: totalBytes,
          dryRun,
          sample: orphans.slice(0, 5)
        },
        { status: 200 }
      );
    }

    const deletedFiles: string[] = [];
    for (const o of orphans) {
      const fullPath = path.join(uploadsDir, o.fileName);
      try {
        await fs.unlink(fullPath);
        deletedFiles.push(o.fileName);
        log.push(`Deleted orphan dump: ${o.fileName} (${o.sizeBytes} bytes)`);
      } catch (e) {
        log.push(`Failed to delete ${o.fileName}: ${String(e)}`);
      }
    }

    // Check disk after cleanup (best effort)
    let rootAvailBytes: number | null = null;
    try {
      const dfRes = await execCommand("df -B1 / | tail -n 1");
      const dfLine = (dfRes.stdout ?? "").trim();
      const parts = dfLine.split(/\s+/);
      const avail = Number(parts[3]);
      if (Number.isFinite(avail)) rootAvailBytes = avail;
    } catch {
      rootAvailBytes = null;
    }

    return NextResponse.json(
      {
        ok: true,
        message: `Deleted ${deletedFiles.length} orphan dump file(s)`,
        uploadsDir,
        orphanCount: orphans.length,
        deletedFiles,
        freedBytes: totalBytes,
        rootAvailBytes,
        dryRun,
        log
      },
      { status: 200 }
    );
  } catch (err) {
    return NextResponse.json(
      { ok: false, message: "Error cleaning orphan dumps", error: String(err) },
      { status: 500 }
    );
  }
}

