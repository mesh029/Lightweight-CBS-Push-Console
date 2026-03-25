import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";

export const runtime = "nodejs";

type DeleteDumpRequest = {
  fileName?: string;
  // Safety: never delete the current-loaded dump unless explicitly allowed.
  allowCurrent?: boolean;
};

function extractDumpTimestamp(fileName: string): string | null {
  const m = fileName.match(/^(\d{14})-/);
  return m?.[1] ?? null;
}

export async function POST(req: Request) {
  const log: string[] = [];
  try {
    const body = (await req.json().catch(() => ({}))) as DeleteDumpRequest;
    const fileName = (body.fileName ?? "").trim();
    const allowCurrent = Boolean(body.allowCurrent);

    if (!fileName) {
      return NextResponse.json({ ok: false, message: "fileName is required", log }, { status: 400 });
    }

    // Safety: only allow our expected dump naming patterns.
    // Examples:
    // - 20260317125225-openmrskineniMarch4.sql
    // - 20260318211745-openmrsKinara06march2026.sql.gz
    if (!/^\d{14}-.+\.(sql|sql\.gz)$/.test(fileName)) {
      return NextResponse.json(
        { ok: false, message: "Invalid fileName format", log: [...log, `fileName=${fileName}`] },
        { status: 400 }
      );
    }

    const uploadsDir = path.join(process.cwd(), "uploaded_dbs");
    const fullPath = path.join(uploadsDir, fileName);

    // Read marker, if present.
    let markerTs: string | null = null;
    const statePath = path.join(uploadsDir, ".lcp_current_openmrs.json");
    try {
      const raw = await fs.readFile(statePath, "utf8");
      const marker = JSON.parse(raw);
      const dbName = typeof marker?.dbName === "string" ? marker.dbName : null;
      markerTs = dbName?.startsWith("openmrs_") ? dbName.slice("openmrs_".length) : null;
    } catch {
      markerTs = null;
    }

    const fileTs = extractDumpTimestamp(fileName);
    if (markerTs && fileTs && markerTs === fileTs && !allowCurrent) {
      return NextResponse.json(
        {
          ok: false,
          message:
            "Refusing to delete the current-loaded dump file. Use the bulk 'Delete orphan dumps' or delete the DB first.",
          log: [...log, `markerTs=${markerTs}`, `fileTs=${fileTs}`]
        },
        { status: 400 }
      );
    }

    await fs.unlink(fullPath);
    log.push(`Deleted ${fileName}`);

    return NextResponse.json(
      {
        ok: true,
        message: "Dump file deleted",
        fileName,
        uploadsDir,
        markerTs
      },
      { status: 200 }
    );
  } catch (err) {
    return NextResponse.json(
      { ok: false, message: "Failed to delete dump file", error: String(err), log },
      { status: 500 }
    );
  }
}

