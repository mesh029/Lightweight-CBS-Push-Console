import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { exec } from "child_process";

export const runtime = "nodejs";

function runCommand(cmd: string): Promise<{ code: number | null; stdout: string; stderr: string }> {
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

export async function POST(req: Request) {
  try {
    const body = (await req.json().catch(() => ({}))) as {
      openmrsDbNameOverride?: string;
      deleteDumpFile?: boolean;
    };

    const dbName = String(body.openmrsDbNameOverride || "").trim();
    if (!dbName) {
      return NextResponse.json(
        { ok: false, message: "openmrsDbNameOverride is required" },
        { status: 400 }
      );
    }

    if (!/^openmrs_[0-9]{14}$/.test(dbName)) {
      return NextResponse.json(
        {
          ok: false,
          message:
            "Refusing delete: only timestamped uploaded DB names are allowed (openmrs_YYYYMMDDHHMMSS)."
        },
        { status: 400 }
      );
    }

    const log: string[] = [];
    const ts = dbName.replace(/^openmrs_/, "");
    const uploadsDir = path.join(process.cwd(), "uploaded_dbs");

    log.push(`Dropping database '${dbName}'...`);
    const dropCmd = `mysql -uroot -ptest -e "DROP DATABASE IF EXISTS ${dbName};"`;
    const dropRes = await runCommand(dropCmd);
    log.push(`DROP DATABASE exit code: ${dropRes.code}`);
    if (dropRes.stderr) log.push(`DROP DATABASE stderr: ${dropRes.stderr.trim()}`);

    let deletedFiles: string[] = [];
    if (body.deleteDumpFile !== false) {
      log.push(`Looking for uploaded dump file(s) matching timestamp '${ts}'...`);
      try {
        const entries = await fs.readdir(uploadsDir, { withFileTypes: true });
        const matches = entries
          .filter((e) => e.isFile() && e.name.startsWith(`${ts}-`) && e.name.endsWith(".sql"))
          .map((e) => e.name);

        for (const fileName of matches) {
          const fullPath = path.join(uploadsDir, fileName);
          await fs.unlink(fullPath);
          deletedFiles.push(fileName);
        }
      } catch {
        // If uploaded_dbs does not exist or cannot be read, continue; DB drop is still useful cleanup.
      }
      if (deletedFiles.length) {
        log.push(`Deleted dump file(s): ${deletedFiles.join(", ")}`);
      } else {
        log.push("No matching dump file found to delete.");
      }
    }

    const ok = dropRes.code === 0;
    return NextResponse.json(
      {
        ok,
        message: ok
          ? `Deleted loaded DB '${dbName}'${deletedFiles.length ? " and matching dump file(s)." : "."}`
          : `Failed to drop DB '${dbName}' (see log).`,
        dbName,
        deletedDumpFiles: deletedFiles,
        log
      },
      { status: ok ? 200 : 500 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Error deleting loaded OpenMRS DB/dump",
        error: String(err)
      },
      { status: 500 }
    );
  }
}

