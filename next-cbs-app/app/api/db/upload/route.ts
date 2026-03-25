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
    const formData = await req.formData();
    const file = formData.get("openmrsDump");

    if (!(file instanceof File)) {
      return NextResponse.json(
        { ok: false, message: "No file uploaded. Expecting 'openmrsDump' field." },
        { status: 400 }
      );
    }

    const arrayBuffer = await file.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    const uploadsDir = path.join(process.cwd(), "uploaded_dbs");
    await fs.mkdir(uploadsDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0, 14);
    const safeName = file.name.replace(/[^a-zA-Z0-9_.-]/g, "_");
    const fileName = `${timestamp}-${safeName || "openmrs.sql"}`;
    const fullPath = path.join(uploadsDir, fileName);

    await fs.writeFile(fullPath, buffer);

    const dbName = `openmrs_${timestamp}`;
    const log: string[] = [];

    // 1) Create database
    log.push(`Creating database '${dbName}'...`);
    // Database names we generate are alphanumeric + underscore, so backticks are not required.
    const createCmd = `mysql -uroot -ptest -e "CREATE DATABASE ${dbName};"`;
    const createRes = await runCommand(createCmd);
    log.push(`CREATE DATABASE exit code: ${createRes.code}`);
    if (createRes.stderr) log.push(`CREATE DATABASE stderr: ${createRes.stderr.trim()}`);

    if (createRes.code !== 0) {
      return NextResponse.json(
        {
          ok: false,
          message: `Failed to create database '${dbName}'`,
          fileName,
          fullPath,
          dbName,
          log
        },
        { status: 500 }
      );
    }

    // 2) Import dump into that database
    log.push(`Importing dump into '${dbName}' from ${fullPath}...`);
    const importCmd = `sh -c "mysql -uroot -ptest ${dbName} < '${fullPath.replace(/'/g, "'\\''")}'"`;
    const importRes = await runCommand(importCmd);
    log.push(`Import exit code: ${importRes.code}`);
    if (importRes.stderr) log.push(`Import stderr: ${importRes.stderr.trim()}`);

    if (importRes.code !== 0) {
      return NextResponse.json(
        {
          ok: false,
          message: `Failed to import dump into '${dbName}'`,
          fileName,
          fullPath,
          dbName,
          log
        },
        { status: 500 }
      );
    }

    // At this point, the OpenMRS DB is created and loaded. ETL refresh will be run
    // by the separate /api/db/run-etl + health pipeline.

    // Record where the "current loaded" dump/db live so later operations (like delete)
    // do not depend on browser localStorage.
    const statePath = path.join(uploadsDir, ".lcp_current_openmrs.json");
    const state = {
      dbName,
      uploadedDumpFileName: fileName,
      uploadedDumpFullPath: fullPath,
      uploadsDir,
      createdAt: new Date().toISOString()
    };
    await fs.writeFile(statePath, JSON.stringify(state, null, 2), "utf8");

    return NextResponse.json(
      {
        ok: true,
        message: "OpenMRS database dump uploaded and imported successfully",
        fileName,
        fullPath,
        dbName,
        log
      },
      { status: 200 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Error uploading/importing OpenMRS database dump",
        error: String(err)
      },
      { status: 500 }
    );
  }
}

