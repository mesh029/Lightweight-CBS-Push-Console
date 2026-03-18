import { NextResponse } from "next/server";
import { runEtlAndSyncMfl } from "@lib/etlRunner";

export async function POST(req: Request) {
  try {
    const body = (await req.json().catch(() => ({}))) as {
      openmrsDbNameOverride?: string;
    };

    const dbName = (body.openmrsDbNameOverride || "").trim();
    if (!dbName) {
      return NextResponse.json(
        { ok: false, message: "openmrsDbNameOverride is required" },
        { status: 400 }
      );
    }
    const result = await runEtlAndSyncMfl(dbName);
    return NextResponse.json(
      {
        ok: result.ok,
        message: result.ok
          ? `ETL procedures + MFL sync completed for DB '${dbName}'.`
          : `ETL procedures and/or MFL sync had issues for DB '${dbName}' (see log).`,
        openmrsDbNameUsed: dbName,
        log: result.log
      },
      { status: result.ok ? 200 : 207 }
    );
  } catch (err) {
    return NextResponse.json(
      {
        ok: false,
        message: "Error running ETL procedures",
        error: String(err)
      },
      { status: 500 }
    );
  }
}

