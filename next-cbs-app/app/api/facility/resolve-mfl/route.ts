import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";

export const runtime = "nodejs";

type ResolveMflRequest = {
  facilityName?: string;
  county?: string; // optional, defaults to NYAMIRA for this console
};

type ResolveMflResponse = {
  ok: boolean;
  message: string;
  code?: string;
  matched?: {
    facility: string;
    county: string;
    subCounty: string;
    partner: string;
  };
};

function normalizeName(s: string) {
  return s
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s]/g, ""); // strip punctuation for safer matching
}

export async function POST(req: Request) {
  const body = (await req.json().catch(() => ({}))) as ResolveMflRequest;
  const facilityName = (body.facilityName ?? "").trim();
  const county = (body.county ?? "NYAMIRA").trim().toUpperCase();

  if (!facilityName) {
    return NextResponse.json(
      { ok: false, message: "facilityName is required" } satisfies ResolveMflResponse,
      { status: 400 }
    );
  }

  const csvPath = path.join(process.cwd(), "MFLCodes .csv");
  let csvText = "";
  try {
    csvText = await fs.readFile(csvPath, "utf8");
  } catch (e) {
    return NextResponse.json(
      {
        ok: false,
        message: `Could not read MFL mapping CSV at ${csvPath}: ${String(e)}`
      } satisfies ResolveMflResponse,
      { status: 500 }
    );
  }

  // Remove UTF-8 BOM if present (the file seems to include one).
  csvText = csvText.replace(/^\uFEFF/, "");

  const lines = csvText.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (lines.length <= 1) {
    return NextResponse.json(
      { ok: false, message: "MFL CSV appears empty or malformed" } satisfies ResolveMflResponse,
      { status: 500 }
    );
  }

  // Skip header line.
  const rows = lines.slice(1).map((line) => {
    // Expected columns:
    // Code,Facility,County,Sub-county,Partner
    const parts = line.split(",");
    const code = (parts[0] ?? "").trim();
    const facility = (parts[1] ?? "").trim();
    const rowCounty = (parts[2] ?? "").trim();
    const subCounty = (parts[3] ?? "").trim();
    const partner = parts.slice(4).join(",").trim();
    return { code, facility, county: rowCounty, subCounty, partner };
  });

  const target = normalizeName(facilityName);

  // Filter to Nyamira first (so we don't accidentally return from other counties).
  const candidates = rows.filter((r) => normalizeName(r.county) === normalizeName(county));

  let best: (typeof rows)[number] | null = null;
  let bestScore = -1;

  for (const r of candidates) {
    const facilityNorm = normalizeName(r.facility);
    if (!facilityNorm) continue;

    // Scoring:
    // - exact match: highest score
    // - substring match: lower but still useful
    if (facilityNorm === target) {
      best = r;
      bestScore = 100;
      break;
    }

    if (facilityNorm.includes(target) || target.includes(facilityNorm)) {
      // Prefer longer facility names / more specific matches.
      const overlap = Math.min(facilityNorm.length, target.length);
      const score = 50 + overlap / 2;
      if (score > bestScore) {
        best = r;
        bestScore = score;
      }
    }
  }

  if (!best || !best.code) {
    return NextResponse.json(
      {
        ok: false,
        message: `No MFL match found for facilityName='${facilityName}' in county='${county}'.`
      } satisfies ResolveMflResponse,
      { status: 404 }
    );
  }

  return NextResponse.json(
    {
      ok: true,
      message: `Resolved facility '${facilityName}' to MFL code '${best.code}'`,
      code: best.code,
      matched: {
        facility: best.facility,
        county: best.county,
        subCounty: best.subCounty,
        partner: best.partner
      }
    } satisfies ResolveMflResponse,
    { status: 200 }
  );
}

