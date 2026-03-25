"use client";

import { useEffect, useState } from "react";

type ApiResult = {
  ok: boolean;
  message: string;
  details?: unknown;
};

type HealthDetails = {
  emrVersion?: string | null;
  host?: {
    nodeVersion?: string;
    platform?: string;
    release?: string;
    arch?: string;
  };
  db?: {
    env?: { host?: string; name?: string };
    status?: string;
    etlCurrentInCareCount?: number | null;
    facility?: { mflCode?: string | null; name?: string | null } | null;
    openmrsMflFromGlobalProperty?: string | null;
    error?: string | null;
  };
  cbs?: {
    env?: { endpointUrl?: string | null; facilityCode?: string | null; facilityMflSource?: string };
    status?: string;
    reachable?: boolean;
    httpStatus?: number | null;
    error?: string | null;
  };
  configIssues?: string[];
  log?: string[];
  openmrsDbNameUsed?: string;
};

export default function HomePage() {
  const [health, setHealth] = useState<ApiResult | null>(null);
  const [pushResult, setPushResult] = useState<ApiResult | null>(null);
  const [casePushResult, setCasePushResult] = useState<ApiResult | null>(null);
  const [previewResult, setPreviewResult] = useState<ApiResult | null>(null);
  const [uploadResult, setUploadResult] = useState<ApiResult | null>(null);
  const [etlResult, setEtlResult] = useState<ApiResult | null>(null);
  const [etlRanDbName, setEtlRanDbName] = useState<string | null>(null);
  const [loadingHealth, setLoadingHealth] = useState(false);
  const [loadingPush, setLoadingPush] = useState(false);
  const [loadingCasePush, setLoadingCasePush] = useState(false);
  const [loadingPreview, setLoadingPreview] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [versionOverride, setVersionOverride] = useState<string>("");
  const [openmrsFile, setOpenmrsFile] = useState<File | null>(null);
  const [facilityCode, setFacilityCode] = useState<string>("");
  const [facilityName, setFacilityName] = useState<string>("");
  const [detectedMflCode, setDetectedMflCode] = useState<string>("");
  const [mflSource, setMflSource] = useState<string>("unknown");
  const [mflNeedsInput, setMflNeedsInput] = useState<boolean>(true);
  const [loadingDetectFacility, setLoadingDetectFacility] = useState(false);
  const [deletingLoadedDb, setDeletingLoadedDb] = useState(false);
  const [detectLog, setDetectLog] = useState<string[]>([]);
  const [openmrsDbName, setOpenmrsDbName] = useState<string>("");
  const lastOpenmrsDbKey = "lcp_last_openmrs_db_name";
  const [step, setStep] = useState<number>(0);
  const [terminalLines, setTerminalLines] = useState<string[]>([]);

  const clearTerminal = () => setTerminalLines([]);
  const appendTerminal = (title: string, lines?: unknown[] | null) => {
    const safeLines =
      (lines ?? [])
        .filter((l) => l != null)
        .map((l) => String(l)) ?? [];

    setTerminalLines((prev) => [
      ...prev,
      `>>> ${title}`,
      ...(safeLines.length ? safeLines : ["(no log lines)"])
    ]);
  };

  // For streaming progress we append lines without repeating the ">>> title" header.
  const appendTerminalLine = (line: string) => {
    setTerminalLines((prev) => [...prev, line]);
  };

  // If the user previously uploaded/imported a DB, let them reuse it without re-uploading.
  // We only prompt on first load of the page.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const last = window.localStorage.getItem(lastOpenmrsDbKey);
    if (!last) return;
    if (openmrsDbName.trim()) return;

    const ok = window.confirm(
      `Use previously loaded OpenMRS DB?\n\nDB: ${last}\n\nIf you proceed, you can run Health/ETL and push again without re-uploading.`
    );
    if (!ok) return;

    setOpenmrsDbName(last);
    clearTerminal();
    appendTerminal("Cache", [`Loaded OpenMRS DB from local cache: ${last}`]);
  }, []);

  // Step mapping:
  // step=0 => Wizard Step 1 (Upload + Health)
  // step=1 => Wizard Step 2 (Confirm MFL)
  // step=2 => Wizard Step 3 (Visualization Preview)
  // step=3 => Wizard Step 4 (Push Visualization + Program Monitoring)
  const wizardActiveStep = step + 1;

  const buildVersionOptions = () => {
    const detected = (health?.details?.emrVersion ?? "").trim();

    // We want versions that exist in your upgrade packets and that you can realistically test
    // when the CS endpoint accepts the payload but the monitoring dashboard doesn't update.
    // Order matters: keep the detected version first.
    const orderedCandidates: string[] = [];
    if (detected) orderedCandidates.push(detected);

    if (detected.startsWith("19.2")) {
      // Gianchore-like: 19.2.2 -> test 19.3.x variants.
      orderedCandidates.push("19.2.2", "19.3.0", "19.3.1", "19.3.2");
    } else if (detected.startsWith("19.3")) {
      orderedCandidates.push("19.3.0", "19.3.1", "19.3.2", "19.2.2");
    } else {
      // Unknown dump: offer the known set.
      orderedCandidates.push("19.2.2", "19.3.0", "19.3.1", "19.3.2");
    }

    const uniq = Array.from(new Set(orderedCandidates)).filter(Boolean);
    // Keep the UI simple: max 4 options.
    return uniq.slice(0, 4);
  };

  const isFacilityMflValid = (code: string) => {
    const v = code.trim();
    if (!v) return false;
    if (v === "12345") return false;
    if (v.toUpperCase() === "UNKNOWN") return false;
    return /^[0-9]+$/.test(v);
  };

  const detectFacility = async (dbNameOverride?: string) => {
    const dbToUse = (dbNameOverride ?? openmrsDbName).trim();
    if (!dbToUse) return;

    setLoadingDetectFacility(true);
    try {
      const res = await fetch("/api/facility/detect", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ openmrsDbNameOverride: dbToUse })
      });
      const data = await res.json();

      if (!res.ok || !data.ok) {
        setFacilityName("");
        setDetectedMflCode("");
        setMflNeedsInput(true);
        return;
      }

      setFacilityName(String(data.facilityName ?? ""));
      setDetectedMflCode(String(data.facilityMflCode ?? ""));
      setMflNeedsInput(Boolean(data.needsMflInput));
      setMflSource(String(data.mflSource ?? "unknown"));
      setDetectLog((data.log ?? []) as string[]);
      appendTerminal("Facility detect", (data.log ?? []) as string[]);

      const mflFromDetect = String(data.facilityMflCode ?? "");
      if (!data.needsMflInput && isFacilityMflValid(mflFromDetect)) {
        setFacilityCode(mflFromDetect);
      } else {
        // Only clear if the user hasn't entered something valid.
        setFacilityCode((prev) => (isFacilityMflValid(prev) ? prev : ""));
      }
    } catch {
      setFacilityName("");
      setDetectedMflCode("");
      setMflSource("unknown");
      setDetectLog([]);
      setMflNeedsInput(true);
    } finally {
      setLoadingDetectFacility(false);
    }
  };

  const callHealth = async () => {
    const dbName = openmrsDbName.trim();
    if (!dbName) {
      setHealth({ ok: false, message: "Please enter an OpenMRS DB name before running health check" });
      return;
    }

    clearTerminal();
    setLoadingHealth(true);
    setHealth(null);
    try {
      try {
        window.localStorage.setItem(lastOpenmrsDbKey, dbName);
      } catch {
        // ignore
      }
      await detectFacility(dbName);
      const res = await fetch("/api/health", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          openmrsDbNameOverride: dbName,
          autoRefreshEtl: false
        })
      });
      const data = (await res.json()) as HealthDetails & { message?: string };
      setHealth({
        ok: res.ok,
        message: data.message ?? (res.ok ? "Health check OK" : "Health check failed"),
        details: data
      });
      appendTerminal("Health check", (data.log ?? []) as string[]);
      setStep(1);
    } catch (e) {
      setHealth({ ok: false, message: String(e) });
      appendTerminal("Health check failed", [String(e)]);
    } finally {
      setLoadingHealth(false);
    }
  };

  const runEtlAndHealth = async () => {
    const dbName = openmrsDbName.trim();
    if (!dbName) {
      setHealth({
        ok: false,
        message: "Please enter an OpenMRS DB name before running ETL + health check"
      });
      return;
    }

    clearTerminal();
    setLoadingHealth(true);
    setEtlResult(null);
    setHealth(null);
    try {
      try {
        window.localStorage.setItem(lastOpenmrsDbKey, dbName);
      } catch {
        // ignore
      }
      await detectFacility(dbName);

      // 1) Run ETL procedures for this DB
      const etlRes = await fetch("/api/db/run-etl", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ openmrsDbNameOverride: dbName })
      });
      const etlData = await etlRes.json();
      setEtlResult({
        ok: etlRes.ok,
        message:
          etlData.message ||
          (etlRes.ok ? "ETL procedures completed" : "ETL procedures had issues"),
        details: etlData
      });
      appendTerminal("ETL + MFL sync", (etlData.log ?? []) as string[]);
      if (etlData?.openmrsDbNameUsed) {
        setEtlRanDbName(String(etlData.openmrsDbNameUsed));
      } else {
        setEtlRanDbName(dbName);
      }

      // 2) Immediately run health check against the same DB
      const res = await fetch("/api/health", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ openmrsDbNameOverride: dbName, autoRefreshEtl: false })
      });
      const data = (await res.json()) as HealthDetails & { message?: string };

      // Prefer the ETL facility siteCode returned by /api/health.
      // This avoids using stale/incorrect global_property.facility.mflcode from some dumps.
      const mflFromEtlHealth = data?.db?.facility?.mflCode;
      const healthFacilityName = (data?.db?.facility?.name ?? "").trim().toLowerCase();
      const currentDetectedName = (facilityName ?? "").trim().toLowerCase();

      // Only auto-fill if the health facility name matches what we detected from the dump.
      const namesMatch =
        !!currentDetectedName &&
        !!healthFacilityName &&
        (healthFacilityName.includes(currentDetectedName) ||
          currentDetectedName.includes(healthFacilityName) ||
          currentDetectedName.split(/\\s+/).some((t) => t.length >= 4 && healthFacilityName.includes(t)));

      if (isFacilityMflValid(String(mflFromEtlHealth ?? "")) && namesMatch) {
        const resolved = String(mflFromEtlHealth);
        setFacilityCode(resolved);
        setDetectedMflCode(resolved);
        setMflNeedsInput(false);
        setMflSource("etl_default_facility_info");
        if (data?.db?.facility?.name) setFacilityName(String(data.db.facility.name));
        appendTerminal("Health ETL facility MFL", [
          `Auto-filled facilityCode from ETL health = ${resolved}`
        ]);
      }

      setHealth({
        ok: res.ok,
        message: data.message ?? (res.ok ? "Health check OK" : "Health check failed"),
        details: data
      });
      appendTerminal("Health check", (data.log ?? []) as string[]);
      setStep(1);
    } catch (e) {
      setHealth({ ok: false, message: String(e) });
      appendTerminal("ETL + health failed", [String(e)]);
    } finally {
      setLoadingHealth(false);
    }
  };

  const callPush = async () => {
    clearTerminal();
    const code = facilityCode.trim();
    if (!isFacilityMflValid(code)) {
      setPushResult({
        ok: false,
        message: "Enter a valid Facility MFL code (missing/placeholder like 12345 is not allowed)."
      });
      return;
    }
    setLoadingPush(true);
    setPushResult(null);
    try {
      appendTerminal("Visualization push", ["Starting visualization push..."]);
      const dbName = openmrsDbName.trim();
      const shouldSkipEtl = Boolean(etlRanDbName && dbName && etlRanDbName === dbName);
      const res = await fetch("/api/cbs/push-sample/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          facilityCodeOverride: facilityCode.trim() || undefined,
          openmrsDbNameOverride: dbName || undefined,
          versionOverride: versionOverride.trim() || undefined,
          skipEtlRefresh: shouldSkipEtl
        })
      });
      const bodyStream = res.body;
      if (!bodyStream) {
        throw new Error("Visualization push stream not available.");
      }

      const reader = bodyStream.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let finalResult: any = null;

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";

        for (const rawLine of lines) {
          const line = rawLine.trim();
          if (!line) continue;
          let parsed: any = null;
          try {
            parsed = JSON.parse(line);
          } catch {
            // Ignore malformed partial lines
            continue;
          }
          if (parsed?.type === "log") {
            appendTerminalLine(String(parsed.line ?? ""));
          }
          if (parsed?.type === "done") {
            finalResult = parsed.result;
            break;
          }
        }

        if (finalResult) break;
      }

      if (!finalResult) {
        throw new Error("Visualization push did not return a final result from the stream.");
      }

      const cbsStatus = finalResult?.cbsStatus;
      const cbsBody = finalResult?.cbsBody;
      let message =
        finalResult?.message ??
        (finalResult?.ok ? "CBS push OK" : "CBS push failed");
      if (!finalResult?.ok && cbsStatus != null) {
        message += ` (CBS HTTP ${cbsStatus})`;
      }
      if (!finalResult?.ok && cbsBody != null) {
        message += `: ${String(cbsBody).slice(0, 300)}`;
      }

      setPushResult({
        ok: Boolean(finalResult?.ok),
        message,
        details: finalResult
      });

      // If ETL actually ran for this push, record it so preview->push doesn't refresh again.
      if (!shouldSkipEtl && finalResult?.ok) setEtlRanDbName(dbName);
    } catch (e) {
      setPushResult({ ok: false, message: String(e) });
      appendTerminal("Visualization push failed", [String(e)]);
    } finally {
      setLoadingPush(false);
    }
  };

  const callPreview = async (): Promise<boolean> => {
    clearTerminal();
    const code = facilityCode.trim();
    if (!isFacilityMflValid(code)) {
      setPreviewResult({
        ok: false,
        message: "Enter a valid Facility MFL code (missing/placeholder like 12345 is not allowed)."
      });
      appendTerminal("Visualization preview", [
        "Aborted: invalid Facility MFL code"
      ]);
      return false;
    }
    setLoadingPreview(true);
    setPreviewResult(null);
    try {
      appendTerminal("Visualization preview", ["Starting preview payload generation..."]);
      const dbName = openmrsDbName.trim();
      const shouldSkipEtl = Boolean(etlRanDbName && dbName && etlRanDbName === dbName);
      const res = await fetch("/api/cbs/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          facilityCodeOverride: facilityCode.trim() || undefined,
          openmrsDbNameOverride: dbName || undefined,
          versionOverride: versionOverride.trim() || undefined,
          skipEtlRefresh: shouldSkipEtl
        })
      });
      const data = await res.json();
      const maybeErr = (data as any)?.error;
      setPreviewResult({
        ok: res.ok,
        message:
          (data as any)?.message ??
          (res.ok ? "Preview generated" : `Preview failed${maybeErr ? `: ${String(maybeErr)}` : ""}`),
        details: data
      });
      appendTerminal("Visualization preview", (data.log ?? []) as string[]);
      if (res.ok) {
        // If preview was forced to refresh ETL, mark it so the subsequent push doesn't refresh again.
        if (!shouldSkipEtl) setEtlRanDbName(dbName);
        setStep(3);
      }
      return res.ok;
    } catch (e) {
      setPreviewResult({ ok: false, message: String(e) });
      appendTerminal("Visualization preview failed", [String(e)]);
      return false;
    } finally {
      setLoadingPreview(false);
    }
  };

  const callCaseSurveillancePush = async () => {
    clearTerminal();
    const code = facilityCode.trim();
    if (!isFacilityMflValid(code)) {
      setCasePushResult({
        ok: false,
        message: "Enter a valid Facility MFL code (missing/placeholder like 12345 is not allowed)."
      });
      return;
    }
    setLoadingCasePush(true);
    setCasePushResult(null);
    try {
      appendTerminal("Case surveillance push", ["Starting case surveillance push..."]);
      const dbName = openmrsDbName.trim();
      const shouldSkipEtl = Boolean(etlRanDbName && dbName && etlRanDbName === dbName);
      const res = await fetch("/api/cbs/push-case-surveillance", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          facilityCodeOverride: facilityCode.trim() || undefined,
          openmrsDbNameOverride: dbName || undefined,
          versionOverride: versionOverride.trim() || undefined,
          skipEtlRefresh: shouldSkipEtl
        })
      });
      const data = await res.json();
      const cbsStatus = (data as any)?.cbsStatus;
      const cbsBody = (data as any)?.cbsBody;
      let message =
        (data as any)?.message ??
        (res.ok ? "Case surveillance push OK" : "Case surveillance push failed");
      if (!res.ok && cbsStatus != null) {
        message += ` (CS HTTP ${cbsStatus})`;
      }
      if (!res.ok && cbsBody != null) {
        message += `: ${String(cbsBody).slice(0, 300)}`;
      }
      setCasePushResult({
        ok: res.ok,
        message,
        details: data
      });
      appendTerminal("Case surveillance push", (data.log ?? []) as string[]);
    } catch (e) {
      setCasePushResult({ ok: false, message: String(e) });
      appendTerminal("Case surveillance push failed", [String(e)]);
    } finally {
      setLoadingCasePush(false);
    }
  };

  const pushAll = async () => {
    clearTerminal();
    appendTerminal("Push all", ["Starting: Visualization push -> Program Monitoring batch (roll_call + cases)"]);

    // Ensure we have a valid preview snapshot before pushing.
    if (!previewResult || !previewResult.ok) {
      appendTerminal("Push all", ["No valid preview snapshot found; generating preview first..."]);
      const ok = await callPreview();
      if (!ok) {
        appendTerminal("Push all", ["Preview generation failed; aborting push all."]);
        return;
      }
    }

    // Preview generation for the currently-selected DB already ran ETL unless skipEtlRefresh was true.
    // Setting this here avoids a second ETL refresh during push (which can trigger upstream 504s).
    if (openmrsDbName.trim()) setEtlRanDbName(openmrsDbName.trim());

    await callPush();
    await callCaseSurveillancePush();

    appendTerminal("Push all", ["Done."]);
  };

  const deleteCurrentLoadedDb = async () => {
    const dbName = openmrsDbName.trim();
    if (!dbName) return;
    const ok = window.confirm(
      `Delete currently loaded DB?\n\nThis will DROP database '${dbName}' and remove the uploaded .sql file if found.`
    );
    if (!ok) return;

    setDeletingLoadedDb(true);
    try {
      const res = await fetch("/api/db/delete-loaded", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          openmrsDbNameOverride: dbName,
          deleteDumpFile: true
        })
      });
      const data = await res.json();
      appendTerminal("DB cleanup", (data?.log ?? []) as string[]);

      if (res.ok && data?.ok) {
        setOpenmrsDbName("");
        setOpenmrsFile(null);
        setFacilityCode("");
        setFacilityName("");
        setDetectedMflCode("");
        setMflNeedsInput(true);
        setMflSource("unknown");
        setDetectLog([]);
        setHealth(null);
        setPreviewResult(null);
        setPushResult(null);
        setCasePushResult(null);
        setUploadResult(null);
        setEtlResult(null);
        setEtlRanDbName(null);
        setStep(0);
        setVersionOverride("");
        try {
          window.localStorage.removeItem(lastOpenmrsDbKey);
        } catch {
          // ignore
        }
        appendTerminalLine("Cleanup complete. You can upload/load another DB.");
      } else {
        appendTerminal("DB cleanup failed", [data?.message ?? "Unknown cleanup error"]);
      }
    } catch (e) {
      appendTerminal("DB cleanup failed", [String(e)]);
    } finally {
      setDeletingLoadedDb(false);
    }
  };

  return (
    <main className="page">
      <header className="page-header">
        <h1 className="page-title">Lightweight CBS Push Console</h1>
        <p className="page-subtitle">
          This UI talks directly to MySQL and CBS (via API routes) without going through Tomcat/OpenMRS.
          It uses your OpenMRS + ETL setup (including current facility MFL and EMR version).
        </p>
      </header>

      <section className="card-grid">
        <div className="card" style={{ display: "none" }}>
          <h2 className="card-title">0. Upload OpenMRS DB Dump</h2>
          <p className="card-text">
            Upload an <code>.sql</code> dump of an OpenMRS database. It will be saved on this server for later import
            and ETL refresh.
          </p>
          <div className="field">
            <label className="field-label" htmlFor="openmrsDump">
              OpenMRS dump file
            </label>
            <input
              id="openmrsDump"
              type="file"
              className="field-input"
              onChange={(e) => {
                const file = e.target.files?.[0] ?? null;
                setOpenmrsFile(file);
              }}
            />
            <p className="field-help">
              Choose an <code>.sql</code> dump exported from MySQL. The app will create a new OpenMRS DB, import it, and
              you can then run ETL + health checks against that DB without Tomcat.
            </p>
          </div>
          <div className="field">
            <label className="field-label" htmlFor="openmrsDbName">
              OpenMRS DB name for health/push
            </label>
            <input
              id="openmrsDbName"
              className="field-input"
              value={openmrsDbName}
              onChange={(e) => setOpenmrsDbName(e.target.value)}
              placeholder="e.g. openmrs_20260317"
            />
            <p className="field-help">
              After you import the dump and run ETL procedures against this DB, the health check and TX_CURR preview /
              push will use it as the OpenMRS source.
            </p>
          </div>
          <button
            type="button"
            className="btn btn-primary"
            style={{ marginTop: "0.25rem" }}
            disabled={!openmrsDbName.trim()}
            onClick={runEtlAndHealth}
          >
            Run ETL + Health Check for This DB
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={!openmrsFile || uploading}
            onClick={async () => {
              if (!openmrsFile) return;
              setUploading(true);
              setUploadProgress(0);
              setEtlRanDbName(null);
              setUploadResult(null);
              clearTerminal();
              appendTerminal("Upload & import", ["Starting upload transfer..."]);
              try {
                const form = new FormData();
                form.append("openmrsDump", openmrsFile);
                const uploadRes = await new Promise<{
                  status: number;
                  data: any;
                }>((resolve, reject) => {
                  const xhr = new XMLHttpRequest();
                  xhr.open("POST", "/api/db/upload", true);
                  xhr.upload.onprogress = (evt) => {
                    if (evt.lengthComputable) {
                      const pct = Math.round((evt.loaded / evt.total) * 100);
                      // Upload transfer reaches 100% quickly, but server-side import can take longer.
                      // Cap at 95% during upload to avoid misleading the user.
                      setUploadProgress(Math.min(95, pct));
                    }
                  };
                  xhr.upload.onloadend = () => {
                    setUploadProgress(95);
                    appendTerminalLine("Upload transfer finished; importing dump on server (please wait)...");
                  };
                  xhr.onload = () => {
                    setUploadProgress(100);
                    const status = xhr.status;
                    const text = xhr.responseText ?? "";
                    let parsed: any = null;
                    try {
                      parsed = text ? JSON.parse(text) : null;
                    } catch {
                      parsed = { raw: text };
                    }
                    resolve({ status, data: parsed });
                  };
                  xhr.onerror = () => reject(new Error("Upload failed (network error)."));
                  xhr.send(form);
                });

                const resOk = uploadRes.status >= 200 && uploadRes.status < 300;
                const data = uploadRes.data ?? {};
                setUploadResult({
                  ok: Boolean(data?.ok) && resOk,
                  message:
                    data.message ||
                    (resOk ? "OpenMRS dump uploaded & imported" : "Failed to upload/import OpenMRS dump"),
                  details: data
                });
                if (resOk && data.dbName) {
                  setOpenmrsDbName(data.dbName);
                  try {
                    window.localStorage.setItem(lastOpenmrsDbKey, data.dbName);
                  } catch {
                    // localStorage may be blocked; ignore.
                  }
                  appendTerminalLine("Server import finished. Detecting facility + MFL...");
                  setEtlRanDbName(null);
                  // Keep the import logs visible; next operations will append more lines.
                  setPreviewResult(null);
                  setPushResult(null);
                  setCasePushResult(null);
                  // Detect facility + MFL for this newly imported DB.
                  await detectFacility(data.dbName);
                  setStep(1);
                }
              } catch (e) {
                setUploadResult({
                  ok: false,
                  message: "Error uploading OpenMRS dump",
                  details: { error: String(e) }
                });
              } finally {
                setUploading(false);
              }
            }}
          >
            {uploading ? "Uploading dump..." : "Upload OpenMRS Dump"}
          </button>
          {uploading && (
            <div style={{ marginTop: "0.6rem" }}>
              <p style={{ fontSize: "0.9rem", color: "var(--text-muted)" }}>
                Uploading dump transfer... then importing on the server.
              </p>
            </div>
          )}
          {uploadResult && (
            <div style={{ marginTop: "0.6rem" }}>
              <pre className="card-output">
                <strong>Upload/import result:</strong> {uploadResult.message}
              </pre>
              <details style={{ marginTop: "0.5rem" }}>
                <summary>Show full response</summary>
                <pre className="card-output" style={{ marginTop: "0.5rem" }}>
                  {JSON.stringify(uploadResult, null, 2)}
                </pre>
              </details>
            </div>
          )}
          {etlResult && (
            <div style={{ marginTop: "0.6rem" }}>
              <pre className="card-output">
                <strong>ETL result:</strong> {etlResult.message}
              </pre>
              <details style={{ marginTop: "0.5rem" }}>
                <summary>Show full response</summary>
                <pre className="card-output" style={{ marginTop: "0.5rem" }}>
                  {JSON.stringify(etlResult, null, 2)}
                </pre>
              </details>
            </div>
          )}
          <div style={{ marginTop: "0.75rem" }}>
            <button onClick={callHealth} disabled={loadingHealth} className="btn btn-primary">
              {loadingHealth ? "Checking..." : "Run Health Check"}
            </button>
            {health && (() => {
              const d = health.details as HealthDetails | undefined;
              const detectedMfl =
                d?.db?.facility?.mflCode ||
                d?.db?.openmrsMflFromGlobalProperty ||
                d?.cbs?.env?.facilityCode ||
                "";
              return (
                <ul className="summary-list" style={{ marginTop: "0.6rem" }}>
                  <li>
                    <strong>EMR version:</strong> {d?.emrVersion || "unknown"}
                  </li>
                  <li>
                    <strong>OpenMRS DB used:</strong> {d?.openmrsDbNameUsed || "openmrs (default)"}
                  </li>
                  <li>
                    <strong>Detected MFL:</strong> {detectedMfl || "unknown"}
                  </li>
                  <li>
                    <strong>CBS:</strong> {(d?.cbs?.status || "unknown").toUpperCase()}{" "}
                    {d?.cbs?.httpStatus != null ? `(HTTP ${d.cbs.httpStatus})` : ""}
                  </li>
                </ul>
              );
            })()}
          </div>
        </div>

        <div className="card">
          <h2 className="card-title">Push Wizard</h2>
          <p className="card-text">
            Follow the steps to upload/import an OpenMRS DB, confirm the Facility MFL, preview the visualization payload,
            and then push visualization + program monitoring (full batch).
          </p>
          <div
            style={{
              display: "flex",
              gap: "0.6rem",
              flexWrap: "wrap",
              marginTop: "0.6rem",
              paddingBottom: "0.1rem"
            }}
          >
            <span
              style={{
                border: `1px solid ${wizardActiveStep === 1 ? "var(--accent-primary)" : "rgba(148,163,184,0.35)"}`,
                color: wizardActiveStep === 1 ? "var(--text-main)" : "var(--text-muted)",
                padding: "0.25rem 0.55rem",
                borderRadius: "999px",
                fontSize: "0.82rem"
              }}
            >
              1) Upload + Health
            </span>
            <span
              style={{
                border: `1px solid ${wizardActiveStep === 2 ? "var(--accent-primary)" : "rgba(148,163,184,0.35)"}`,
                color: wizardActiveStep === 2 ? "var(--text-main)" : "var(--text-muted)",
                padding: "0.25rem 0.55rem",
                borderRadius: "999px",
                fontSize: "0.82rem"
              }}
            >
              2) Confirm MFL
            </span>
            <span
              style={{
                border: `1px solid ${wizardActiveStep === 3 ? "var(--accent-primary)" : "rgba(148,163,184,0.35)"}`,
                color: wizardActiveStep === 3 ? "var(--text-main)" : "var(--text-muted)",
                padding: "0.25rem 0.55rem",
                borderRadius: "999px",
                fontSize: "0.82rem"
              }}
            >
              3) Preview
            </span>
            <span
              style={{
                border: `1px solid ${wizardActiveStep === 4 ? "var(--accent-primary)" : "rgba(148,163,184,0.35)"}`,
                color: wizardActiveStep === 4 ? "var(--text-main)" : "var(--text-muted)",
                padding: "0.25rem 0.55rem",
                borderRadius: "999px",
                fontSize: "0.82rem"
              }}
            >
              4) Push All
            </span>
          </div>
          {step === 0 && (
            <div>
              <div className="field">
                <label className="field-label" htmlFor="openmrsDump">
                  OpenMRS dump file
                </label>
                <input
                  id="openmrsDump"
                  type="file"
                  className="field-input"
                  onChange={(e) => {
                    const file = e.target.files?.[0] ?? null;
                    setOpenmrsFile(file);
                  }}
                />
                <p className="field-help">
                  Upload an <code>.sql</code> dump; the system creates a new DB and runs detection/health for it.
                </p>
              </div>

              <div className="field">
                <label className="field-label" htmlFor="openmrsDbName">
                  OpenMRS DB name for operations
                </label>
                <input
                  id="openmrsDbName"
                  className="field-input"
                  value={openmrsDbName}
                  onChange={(e) => setOpenmrsDbName(e.target.value)}
                  placeholder="e.g. openmrs_20260317"
                />
                <p className="field-help">
                  This DB is used for health checks and visualization payload generation.
                </p>
              </div>

              <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
                <button
                  type="button"
                  className="btn btn-primary"
                  disabled={!openmrsDbName.trim() || uploading || loadingHealth}
                  onClick={runEtlAndHealth}
                >
                  {loadingHealth ? "Running..." : "Run ETL + Health Check"}
                </button>

                <button
                  type="button"
                  className="btn btn-primary"
                  disabled={!openmrsFile || uploading}
                  onClick={async () => {
                    if (!openmrsFile) return;
                    setUploading(true);
                    setEtlRanDbName(null);
                    setUploadResult(null);
                    try {
                      const form = new FormData();
                      form.append("openmrsDump", openmrsFile);
                      const uploadRes = await new Promise<{
                        status: number;
                        data: any;
                      }>((resolve, reject) => {
                        const xhr = new XMLHttpRequest();
                        xhr.open("POST", "/api/db/upload", true);
                        xhr.onload = () => {
                          const status = xhr.status;
                          const text = xhr.responseText ?? "";
                          let parsed: any = null;
                          try {
                            parsed = text ? JSON.parse(text) : null;
                          } catch {
                            parsed = { raw: text };
                          }
                          resolve({ status, data: parsed });
                        };
                        xhr.onerror = () => reject(new Error("Upload failed (network error)."));
                        xhr.send(form);
                      });

                      const resOk = uploadRes.status >= 200 && uploadRes.status < 300;
                      const data = uploadRes.data ?? {};
                      setUploadResult({
                        ok: Boolean(data?.ok) && resOk,
                        message:
                          data.message ||
                          (resOk ? "OpenMRS dump uploaded & imported" : "Failed to upload/import OpenMRS dump"),
                        details: data
                      });

                      if (resOk && data.dbName) {
                        setOpenmrsDbName(data.dbName);
                        setEtlRanDbName(null);
                        clearTerminal();
                        setPreviewResult(null);
                        setPushResult(null);
                        setCasePushResult(null);
                        await detectFacility(data.dbName);
                        setStep(1);
                      }
                    } catch (e) {
                      setUploadResult({
                        ok: false,
                        message: "Error uploading OpenMRS dump",
                        details: { error: String(e) }
                      });
                    } finally {
                      setUploading(false);
                    }
                  }}
                >
                  {uploading ? "Uploading dump..." : "Upload OpenMRS Dump"}
                </button>
                {uploading && (
                  <p style={{ marginTop: "0.6rem", fontSize: "0.9rem", color: "var(--text-muted)" }}>
                    Uploading transfer... then server imports the dump.
                  </p>
                )}

                <button
                  type="button"
                  className="btn btn-secondary"
                  disabled={!openmrsDbName.trim() || loadingHealth || deletingLoadedDb || uploading}
                  onClick={callHealth}
                >
                  {loadingHealth ? "Checking..." : "Run Health Check"}
                </button>
                <button
                  type="button"
                  className="btn btn-secondary"
                  disabled={!openmrsDbName.trim() || deletingLoadedDb || uploading || loadingHealth}
                  onClick={deleteCurrentLoadedDb}
                >
                  {deletingLoadedDb ? "Deleting loaded DB..." : "Delete Current Loaded DB"}
                </button>
              </div>

              {(uploadResult || etlResult || health) && (
                <div style={{ marginTop: "0.75rem" }}>
                  {uploadResult && (
                    <div>
                      <pre className="card-output">
                        <strong>Upload result:</strong> {uploadResult.message}
                      </pre>
                      <details style={{ marginTop: "0.4rem" }}>
                        <summary>Show full response</summary>
                        <pre className="card-output" style={{ marginTop: "0.5rem" }}>
                          {JSON.stringify(uploadResult, null, 2)}
                        </pre>
                      </details>
                    </div>
                  )}
                  {etlResult && (
                    <div style={{ marginTop: "0.6rem" }}>
                      <pre className="card-output">
                        <strong>ETL result:</strong> {etlResult.message}
                      </pre>
                      <details style={{ marginTop: "0.4rem" }}>
                        <summary>Show full response</summary>
                        <pre className="card-output" style={{ marginTop: "0.5rem" }}>
                          {JSON.stringify(etlResult, null, 2)}
                        </pre>
                      </details>
                    </div>
                  )}
                  {health && (
                    <div style={{ marginTop: "0.6rem" }}>
                      <pre className="card-output">
                        <strong>Health:</strong> {health.message}
                      </pre>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          <div className="field" style={{ display: step === 0 ? "none" : "block" }}>
            <label className="field-label" htmlFor="facilityCode">
              Facility MFL code
            </label>
            <p className="field-current">
              Detected facility: <strong>{facilityName || "unknown"}</strong>
              {mflNeedsInput ? (
                <span style={{ marginLeft: "0.5rem" }}>
                  MFL missing/placeholder (e.g. 12345). Please enter it below.
                  <span style={{ marginLeft: "0.5rem", opacity: 0.85 }}>
                    (source: {mflSource})
                  </span>
                </span>
              ) : (
                <span style={{ marginLeft: "0.5rem" }}>
                  Detected MFL: <strong>{detectedMflCode}</strong>{" "}
                  <span style={{ opacity: 0.85 }}>(source: {mflSource})</span>
                </span>
              )}
            </p>
            <input
              id="facilityCode"
              className="field-input"
              value={facilityCode}
              onChange={(e) => setFacilityCode(e.target.value)}
              disabled={step !== 1}
              placeholder={
                mflNeedsInput ? `Enter MFL for ${facilityName || "facility"}` : "e.g. 13917"
              }
            />
            <p className="field-help">
              This MFL will be used in the payload (pre-fills from ETL/OpenMRS config when available). You can also
              save it back into EMR + ETL before pushing.
            </p>
            {step === 1 && (
              <button
                type="button"
                className="btn btn-primary"
                style={{ marginTop: "0.35rem" }}
                disabled={!isFacilityMflValid(facilityCode) || loadingPush}
                onClick={async () => {
                  const code = facilityCode.trim();
                  if (!code) return;
                  setLoadingPush(true);
                  try {
                    appendTerminal("MFL update", ["Updating facility.mflcode in selected OpenMRS DB..."]);
                    const res = await fetch("/api/facility/update-mfl", {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({
                        newMfl: code,
                        openmrsDbNameOverride: openmrsDbName.trim() || undefined
                      })
                    });
                    const data = await res.json();
                    appendTerminal("MFL update", [
                      ...(Array.isArray(data?.log) ? data.log : []),
                      data?.message ? String(data.message) : ""
                    ]);

                    // Clear downstream results because the payload inputs changed.
                    setPreviewResult(null);
                    setPushResult(null);
                    setCasePushResult(null);

                    await callHealth();
                  } catch (e) {
                    appendTerminal("MFL update failed", [String(e)]);
                  } finally {
                    setLoadingPush(false);
                  }
                }}
              >
                {loadingPush ? "Saving MFL..." : "Save MFL to EMR + ETL (optional)"}
              </button>
            )}
            {step === 1 && (
              <div style={{ marginTop: "0.85rem" }}>
                <label className="field-label" htmlFor="versionOverride">
                  EMR Version Override (optional)
                </label>
                <p className="field-help" style={{ marginTop: "0.15rem" }}>
                  Leave empty to use the detected version from the uploaded DB.
                </p>
                <select
                  id="versionOverride"
                  className="field-input"
                  value={versionOverride}
                  onChange={(e) => setVersionOverride(e.target.value)}
                  disabled={loadingHealth}
                  style={{ padding: "0.55rem" }}
                >
                  <option value="">Auto (detected)</option>
                  {buildVersionOptions().map((v) => (
                    <option key={v} value={v}>
                      {v}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>
          {step === 1 && (
            <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
              <button
                type="button"
                onClick={() => setStep(2)}
                disabled={!isFacilityMflValid(facilityCode) || loadingPreview || loadingPush}
                className="btn btn-primary"
              >
                Continue to Preview
              </button>
            </div>
          )}

          {step === 2 && (
            <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
              <button
                type="button"
                onClick={async () => {
                  const ok = await callPreview();
                  if (ok) setStep(3);
                }}
                disabled={loadingPreview || !isFacilityMflValid(facilityCode)}
                className="btn btn-primary"
              >
                {loadingPreview ? "Generating Preview..." : "Preview Visualization Payload"}
              </button>
              <button
                type="button"
                onClick={() => setStep(1)}
                disabled={loadingPreview}
                className="btn btn-secondary"
              >
                Back
              </button>
            </div>
          )}

          {step === 3 && (
            <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginTop: "0.4rem" }}>
              <button
                type="button"
                onClick={async () => {
                  await pushAll();
                }}
                disabled={!isFacilityMflValid(facilityCode) || loadingPreview || loadingPush || loadingCasePush}
                className="btn btn-primary"
              >
                {loadingPush || loadingCasePush ? "Pushing..." : "Push Visualization + Program Monitoring"}
              </button>
              <button
                type="button"
                onClick={() => setStep(2)}
                disabled={loadingPreview || loadingPush || loadingCasePush}
                className="btn btn-secondary"
              >
                Back to Preview
              </button>
            </div>
          )}

          <div style={{ marginTop: "0.9rem" }}>
            <h3 className="card-title" style={{ marginTop: 0 }}>
              Process Console
            </h3>
            <pre className="card-output" style={{ marginTop: "0.5rem" }}>
              {terminalLines.length ? terminalLines.join("\n") : "Console will appear here as you run each step."}
            </pre>
          </div>

          {step >= 2 && previewResult && (() => {
            const details = (previewResult.details ?? {}) as any;
            const payload = details?.payload ?? null;
            return (
              <div className="card-output-group">
                <pre className="card-output" style={{ marginTop: "0.6rem" }}>
                  <strong>Preview Log:</strong>
                  {(details?.log && Array.isArray(details.log) ? details.log.join("\n") : "n/a")}
                </pre>
                {payload && (
                  <pre className="card-output" style={{ marginTop: "0.6rem" }}>
                    <strong>Visualization Payload (to be sent):</strong>
                    {JSON.stringify(payload, null, 2)}
                  </pre>
                )}
              </div>
            );
          })()}

          {step === 3 && pushResult && (
            <div style={{ marginTop: "0.6rem" }}>
              <pre className="card-output">
                <strong>Visualization push result:</strong> {pushResult.message}
              </pre>
              <details style={{ marginTop: "0.5rem" }}>
                <summary>Show full response</summary>
                <pre className="card-output" style={{ marginTop: "0.5rem" }}>
                  {JSON.stringify(pushResult, null, 2)}
                </pre>
              </details>
            </div>
          )}

          {step === 3 && (
            <div style={{ marginTop: "1rem", paddingTop: "0.9rem", borderTop: "1px solid rgba(0,0,0,0.1)" }}>
              <h3 className="card-title" style={{ marginTop: 0 }}>
                Program Monitoring (Case Surveillance)
              </h3>
              <p className="card-text" style={{ marginTop: "0.25rem" }}>
                Pushes the full Program Monitoring batch (roll_call + new_case + linked_case + eligible_for_vl + hei_at_6_to_8_weeks).
              </p>
              {casePushResult && (
                <div style={{ marginTop: "0.6rem" }}>
                  <pre className="card-output">
                    <strong>Case surveillance push result:</strong> {casePushResult.message}
                  </pre>
                  <details style={{ marginTop: "0.5rem" }}>
                    <summary>Show full response</summary>
                    <pre className="card-output" style={{ marginTop: "0.5rem" }}>
                      {JSON.stringify(casePushResult, null, 2)}
                    </pre>
                  </details>
                </div>
              )}
            </div>
          )}
        </div>
      </section>
    </main>
  );
}

