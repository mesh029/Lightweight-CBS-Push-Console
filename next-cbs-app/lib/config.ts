export type DbConfig = {
  host: string;
  port: number;
  user: string;
  password: string | undefined;
  name: string;
};

export type CbsConfig = {
  endpointUrl: string | null;
  token: string | null;
  facilityCode: string | null;
  facilityMflSource: "env" | "global_property" | "unknown";
};

export function loadDbConfig(): DbConfig {
  return {
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT ? parseInt(process.env.DB_PORT, 10) : 3306,
    user: process.env.DB_USER || "openmrs_user",
    password: process.env.DB_PASS || "GDPZtgqIa@kG",
    // Default to ETL DB which holds indicators; can be overridden via env.
    name: process.env.DB_NAME || "kenyaemr_etl"
  };
}

export function loadCbsConfig(): CbsConfig {
  // Facility MFL: prefer explicit FACILITY_CODE, else OpenMRS global_property facility.mflcode if present
  const facilityMflFromEnv = process.env.FACILITY_CODE ?? null;
  let facilityMflFromGp: string | null = null;

  // We can't safely query MySQL from here (this runs at import time on both server & build),
  // but we can allow the API layer to pass a facility code override when needed.
  // For now, we just tag the source so the UI can show if it's coming from env or unknown.

  return {
    // Default to the visualization server URL detected from this system's global_property
    // (visualization.metrics.post.api) so env configuration is optional.
    endpointUrl:
      process.env.CBS_ENDPOINT_URL ??
      "https://openhimapi.kenyahmis.org/rest/api/IL/superset",
    token: process.env.CBS_API_TOKEN ?? null,
    facilityCode: facilityMflFromEnv ?? facilityMflFromGp,
    facilityMflSource: facilityMflFromEnv
      ? "env"
      : facilityMflFromGp
      ? "global_property"
      : "unknown"
  };
}

