export type IndicatorPayload = {
  indicatorCode: string;
  period: string;
  facilityCode: string;
  value: number;
  meta?: Record<string, unknown>;
};

export async function pushToCbs(payloads: IndicatorPayload[]) {
  // Endpoint + token should be resolved from config defaults so the app
  // works without manual env setup.
  const { endpointUrl, token } = (() => {
    try {
      // Lazy import to avoid circular dependencies
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const cfg = require("@lib/config") as { loadCbsConfig: () => { endpointUrl: string | null; token: string | null } };
      return cfg.loadCbsConfig();
    } catch {
      return { endpointUrl: null, token: null };
    }
  })();

  const url = endpointUrl;
  if (!url) throw new Error("CBS endpointUrl is not configured (CBS_ENDPOINT_URL missing and default not available)");

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {})
    },
    body: JSON.stringify({ indicators: payloads })
  });

  const text = await res.text();

  return {
    status: res.status,
    ok: res.ok,
    body: text
  };
}

