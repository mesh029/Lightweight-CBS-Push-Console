# Next CBS App (Lightweight CBS Push)

This is a very small Next.js app that connects **directly to MySQL** and a **CBS HTTP endpoint**, without going through Tomcat/OpenMRS. It lets you:

- Test DB connectivity against `kenyaemr_etl` (or any DB you point it to)
- Push a **sample indicator** (TX_CURR-style count) to a CBS endpoint with detailed JSON output

## Configuration

Create a `.env.local` file in `next-cbs-app`:

```bash
DB_HOST=localhost
DB_PORT=3306
DB_USER=openmrs_user
DB_PASS=GDPZtgqIa@kG
DB_NAME=kenyaemr_etl

CBS_ENDPOINT_URL=http://your-cbs-endpoint.example.com/api/indicators
CBS_API_TOKEN=your_cbs_token_here

FACILITY_CODE=12345
```

Adjust `DB_NAME` if you want to point at `kenyaemr_datatools` instead of `kenyaemr_etl`.

## Running the app

From `lightweight_cbs_push/next-cbs-app`:

```bash
npm install
npm run dev
```

Then open `http://localhost:3000` in your browser.

## What the API routes do

- `GET /api/health`
  - Connects to the DB using the `DB_*` env vars
  - Checks for `etl_current_in_care` and, if present, returns a row count

- `POST /api/cbs/push-sample`
  - Counts rows in `etl_current_in_care`
  - Builds a simple indicator payload:
    - `indicatorCode: "TX_CURR_SAMPLE"`
    - `period: YYYY-MM-DD` (today)
    - `facilityCode: FACILITY_CODE`
    - `value: count from etl_current_in_care`
  - Sends it to `CBS_ENDPOINT_URL` with optional `CBS_API_TOKEN` as a Bearer token
  - Returns CBS HTTP status and body for verification

This gives you a **starting point** to expand into full Program Monitoring + Visualization CBS payloads, still independent of Tomcat.

