# Lightweight CBS Push Console

Next.js UI + API routes to test and push KenyaEMR data to CBS/Interop **without Tomcat**.

It supports:
- Uploading an OpenMRS dump (`.sql`) into a new DB
- Running ETL procedures and facility MFL sync
- Previewing visualization payload
- Pushing visualization payload
- Pushing full Program Monitoring batch (`roll_call`, `new_case`, `linked_case`, `eligible_for_vl`, `hei_at_6_to_8_weeks`)
- Deleting currently loaded uploaded DB + matching dump file (space cleanup)

## 1) System requirements

- Linux server (Ubuntu recommended)
- Node.js 18+ and npm
- MySQL 5.7+ or 8+
- MySQL CLI client (`mysql`) available in PATH
- Network access from this server to:
  - CBS/Interop visualization endpoint (`visualization.metrics.post.api` in OpenMRS global_property)
  - CS token + CS sync endpoints (`case.surveillance.*` global properties)

## 2) What you need from the DB

You need:
- One OpenMRS database dump (`.sql`) per facility/version you want to test
- A working ETL target database (`kenyaemr_etl`)
- In uploaded OpenMRS DB, these global properties should exist:
  - `visualization.metrics.post.api`
  - `facility.mflcode` (or you provide MFL via UI and save it)
  - `kenyaemr.defaultLocation` / facility-related metadata
  - `case.surveillance.base.url.api`
  - `case.surveillance.token.url`
  - `case.surveillance.client.id`
  - `case.surveillance.client.secret`

Recommended ETL/OpenMRS structures:
- OpenMRS procedures used by ETL runner:
  - `openmrs.create_etl_tables()`
  - `openmrs.sp_first_time_setup()`
  - `openmrs.create_dwapi_tables()`
  - `openmrs.sp_dwapi_etl_refresh()`
- ETL tables used for case/visualization building:
  - `kenyaemr_etl` datasets used by preview/push APIs
  - `etl_default_facility_info` (used for MFL fallback and sync)

## 3) Do you need `.env.local`?

Short answer: **not always**.

If your new system uses the same defaults already in code, `.env.local` is optional.

Current defaults:
- `DB_HOST=localhost`
- `DB_PORT=3306`
- `DB_USER=openmrs_user`
- `DB_PASS=GDPZtgqIa@kG`
- `DB_NAME=kenyaemr_etl`
- `CBS_ENDPOINT_URL=https://openhimapi.kenyahmis.org/rest/api/IL/superset` (fallback only)

Important:
- Visualization + CS routes mainly read endpoint/auth from the uploaded OpenMRS DB global properties.
- Upload/import route currently uses MySQL CLI as:
  - `mysql -uroot -ptest ...`
  So if root/test differs on target server, update that route or create matching credentials.

Create `.env.local` only when host/user/password/DB differ from defaults.

Example:

```bash
DB_HOST=localhost
DB_PORT=3306
DB_USER=openmrs_user
DB_PASS=your_password
DB_NAME=kenyaemr_etl
```

## 4) Install on a new system

From the project root:

```bash
cd next-cbs-app
npm install
```

Run in development:

```bash
npm run dev
```

Then open [http://localhost:3000](http://localhost:3000).

## 5) Basic workflow

1. Upload OpenMRS dump in Step 1 (or reuse cached DB name)
2. Run ETL + Health Check
3. Confirm facility MFL (save to EMR + ETL if needed)
4. Preview visualization payload
5. Push Visualization + Program Monitoring
6. If done with that dump, click **Delete Current Loaded DB** to free disk space

## 6) Storage and cleanup

- Uploaded dumps are stored under `next-cbs-app/uploaded_dbs/`
- Imported DB names are created as `openmrs_<timestamp>`
- The UI button **Delete Current Loaded DB** will:
  - drop that imported DB
  - remove matching uploaded dump file (same timestamp) when found

## 7) Production notes

- Run behind process manager/reverse proxy if needed (`pm2`, `systemd`, `nginx`)
- Restrict access: this tool can import/drop DBs
- Keep backups before bulk tests
- Monitor disk usage in `uploaded_dbs` and MySQL data directory

