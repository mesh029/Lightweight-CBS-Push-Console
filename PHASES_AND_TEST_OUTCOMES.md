# Lightweight CBS Push – Phases & Test Outcomes

## Phase 0 — Push Contract (endpoints + payloads)
- Goal: Identify exact destination endpoints and payload JSON shapes used by KenyaEMR scheduled tasks.
- Outcome: (to fill after code/payload validation)

## Phase 1 — ETL Scope Audit (prove whether ETL outputs are per uploaded DB)
- Goal: For two different uploaded OpenMRS DBs, confirm what:
  - `openmrs_X.global_property.facility.mflcode` is
  - `kenyaemr_etl.etl_default_facility_info.siteCode` is after running ETL for `openmrs_X`
  - `kenyaemr_etl` tables used for CBS indicators contain rows relevant to `openmrs_X`
- Test 1 (DB A):
  - DB name: `openmrs_20260317142433`
  - openmrs_X.facility.mflcode: `13684`
  - ETL stored procedures exist ONLY in schema `openmrs`:
    - `openmrs.create_etl_tables`, `openmrs.sp_first_time_setup`, `openmrs.create_dwapi_tables`, `openmrs.sp_dwapi_etl_refresh`
    - Not present in `openmrs_2026*` schemas.
  - ETL invocation behavior:
    - `mysql -D openmrs_20260317142433 -e "CALL openmrs.create_etl_tables()"` succeeds (procedures read from the currently-selected DB because they use unqualified table names).
  - After ETL + facility sync (manual, mirroring app logic):
    - `kenyaemr_etl.etl_default_facility_info.siteCode`: `13684`
    - `kenyaemr_etl.etl_current_in_care` count: `152`
  - Result: MFL + ETL facility sync matches `facility.mflcode` for the selected DB.

- Test 2 (DB B):
  - DB name: `openmrs_20260317142722`
  - openmrs_X.facility.mflcode: `13684` (same as DB A in this test dataset)
  - ETL procedure existence: same as Test 1 (procedures exist only in schema `openmrs`)
  - After ETL + facility sync:
    - `kenyaemr_etl.etl_default_facility_info.siteCode`: `13684`
    - `kenyaemr_etl.etl_current_in_care` count: `152`
  - Result: Because both uploaded DBs had identical `facility.mflcode` in this dataset, MFL did not change between A and B (expected given the inputs).

## Phase 2 — ETL Isolation Strategy
- Decision:
  - [ ] If ETL outputs change per selected DB after refresh: use shared ETL + strict pre-run refresh.
  - [ ] If ETL outputs don’t change / are stale: implement per-upload ETL schema (or hard scoping) and adjust queries accordingly.
- Implementation status:
- Test outcome:
  - Current ETL strategy (implemented in app):
    - Connect to the selected uploaded DB as the active schema.
    - Call stored procedures as `openmrs.<procedure>()` (procedures live in schema `openmrs` but use unqualified table names so they read from the currently-selected schema).
    - Then force ETL facility `siteCode` to match `facility.mflcode` from the selected DB.
  - Verified (via direct SQL):
    - `CALL openmrs.create_etl_tables()` succeeds when `database` is set to `openmrs_2026*`.
    - After facility sync, `kenyaemr_etl.etl_default_facility_info.siteCode` matches `facility.mflcode` for the selected DB.

- Implementation test (API-level):
  - `/api/db/run-etl` for `openmrs_20260317142433` now returns `ok=true`.
  - `/api/health` immediately after reports:
    - `openmrsDbNameUsed = openmrs_20260317142433`
    - `db.facility.mflCode = openmrs.facility.mflcode`
    - `kenyaemr_etl.etl_current_in_care` row count present
  - Auto-refresh enforcement for correctness (new/old DBs):
    - `/api/health` can auto-run ETL when `openmrsDbNameOverride` is provided
    - `/api/cbs/preview` and `/api/cbs/push-sample` auto-run ETL by default (can set `skipEtlRefresh` for faster tests)

## Phase 3 — External Push Worker (no Tomcat dependency)
- Goal: Execute ETL refresh + payload generation + push end-to-end without waiting for Tomcat module startup.
- Implementation status:
  - Implemented a first worker endpoint: `POST /api/worker/process-visualization`.
  - It auto-runs `runEtlAndSyncMfl()` for the selected `openmrsDbNameOverride`, then pushes a visualization/superset wrapper payload.
- Test outcome: ETL+facility sync works and visualization payload push succeeds (HTTP 200) using the new worker endpoint.

## Phase 4 — Payload Generator Accuracy
- Goal: Generate the exact JSON payloads that KenyaEMR IL push services send (visualization/superset and case surveillance at minimum).
- Test:
  - Compare external-generated payload vs legacy logged payload for the same facility/time window.
- Outcome:
  - Verified visualization payload wrapper schema/keys:
    - `sha_enrollments`, `Immunization`, `wait_time`, `waivers`, `payments`, `mfl_code`, `diagnosis`, `workload`, `admissions`, `inventory`, `version`, `billing`, `visits`, `bed_management`, `mortality`, `staff_count`, `timestamp`
  - `/api/cbs/preview` generates the wrapper with at least:
    - `staff_count` (from `user_role`)
    - `visits` (from `kenyaemr_etl` ETL tables grouped by visit_type)
  - `/api/cbs/push-sample` successfully POSTs to `visualization.metrics.post.api` and returns:
    - HTTP `200`
    - Body includes `success=true` and `Record/s Created Successfully`
  - Phase 4 enhancement (visualization datasets):
    - Populated `staff_count` using the selected OpenMRS DB’s `user_role` table:
      - `staff_count: [{staff, staff_count}, ...]`
    - Test result (end-to-end):
      - `/api/cbs/preview` returns non-empty `staff_count` and non-empty `visits.details`
      - `/api/cbs/push-sample` returns HTTP `200` and CBS accepts the payload
    - Remaining placeholders:
      - `admissions`, `workload`, `payments`, `inventory`, `billing`, `waivers`, `bed_management`, `mortality`, etc.
      - Next: fill these from OpenMRS/ETL tables using the IL `VisualizationDataExchange` logic (Phase 4 dataset parity).

- Program Monitoring (Case Surveillance) schema progress:
  - Added `/api/cbs/push-case-surveillance` which:
    - auto-runs ETL+facility sync for the selected OpenMRS DB
    - reads `case.surveillance.*` globals from that same DB
    - fetches bearer token from `case.surveillance.token.url`
    - sends HTTP `PUT` to `case.surveillance.base.url.api`
  - Initial connectivity test: sends minimal IL-like `roll_call` EventBase inside a root JSON array.
  - Result: HTTP `202` with `Successfully added client events`.
  - Full `generateCaseSurveillancePayload()` event list expansion remains for the next iteration.

  - Phase 4 expansion (working test):
    - `/api/cbs/push-case-surveillance` now builds an EventBase array from ETL tables:
      - `roll_call`
      - `new_case` (from `kenyaemr_etl.etl_hts_test.final_test_result='Positive'`)
      - `linked_case` (from `kenyaemr_etl.etl_hiv_enrollment.date_started_art_at_transferring_facility`)
    - Added strict gender normalization to satisfy server validation:
      - `client.sex` must be `MALE` or `FEMALE`
    - Test result:
      - HTTP `202` with `Successfully added client events`

## Phase 5 — Push Reliability + Idempotency
- Goal: Retries, backoff, request IDs, and “don’t resend same batch”.
- Outcome:
  - Implemented request correlation IDs and simple retry/backoff (3 attempts) for visualization pushes in:
    - `/api/cbs/push-sample`
    - `/api/worker/process-visualization`
  - Note: full idempotency (“don’t resend same batch”) is not implemented yet.
  - Extended retry/backoff + correlation IDs to program monitoring push:
    - `/api/cbs/push-case-surveillance` now retries the HTTP PUT up to 3 times on non-OK responses.

## Dump Diversity Tests (/home/kenyaemr/DB)
Tested with real dump files from `/home/kenyaemr/DB` by importing them into separate MySQL schemas and selecting each via `openmrsDbNameOverride`:

- DB: `openmrs_esani_20260318`
  - Detected via `global_property`:
    - `facility.mflcode = 13548`
    - `kenyaemr.version = 19.3.0`
  - Visualization push:
    - `POST /api/cbs/push-sample` with `skipEtlRefresh:true` -> HTTP `200`
    - Payload log included `mfl_code=13548`, `version=19.3.0`
  - Program monitoring push:
    - `POST /api/cbs/push-case-surveillance` with `skipEtlRefresh:true` -> HTTP `202`
    - Payload log included `mfl_code=13548`, `version=19.3.0`

- DB: `openmrs_nyangweta_20260318`
  - Detected via `global_property`:
    - `facility.mflcode = 16988`
    - `kenyaemr.version = 19.3.0`
  - Visualization push:
    - `POST /api/cbs/push-sample` with `skipEtlRefresh:true` -> HTTP `200`
    - Payload log included `mfl_code=16988`, `version=19.3.0`
  - Program monitoring push:
    - `POST /api/cbs/push-case-surveillance` with `skipEtlRefresh:true` -> HTTP `202`
    - Payload log included `mfl_code=16988`, `version=19.3.0`

Upgrade mismatch note:
- `/api/health` now supports `skipEtlRefresh` as an alias for `autoRefreshEtl` (so we can test non-ETL workflows safely).
- ETL stored procedures are invoked as `openmrs.<proc>()` from the existing MySQL schema `openmrs`, so even if the uploaded dump DB itself doesn’t define the procedures, the ETL runner can still execute; mismatch failures typically come from missing/changed tables/columns or missing `global_property` keys required by the push contract.

## Upgrade / Version Mismatch Tests (kenyaemr.version != system)
System `openmrs` reports `kenyaemr.version = 19.3.0`.

From `/home/kenyaemr/DB`, these dumps were identified as mismatch candidates:
- `openmrsbosiango04march2026.sql.gz` -> `kenyaemr.version = 19.2.1`, `facility.mflcode = 13514`
- `openmrskenyambi04march2026.sql.gz` -> `kenyaemr.version = 19.2.2`, `facility.mflcode = 16980`
- `openmrsmagombo05march2026.sql.gz` -> `kenyaemr.version = 19.2.1`, `facility.mflcode = (missing)`

### Results
- `openmrs_bosiango_20260318` (19.2.1, facility mfl present)
  - `/api/db/run-etl` -> `ok=true`
  - `POST /api/cbs/push-sample` (ETL enabled) -> HTTP `200` (mfl_code=13514, version=19.2.1)
  - `POST /api/cbs/push-case-surveillance` (ETL enabled) -> HTTP `202`
  - Conclusion: version mismatch alone did NOT break ETL or CBS visualization push when `facility.mflcode` exists.

- `openmrs_kenyambi_20260318` (19.2.2, facility mfl present)
  - `/api/db/run-etl` -> `ok=true`
  - `POST /api/cbs/push-sample` (ETL skipped after successful ETL) -> HTTP `200` (mfl_code=16980, version=19.2.2)
  - `POST /api/cbs/push-case-surveillance` (ETL skipped after successful ETL) -> HTTP `202`
  - Conclusion: version mismatch alone did NOT break pushes when `facility.mflcode` exists.

- `openmrs_magombo_20260318` (19.2.1, facility mfl missing)
  - `/api/db/run-etl` -> `ok=false` with log: `facility.mflcode not found; cannot sync facility`
  - `POST /api/cbs/push-sample` with `skipEtlRefresh=true` -> HTTP `500` because `mfl_code=UNKNOWN` was sent to CBS
  - `POST /api/cbs/push-case-surveillance` with `skipEtlRefresh=true` -> HTTP `202` (server accepted, but payload used `mflCode=UNKNOWN`)
  - Workaround validated: passing `facilityCodeOverride` in push requests fixed visualization push:
    - `POST /api/cbs/push-sample` with `facilityCodeOverride=13514` -> HTTP `200`
    - `POST /api/cbs/push-case-surveillance` with `facilityCodeOverride=13514` -> HTTP `202`
  - Conclusion: in practice, the most dangerous “upgrade mismatch” symptom we observed was missing required `global_property.facility.mflcode` (not the `kenyaemr.version` value itself).
