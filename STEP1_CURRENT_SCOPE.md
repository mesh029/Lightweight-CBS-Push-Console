# Step 1 - Current System Push Scope (Live OpenMRS)

## 1. CHAI / CBS Configuration (from `openmrs.global_property`)

Using `mysql -uroot -ptest openmrs` we found these relevant properties:

- `chai_vl_server_url = https://wrpkericho.nascop.org/api/vl`
- `chai_vl_server_result_url = https://wrpkericho.nascop.org/api/function`
- `chai_vl_server_api_token = eFJL5pUo2kvq9A31vNOaIL1tzTPv7xkdsA7`
- `chai_eid_server_url = https://wrpkericho.nascop.org/api/vl`
- `chai_eid_server_result_url = https://wrpkericho.nascop.org/api/function`
- `chai_eid_server_api_token = NULL` (no EID token currently set)

Lab system identifiers:

- `kemrorder.labsystem_identifier = CHAI` (default lab system)
- `kemrorder.vl.labsystem_identifier = CHAI` (VL goes to CHAI)
- `kemrorder.eid.labsystem_identifier = LABWARE` (EID mapped to LABWARE)
- `kemrorder.flu.labsystem_identifier = LABWARE`

Other kemrorder settings:

- `kemrorder.last_processed_manifest = NULL`
- `kemrorder.manifest_last_update_time = NULL`
- `kemrorder.ssl_verification_enabled = true`
- `kemrorder.viral_load_result_tat_in_days = 10`
- `kemrorder.retry_period_for_incomplete_vl_result = 2`

This tells us:

- Viral load (VL) traffic is configured to go to **wrpkericho.nascop.org** using a **valid token**.
- EID CHAI token is missing (so EID push may not be active from this instance).
- kemrorder module is aware of CHAI as the VL lab system and LABWARE for EID/flu.

## 2. Scheduled Tasks Related to CBS / Visualization / Lab Push

From `scheduler_task_config` in `openmrs` we found these key tasks (name → class):

- **Program Monitoring (CBS)**
  - `Program Monitoring`
  - Class: `org.openmrs.module.kenyaemrIL.ProgramMonitorPushTask`
  - `started = 1`, `start_on_startup = 1`, `repeat_interval = 180 seconds` (every 3 minutes)

- **Visualization Metrics (CBS / Dashboards)**
  - `Push messages to Visualization server`
  - Class: `org.openmrs.module.kenyaemrIL.VisualizationMetricsPushTask`
  - `started = 1`, `start_on_startup = 1`, `repeat_interval = 180 seconds` (every 3 minutes)

- **Lab Requests / VL Push (CHAI)**
  - `Push Lab Requests to Lab`
  - Class: `org.openmrs.module.kenyaemrorderentry.task.PushLabRequestsTask`
  - `started = 1`, `start_on_startup = 0`, `repeat_interval = 300 seconds` (every 5 minutes)
  - Uses kemrorder + CHAI VL settings to send manifests / VL requests.

- **VL Results Pull (from CHAI)**
  - `Pull Viral Load results`
  - Class: `org.openmrs.module.kenyaemrorderentry.task.PullViralLoadLabResultsTask`
  - `started = 1`, `start_on_startup = 0`, `repeat_interval = 300 seconds`

- **LIMS / Facility-wide Lab integration**
  - `Pull LIMS Facility Wide Lab results`
  - Class: `org.openmrs.module.kenyaemrorderentry.task.PullLimsFacilityWideLabResultsTask`
  - Currently `started = 0`
  
- **OpenHIM / IL Push** (not CBS directly but message transport)
  - `OpenHIM Message Publisher`
    - Class: `org.openmrs.module.kenyaemrIL.KenyaEmrInteropDirectPushTask`
    - `started = 1`, `start_on_startup = 1`, `repeat_interval = 18000 seconds`

- **Other IL / inbox/outbox tasks** (supporting interoperability)
  - `Process Inbox Task` – `org.openmrs.module.kenyaemrIL.ProcessInboxTask` (started)
  - `Process Outbox Task` – `org.openmrs.module.kenyaemrIL.ProcessOutboxTask` (started)

## 3. Practical Current Push Scope (High Level)

Based on the above, your live system currently has **three main CBS / external push scopes**:

1. **Lab / VL Push via CHAI (kemrorder + CHAI global properties)**
   - `Push Lab Requests to Lab` sends VL lab manifests / orders to CHAI using:
     - `chai_vl_server_url`, `chai_vl_server_api_token`, `chai_vl_server_result_url`
   - `Pull Viral Load results` fetches back VL results from CHAI.

2. **Program Monitoring Push (IL / CBS)**
   - `Program Monitoring` task pushes program monitoring metrics through IL (Integration Layer) outward to CBS / analytics.

3. **Visualization Metrics Push (Dashboards / CBS-like)**
   - `Push messages to Visualization server` sends aggregate / metrics to a visualization server, likely consuming data similar to CBS.

OpenHIM + IL tasks wrap some of this traffic (routing, transport), but the **core logical scopes** for us are:

- VL lab requests & results (CHAI)
- Program monitoring metrics
- Visualization metrics

These are what our future lightweight push service should be able to reproduce, without depending on Tomcat.

## 4. What We Will Reuse in the Lightweight Service

From this step we know we can safely reuse:

- **Endpoints + tokens** for VL:
  - URL: `https://wrpkericho.nascop.org/api/vl`
  - Result URL: `https://wrpkericho.nascop.org/api/function`
  - Token: `chai_vl_server_api_token` (already trusted by CHAI/CBS).

- **Module semantics**:
  - kemrorder identifies CHAI vs LABWARE via `kemrorder.*.labsystem_identifier`.
  - Scheduler tasks define run frequencies and names we can mirror in logs.

Next, we will narrow this into the **v1 scope** of the lightweight push API (which feeds to include first, and how the DB-from-folder workflow should see them).

