# ServiceNow Event Log — Databricks Medallion Pipeline

## Overview

This project implements a **Bronze → Silver → Gold** medallion architecture on Databricks (Unity Catalog) to ingest, clean, and model ServiceNow incident/event log data. The final Gold layer is consumed by Power BI for reporting and analytics.

---

## Architecture

```
Azure Data Lake Storage (ADLS Gen2)
        │
        ▼
  [Bronze Layer]
  Raw CSV file from SAP/ServiceNow extract
  (abfss://datalake/.../bronze/finance/SAP/CSV/)
        │
        ▼  bronze_to_silver.ipynb
  [Silver Layer]  ──  edw_silver.servicenow_event_log  (Delta Table)
  Deduplicated, append-merged staging table
        │
        ▼  silver_to_gold.ipynb
  [Gold Layer]   ──  edw_gold  (Star Schema - Delta Tables)
  ├── dim_category
  ├── dim_user
  └── fact_event_log
        │
        ▼
   Power BI Dashboard
```

---

## Notebooks

| Notebook | Purpose |
|---|---|
| `DDL.ipynb` | Creates all databases and Delta table schemas |
| `bronze_to_silver.ipynb` | Ingests raw CSV from ADLS bronze zone → Silver Delta table |
| `silver_to_gold.ipynb` | Transforms Silver → Gold dimensional model (MERGE/upsert) |

---

## Notebook Details

### 1. `DDL.ipynb` — Schema Setup

Creates the Unity Catalog databases and all Delta table definitions backed by ADLS Gen2 storage.

**Databases created:**
- `servicenowapril26.edw_silver`
- `servicenowapril26.edw_gold`

**Tables created:**

#### `edw_silver.servicenow_event_log`
Staging table holding raw ServiceNow incident records.

| Column | Type | Description |
|---|---|---|
| number | STRING | Incident number (primary key) |
| incident_state | STRING | Current state of the incident |
| active | BOOLEAN | Whether incident is active |
| reassignment_count | BIGINT | Number of times reassigned |
| reopen_count | BIGINT | Number of times reopened |
| sys_mod_count | BIGINT | System modification count |
| made_sla | BOOLEAN | Whether SLA was met |
| caller_id | STRING | ID of the caller |
| opened_by | STRING | User who opened the incident |
| opened_at | STRING | Timestamp of opening |
| sys_created_by | STRING | System creator |
| sys_created_at | STRING | System creation timestamp |
| sys_updated_by | STRING | Last system updater |
| sys_updated_at | STRING | Last system update timestamp |
| contact_type | STRING | Channel of contact |
| location | STRING | Location of the incident |
| category | STRING | Incident category |
| subcategory | STRING | Incident subcategory |
| u_symptom | STRING | User-defined symptom |
| cmdb_ci | STRING | Configuration item |
| impact | STRING | Impact level |
| urgency | STRING | Urgency level |
| priority | STRING | Priority level |
| assignment_group | STRING | Group assigned to |
| assigned_to | STRING | Individual assigned to |
| knowledge | BOOLEAN | Linked to knowledge base |
| u_priority_confirmation | BOOLEAN | Priority confirmed flag |
| notify | STRING | Notification setting |
| problem_id | STRING | Related problem ID |
| rfc | STRING | Related change request |
| vendor | STRING | Vendor involved |
| caused_by | STRING | Root cause reference |
| closed_code | STRING | Closure code |
| resolved_by | STRING | Resolver |
| resolved_at | STRING | Resolution timestamp |
| closed_at | STRING | Closure timestamp |

**ADLS Location:** `abfss://datalake@datalake2026p1.dfs.core.windows.net/silver/servicenow_event_log`

---

#### `edw_gold.dim_category`
Category dimension — unique category/subcategory combinations.

| Column | Type | Description |
|---|---|---|
| CategoryId | BIGINT (IDENTITY PK) | Surrogate key |
| Category | STRING | Incident category |
| Subcategory | STRING | Incident subcategory |

**ADLS Location:** `.../gold/dim_category/`

---

#### `edw_gold.dim_user`
User dimension — unique users who opened incidents.

| Column | Type | Description |
|---|---|---|
| UserId | BIGINT (IDENTITY PK) | Surrogate key |
| OpenedBy | STRING | Username who opened the incident |
| SysCreatedBy | STRING | System creation user |
| SysUpdatedBy | STRING | System update user |

**ADLS Location:** `.../gold/dim_user/`

---

#### `edw_gold.fact_event_log`
Central fact table joining incidents with dimension surrogate keys.

| Column | Type | Description |
|---|---|---|
| Number | STRING | Incident number (natural key) |
| UserId | BIGINT (FK → dim_user) | User foreign key |
| CallerId | STRING | Caller identifier |
| AssignmentGroup | STRING | Assignment group |
| CategoryId | BIGINT (FK → dim_category) | Category foreign key |
| Location | STRING | Incident location |
| CmdbCi | STRING | Configuration item |
| Impact | STRING | Impact level |
| Urgency | STRING | Urgency level |
| Priority | STRING | Priority level |
| IncidentState | STRING | Current state |
| Active | BOOLEAN | Active flag |
| ReassignmentCount | INT | Reassignment count |
| ReopenCount | INT | Reopen count |
| SysModCount | INT | System modification count |
| MadeSla | BOOLEAN | SLA met flag |
| OpenedAt | TIMESTAMP | Opened timestamp |
| SysCreatedAt | TIMESTAMP | System created timestamp |
| SysUpdatedAt | TIMESTAMP | System updated timestamp |
| ContactType | STRING | Contact channel |
| USymptom | STRING | User symptom |
| AssignedTo | STRING | Individual assignee |
| Knowledge | BOOLEAN | Knowledge base flag |
| UPriorityConfirmation | STRING | Priority confirmation |
| Notify | STRING | Notification setting |
| ProblemId | STRING | Related problem |
| Rfc | STRING | Change request reference |
| Vendor | STRING | Vendor reference |
| CausedBy | STRING | Root cause reference |
| ClosedCode | STRING | Closure code |
| ResolvedBy | STRING | Resolver |
| ResolvedAt | TIMESTAMP | Resolution timestamp |
| ClosedAt | TIMESTAMP | Closure timestamp |

**ADLS Location:** `.../gold/fact_event_log/`

---

### 2. `bronze_to_silver.ipynb` — Ingestion

Reads the raw ServiceNow CSV extract from the ADLS bronze zone and upserts it into the Silver Delta table.

**Steps:**
1. Read CSV from ADLS bronze path using PySpark (`inferSchema=True`, `header=True`).
2. Append raw data to `edw_silver.servicenow_event_log`.
3. Create a temp view of the loaded data.
4. Deduplicate using `ROW_NUMBER()` partitioned by `(number, incident_state, sys_updated_at)`.
5. **MERGE** deduplicated source into the Silver table:
   - Match on `number`, `incident_state`, `sys_updated_at`
   - `WHEN MATCHED → UPDATE SET *`
   - `WHEN NOT MATCHED → INSERT *`

**Source path:**
```
abfss://datalake@datalake2026p1.dfs.core.windows.net/bronze/finance/SAP/CSV/ServiceNow_extracts.csv
```

---

### 3. `silver_to_gold.ipynb` — Dimensional Modeling

Populates the Gold star schema from the cleaned Silver table using MERGE (upsert) statements.

**Steps:**

| Step | Target Table | Logic | Rows Inserted |
|---|---|---|---|
| 1 | `dim_category` | MERGE distinct `(category, subcategory)` pairs; insert only new combinations | 62 |
| 2 | `dim_user` | MERGE distinct non-null `opened_by` values; insert only new users | 24 |
| 3 | `fact_event_log` | JOIN Silver with both dims for surrogate keys; deduplicate; MERGE on `(Number, UserId, CategoryId)` | 229 |

---

## Storage Configuration

| Layer | ADLS Path |
|---|---|
| Bronze (raw) | `abfss://datalake@datalake2026p1.dfs.core.windows.net/bronze/finance/SAP/CSV/` |
| Silver | `.../silver/servicenow_event_log` |
| Gold — dim_category | `.../gold/dim_category/` |
| Gold — dim_user | `.../gold/dim_user/` |
| Gold — fact_event_log | `.../gold/fact_event_log/` |

**Storage Account:** `datalake2026p1` | **Container:** `datalake`

---

## Unity Catalog

| Object | Name |
|---|---|
| Catalog | `servicenowapril26` |
| Silver Database | `edw_silver` |
| Gold Database | `edw_gold` |

---

## Execution Order

Run the notebooks in the following sequence:

```
1. DDL.ipynb                  →  Creates schemas and empty Delta tables
2. bronze_to_silver.ipynb     →  Ingests and deduplicates raw CSV into Silver
3. silver_to_gold.ipynb       →  Populates Gold dimensional model from Silver
```

---

## Dependencies

- Databricks Runtime with Unity Catalog enabled
- Azure Data Lake Storage Gen2 (ABFS connector configured)
- PySpark (included in Databricks Runtime)
- Delta Lake (included in Databricks Runtime)
- Power BI (downstream reporting)

---

## Notes

- All tables use **Delta format** to support MERGE (upsert), time travel, and ACID transactions.
- Silver table supports **time travel** — historical versions queryable via `VERSION AS OF <n>`.
- Silver-to-Gold MERGE logic is **idempotent**: re-running will not create duplicate Gold records.
- `opened_at` is stored as `STRING` in Silver and cast to `TIMESTAMP` with format `d/M/yyyy H:mm` during the Gold load.
