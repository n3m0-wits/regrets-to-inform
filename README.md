# Regrets to Inform — AI Data Extraction & Insights Pipeline

> **Job Tracking · Data Science Skills Showcase**

A serverless, end-to-end data pipeline that ingests job-application emails and LinkedIn export data, enriches them with LLM-extracted insights, warehouses the results in Azure CosmosDB, and visualises live job-search analytics.

---

## Tech Stack

| Layer | Technologies |
|---|---|
| Language | Python 3 |
| Data Processing | Pandas, NumPy, RapidFuzz |
| Visualisation | Seaborn, Matplotlib, Power BI *(experimental)* |
| Cloud Compute | Azure Functions (serverless) + Logic Apps |
| Cloud Storage | Azure Blob Storage (ADLS Gen2) |
| Database | Azure CosmosDB |
| AI / LLM | OpenAI / Azure OpenAI (Microsoft Foundry) |
| Parsing | BeautifulSoup4, Python `email` stdlib |

---

## Architecture Overview

```
Inbox (raw .eml files)
        │
        ▼
[Logic App trigger]
        │
        ▼
[Azure Blob Storage]  ←──────────────────────────────┐
   /unprocessed/                                      │
        │                                             │
        ▼                                      bulk_upload.py
[Azure Function — ETL]                         (local ingest)
   function_app.py
   ├── HTML email → clean text (BeautifulSoup)
   ├── LLM analysis (Azure OpenAI / Foundry)
   │     └── extract: company, role, outcome, sentiment …
   ├── LinkedIn data merge (RapidFuzz matching)
   └── Upsert enriched document
        │
        ▼
[Azure CosmosDB]
   job_application documents
        │
        ▼
[wrangling_notebook.ipynb]
   Pandas post-processing
   Seaborn / Matplotlib visualisations
   Power BI dashboard (live)
```

---

## Repository Structure

| File | Purpose |
|---|---|
| `function_app.py` | Core Azure Function — ingests blobs, cleans emails, calls LLM, matches LinkedIn data, upserts to CosmosDB |
| `clean_email_raw.py` | Standalone HTML→JSON email sanitiser (local batch processing) |
| `linkedin_parser.py` | Parses copy-pasted LinkedIn "Saved Jobs" text exports into a structured DataFrame |
| `local_load.py` | Processes LinkedIn QuickApply CSV exports, extracts skill/requirement features via keyword taxonomy |
| `local_sync.py` | Runs the full `SyncOrchestrator` locally (mirrors the Azure Function behaviour) |
| `bulk_upload.py` | Uploads locally cleaned JSON files to Azure Blob Storage; includes blob-move utilities |
| `fetch_all.py` | Downloads blobs from Azure Blob Storage to a local folder |
| `wrangling_notebook.ipynb` | Pandas wrangling, Seaborn/Matplotlib visualisations, exploratory analysis |
| `host.json` | Azure Functions host configuration |
| `requirements.txt` | Python dependencies |

---

## Pipeline Stages

### 1 — Email Ingestion & Sanitisation
Raw `.eml` / MIME files are picked up from Azure Blob Storage (or processed locally via `clean_email_raw.py`). The pipeline:
- Decodes MIME multipart messages (plain text preferred, HTML fallback).
- Strips scripts, styles, and hyperlinks with BeautifulSoup.
- Normalises Unicode, invisible whitespace, and URL noise.
- Outputs deterministically named JSON files (`<timestamp>_<sender>.json`).

### 2 — LLM Insight Extraction
Each cleaned email body is submitted to an Azure OpenAI model (deployed via Microsoft Foundry). The model extracts structured fields such as:
- Company applied for & role title
- Application outcome (rejection, interview, ghosted, etc.)
- Sentiment and key qualitative signals

### 3 — LinkedIn Data Merge
Job applications exported from LinkedIn (saved-jobs text dumps and QuickApply CSVs) are parsed and fuzzy-matched against email records using **RapidFuzz** — scoring on company name similarity, job title similarity, and application-date proximity.

### 4 — CosmosDB Warehousing
Enriched, deduplicated job-application documents are upserted into Azure CosmosDB with a consistent schema and partition key, enabling efficient querying and live dashboard refresh.

### 5 — Visualisation & Analysis
`wrangling_notebook.ipynb` pulls data from CosmosDB, applies further Pandas transformations, and produces:
- Application volume over time
- Response-rate and outcome breakdowns
- Skill/requirement frequency heatmaps (from the keyword taxonomy in `local_load.py`)
- Work-type and location distributions

Power BI is connected to the same CosmosDB source for live interactive dashboards.

---

## Local Development

### Prerequisites
- Python 3.11+
- An Azure subscription with Blob Storage, CosmosDB, and Azure OpenAI resources provisioned
- Azure Functions Core Tools (for running `function_app.py` locally)

### Setup

```bash
# Clone and install dependencies
pip install -r requirements.txt

# Copy and populate environment variables
cp local.settings.json.example local.settings.json
# Fill in: deepstatedatabase_STORAGE, ADLS_ACCOUNT_KEY,
#          COSMOS_ENDPOINT, COSMOS_KEY, OPENAI_API_KEY, etc.
```

### Running the email sanitiser locally

```bash
# Place raw .eml files in mime_raw_files/
python clean_email_raw.py
# Outputs cleaned JSON to output_files/
```

### Uploading to Blob Storage

```bash
python bulk_upload.py
```

### Running the full sync locally

```bash
python local_sync.py
```

### Processing LinkedIn exports

```bash
# QuickApply CSV → feature matrix
python local_load.py

# Saved-jobs text dump → DataFrame
python -c "from linkedin_parser import parse_jobs; print(parse_jobs())"
```

### Azure Function (local)

```bash
func start
```

---

## Key Design Decisions

- **Serverless-first**: Azure Functions + Logic Apps eliminate always-on infrastructure costs for a low-frequency personal pipeline.
- **Idempotent upserts**: Documents are keyed by a content hash so re-processing a blob never creates duplicates in CosmosDB.
- **Fuzzy matching**: Company names and job titles are normalised (suffixes stripped, case-folded) before RapidFuzz scoring, making the LinkedIn↔email merge robust to formatting differences.
- **LLM as a structured extractor**: Rather than rule-based parsing of free-text rejection emails, an LLM returns a consistent JSON schema, reducing maintenance burden as email templates vary across employers.
