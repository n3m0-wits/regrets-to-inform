import azure.functions as func
import logging
import json
import email
from email import policy
from bs4 import BeautifulSoup
import re
import os
import codecs
import unicodedata
import time
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import openai
from openai import AzureOpenAI, OpenAI
import hashlib
from io import BytesIO
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta, timezone
import pandas as pd
from dateutil import parser as date_parser
from rapidfuzz import fuzz
from azure.cosmos import CosmosClient, PartitionKey, exceptions

app = func.FunctionApp()
# Shut up CosmosDB SDK
logging.getLogger('azure.cosmos').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

SOUTH_AFRICA_TZ = timezone(timedelta(hours=2))


def normalize_to_utc(dt: datetime, assume_tz=None) -> datetime:
    """Convert a datetime to UTC. If naive, attach assume_tz (defaults to UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=assume_tz or timezone.utc)
    return dt.astimezone(timezone.utc)

def _clean_str(v):
    """Empty / 'None' / 'null' / 'n/a' (any case) -> None. Real strings preserved."""
    if v is None:
        return None
    if isinstance(v, str) and v.strip().lower() in ("", "none", "null", "n/a"):
        return None
    return v


def _clean_bool(v):
    """Accept actual bools or stringified 'true'/'false'. Anything unparseable -> None."""
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s == "true":
            return True
        if s == "false":
            return False
    return None


def _clean_int(v):
    """Accept ints or stringified ints. Reject bools (which are int-subclass)."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s.lower() in ("", "none", "null", "n/a"):
            return None
        try:
            return int(s)
        except ValueError:
            return None
    return None


@dataclass
class EmailMessage:
    filename: str
    to: str
    from_address: str
    subject: str
    date: datetime
    body: str
    analysis: Dict[str, Any]

    @property
    def company(self) -> str:
        return _clean_str(self.analysis.get("company_applied_for")) or ""

    @property
    def role(self) -> str:
        return _clean_str(self.analysis.get("role")) or ""


@dataclass
class LinkedInJob:
    application_date: datetime
    company_name: str
    job_title: str
    job_url: str
    raw_data: Dict[str, Any]


class TextNormalizer:
    SUFFIXES = [
        " careers africa", " careers", " south africa", " sa",
        " pty ltd", " (pty) ltd", " ltd", " limited",
        " inc.", " inc", " corporation", " corp.", " corp",
        " group", " holdings", " holding",
    ]

    @classmethod
    def company(cls, name: str) -> str:
        if not name:
            return ""
        n = name.lower().strip()
        for suffix in cls.SUFFIXES:
            if n.endswith(suffix):
                n = n[: -len(suffix)].strip()
        return n

    @classmethod
    def title(cls, name: str) -> str:
        if not name:
            return ""
        return name.lower().strip().replace("-", " ").replace("/", " ").replace("_", " ")


class JobMatcher:
    def __init__(self, tolerance_days: int = 5, threshold: float = 0.72):
        self.tolerance = timedelta(days=tolerance_days)
        self.threshold = threshold

    def calculate_score(self, em: EmailMessage, job: LinkedInJob) -> Tuple[float, Dict]:
        nc = TextNormalizer.company
        nt = TextNormalizer.title

        comp_score = fuzz.ratio(nc(em.company), nc(job.company_name)) / 100.0
        title_score = fuzz.ratio(nt(em.role), nt(job.job_title)) / 100.0

        date_diff = abs((em.date - job.application_date).days)
        date_score = max(0.0, 1.0 - (date_diff / self.tolerance.days)) if self.tolerance.days > 0 else 0.0

        score = (comp_score * 0.45) + (title_score * 0.45) + (date_score * 0.10)
        details = {
            "company_score": round(comp_score, 2),
            "title_score": round(title_score, 2),
            "date_score": round(date_score, 2),
            "days_diff": date_diff,
        }
        return score, details

    def find_best_match(self, em: EmailMessage, jobs: List[LinkedInJob]) -> Optional[Tuple[LinkedInJob, float, Dict]]:
        best_job, best_score, best_details = None, 0.0, {}
        for job in jobs:
            if abs((em.date - job.application_date).days) > self.tolerance.days * 2:
                continue
            score, details = self.calculate_score(em, job)
            if score > best_score:
                best_job, best_score, best_details = job, score, details
        if best_job and best_score >= self.threshold:
            return best_job, best_score, best_details
        return None


class CosmosManager:
    def __init__(self, endpoint: str, key: str, database: str, container: str):
        self.client = CosmosClient(endpoint, key)
        self.db = self.client.get_database_client(database)
        self.container = self.db.get_container_client(container)

    def init_container(self):
        try:
            self.db.create_container_if_not_exists(
                id=self.container.id,
                partition_key=PartitionKey(path="/partitionKey"),
                offer_throughput=400,
            )
        except exceptions.CosmosResourceExistsError:
            pass

    def fetch_all_jobs(self) -> List[Dict]:
        query = "SELECT * FROM c WHERE c.type = 'job_application'"
        return list(self.container.query_items(query=query, enable_cross_partition_query=True))

    def upsert(self, doc: Dict):
        self.container.upsert_item(doc)


def decode_unicode_escapes(text):
    if not text:
        return ""
    try:
        if '\\u' in text:
            text = codecs.decode(text, 'unicode_escape')
    except Exception:
        pass
    return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')


def nuke_invisible_whitespace(text):
    if not text:
        return ""
    text = re.sub(r'[\u200b-\u200f\u2028\u202a-\u202e\u2060-\u206f\ufeff]', '', text)
    text = text.replace('\xa0', ' ')
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def remove_long_urls(input_str):
    if not input_str:
        return ""
    url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
    return re.sub(url_pattern, "", str(input_str))


def clean_html_to_text(html_content):
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, 'html.parser')
    for element in soup(["script", "style", "a"]):
        element.decompose()
    text = soup.get_text(separator='\n', strip=True)
    text = decode_unicode_escapes(text)
    text = nuke_invisible_whitespace(text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


@app.timer_trigger(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def clean_emails_daily(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function started parsing emails.')

    conn_str = os.environ.get("deepstatedatabase_STORAGE")
    if not conn_str:
        logging.error("Missing 'deepstatedatabase_STORAGE' connection string in environment.")
        return

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)

        input_container_client = blob_service_client.get_container_client("rawdumps")
        output_container_client = blob_service_client.get_container_client("cleanmail")

        blobs = input_container_client.list_blobs(name_starts_with="unprocessed/")

        processed_count = 0
        for blob in blobs:
            if blob.name.endswith('/'):
                continue

            logging.info(f"Processing blob: {blob.name}")
            blob_client = input_container_client.get_blob_client(blob)

            try:
                contents = blob_client.download_blob().readall().decode('utf-8', errors='ignore')
                json_payload = json.loads(contents)

                subject = json_payload.get('Subject', '')
                if subject:
                    subject = decode_unicode_escapes(str(subject))
                    subject = re.sub(r'[—\-]{3,}', '', subject)
                    subject = nuke_invisible_whitespace(subject)

                body_text = json_payload.get('Body', '')
                if body_text:
                    body_text = remove_long_urls(clean_html_to_text(body_text))
                    body_text = re.sub(r'[—\-]{3,}', ' ', body_text)

                email_data = {
                    "filename": blob.name.split('/')[-1],
                    "to": json_payload.get('To', ''),
                    "from": json_payload.get('From', ''),
                    "subject": subject,
                    "cc": json_payload.get('Cc', ''),
                    "date": json_payload.get('DateTimeReceived', ''),
                    "body": body_text
                }

                original_filename = blob.name.split('/')[-1]

                sender_raw = str(json_payload.get('From', 'unknown')).strip()
                sender_safe = re.sub(r'[^a-zA-Z0-9]', '_', sender_raw)[:40]

                date_str = str(json_payload.get('DateTimeReceived', ''))
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(date_str)
                    time_prefix = str(int(dt.timestamp() * 1000))
                except Exception:
                    time_prefix = str(int(time.time() * 1000))

                new_base_name = f"{time_prefix}_{sender_safe}" if sender_safe else time_prefix
                output_blob_name = f"unprocessed/{new_base_name}.json"

                output_blob_client = output_container_client.get_blob_client(output_blob_name)
                output_blob_client.upload_blob(json.dumps(email_data, indent=4), overwrite=True)

                processed_blob_client = input_container_client.get_blob_client(f"processed/{original_filename}")
                processed_blob_client.start_copy_from_url(blob_client.url)

                blob_client.delete_blob()

                processed_count += 1
            except Exception as inner_e:
                logging.error(f"Error parsing blob {blob.name}: {inner_e}")
                continue

        logging.info(f"Python timer trigger successfully finished. Processed {processed_count} emails.")

    except Exception as e:
        logging.error(f"Fatual error connecting to blob storage or iterating: {e}")

LLM_BATCH_CAP = 120

ANALYSIS_SYSTEM_PROMPT = """You are a strict data-extraction assistant for job-related emails.

Read the subject and body, then return ONE valid JSON object — no markdown, no fences, no commentary.

OUTPUT RULES (non-negotiable):
- Use JSON null for unknown values. NEVER output the string "None"; use real null.
- Booleans: true / false / null only. No strings.
- Integers: numbers, not strings.
- Date fields ending in _iso: ISO 8601 with timezone if known
  (e.g. "2026-05-03T14:00:00+02:00"). Use null if no specific date is given.

SCHEMA:
{
  "company_applied_for":      string | null,
  "portal":                   string | null,            // "SmartRecruiters", "LinkedIn", "Workday", "Greenhouse", "Indeed", ...
  "type":                     "Job ad" | "Application update" | "Rejection" | "Offer" | "Unrelated" | "Unknown",
  "role":                     string | null,            // job title, max 35 chars
  "category":                 "Job Ad" | "Recruiter Reach-out" | "Bot/Auto-Reply" | "Confirmation of Application"
                            | "Rejection" | "Request for Information" | "Technical Test Request"
                            | "Personality Test Request" | "Interview Invitation" | "Job Offer"
                            | "Not Job Related" | "Unknown",
  "is_automated":             boolean | null,
  "urgency":                  "High" | "Medium" | "Low",   // High if any deadline <=48h, Medium <=72h, else Low
  "interview_type":           "Online-Live" | "In-Person" | "One-Way-Robot" | "Phone-Call" | "Unclear" | null,
  "interview_platform":       string | null,           // "Teams", "Zoom", "Spark Hire", ...
  "interview_date_iso":       string | null,           // ISO 8601 if a specific time is given
  "test_platform":            string | null,           // "Codility", "HackerRank", ...
  "test_duration_mins":       integer | null,
  "application_deadline_iso": string | null,           // ISO 8601 if explicitly stated
  "action_required":          boolean | null,
  "action_type":              "Respond" | "Click Link" | "Schedule Interview" | "Submit Document"
                            | "Take Test" | "Do Nothing" | null,
  "action_deadline_iso":      string | null,           // ISO 8601 by which action_type must be done
  "work_mode":                "Remote" | "Hybrid" | "On-site" | null,
  "location":                 string | null,           // e.g. "Cape Town, ZA"
  "recruiter_name":           string | null,
  "recruiter_email":          string | null,           // only if explicitly stated in the body
  "summary":                  string | null            // <=25 words: what was said + what to do next
}

Return only the JSON object."""


@app.timer_trigger(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)
def analyze_emails_daily(myTimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started analyzing emails via Foundry LLM.')

    conn_str = os.environ["deepstatedatabase_STORAGE"]
    azure_oai_key = os.environ["AZURE_OPENAI_API_KEY"]
    endpoint = os.environ["COSMOS_ENDPOINT"]
    deployment_name = "o4-mini-1"

    client = OpenAI(
        base_url=endpoint,
        api_key=azure_oai_key,
        default_headers={"api-key": azure_oai_key}
    )

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client("cleanmail")

        blobs = container_client.list_blobs(name_starts_with="unprocessed/")

        attempted_count = 0
        for blob in blobs:
            if attempted_count >= LLM_BATCH_CAP:
                logging.info(f"Reached maximum batch size of {LLM_BATCH_CAP}. Stopping for this run.")
                break

            if blob.name.endswith('/'):
                continue

            attempted_count += 1
            logging.info(f"Analyzing blob: {blob.name}")
            blob_client = container_client.get_blob_client(blob)

            try:
                contents = blob_client.download_blob().readall().decode('utf-8', errors='ignore')
                email_data = json.loads(contents)

                subject = email_data.get('subject', '')
                body = email_data.get('body', '')

                user_prompt = f"Email Subject: {subject}\n\nEmail Body: {body}"

                # Exponential backoff for rate limits
                max_retries = 5
                response = None
                for attempt in range(max_retries):
                    try:
                        response = client.chat.completions.create(
                            model=deployment_name,
                            messages=[
                                {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
                                {"role": "user", "content": user_prompt}
                            ],
                            response_format={"type": "json_object"}
                        )
                        break
                    except openai.RateLimitError:
                        if attempt == max_retries - 1:
                            raise
                        sleep_time = (2 ** attempt) + 1
                        logging.warning(f"Azure OpenAI Rate Limit Hit! Sleeping for {sleep_time}s...")
                        time.sleep(sleep_time)
                    except openai.BadRequestError as e:
                        # Azure content filter (400). The blob will never pass — quarantine it
                        # immediately rather than leaving it in unprocessed/ to retry forever.
                        err_code = getattr(e, 'code', '') or ''
                        if 'content_filter' in str(err_code).lower() or 'content_filter' in str(e).lower():
                            logging.warning(f"Content filter blocked {blob.name} — moving to quarantine/")
                            original_filename = blob.name.split('/')[-1]
                            email_data['analysis'] = {
                                "error": "content_filter",
                                "category": "Not Job Related",
                                "is_automated": True,
                                "summary": "Blocked by Azure content filter — not analysed."
                            }
                            q_client = container_client.get_blob_client(f"quarantine/{original_filename}")
                            q_client.upload_blob(json.dumps(email_data, indent=4), overwrite=True)
                            try:
                                blob_client.delete_blob()
                            except ResourceNotFoundError:
                                pass  # already removed by a concurrent run — fine
                            response = None  # signal to skip normal processing below
                            break
                        raise  # non-filter 400 — surface it

                if response is None:
                    # Blob was quarantined above — skip to next blob
                    continue

                # Baseline throttle
                time.sleep(0.5)

                analysis_result = json.loads(response.choices[0].message.content)
                email_data['analysis'] = analysis_result

                original_filename = blob.name.split('/')[-1]
                output_blob_name = f"processed/{original_filename}"

                output_blob_client = container_client.get_blob_client(output_blob_name)
                output_blob_client.upload_blob(json.dumps(email_data, indent=4), overwrite=True)

                # Bug fix: concurrent timer runs can process the same blob simultaneously.
                # The upload above uses overwrite=True so the last writer wins — data is safe.
                # If the blob is already gone (deleted by another instance), treat as success.
                try:
                    blob_client.delete_blob()
                except ResourceNotFoundError:
                    logging.info(f"Blob {blob.name} already deleted by concurrent run — skipping delete.")

            except Exception as inner_e:
                logging.error(f"Error analyzing blob {blob.name}: {inner_e}")
                continue

        logging.info(f"Analysis trigger successfully finished. Attempted {attempted_count} emails.")

    except Exception as e:
        logging.error(f"Fatal error during analysis job: {e}")



def _build_flat_doc(
    job_slug: str,
    company_name: str,
    job_title: str,
    app_date: datetime,
    record_type: str,
    email_msg: Optional[EmailMessage] = None,
    linkedin: Optional[LinkedInJob] = None,
    master_doc: Optional[Dict] = None,
) -> Dict:
    """Build a fully flat CosmosDB document. Pulled out of the trigger for testability."""

    def _make_email_id(slug: str, filename: str) -> str:
        return f"{slug}_{hashlib.md5(filename.encode()).hexdigest()[:8]}"

    def _extract_linkedin_fields(raw_data: Dict) -> Dict:
        if not raw_data:
            return {}
        flat = {}
        for k, v in raw_data.items():
            if k in ("Application_Date", "Company_Name", "Job_Title", "Job_Url"):
                continue
            key = f"linkedin_{re.sub(r'[^a-zA-Z0-9_]', '_', k).lower()}"
            flat[key] = v
        return flat

    doc: Dict[str, Any] = {
        "id": "",
        "type": "job_application",
        "job_id": job_slug,
        "partitionKey": TextNormalizer.company(company_name)[:20] or "unknown",
        "record_type": record_type,
        "company_name": company_name,
        "company_normalized": TextNormalizer.company(company_name),
        "job_title": job_title,
        "job_title_normalized": TextNormalizer.title(job_title),
        "application_date": app_date.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if email_msg:
        doc["id"] = _make_email_id(job_slug, email_msg.filename)
        doc["email_filename"] = email_msg.filename
        doc["email_date"] = email_msg.date.isoformat()
        doc["email_to"] = email_msg.to or None
        doc["email_from"] = email_msg.from_address or None
        doc["email_subject"] = email_msg.subject or None
        doc["email_body_preview"] = email_msg.body[:600] if email_msg.body else None
    else:
        doc["id"] = f"{job_slug}_linkedin"
        doc["email_filename"] = None
        doc["email_date"] = None
        doc["email_to"] = None
        doc["email_from"] = None
        doc["email_subject"] = None
        doc["email_body_preview"] = None

    a = email_msg.analysis if email_msg else {}
    doc["analysis_company_applied_for"]      = _clean_str(a.get("company_applied_for"))
    doc["analysis_portal"]                   = _clean_str(a.get("portal"))
    doc["analysis_type"]                     = _clean_str(a.get("type"))
    doc["analysis_role"]                     = _clean_str(a.get("role"))
    doc["analysis_category"]                 = _clean_str(a.get("category"))
    doc["analysis_is_automated"]             = _clean_bool(a.get("is_automated"))
    doc["analysis_urgency"]                  = _clean_str(a.get("urgency"))
    doc["analysis_interview_type"]           = _clean_str(a.get("interview_type"))
    doc["analysis_interview_platform"]       = _clean_str(a.get("interview_platform"))
    doc["analysis_interview_date_iso"]       = _clean_str(a.get("interview_date_iso"))
    doc["analysis_test_platform"]            = _clean_str(a.get("test_platform"))
    doc["analysis_test_duration_mins"]       = _clean_int(a.get("test_duration_mins"))
    doc["analysis_application_deadline_iso"] = _clean_str(a.get("application_deadline_iso"))
    doc["analysis_action_required"]          = _clean_bool(a.get("action_required"))
    doc["analysis_action_type"]              = _clean_str(a.get("action_type"))
    doc["analysis_action_deadline_iso"]      = _clean_str(a.get("action_deadline_iso"))
    doc["analysis_work_mode"]                = _clean_str(a.get("work_mode"))
    doc["analysis_location"]                 = _clean_str(a.get("location"))
    doc["analysis_recruiter_name"]           = _clean_str(a.get("recruiter_name"))
    doc["analysis_recruiter_email"]          = _clean_str(a.get("recruiter_email"))
    doc["analysis_summary"]                  = _clean_str(a.get("summary"))
    if linkedin:
        doc["linkedin_job_url"] = linkedin.job_url or None
        doc["linkedin_application_date"] = linkedin.application_date.isoformat()
        doc["linkedin_company_name"] = linkedin.company_name or None
        doc["linkedin_job_title"] = linkedin.job_title or None
        doc.update(_extract_linkedin_fields(linkedin.raw_data))
    elif master_doc:
        doc["linkedin_job_url"] = master_doc.get("linkedin_job_url")
        doc["linkedin_application_date"] = master_doc.get("linkedin_application_date")
        doc["linkedin_company_name"] = master_doc.get("linkedin_company_name")
        doc["linkedin_job_title"] = master_doc.get("linkedin_job_title")
        for k, v in master_doc.items():
            if k.startswith("linkedin_") and k not in doc:
                doc[k] = v
    else:
        doc["linkedin_job_url"] = None
        doc["linkedin_application_date"] = None
        doc["linkedin_company_name"] = None
        doc["linkedin_job_title"] = None

    return doc


def _make_slug(company: str, title: str, app_date: datetime) -> str:
    date_part = app_date.strftime('%Y%m%d') if app_date else "unknown"
    raw = f"{TextNormalizer.company(company)}|{TextNormalizer.title(title)}|{date_part}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _move_blob(container_client, src_name: str, dst_name: str) -> None:
    """Server-side copy then delete source. Mirrors the pattern in clean_emails_daily."""
    src = container_client.get_blob_client(src_name)
    dst = container_client.get_blob_client(dst_name)
    dst.start_copy_from_url(src.url)
    src.delete_blob()


@app.timer_trigger(schedule="0 0 10 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def sync_to_cosmos_daily(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Cosmos sync timer is past due!')

    logging.info('Starting CosmosDB sync job.')

    conn_str = os.environ.get("deepstatedatabase_STORAGE")
    if not conn_str:
        logging.error("Missing 'deepstatedatabase_STORAGE' connection string.")
        return

    cosmos_endpoint = os.environ.get("COSMOS_ENDPOINT")
    cosmos_key = os.environ.get("COSMOS_KEY")
    if not cosmos_endpoint or not cosmos_key:
        logging.error("Missing CosmosDB credentials.")
        return

    max_docs_per_run = int(os.environ.get("SYNC_MAX_DOCS", "7000"))
    docs_written = 0

    try:
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        cosmos = CosmosManager(
            cosmos_endpoint,
            cosmos_key,
            os.environ.get("COSMOS_DATABASE", "jobtracker"),
            os.environ.get("COSMOS_CONTAINER", "applications")
        )
        cosmos.init_container()
        matcher = JobMatcher(tolerance_days=5, threshold=0.72)
        cleanmail_client = blob_service_client.get_container_client("cleanmail")
        email_blobs = cleanmail_client.list_blobs(name_starts_with="processed/")

        email_records: List[Tuple[EmailMessage, str]] = []
        for blob in email_blobs:
            if not blob.name.endswith(".json"):
                continue
            try:
                bc = cleanmail_client.get_blob_client(blob)
                payload = json.loads(bc.download_blob().readall().decode('utf-8', errors='ignore'))
                date_str = payload.get("date", "")
                parsed_date = (
                    normalize_to_utc(date_parser.parse(date_str))
                    if date_str else datetime.now(timezone.utc)
                )
                em = EmailMessage(
                    filename=payload.get("filename", blob.name.split("/")[-1]),
                    to=payload.get("to", ""),
                    from_address=payload.get("from", ""),
                    subject=payload.get("subject", ""),
                    date=parsed_date,
                    body=payload.get("body", ""),
                    analysis=payload.get("analysis", {}) or {},
                )
                email_records.append((em, blob.name))
            except Exception as e:
                logging.warning(f"Skipping email blob {blob.name}: {e}")

        emails = [r[0] for r in email_records]
        filename_to_blob: Dict[str, str] = {em.filename: src for em, src in email_records}
        logging.info(f"Loaded {len(emails)} processed emails.")
        
        # Load LinkedIn CSV
        linkedin_jobs: List[LinkedInJob] = []
        linkedin_path = os.environ.get("LINKEDIN_BLOB_PATH", "rawdumps/linkedin/Linkedin_QuickApply.csv")

        try:
            parts = linkedin_path.split("/", 1)
            li_container = parts[0]
            li_blob = parts[1] if len(parts) > 1 else linkedin_path

            li_blob_client = blob_service_client.get_blob_client(container=li_container, blob=li_blob)
            csv_bytes = li_blob_client.download_blob().readall()
            df = pd.read_csv(BytesIO(csv_bytes), index_col=0)
            df = df.where(pd.notnull(df), None)

            for _, row in df.iterrows():
                try:
                    d = normalize_to_utc(
                        date_parser.parse(str(row["Application_Date"])),
                        assume_tz=SOUTH_AFRICA_TZ
                    )
                except Exception:
                    continue
                linkedin_jobs.append(LinkedInJob(
                    application_date=d,
                    company_name=str(row.get("Company_Name", "")),
                    job_title=str(row.get("Job_Title", "")),
                    job_url=str(row.get("Job_Url", "")),
                    raw_data=row.to_dict(),
                ))
            logging.info(f"Loaded {len(linkedin_jobs)} LinkedIn rows.")
        except Exception as e:
            logging.error(f"Could not load LinkedIn CSV from {linkedin_path}: {e}")
            linkedin_jobs = []

        # Load existing CosmosDB docs to build job masters
        existing_docs = cosmos.fetch_all_jobs()
        job_masters: Dict[str, Dict] = {}
        for doc in existing_docs:
            sid = doc.get("job_id")
            if sid and sid not in job_masters:
                job_masters[sid] = doc
        logging.info(f"Loaded {len(existing_docs)} existing CosmosDB docs ({len(job_masters)} unique jobs).")

        docs_to_upsert: Dict[str, Dict] = {}
        matched_linkedin_ids: set = set()

        #Match & merge emails into flat rows
        for email_msg in emails:
            try:
                best_master_slug = None
                best_master_score = 0.0
                best_master_doc = None
                for slug, master in job_masters.items():
                    try:
                        master_date = normalize_to_utc(date_parser.parse(master["application_date"]))
                    except Exception:
                        continue
                    temp_job = LinkedInJob(
                        application_date=master_date,
                        company_name=master.get("company_name", "") or "",
                        job_title=master.get("job_title", "") or "",
                        job_url=master.get("linkedin_job_url", "") or "",
                        raw_data={},
                    )
                    score, _ = matcher.calculate_score(email_msg, temp_job)
                    if score >= matcher.threshold and score > best_master_score:
                        best_master_slug = slug
                        best_master_score = score
                        best_master_doc = master

                if best_master_slug:
                    master_date = normalize_to_utc(date_parser.parse(best_master_doc["application_date"]))
                    doc = _build_flat_doc(
                        job_slug=best_master_slug,
                        company_name=best_master_doc["company_name"],
                        job_title=best_master_doc["job_title"],
                        app_date=master_date,
                        record_type="email",
                        email_msg=email_msg,
                        master_doc=best_master_doc,
                    )
                    docs_to_upsert[doc["id"]] = doc
                    continue

                result = matcher.find_best_match(email_msg, linkedin_jobs)
                if result:
                    li_job, score, details = result
                    li_id = hashlib.md5(
                        f"{li_job.company_name}|{li_job.job_title}|{li_job.application_date.isoformat()}".encode()
                    ).hexdigest()
                    matched_linkedin_ids.add(li_id)

                    slug = _make_slug(li_job.company_name, li_job.job_title, li_job.application_date)
                    doc = _build_flat_doc(
                        job_slug=slug,
                        company_name=li_job.company_name,
                        job_title=li_job.job_title,
                        app_date=li_job.application_date,
                        record_type="email",
                        email_msg=email_msg,
                        linkedin=li_job,
                    )
                    docs_to_upsert[doc["id"]] = doc
                    job_masters[slug] = doc
                    logging.info(
                        f"Matched email '{email_msg.filename}' -> {li_job.company_name} / {li_job.job_title} "
                        f"(score={score:.2f}, days_diff={details['days_diff']})"
                    )
                    continue

                slug = _make_slug(email_msg.company, email_msg.role, email_msg.date)
                doc = _build_flat_doc(
                    job_slug=slug,
                    company_name=email_msg.company or "Unknown",
                    job_title=email_msg.role or "Unknown",
                    app_date=email_msg.date,
                    record_type="email",
                    email_msg=email_msg,
                )
                docs_to_upsert[doc["id"]] = doc
                job_masters[slug] = doc
                logging.info(f"Orphan email created for '{email_msg.filename}'")

            except Exception as e:
                logging.error(f"Failed processing email {email_msg.filename}: {e}")
                continue

        for job in linkedin_jobs:
            li_id = hashlib.md5(
                f"{job.company_name}|{job.job_title}|{job.application_date.isoformat()}".encode()
            ).hexdigest()
            if li_id in matched_linkedin_ids:
                continue
            slug = _make_slug(job.company_name, job.job_title, job.application_date)
            if slug in job_masters:
                continue
            doc = _build_flat_doc(
                job_slug=slug,
                company_name=job.company_name,
                job_title=job.job_title,
                app_date=job.application_date,
                record_type="linkedin",
                linkedin=job,
            )
            docs_to_upsert[doc["id"]] = doc
            job_masters[slug] = doc

        logging.info(f"Upserting up to {len(docs_to_upsert)} flat documents to CosmosDB...")
        synced_email_filenames: set = set()
        hit_cap = False

        for doc in docs_to_upsert.values():
            if docs_written >= max_docs_per_run:
                logging.warning(f"Hit safety cap of {max_docs_per_run} docs. Remaining will be picked up next run.")
                hit_cap = True
                break
            try:
                cosmos.upsert(doc)
                docs_written += 1
                fname = doc.get("email_filename")
                if fname:
                    synced_email_filenames.add(fname)
            except Exception as e:
                logging.error(f"Upsert failed for id={doc.get('id')}: {e}")

        moved = 0
        for fname in synced_email_filenames:
            src_blob = filename_to_blob.get(fname)
            if not src_blob:
                continue
            dst_blob = src_blob.replace("processed/", "synced/", 1)
            try:
                _move_blob(cleanmail_client, src_blob, dst_blob)
                moved += 1
            except Exception as e:
                logging.warning(f"Could not move blob {src_blob} -> {dst_blob}: {e}")

        logging.info(
            f"CosmosDB sync done. Upserts: {docs_written}/{len(docs_to_upsert)}. "
            f"Moved {moved} email blobs to synced/. {'(cap hit)' if hit_cap else ''}"
        )

    except Exception as e:
        logging.error(f"Fatal error during CosmosDB sync: {e}")