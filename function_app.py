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

