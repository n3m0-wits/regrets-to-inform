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

