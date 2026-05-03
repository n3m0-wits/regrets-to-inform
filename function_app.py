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
