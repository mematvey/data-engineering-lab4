import os
import re
from pathlib import Path

import boto3
from botocore.config import Config


def load_env():
    path = Path(__file__).parent.parent / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_env()

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9200")
ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]
ZK_HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
BUCKET = "hh-lake"
CSV_KEY = "raw/it_vacancies_2024.csv"

ROLES = ("Программист, разработчик", "Руководитель группы разработки")
NAME_RE = re.compile(
    r"android|андроид|(^|[^a-z])ios([^a-z]|$)|swift|kotlin|flutter|react.native"
    r"|objective-c|xamarin|ionic|mobile|мобильн"
)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(retries={"max_attempts": 3}, connect_timeout=10, read_timeout=120),
    )


def is_variant(role, name):
    return role in ROLES and NAME_RE.search(name.lower()) is not None


def parse_skills(raw):
    if not raw:
        return []
    return sorted({s.strip().lower() for s in raw.split("; ") if s.strip()})
