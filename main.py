"""Hourly Upwork scraper via Apify — emails matching postings as CSV."""
import csv
import os
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage
from io import StringIO

import requests
from dotenv import load_dotenv

load_dotenv()

APIFY_ACTOR = "jupri~upwork"
APIFY_URL = (
    f"https://api.apify.com/v2/acts/{APIFY_ACTOR}/run-sync-get-dataset-items"
)

ACTOR_INPUT = {
    "age": 1,
    "age_unit": "hour",
    "client_location": ["United States"],
    "contract_to_hire": False,
    "dev_dataset_clear": False,
    "dev_no_strip": False,
    "fixed": True,
    "hourly": True,
    "hourly_min": 60,
    "includes.attachments": False,
    "includes.history": False,
    "no_hires": False,
    "payment_verified": False,
    "previous_clients": False,
    "price_min": 1000,
    "query": [
        "Data Analysis",
        "Data Analyst",
        "Salesforce Data",
        "Data Pipeline",
        "HubSpot Data",
        "GA4 data",
        "Data Dashboard",
        "Data Analytics",
    ],
}

CSV_FIELDS = [
    "Date Posted",
    "Title",
    "Description",
    "URL",
    "Type",
    "Fixed Price Budget",
    "Hourly min",
    "Hourly max",
    "Duration",
]


def fetch_jobs() -> list:
    token = os.environ["APIFY_TOKEN"]
    r = requests.post(
        APIFY_URL,
        params={"token": token},
        json=ACTOR_INPUT,
        timeout=600,
    )
    r.raise_for_status()
    return r.json() or []


def row_for(job: dict) -> dict:
    fixed = job.get("fixed") or {}
    hourly = job.get("hourly") or {}
    duration = hourly.get("duration") or fixed.get("duration") or {}
    return {
        "Date Posted": job.get("ts_publish"),
        "Title": job.get("title"),
        "Description": job.get("description"),
        "URL": job.get("url"),
        "Type": job.get("type"),
        "Fixed Price Budget": fixed.get("budget"),
        "Hourly min": hourly.get("min"),
        "Hourly max": hourly.get("max"),
        "Duration": duration.get("label"),
    }


def build_csv(jobs: list) -> str:
    buf = StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_FIELDS)
    w.writeheader()
    for j in jobs:
        w.writerow(row_for(j))
    return buf.getvalue()


def send_email(csv_text: str, row_count: int) -> None:
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]
    password = os.environ["SMTP_PASSWORD"]
    to_addr = os.environ.get("EMAIL_TO", "jeinhorn92@gmail.com")
    from_addr = os.environ.get("EMAIL_FROM", user)

    stamp = datetime.utcnow().strftime("%Y-%m-%d-%H%MZ")
    msg = EmailMessage()
    msg["Subject"] = f"Upwork Postings — {stamp} ({row_count} results)"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(
        f"Attached: {row_count} Upwork postings from the last hour."
    )
    msg.add_attachment(
        csv_text.encode("utf-8"),
        maintype="text",
        subtype="csv",
        filename=f"upwork-{stamp}.csv",
    )

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, password)
        s.send_message(msg)
    print(f"[email] sent {row_count} rows to {to_addr}", flush=True)


def main() -> None:
    jobs = fetch_jobs()
    print(f"[apify] fetched {len(jobs)} jobs", flush=True)
    send_email(build_csv(jobs), len(jobs))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        raise
