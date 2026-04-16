"""Daily job posting scraper — emails raw results as a CSV."""
import os
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage
from io import StringIO

import pandas as pd
from dotenv import load_dotenv
from jobspy import scrape_jobs

load_dotenv()

SEARCH_TERMS = [
    "Data Engineer",
    "Data Analyst",
    "AI Engineer",
    "AI Consultant",
    "AI Specialist",
    "Analytics Engineer",
    "Machine Learning Engineer",
    "ML Engineer",
    "Data Scientist",
    "Analytics Consultant",
    "Business Intelligence Engineer",
    "BI Analyst",
    "Data Architect",
    "Applied AI Engineer",
    "GenAI Engineer",
    "LLM Engineer",
    "Data Platform Engineer",
]

SITES = ["linkedin", "indeed", "zip_recruiter", "glassdoor", "google"]


def scrape_all():
    frames = []
    for term in SEARCH_TERMS:
        print(f"[scrape] {term}", flush=True)
        try:
            df = scrape_jobs(
                site_name=SITES,
                search_term=term,
                google_search_term=f"{term} jobs in United States since yesterday",
                location="United States",
                results_wanted=40,
                hours_old=24,
                country_indeed="USA",
                linkedin_fetch_description=False,
                verbose=0,
            )
            if df is not None and len(df) > 0:
                df["search_term"] = term
                frames.append(df)
        except Exception as e:
            print(f"  ! {term}: {e}", flush=True)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def send_email(csv_text, row_count):
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]
    password = os.environ["SMTP_PASSWORD"]
    to_addr = os.environ.get("EMAIL_TO", "jeinhorn92@gmail.com")
    from_addr = os.environ.get("EMAIL_FROM", user)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["Subject"] = f"Job Postings — {today} ({row_count} results)"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(f"Attached: {row_count} raw job postings from today's scrape.")
    msg.add_attachment(
        csv_text.encode("utf-8"),
        maintype="text",
        subtype="csv",
        filename=f"job-postings-{today}.csv",
    )

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, password)
        s.send_message(msg)
    print(f"[email] sent {row_count} rows to {to_addr}", flush=True)


def main():
    raw = scrape_all()
    print(f"[scrape] total raw rows: {len(raw)}", flush=True)

    # Dedupe across sites/search terms
    if not raw.empty:
        raw = raw.drop_duplicates(subset=["company", "title"]).reset_index(drop=True)
        print(f"[dedup] unique rows: {len(raw)}", flush=True)

    buf = StringIO()
    raw.to_csv(buf, index=False)
    send_email(buf.getvalue(), len(raw))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        raise
