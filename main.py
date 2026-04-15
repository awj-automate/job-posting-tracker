"""Daily job posting scraper — emails a CSV of matching postings.

JobSpy returns raw postings; Apollo is the sole source of truth for company
size, industry, and country (JobSpy does not expose those fields).
"""
import os
import smtplib
import sys
from datetime import datetime
from email.message import EmailMessage
from io import StringIO

import pandas as pd
from dotenv import load_dotenv
from jobspy import scrape_jobs

from enrich import enrich

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

EXCLUDE_INDUSTRY_KEYWORDS = [
    "staffing", "recruit", "non-profit", "nonprofit", "non profit",
    "religious", "charit",
]

EXCLUDE_COMPANY_KEYWORDS = [
    "indeed", "linkedin", "ziprecruiter", "glassdoor", "dice", "monster",
    "simplyhired", "snagajob", "careerbuilder", "jobot", "cybercoders",
    "robert half", "insight global", "teksystems", "kforce", "randstad",
    "adecco", "manpower", "kelly services", "aerotek", "beacon hill",
    "motion recruitment", "judge group", "apex systems", "collabera",
    "mindlance", "diverse lynx", "tata consultancy", "infosys", "wipro",
    "cognizant", "accenture", "deloitte", "capgemini", "hcl",
]


def industry_excluded(industry):
    if not isinstance(industry, str):
        return False
    s = industry.lower()
    return any(k in s for k in EXCLUDE_INDUSTRY_KEYWORDS)


def company_excluded(company):
    if not isinstance(company, str):
        return False
    s = company.lower()
    return any(k in s for k in EXCLUDE_COMPANY_KEYWORDS)


def is_us_country(country):
    if not isinstance(country, str):
        return False
    s = country.strip().lower()
    return s in ("united states", "usa", "us", "u.s.", "u.s.a.")


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
                df["_search_term"] = term
                frames.append(df)
        except Exception as e:
            print(f"  ! {term}: {e}", flush=True)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def filter_jobs(df):
    if df.empty:
        return df

    # Ensure expected columns exist — JobSpy's schema varies by site.
    for col in ["company", "title", "job_url", "company_url"]:
        if col not in df.columns:
            df[col] = None

    df = df.dropna(subset=["company", "title"])

    # Early exclude: known staffing / job-platform company names.
    df = df[~df["company"].apply(company_excluded)]

    # Dedupe before enrichment so we don't pay Apollo for duplicates.
    df = df.drop_duplicates(subset=["company", "title"]).reset_index(drop=True)

    # Enrich every unique company via Apollo (US + 10-100 is filtered at the
    # API level, so a returned record already satisfies those constraints).
    unique_companies = df.drop_duplicates(subset=["company"])[
        ["company", "company_url"]
    ]
    print(f"[enrich] unique companies to resolve: {len(unique_companies)}",
          flush=True)
    enrichment = {}
    for _, row in unique_companies.iterrows():
        enrichment[row["company"]] = enrich(row["company"], row.get("company_url"))

    df["_employees"] = df["company"].map(
        lambda c: enrichment.get(c, {}).get("employee_count")
    )
    df["_industry"] = df["company"].map(
        lambda c: enrichment.get(c, {}).get("industry")
    )
    df["_country"] = df["company"].map(
        lambda c: enrichment.get(c, {}).get("country")
    )

    # Drop rows Apollo couldn't resolve — we can't verify the constraints.
    before = len(df)
    df = df[df["_employees"].notna()]
    print(f"[filter] dropped {before - len(df)} unresolved companies",
          flush=True)

    # Size: 10-100 employees (exact count from Apollo).
    df = df[df["_employees"].apply(lambda n: 10 <= int(n) <= 100)]

    # Company must be US-headquartered per Apollo.
    df = df[df["_country"].apply(is_us_country)]

    # Exclude staffing / nonprofit by Apollo's industry field.
    df = df[~df["_industry"].apply(industry_excluded)]

    out = pd.DataFrame({
        "Company Name": df["company"],
        "Company Number of Employees": df["_employees"].astype(int),
        "Job Posting Title": df["title"],
        "Job Posting Link": df["job_url"],
    })
    return out.reset_index(drop=True)


def send_email(csv_text, row_count):
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ["SMTP_USER"]
    password = os.environ["SMTP_PASSWORD"]
    to_addr = os.environ.get("EMAIL_TO", "jeinhorn92@gmail.com")
    from_addr = os.environ.get("EMAIL_FROM", user)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    msg = EmailMessage()
    msg["Subject"] = f"Job Postings — {today} ({row_count} matches)"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(
        f"Attached: {row_count} job postings matching your filters "
        f"(US-headquartered, 10-100 employees per Apollo, excluding "
        f"staffing/nonprofits/job platforms)."
    )
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
    filtered = filter_jobs(raw)
    print(f"[filter] matched rows: {len(filtered)}", flush=True)

    buf = StringIO()
    filtered.to_csv(buf, index=False)
    send_email(buf.getvalue(), len(filtered))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        raise
