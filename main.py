"""Daily job posting scraper — emails a CSV of matching postings."""
import os
import re
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
    "BI Engineer",
    "BI Analyst",
    "Data Architect",
    "AI/ML Engineer",
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


def parse_employee_count(raw):
    """Extract (min, max) employee count from strings like '51-200 employees'."""
    if not raw or not isinstance(raw, str):
        return (None, None)
    s = raw.lower().replace(",", "")
    nums = [int(n) for n in re.findall(r"\d+", s)]
    if not nums:
        return (None, None)
    if len(nums) == 1:
        return (nums[0], nums[0])
    return (min(nums), max(nums))


def in_target_size(raw):
    lo, hi = parse_employee_count(raw)
    if lo is None:
        return False
    # Overlap with [10, 100]
    return hi >= 10 and lo <= 100


def is_us_location(loc):
    if not isinstance(loc, str):
        return False
    s = loc.lower()
    if any(x in s for x in ["united states", "usa", ", us"]):
        return True
    # state abbreviations after a comma: ", CA" etc.
    return bool(re.search(r",\s*[A-Z]{2}\b", loc))


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

    # Normalize columns that may be missing on some sites
    for col in ["company", "title", "job_url", "location",
                "company_num_employees", "company_industry"]:
        if col not in df.columns:
            df[col] = None

    df = df.dropna(subset=["company", "title"])

    # US only
    df = df[df["location"].apply(is_us_location)]

    # Exclude staffing/nonprofit by industry
    df = df[~df["company_industry"].apply(industry_excluded)]

    # Exclude known job platforms / staffing agencies by company name
    df = df[~df["company"].apply(company_excluded)]

    # 10-100 employees (requires the data; drop rows without it since we
    # can't verify the size constraint)
    df = df[df["company_num_employees"].apply(in_target_size)]

    # Dedupe on company + title
    df = df.drop_duplicates(subset=["company", "title"])

    out = pd.DataFrame({
        "Company Name": df["company"],
        "Company Number of Employees": df["company_num_employees"],
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
        f"(US, 10-100 employees, excluding staffing/nonprofits/job platforms)."
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
