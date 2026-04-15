# job-posting-tracker

Scrapes LinkedIn, Indeed, Glassdoor, ZipRecruiter, and Google Jobs daily for
data/AI/analytics roles. Filters to **US-based companies with 10–100 employees**,
excluding staffing agencies, nonprofits, and job-platform reposts. Emails the
results as a CSV.

Built to run on [Railway](https://railway.app) as a cron job.

## Roles searched

Data Engineer, Data Analyst, AI Engineer, AI Consultant, AI Specialist,
Analytics Engineer, Machine Learning Engineer, ML Engineer, Data Scientist,
Analytics Consultant, BI Engineer, BI Analyst, Data Architect, Applied AI
Engineer, GenAI Engineer, LLM Engineer, Data Platform Engineer.

## CSV columns

- Company Name
- Company Number of Employees
- Job Posting Title
- Job Posting Link

## Local run

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in SMTP creds
python main.py
```

## Railway deploy

1. Push this repo to GitHub.
2. On Railway: **New Project → Deploy from GitHub repo** → select this repo.
3. In the service **Variables** tab, set:
   - `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`
   - `EMAIL_FROM`, `EMAIL_TO`
4. In the service **Settings → Cron Schedule**, confirm: `0 10 * * *`
   (10:00 UTC = 6:00 AM ET; adjust if you want a different timezone).
5. Deploy. Railway will run `python main.py` on schedule.

### Gmail SMTP

Use an [App Password](https://myaccount.google.com/apppasswords) (requires 2FA
on the Google account). Set `SMTP_USER` to your Gmail address and
`SMTP_PASSWORD` to the 16-character app password.

## Notes on filtering

- **Employee count** comes from LinkedIn/Glassdoor company data via JobSpy.
  Postings whose companies don't expose a size are dropped (can't verify the
  10–100 requirement).
- **US filter** checks for "United States", "USA", or a `, XX` state-abbreviation
  suffix in the location field.
- **Staffing / nonprofit filter** is keyword-based on company industry *and*
  company name (see `EXCLUDE_*` lists in `main.py` — easy to extend).
