"""Apollo.io company enrichment with a persistent JSON cache.

Two-step lookup:
  1. If we have a real company domain, call Organization Enrichment (GET).
  2. Otherwise, call Organization Search (POST) with pre-filters for
     US headquarters and 10-100 employees — so any match already satisfies
     the headcount + country constraint, which kills most false-name matches.
"""
import json
import os
import re
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY")
CACHE_PATH = Path(os.environ.get("CACHE_PATH", "/data/apollo_cache.json"))
CACHE_TTL_DAYS = int(os.environ.get("CACHE_TTL_DAYS", "30"))

ENRICH_URL = "https://api.apollo.io/api/v1/organizations/enrich"
SEARCH_URL = "https://api.apollo.io/api/v1/mixed_companies/search"


def _load_cache() -> dict:
    try:
        if CACHE_PATH.exists():
            return json.loads(CACHE_PATH.read_text())
    except Exception:
        pass
    return {}


def _save_cache(cache: dict) -> None:
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(cache))
    except Exception as e:
        print(f"[cache] write failed: {e}", flush=True)


_CACHE = _load_cache()


def _normalize_key(company: str, domain: Optional[str]) -> str:
    if domain:
        return f"d:{domain.lower().strip()}"
    return f"n:{re.sub(r'[^a-z0-9]', '', (company or '').lower())}"


def _extract_domain(company_url: Optional[str]) -> Optional[str]:
    if not company_url or not isinstance(company_url, str):
        return None
    try:
        url = company_url if "://" in company_url else f"https://{company_url}"
        host = urlparse(url).netloc.lower().replace("www.", "")
        # Skip job-board profile URLs — those aren't company domains.
        bad = ("linkedin.com", "indeed.com", "glassdoor.com",
               "ziprecruiter.com", "google.com", "facebook.com")
        if any(b in host for b in bad) or not host:
            return None
        return host
    except Exception:
        return None


def _apollo_headers() -> dict:
    return {
        "X-Api-Key": APOLLO_API_KEY,
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "accept": "application/json",
    }


def _apollo_enrich_by_domain(domain: str) -> Optional[dict]:
    try:
        r = requests.get(
            ENRICH_URL,
            headers=_apollo_headers(),
            params={"domain": domain},
            timeout=20,
        )
        if r.status_code == 429:
            time.sleep(2)
            return None
        r.raise_for_status()
        return (r.json() or {}).get("organization")
    except Exception as e:
        print(f"[apollo] enrich domain={domain} failed: {e}", flush=True)
        return None


def _apollo_search_by_name(name: str) -> Optional[dict]:
    """Search with US + 10-100 employees pre-filters baked in.

    If Apollo returns any organization, it already satisfies the country
    and size constraints — so we only need to verify the name match.
    """
    try:
        r = requests.post(
            SEARCH_URL,
            headers=_apollo_headers(),
            json={
                "q_organization_name": name,
                "organization_locations": ["United States"],
                "organization_num_employees_ranges": ["10,100"],
                "page": 1,
                "per_page": 5,
            },
            timeout=20,
        )
        if r.status_code == 429:
            time.sleep(2)
            return None
        r.raise_for_status()
        orgs = (r.json() or {}).get("organizations") or []
        if not orgs:
            return None
        # Prefer exact (case-insensitive) name match; else first reasonable one.
        want = name.lower().strip()
        for o in orgs:
            if (o.get("name") or "").lower().strip() == want:
                return o
        # Loose fallback: substring match either direction.
        for o in orgs:
            got = (o.get("name") or "").lower().strip()
            if got and (got in want or want in got):
                return o
        return None
    except Exception as e:
        print(f"[apollo] search name={name} failed: {e}", flush=True)
        return None


def enrich(company_name: str, company_url: Optional[str] = None) -> dict:
    """Return {'employee_count': int|None, 'industry': str|None, 'country': str|None}."""
    domain = _extract_domain(company_url)
    key = _normalize_key(company_name, domain)

    cached = _CACHE.get(key)
    now = time.time()
    if cached and (now - cached.get("ts", 0)) < CACHE_TTL_DAYS * 86400:
        return cached["data"]

    if not APOLLO_API_KEY:
        return {"employee_count": None, "industry": None, "country": None}

    org = None
    if domain:
        org = _apollo_enrich_by_domain(domain)
    if not org and company_name:
        org = _apollo_search_by_name(company_name)

    data = {
        "employee_count": (org or {}).get("estimated_num_employees"),
        "industry": (org or {}).get("industry"),
        "country": (org or {}).get("country"),
    }

    _CACHE[key] = {"ts": now, "data": data}
    _save_cache(_CACHE)
    return data
