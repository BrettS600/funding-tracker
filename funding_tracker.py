#!/usr/bin/env python3
"""
Biotech Department Funding Tracker  (weekly, NIH-only)
======================================================
Runs once a week (GitHub Actions, Monday morning, right after NIH RePORTER's
Sunday-night refresh) and maintains a ledger of NIH awards GRANTED during the
just-completed week to biotech-adjacent university departments.

  Each run also re-checks every earlier week, because RePORTER often posts an
  award, or its dollar amount, weeks after the notice date; awards already
  recorded are skipped, so nothing is counted twice. A week is marked complete
  after QUIET_CHECKS_TO_COMPLETE weekly checks in a row find nothing new (and
  no matching award still waiting for an amount); complete weeks are checked
  once a month after that and reopen if anything turns up. Awards that arrive
  after their week was first checked are tagged "Added: <date>".

  Column A = #   B = University   C = Department   D = Funder (NIH institute)
  Every completed Sunday-Saturday week = one column, labeled like
  "Sep 6 - Sep 12". An award appears only once, in the week of its NIH award
  notice date. Cells show each award hyperlinked to its RePORTER project
  page, a ruled "= total" when there are several, and the contact PI's name
  and email.

PI emails, in order of preference:
  1. RePORTER's own project-info service (what the "View Email" button on a
     project page reveals) - undocumented endpoint, so failures are logged
     and tolerated;
  2. the PI's own recent PubMed publications (.edu addresses that contain
     the PI's name);
  3. "--" if neither yields one. All lookups (hits and misses) are cached in
     data/pi_email_cache.json so each PI costs at most one lookup.

Outputs
  data/tracker_data.json   the ledger (source of truth, committed to repo)
  data/seen_awards.json    every award id ever emitted (dedup memory)
  data/pi_email_cache.json PI email lookup cache
  data/nih_emails.csv      emails revealed on NIH's project pages (you add these;
                           docs/nih-email-helper.user.js collects them in Chrome)
  data/nih_links.csv       every PI with their NIH View Email link, rebuilt each run;
                           emails typed into its nih_email column are kept and shown
  data/nih_links.md        the same list with clickable links (GitHub's CSV view
                           shows links as plain text)
  docs/index.html          sortable/filterable page served by GitHub Pages

Usage
  python funding_tracker.py              normal weekly run
  python funding_tracker.py --selftest   offline test with mock data
  python funding_tracker.py --skip-emails  skip email lookups this run
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------

# NIH standardized dept_type values to INCLUDE (verbatim strings). Awards
# whose org reports NO department (common outside medical schools) are also
# kept when the project title/terms match BIO_KEYWORDS, shown as "--".
NIH_INCLUDE_DEPTS = [
    "BIOCHEMISTRY",
    "BIOMEDICAL ENGINEERING",
    "BIOPHYSICS",
    "BIOLOGY",
    "MICROBIOLOGY/IMMUN/VIROLOGY",
    "GENETICS",
    "ENGINEERING (ALL TYPES)",   # bioprocess/chem-eng lives here; keyword-gated
    "CHEMISTRY",
    "PHARMACOLOGY",
    "PHYSIOLOGY",
    "ANATOMY/CELL BIOLOGY",
    "NUTRITION",
    "VETERINARY SCIENCES",
]

BIO_KEYWORDS = [
    "bio", "cell", "tissue", "protein", "gene", "genom", "rna", "dna",
    "microb", "ferment", "enzym", "vaccin", "antibod", "therapeut",
    "pharma", "drug", "organoid", "biomanufactur", "bioprocess", "biosens",
    "stem", "immun", "virus", "viral", "crispr", "molecul",
]

NON_NIH_AGENCIES = {"CDC", "FDA", "AHRQ", "ACF", "VA", "HRSA", "CMS", "SAMHSA"}

NO_DEPT_LABEL = "--"

UNIVERSITY_MARKERS = ("UNIVERSITY", "COLLEGE", "INSTITUTE", "POLYTECH",
                      "SCHOOL OF", "ACADEMY OF")

SCHEMA = "nih-weekly-1"  # ledger format tag; older data is reset automatically

# Every run re-checks every week in the ledger, because RePORTER often posts an
# award (or its dollar amount) weeks after its notice date; awards already
# recorded are skipped, so nothing is counted twice. A week is checked off as
# complete after QUIET_CHECKS_TO_COMPLETE weekly checks in a row that found
# nothing new and no matching award still waiting for a dollar amount.
# Complete weeks still get a light re-check every COMPLETE_RECHECK_DAYS and
# reopen if anything new turns up.
QUIET_CHECKS_TO_COMPLETE = 8
COMPLETE_RECHECK_DAYS = 28
PENDING_GRACE_DAYS = 182   # after ~6 months a $0 listing no longer holds a week open

# --- PI email lookup --------------------------------------------------------
ENABLE_EMAIL_LOOKUP = True
# NOTE: RePORTER's own "View Email" button is reCAPTCHA-gated and its
# project-info service carries no email field (verified), so PubMed /
# Europe PMC publications are the email source.
NCBI_API_KEY = os.environ.get("NCBI_API_KEY", "").strip()  # optional, free
EMAIL_WORKERS = 6                 # parallel PI lookups
NCBI_RATE = 8.0 if NCBI_API_KEY else 2.5   # requests/sec (limit: 10 / 3)
EPMC_RATE = 4.0                   # Europe PMC requests/sec (be polite)
MAX_PMC_FULLTEXT_PER_PI = 1       # open-access full texts to inspect per PI
EMAIL_RETRY_DAYS = 45
MAX_EMAIL_LOOKUPS_PER_RUN = 900
CACHE_VERSION = 7                 # bump ONLY to force a re-try of cached misses

# --- Publications (OpenAlex) ------------------------------------------------
OPENALEX_API_KEY = os.environ.get("OPENALEX_API_KEY", "").strip()
OPENALEX_RATE = 5.0               # requests/sec (limit is 100/s; budget is $1/day)
OPENALEX_WORKERS = 4
PUBS_YEARS = 5                    # rolling window for "publications within..."
MAX_PUBS_LOOKUPS_PER_RUN = 400    # ~2 calls each; keeps a run inside the free $1/day
PUBS_REFRESH_DAYS = 60            # re-pull a PI's list this often (they keep publishing)
LINKED_RECHECK_DAYS = 30          # re-ask NIH for grant-linked papers this often
MAX_PUBS_PER_PI = 300

# ----------------------------------------------------------------------------
# Paths & session
# ----------------------------------------------------------------------------

HOME = Path(os.environ.get("TRACKER_HOME", Path(__file__).resolve().parent))
DATA_DIR = HOME / "data"
DOCS_DIR = HOME / "docs"
DATA_FILE = DATA_DIR / "tracker_data.json"
SEEN_FILE = DATA_DIR / "seen_awards.json"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "biotech-funding-tracker/2.0 (personal research tool)"})
TIMEOUT = 40


class _RateLimiter:
    """Shared, thread-safe pacing so N workers never exceed a host's limit."""

    def __init__(self, per_sec: float):
        self.interval = 1.0 / per_sec
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        with self._lock:
            slot = max(time.monotonic(), self._next)
            self._next = slot + self.interval
        delay = slot - time.monotonic()
        if delay > 0:
            time.sleep(delay)


NCBI_LIMIT = _RateLimiter(NCBI_RATE)
EPMC_LIMIT = _RateLimiter(EPMC_RATE)
_STATS_LOCK = threading.Lock()
_TLS = threading.local()


def _session():
    """requests.Session per worker thread; the main thread uses SESSION."""
    if threading.current_thread() is threading.main_thread():
        return SESSION
    s = getattr(_TLS, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update(SESSION.headers)
        _TLS.session = s
    return s


def _bump(stats: dict, key: str = "net", n: int = 1):
    with _STATS_LOCK:
        stats[key] = stats.get(key, 0) + n


_LOG_LOCK = threading.Lock()


def log(msg: str) -> None:
    with _LOG_LOCK:
        print(f"[tracker] {msg}", flush=True)


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception as e:
            log(f"WARNING could not read {path.name}: {e}")
    return default


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=1, sort_keys=True))


def email_cache_path() -> Path:
    return DATA_DIR / "pi_email_cache.json"


# ----------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------

def nice_name(s: str) -> str:
    small = {"of", "at", "and", "the", "for", "in", "on"}
    out = []
    for i, w in enumerate(re.split(r"\s+", s.strip())):
        lw = w.lower()
        out.append(lw if (lw in small and i > 0) else lw.capitalize())
    return " ".join(out)


def fmt_money(n: int) -> str:
    return f"${n:,.0f}"


def flip_name(name: str) -> str:
    """NIH gives 'DOE, JANE A' -> 'Jane A Doe'."""
    name = (name or "").strip()
    if "," in name:
        last, _, rest = name.partition(",")
        name = f"{rest.strip()} {last.strip()}"
    return nice_name(name) if name else ""


def looks_like_university(name: str) -> bool:
    up = name.upper()
    return any(marker in up for marker in UNIVERSITY_MARKERS)


def has_bio_keyword(text: str) -> bool:
    low = (text or "").lower()
    return any(k in low for k in BIO_KEYWORDS)


def last_completed_week(today: date):
    """The most recently completed Sunday-Saturday week."""
    days_since_sunday = (today.weekday() + 1) % 7  # Mon=0..Sun=6 -> Sun->0
    this_sunday = today - timedelta(days=days_since_sunday)
    start = this_sunday - timedelta(days=7)
    return start, start + timedelta(days=6)


def all_weeks(data, today: date):
    """Every week from the ledger's first week through the last completed one."""
    last, _ = last_completed_week(today)
    starts = [date.fromisoformat(w) for w in data.get("dates", [])]
    w = min(starts) if starts else last
    out = []
    while w <= last:
        out.append(w)
        w += timedelta(days=7)
    return out


def weeks_to_check(data, today: date):
    """(start, end) of every week due a check this run, oldest first: all open
    weeks, plus complete weeks whose monthly safety check is due."""
    status = data.get("meta", {}).get("weeks", {})
    due = []
    for w in all_weeks(data, today):
        st = status.get(w.isoformat()) or {}
        if st.get("complete"):
            last = date.fromisoformat(st.get("last_checked") or "1970-01-01")
            if (today - last).days < COMPLETE_RECHECK_DAYS:
                continue
        due.append((w, w + timedelta(days=6)))
    return due


def update_week_status(data, week_iso: str, new: int, pending: int, today: date):
    """Record one check of a week. A check is quiet when it found nothing new
    and no matching award waiting for a dollar amount (after PENDING_GRACE_DAYS
    a listing that never got an amount stops counting). Quiet checks count at
    most once per NIH weekly refresh (Sunday night), so extra runs in the same
    week don't speed things up."""
    weeks = data.setdefault("meta", {}).setdefault("weeks", {})
    st = weeks.setdefault(week_iso, {"first_checked": today.isoformat(), "quiet": 0})
    st["last_checked"] = today.isoformat()
    st["pending"] = pending
    young = (today - date.fromisoformat(week_iso)).days < PENDING_GRACE_DAYS
    if new > 0 or (pending > 0 and young):
        st["quiet"] = 0
        st.pop("quiet_counted", None)
        if st.get("complete"):
            st["complete"] = False
            st.pop("completed_on", None)
            st["reopened"] = today.isoformat()
        return st
    counted = st.get("quiet_counted")
    if not counted or (last_completed_week(date.fromisoformat(counted))[0]
                       != last_completed_week(today)[0]):
        st["quiet"] = st.get("quiet", 0) + 1
        st["quiet_counted"] = today.isoformat()
    if st["quiet"] >= QUIET_CHECKS_TO_COMPLETE and not st.get("complete"):
        st["complete"] = True
        st["completed_on"] = today.isoformat()
    return st


def short_date(iso: str) -> str:
    """'2026-09-16' -> 'Sep 16'."""
    d = date.fromisoformat(iso)
    return f"{d:%b} {d.day}"


def wlabel(week_start_iso: str) -> str:
    s = datetime.strptime(week_start_iso, "%Y-%m-%d").date()
    e = s + timedelta(days=6)
    return f"{s:%b} {s.day} – {e:%b} {e.day}"


def wshort(week_start_iso: str) -> str:
    """Compact label for phone tabs: 'Sep 20–26', or 'Sep 27–Oct 3' across months."""
    s = datetime.strptime(week_start_iso, "%Y-%m-%d").date()
    e = s + timedelta(days=6)
    if s.month == e.month:
        return f"{s:%b} {s.day}–{e.day}"
    return f"{s:%b} {s.day}–{e:%b} {e.day}"


# ----------------------------------------------------------------------------
# PI email lookup: grant-linked publications, then PubMed author search,
# then Europe PMC open-access full text
# ----------------------------------------------------------------------------

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# Institutional addresses are not all .edu - research institutes (.org),
# hospitals, government labs (.gov) and foreign universities (.ac.uk, .ca)
# are all legitimate. Reject only free consumer providers.
FREE_EMAIL_DOMAINS = {
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mac.com", "proton.me", "protonmail.com", "gmx.com", "gmx.de",
    "mail.com", "yandex.ru", "qq.com", "163.com", "126.com", "sina.com",
    "example.com",
}


def _institutional(email: str) -> bool:
    dom = email.split("@")[-1].lower().rstrip(".")
    return bool(dom) and dom not in FREE_EMAIL_DOMAINS
AFFIL_STOP = {"university", "of", "the", "at", "and", "a", "an", "in", "for",
              "system", "medical", "college", "school", "institute", "center",
              "centre", "hospital", "health", "sciences", "science",
              "research", "graduate", "state"}


def _walk_for_email(obj, require_pi=True):
    """Recursively scan a JSON payload for an email under an email-ish key.
    First pass prefers keys mentioning both 'pi' and 'email'; second pass
    accepts any 'email' key."""
    hits = []

    def walk(o, keypath=""):
        if isinstance(o, dict):
            for k, v in o.items():
                walk(v, f"{keypath}.{k}".lower())
        elif isinstance(o, list):
            for v in o:
                walk(v, keypath)
        elif isinstance(o, str) and "email" in keypath:
            m = EMAIL_RE.search(o)
            if m:
                hits.append((keypath, m.group(0)))

    walk(obj)
    for kp, e in hits:
        if "pi" in kp or "contact" in kp:
            return e
    if not require_pi and hits:
        return hits[0][1]
    return hits[0][1] if hits else ""


def _uni_affil_terms(university: str) -> str:
    """One distinctive token, e.g. 'Vanderbilt University Medical Center' ->
    Vanderbilt. Stitching two non-adjacent words into a quoted phrase (the
    old behaviour) never matched real affiliation strings."""
    toks = [w for w in re.split(r"[^A-Za-z]+", university)
            if w and w.lower() not in AFFIL_STOP and len(w) > 2]
    if not toks:
        toks = [w for w in re.split(r"[^A-Za-z]+", university)
                if w and w.lower() not in {"of", "the", "at", "and"}]
    if not toks:
        return ""
    # longest wins (ties -> earliest): "Ada Forsyth" -> Forsyth, not Ada
    return max(toks, key=lambda w: (len(w), -toks.index(w)))


def _name_forms(pi: str):
    """('Jessica Leigh Mark Welch') -> [('Jessica','Welch'),('Jessica','Mark Welch')]
    so compound surnames are not silently truncated."""
    parts = [x for x in pi.split() if x]
    if len(parts) < 2:
        return []
    forms = [(parts[0], parts[-1])]
    if len(parts) >= 3:
        forms.append((parts[0], " ".join(parts[-2:])))
    return forms


def _plausible_own_email(email: str, first: str, last: str) -> bool:
    """Accept jane_big@, jbig@, big@, bigj@, j.big@ - reject co-authors."""
    lp = email.split("@")[0].lower()
    last_l = re.sub(r"[^a-z]", "", last.lower())
    fi = re.sub(r"[^a-z]", "", first.lower())[:1]
    if not last_l:
        return False
    if len(last_l) >= 4 and last_l[:4] in lp:
        return True
    if len(last_l) == 3 and (lp.startswith(last_l) or lp.endswith(last_l)):
        return True
    if fi and lp.startswith(fi) and len(last_l) >= 3 and last_l[:3] in lp:
        return True
    return False


def _extract_email(xml_text: str, forms) -> str:
    """Pull a PI's own institutional email out of PubMed efetch XML.
    `forms` is [(first, last), ...] - any form may match."""
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return ""
    lasts = {l.lower() for _, l in forms} | {l.split()[-1].lower() for _, l in forms}

    def ok(e):
        return _institutional(e) and any(
            _plausible_own_email(e, f, l) for f, l in forms)

    # pass 1: affiliations attached to an author whose surname matches
    for au in root.iter("Author"):
        if (au.findtext("LastName") or "").lower() not in lasts:
            continue
        for aff in au.iter("Affiliation"):
            for e in EMAIL_RE.findall(aff.text or ""):
                if ok(e):
                    return e
    # pass 2: any affiliation line, name match still required
    for aff in root.iter("Affiliation"):
        for e in EMAIL_RE.findall(aff.text or ""):
            if ok(e):
                return e
    return ""


def _ncbi_get(url: str, params: dict):
    """Rate-limited GET with polite retries: throttling (429), NCBI server
    hiccups (500/502/503/504) and dropped connections, growing pauses."""
    last_exc = None
    for attempt, pause in enumerate((0, 2.5, 6.0)):
        if pause:
            time.sleep(pause)
        NCBI_LIMIT.wait()
        try:
            r = _session().get(url, params=params, timeout=TIMEOUT)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            continue
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
            last_exc = requests.HTTPError(f"{r.status_code}")
            continue
        r.raise_for_status()
        return r
    raise last_exc or RuntimeError("NCBI request failed")


def _ncbi_post(url: str, data: dict):
    """Rate-limited POST (for long term lists / big id batches), same retries
    as _ncbi_get."""
    last_exc = None
    for attempt, pause in enumerate((0, 2.5, 6.0)):
        if pause:
            time.sleep(pause)
        NCBI_LIMIT.wait()
        try:
            r = _session().post(url, data=data, timeout=TIMEOUT)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            continue
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 2:
            last_exc = requests.HTTPError(f"{r.status_code}")
            continue
        r.raise_for_status()
        return r
    raise last_exc or RuntimeError("NCBI request failed")


_REPORTER_FAIL_LOGS = [0]


def _reporter_post(url: str, payload: dict):
    """POST to RePORTER politely (~1 req/s, main thread only); log the first
    few failures so an HTTP error is never mistaken for 'no results'."""
    r = SESSION.post(url, json=payload, timeout=TIMEOUT)
    time.sleep(1.0)
    if r.status_code != 200:
        if _REPORTER_FAIL_LOGS[0] < 3:
            _REPORTER_FAIL_LOGS[0] += 1
            log(f"    RePORTER {url.rsplit('/', 2)[-2]} HTTP {r.status_code}: "
                f"{r.text[:160]!r}")
        return None
    return r.json()


def _batch_grant_pmids(cores) -> dict:
    """{core project number: [pmids, newest first]} for many grants in a few
    calls, instead of one RePORTER call per PI. Papers NIH links to the
    grant are the most precise identity match for common names."""
    cores = [c for c in dict.fromkeys(cores) if c]
    out = {}
    shape_logged = False
    for i in range(0, len(cores), 40):
        chunk = cores[i:i + 40]
        try:
            for offset in (0, 500, 1000):
                data = _reporter_post(
                    "https://api.reporter.nih.gov/v2/publications/search",
                    {"criteria": {"core_project_nums": chunk},
                     "limit": 500, "offset": offset})
                results = (data or {}).get("results") or []
                if results and not shape_logged:
                    shape_logged = True
                    log(f"  publications record keys: {sorted(results[0].keys())[:8]}")
                for x in results:
                    core = x.get("coreproject") or x.get("core_project_num") or ""
                    pm = x.get("pmid")
                    if core and pm:
                        out.setdefault(core, []).append(str(pm))
                if len(results) < 500:
                    break
        except Exception as exc:
            log(f"  grant publications batch failed: {exc}")
    for core, lst in out.items():
        lst = sorted(set(lst), key=int, reverse=True)  # PMIDs grow over time
        out[core] = lst[:20]
    return out


def _pmcids_in(xml_text: str):
    """PMC ids listed in PubMed efetch XML (open-access full text exists)."""
    return re.findall(r'IdType="pmc">\s*(PMC\d+)', xml_text)


def _europepmc_email(pmcid: str, forms, stats, verbose=False) -> str:
    """Open-access full text (JATS) carries explicit corresponding-author
    <email> tags that PubMed's affiliation field often omits."""
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
    try:
        _bump(stats)
        EPMC_LIMIT.wait()
        r = _session().get(url, timeout=TIMEOUT)
        if r.status_code != 200 or not r.text:
            return ""
        for e in EMAIL_RE.findall(r.text):
            if _institutional(e) and any(
                    _plausible_own_email(e, f, l) for f, l in forms):
                if verbose:
                    log(f"    -> {e} (Europe PMC full text, {pmcid})")
                return e
        return ""
    except Exception:
        return ""


def _efetch_email(pmids, forms, stats, verbose=False):
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    common = {"db": "pubmed", "tool": "biotech-funding-tracker"}
    if NCBI_API_KEY:
        common["api_key"] = NCBI_API_KEY
    _bump(stats)
    r = _ncbi_get(base + "efetch.fcgi",
                  dict(common, id=",".join(pmids), retmode="xml"))
    email = _extract_email(r.text, forms)
    if verbose:
        log(f"    -> {email or 'no matching institutional email in affiliations'}")
    if email:
        return email
    for pmcid in _pmcids_in(r.text)[:MAX_PMC_FULLTEXT_PER_PI]:
        email = _europepmc_email(pmcid, forms, stats, verbose)
        if email:
            return email
    return ""


def _pubmed_email(pi: str, university: str, linked, stats: dict,
                  verbose: bool = False):
    """Return an email str, '' for a clean no-hit, or None on transient error
    (None is never cached, so the PI is retried next run).

    Order: papers NIH links to this grant first, then author-name search."""
    forms = _name_forms(pi)
    if not forms:
        return ""
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    common = {"db": "pubmed", "tool": "biotech-funding-tracker"}
    if NCBI_API_KEY:
        common["api_key"] = NCBI_API_KEY
    try:
        if verbose:
            log(f"    grant-linked publications: {len(linked or [])} pmid(s)")
        if linked:
            email = _efetch_email(linked, forms, stats, verbose)
            if email:
                return email
        term = _uni_affil_terms(university)
        queries = []
        for first, last in forms:
            surname = last.split()[-1]
            author = f"{surname} {first[:1]}[Author]"
            if term:
                queries.append(f"{author} AND {term}[Affiliation]")
            queries.append(author)
        seen_q = set()
        for q in queries:
            if q in seen_q:
                continue
            seen_q.add(q)
            _bump(stats)
            r = _ncbi_get(base + "esearch.fcgi",
                          dict(common, term=q, retmax="20", retmode="json",
                               reldate="4000", datetype="pdat"))
            ids = ((r.json().get("esearchresult") or {}).get("idlist")) or []
            if verbose:
                log(f"    pubmed q=[{q}] -> {len(ids)} pmid(s)")
            if not ids:
                continue
            email = _efetch_email(ids, forms, stats, verbose)
            if email:
                return email
        return ""
    except Exception as exc:
        log(f"  PubMed lookup errored for {pi}: {exc}")
        return None


def lookup_pi_email(pi: str, university: str, linked, stats: dict,
                    verbose: bool = False):
    """Return (email, source); ('', '') for a clean miss; (None, None) on a
    transient error (never cached). Runs inside a worker thread."""
    pm = _pubmed_email(pi, university, linked, stats, verbose)
    if pm is None:
        return None, None
    return pm, ("PubMed" if pm else "")


def _backfill_core_numbers(data) -> None:
    """One-time: entries recorded before core project numbers were stored
    get them via batched RePORTER lookups (500 appl ids per call)."""
    missing = {}
    for row in data["rows"].values():
        for entries in row["cells"].values():
            for e in entries:
                if "core" in e:
                    continue
                m = re.search(r"project-details/(\d+)", e.get("url", ""))
                if m:
                    missing.setdefault(m.group(1), []).append(e)
                else:
                    e["core"] = ""
    if not missing:
        return
    ids = list(missing)
    found = 0
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        try:
            data_json = _reporter_post(
                "https://api.reporter.nih.gov/v2/projects/search",
                {"criteria": {"appl_ids": [int(x) for x in chunk]},
                 "include_fields": ["ApplId", "CoreProjectNum"],
                 "limit": 500, "offset": 0})
            for p in (data_json or {}).get("results") or []:
                core = p.get("core_project_num") or ""
                for e in missing.get(str(p.get("appl_id")), []):
                    e["core"] = core
                    found += 1
        except Exception as exc:
            log(f"  core-number backfill chunk failed: {exc}")
    for lst in missing.values():
        for e in lst:
            e.setdefault("core", "")
    save_json(DATA_FILE, data)
    log(f"backfilled core project numbers for {found} entries")


def sweep_pi_emails(data):
    """Fill missing PI emails. Cache-first (so re-runs cost nothing), then
    the remaining PIs are looked up in parallel, newest weeks first.
    Progress-logged and checkpointed so an interrupted run keeps its work."""
    if not ENABLE_EMAIL_LOOKUP:
        return data
    cache = load_json(email_cache_path(), {})
    meta = cache.get("_meta") or {}
    if meta.get("v", 0) < 5:
        # legacy caches (before v5) stored transient errors as misses
        cache = {k: v for k, v in cache.items()
                 if isinstance(v, dict) and v.get("email")}
    cache["_meta"] = {"v": CACHE_VERSION}
    _backfill_core_numbers(data)

    # group every entry that still lacks an email by PI + university
    groups, order = {}, []
    for wk in reversed(data["dates"]):
        for row in data["rows"].values():
            for e in row["cells"].get(wk, []):
                if not e.get("pi") or e.get("pi_email"):
                    continue
                key = f"{e['pi']}|{row['university']}".lower()
                if key not in groups:
                    groups[key] = {"pi": e["pi"], "university": row["university"],
                                   "core": e.get("core", ""), "entries": []}
                    order.append(key)
                groups[key]["entries"].append(e)
    pending = sum(len(g["entries"]) for g in groups.values())
    if not pending:
        log("PI email sweep: nothing to look up")
        save_json(email_cache_path(), cache)
        return data

    changed = False
    filled = 0
    todo = []
    today = date.today()
    for key in order:
        g = groups[key]
        ent = cache.get(key)
        if ent and ent.get("email"):
            for e in g["entries"]:
                e["pi_email"] = ent["email"]
                e["pi_email_via"] = ent.get("src", "PubMed")
            filled += len(g["entries"])
            changed = True
            continue
        if ent:
            try:
                checked = date.fromisoformat(ent.get("checked", "1970-01-01"))
            except ValueError:
                checked = date(1970, 1, 1)
            if (today - checked).days < EMAIL_RETRY_DAYS:
                continue  # recent miss: wait it out
        todo.append(g)
    skipped_cap = max(len(todo) - MAX_EMAIL_LOOKUPS_PER_RUN, 0)
    todo = todo[:MAX_EMAIL_LOOKUPS_PER_RUN]
    log(f"PI email sweep: {pending} entries need emails; {filled} filled from "
        f"cache, {len(todo)} PI(s) to look up"
        + (f", {skipped_cap} deferred to next run" if skipped_cap else ""))

    stats = {"net": 0, "pis": 0}
    if todo:
        core_map = _batch_grant_pmids(g["core"] for g in todo)
        log(f"  grant-linked papers found for {len(core_map)} of {len(todo)} grant(s)")

        def work(g, verbose):
            if verbose:
                log(f"  lookup: {g['pi']} @ {g['university']}")
            return lookup_pi_email(g["pi"], g["university"],
                                   core_map.get(g["core"], []), stats, verbose)

        found_new = 0
        with ThreadPoolExecutor(max_workers=EMAIL_WORKERS) as pool:
            futures = {pool.submit(work, g, i < 3): g for i, g in enumerate(todo)}
            for fut in as_completed(futures):
                g = futures[fut]
                key = f"{g['pi']}|{g['university']}".lower()
                try:
                    email, src = fut.result()
                except Exception as exc:
                    log(f"  lookup crashed for {g['pi']}: {exc}")
                    email, src = None, None
                stats["pis"] += 1
                if email is not None:  # None = transient error: not cached
                    cache[key] = {"email": email, "src": src,
                                  "checked": today.isoformat()}
                    if email:
                        for e in g["entries"]:
                            e["pi_email"] = email
                            e["pi_email_via"] = src
                        filled += len(g["entries"])
                        found_new += 1
                        changed = True
                if stats["pis"] % 25 == 0:
                    log(f"  progress: {stats['pis']}/{len(todo)} PIs, "
                        f"{found_new} new emails")
                if stats["pis"] % 50 == 0:
                    save_json(email_cache_path(), cache)
                    if changed:
                        save_json(DATA_FILE, data)

    save_json(email_cache_path(), cache)
    if changed:
        save_json(DATA_FILE, data)
    log(f"emails: found={filled}, none={max(pending - filled, 0)} "
        f"(PIs looked up: {stats['pis']}, API calls: {stats['net']}, "
        f"cache: {len(cache) - 1})")
    return data


# ----------------------------------------------------------------------------
# Publications: grant-linked paper (NIH) or the PI's last-5-years (OpenAlex)
# ----------------------------------------------------------------------------

OA_LIMIT = _RateLimiter(OPENALEX_RATE)


class OpenAlexBudget(Exception):
    """Raised on HTTP 429: the daily budget (or rate) is exhausted."""


def pubs_cache_path() -> Path:
    return DATA_DIR / "pubs_cache.json"


def linked_cache_path() -> Path:
    return DATA_DIR / "linked_pubs_cache.json"


def _slug(key: str) -> str:
    import hashlib
    return hashlib.sha1(key.lower().encode()).hexdigest()[:16]


def _openalex_get(path: str, params: dict):
    """Rate-limited GET against api.openalex.org. None on 404; raises
    OpenAlexBudget on 429 so the sweep stops cleanly for this run."""
    params = dict(params, api_key=OPENALEX_API_KEY)
    for attempt, pause in enumerate((0, 3.0)):
        if pause:
            time.sleep(pause)
        OA_LIMIT.wait()
        r = _session().get("https://api.openalex.org" + path, params=params,
                           timeout=TIMEOUT)
        if r.status_code == 429:
            raise OpenAlexBudget("OpenAlex returned 429 (daily budget or rate)")
        if r.status_code == 404:
            return None
        if r.status_code in (500, 502, 503, 504) and attempt == 0:
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("OpenAlex request failed")


# NIH abbreviates organization names; expand the common ones before matching.
_INST_ABBREV = {
    "univ": "university", "u": "university", "tx": "texas", "ca": "california",
    "ny": "new york", "pa": "pennsylvania", "hlth": "health", "med": "medical",
    "ctr": "center", "cntr": "center", "sci": "science", "scis": "sciences",
    "coll": "college", "inst": "institute", "sch": "school", "hosp": "hospital",
    "rsch": "research", "biomed": "biomedical", "cncr": "cancer", "can": "cancer",
    "tech": "technology", "natl": "national", "intl": "international",
}
_INST_GENERIC = {
    "university", "college", "medical", "medicine", "center", "centre",
    "school", "of", "the", "at", "and", "in", "for", "health", "sciences",
    "science", "institute", "hospital", "research", "state", "system",
    "campus", "inc", "llc", "foundation", "cancer", "national", "general",
    "graduate", "technology", "children", "childrens", "regents", "board",
}


def _inst_tokens(name: str):
    """Distinctive words of an institution name, abbreviations expanded."""
    out = []
    for w in re.split(r"[^A-Za-z]+", name.lower()):
        if not w:
            continue
        w = _INST_ABBREV.get(w, w)
        for part in w.split():
            if part not in _INST_GENERIC and len(part) >= 4:
                out.append(part)
    return out


def _inst_score(nih_name: str, candidate_institutions) -> int:
    """How many distinctive words of the NIH org name appear in any of the
    candidate's affiliation names (current or past)."""
    toks = _inst_tokens(nih_name)
    names = " | ".join((n or "").lower() for n in candidate_institutions)
    return sum(1 for tk in toks if tk in names)


def _norm_name(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z ]", "", s.lower()).strip()


def _name_matches(candidate_name: str, pi: str) -> bool:
    cand = _norm_name(candidate_name).split()
    if not cand:
        return False
    for first, last in _name_forms(pi):
        last_n = _norm_name(last).split()
        if not last_n:
            continue
        # surname (possibly compound) must end the candidate's name
        if cand[-len(last_n):] != last_n:
            continue
        fi = _norm_name(first)[:1]
        if not fi or cand[0][:1] == fi:
            return True
    return False


def _candidate_affiliations(c) -> list:
    names = []
    for a in c.get("affiliations") or []:
        inst = (a or {}).get("institution") or {}
        if inst.get("display_name"):
            names.append(inst["display_name"])
    for inst in c.get("last_known_institutions") or []:
        if (inst or {}).get("display_name"):
            names.append(inst["display_name"])
    return names


def _pick_author_profiles(pi: str, university: str, candidates):
    """Return (author_ids, match) where match is 'exact', 'merged',
    'uncertain' or 'none'. Name must match; institution history decides."""
    named = [c for c in candidates if _name_matches(c.get("display_name", ""), pi)]
    scored = [(c, _inst_score(university, _candidate_affiliations(c))) for c in named]
    matched = [c for c, s in scored if s > 0]
    if len(matched) == 1:
        return [matched[0]["id"]], "exact"
    if len(matched) > 1:
        return [c["id"] for c in matched], "merged"  # one person split in two
    if named:
        best = max(named, key=lambda c: c.get("works_count", 0))
        if best.get("works_count", 0) > 0:
            return [best["id"]], "uncertain"
    return [], "none"


def _oa_id(full: str) -> str:
    return (full or "").rsplit("/", 1)[-1]


def _work_link(w) -> str:
    doi = w.get("doi") or ""
    if doi:
        return doi if doi.startswith("http") else f"https://doi.org/{doi}"
    loc = (w.get("primary_location") or {}) or {}
    return loc.get("landing_page_url") or ""


def _position_for(w, author_ids=(), pi: str = "") -> str:
    """first / middle / last for our author on this work (by id, else by name)."""
    ids = {_oa_id(a) for a in author_ids}
    for au in w.get("authorships") or []:
        aid = _oa_id(((au.get("author") or {}).get("id")) or "")
        if ids and aid in ids:
            return au.get("author_position") or "middle"
    if pi:
        for au in w.get("authorships") or []:
            nm = ((au.get("author") or {}).get("display_name")
                  or au.get("raw_author_name") or "")
            if _name_matches(nm, pi):
                return au.get("author_position") or "middle"
    return "unknown"


def _pmid_from_ids(w) -> str:
    """OpenAlex lists PubMed ids as 'https://pubmed.ncbi.nlm.nih.gov/12345'."""
    v = ((w.get("ids") or {}).get("pmid")) or ""
    m = re.search(r"(\d+)\s*$", str(v))
    return m.group(1) if m else ""


def _doi_from_url(url: str) -> str:
    m = re.search(r"doi\.org/(10\.\S+)", url or "", re.I)
    return m.group(1).strip().rstrip("/").lower() if m else ""


def _works_to_rows(works, author_ids, pi: str = ""):
    rows, seen = [], set()
    for w in works:
        key = (w.get("doi") or w.get("id") or "").lower()
        if key in seen:
            continue
        seen.add(key)
        rows.append({
            "year": w.get("publication_year") or 0,
            "position": _position_for(w, author_ids, pi),
            "title": (w.get("title") or "").strip() or "(untitled)",
            "url": _work_link(w),
            "pmid": _pmid_from_ids(w),
        })
    rows.sort(key=lambda r: r["year"], reverse=True)
    return rows[:MAX_PUBS_PER_PI]


def _openalex_profile_and_works(pi: str, university: str):
    """(match, author_ids, rows) for a PI's journal articles in the window."""
    q = pi
    data = _openalex_get("/authors", {
        "search": q, "per_page": 25,
        "select": "id,display_name,display_name_alternatives,works_count,"
                  "affiliations,last_known_institutions",
    })
    candidates = (data or {}).get("results") or []
    ids, match = _pick_author_profiles(pi, university, candidates)
    if not ids:
        return "none", [], []
    since = (date.today() - timedelta(days=365 * PUBS_YEARS)).isoformat()
    filt = (f"author.id:{'|'.join(_oa_id(i) for i in ids)},"
            f"type:article,from_publication_date:{since}")
    works, cursor = [], "*"
    for _ in range(3):  # up to 300 works
        page = _openalex_get("/works", {
            "filter": filt, "per_page": 100, "cursor": cursor,
            "sort": "publication_year:desc",
            "select": "id,title,publication_year,doi,authorships,primary_location,ids",
        })
        results = (page or {}).get("results") or []
        works.extend(results)
        cursor = ((page or {}).get("meta") or {}).get("next_cursor")
        if not cursor or len(results) < 100:
            break
    return match, ids, _works_to_rows(works, ids, pi)


def _linked_pub_details(pmid: str, pi: str):
    """Title/year/link/position for a grant-linked PMID: OpenAlex first
    (free single-record lookup), PubMed for title/year if OpenAlex lacks it."""
    if OPENALEX_API_KEY:
        try:
            w = _openalex_get(f"/works/pmid:{pmid}", {
                "select": "id,title,publication_year,doi,authorships,primary_location"})
        except OpenAlexBudget:
            raise
        except Exception:
            w = None
        if w and w.get("title"):
            return {"pmid": pmid, "title": w["title"].strip(),
                    "year": w.get("publication_year") or 0,
                    "url": _work_link(w), "position": _position_for(w, (), pi)}
    # PubMed fallback: title + year only, no link (per spec)
    try:
        import xml.etree.ElementTree as ET
        base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
        common = {"db": "pubmed", "tool": "biotech-funding-tracker"}
        if NCBI_API_KEY:
            common["api_key"] = NCBI_API_KEY
        r = _ncbi_get(base + "efetch.fcgi", dict(common, id=pmid, retmode="xml"))
        root = ET.fromstring(r.text)
        title = "".join((root.find(".//ArticleTitle") or ET.Element("x")).itertext()).strip()
        year_el = root.find(".//PubDate/Year") or root.find(".//PubDate/MedlineDate")
        year = int(re.search(r"\d{4}", (year_el.text if year_el is not None else "") or "0").group(0)) \
            if year_el is not None and re.search(r"\d{4}", year_el.text or "") else 0
        return {"pmid": pmid, "title": title or "(untitled)", "year": year,
                "url": "", "position": "unknown"}
    except Exception:
        return {"pmid": pmid, "title": f"PubMed {pmid}", "year": 0,
                "url": "", "position": "unknown"}


def _top_entries(data):
    """(entry, university) for the largest award in every non-empty cell."""
    for row in data["rows"].values():
        for entries in row["cells"].values():
            if entries:
                yield max(entries, key=lambda e: e["amount"]), row["university"]


def _stale(entry: dict, days: int) -> bool:
    try:
        return (date.today() - date.fromisoformat(entry.get("checked", "1970-01-01"))).days >= days
    except ValueError:
        return True


def sweep_publications(data):
    """Grant-linked paper per grant (NIH), else the PI's last-5-years list
    (OpenAlex). Cached; writes one small JSON per popup under docs/pubs."""
    if not OPENALEX_API_KEY:
        log("publications: OPENALEX_API_KEY not set - skipping this stage")
        return data
    pubs = load_json(pubs_cache_path(), {})
    linked = load_json(linked_cache_path(), {})
    today = date.today().isoformat()

    # --- 1. grant-linked papers (batched RePORTER, free OpenAlex lookups) ---
    core_pi = {}
    for e, uni in _top_entries(data):
        core = e.get("core") or ""
        if core and (core not in linked or (not linked[core].get("pmid")
                                            and _stale(linked[core], LINKED_RECHECK_DAYS))):
            core_pi.setdefault(core, e.get("pi", ""))
    if core_pi:
        pmid_map = _batch_grant_pmids(core_pi)
        got = 0
        try:
            for core, pi in core_pi.items():
                pmids = pmid_map.get(core) or []
                if pmids:
                    linked[core] = dict(_linked_pub_details(pmids[0], pi), checked=today)
                    got += 1
                else:
                    linked[core] = {"pmid": None, "checked": today}
        except OpenAlexBudget as exc:
            log(f"  {exc}; grant-linked details continue next run")
        save_json(linked_cache_path(), linked)
        log(f"grant-linked papers: {got} found among {len(core_pi)} grant(s) checked")

    # --- 2. OpenAlex lists for PIs whose grant has no linked paper ---
    todo, seen_keys = [], set()
    for e, uni in _top_entries(data):
        core = e.get("core") or ""
        if core and (linked.get(core) or {}).get("pmid"):
            continue  # linked paper wins; no list needed
        if not e.get("pi"):
            continue
        key = f"{e['pi']}|{uni}".lower()
        if key in seen_keys:
            continue
        seen_keys.add(key)
        ent = pubs.get(key)
        if ent and not _stale(ent, PUBS_REFRESH_DAYS):
            continue
        todo.append((key, e["pi"], uni))
    deferred = max(len(todo) - MAX_PUBS_LOOKUPS_PER_RUN, 0)
    todo = todo[:MAX_PUBS_LOOKUPS_PER_RUN]
    log(f"publications: {len(todo)} PI(s) to look up on OpenAlex"
        + (f", {deferred} deferred to next run" if deferred else ""))
    if todo:
        stopped = threading.Event()
        counts = {"done": 0, "yes": 0, "no": 0, "uncertain": 0}

        def work(key, pi, uni):
            if stopped.is_set():
                return key, None
            try:
                return key, _openalex_profile_and_works(pi, uni)
            except OpenAlexBudget:
                stopped.set()
                return key, None
            except Exception as exc:
                log(f"  OpenAlex lookup failed for {pi}: {exc}")
                return key, None

        with ThreadPoolExecutor(max_workers=OPENALEX_WORKERS) as pool:
            futures = [pool.submit(work, *item) for item in todo]
            for fut in as_completed(futures):
                key, res = fut.result()
                if res is None:
                    continue  # error or budget: not cached, retried next run
                match, ids, rows = res
                pubs[key] = {"match": match, "author_ids": ids, "rows": rows,
                             "checked": today}
                counts["done"] += 1
                counts["yes" if rows else "no"] += 1
                if match == "uncertain":
                    counts["uncertain"] += 1
                if counts["done"] % 50 == 0:
                    log(f"  progress: {counts['done']}/{len(todo)} PIs")
                    save_json(pubs_cache_path(), pubs)
        if stopped.is_set():
            log("OpenAlex daily budget reached; remaining PIs continue next run")
        log(f"publications: {counts['done']} looked up - {counts['yes']} with "
            f"recent papers, {counts['no']} without, {counts['uncertain']} uncertain match")
    save_json(pubs_cache_path(), pubs)
    try:
        sweep_abstract_flags(pubs, linked)
    except Exception as exc:
        log(f"abstract flag sweep failed: {exc}")
    return data


MAX_ABSTRACT_FLAG_PIS_PER_RUN = 800


def _parse_pubmed_flags(xml_text: str) -> dict:
    """{pmid: {"doi": lowercase doi or "", "abs": bool}} from efetch XML.
    Only presence is recorded - abstract text is never stored."""
    import xml.etree.ElementTree as ET
    out = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out
    for art in root.iter("PubmedArticle"):
        pmid = (art.findtext(".//MedlineCitation/PMID") or "").strip()
        if not pmid:
            continue
        doi = ""
        for aid in art.iter("ArticleId"):
            if (aid.get("IdType") or "").lower() == "doi" and aid.text:
                doi = aid.text.strip().lower()
                break
        has_abs = any((el.text or "").strip() or list(el)
                      for el in art.iter("AbstractText"))
        out[pmid] = {"doi": doi, "abs": bool(has_abs)}
    return out


def _pubmed_flags_for(pmids, dois, stats) -> dict:
    """Resolve DOIs to PMIDs (batched esearch) and check abstract presence
    (batched efetch). Returns {pmid: {...}} plus {doi: pmid} under '_bydoi'."""
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    common = {"db": "pubmed", "tool": "biotech-funding-tracker"}
    if NCBI_API_KEY:
        common["api_key"] = NCBI_API_KEY
    found = set(p for p in pmids if p)
    dois = [d for d in dict.fromkeys(dois) if d]
    for i in range(0, len(dois), 50):
        chunk = dois[i:i + 50]
        term = " OR ".join(f'"{d}"[DOI]' for d in chunk)
        _bump(stats)
        r = _ncbi_post(base + "esearch.fcgi",
                       dict(common, term=term, retmax="200", retmode="json"))
        ids = ((r.json().get("esearchresult") or {}).get("idlist")) or []
        found.update(ids)
    result, bydoi = {}, {}
    ids = sorted(found)
    for i in range(0, len(ids), 200):
        chunk = ids[i:i + 200]
        _bump(stats)
        r = _ncbi_post(base + "efetch.fcgi",
                       dict(common, id=",".join(chunk), retmode="xml"))
        parsed = _parse_pubmed_flags(r.text)
        result.update(parsed)
        for pm, info in parsed.items():
            if info["doi"]:
                bydoi[info["doi"]] = pm
    result["_bydoi"] = bydoi
    return result


def sweep_abstract_flags(pubs: dict, linked: dict) -> None:
    """Mark every publication row / linked paper with whether PubMed has an
    abstract for it (row['abs']). Text is fetched by the browser on click,
    never stored. Works in place on cached lists, so no OpenAlex re-pull."""
    stats = {"net": 0}
    todo = [k for k, v in pubs.items()
            if not k.startswith("_") and isinstance(v, dict)
            and any("abs" not in r for r in v.get("rows") or [])]
    todo = todo[:MAX_ABSTRACT_FLAG_PIS_PER_RUN]
    linked_todo = [c for c, v in linked.items()
                   if isinstance(v, dict) and v.get("pmid") and "abs" not in v]
    if not todo and not linked_todo:
        return
    log(f"abstract flags: {len(todo)} PI list(s) + {len(linked_todo)} linked paper(s) to check")
    done = 0
    for key in todo:
        rows = pubs[key].get("rows") or []
        pending = [r for r in rows if "abs" not in r]
        pmids = [r.get("pmid") for r in pending if r.get("pmid")]
        dois = [_doi_from_url(r.get("url", "")) for r in pending if not r.get("pmid")]
        try:
            info = _pubmed_flags_for(pmids, dois, stats)
        except Exception as exc:
            log(f"  abstract flags failed for {key.split('|')[0]}: {exc}")
            continue  # left unmarked: retried next run
        bydoi = info.get("_bydoi", {})
        for r in pending:
            pm = r.get("pmid") or bydoi.get(_doi_from_url(r.get("url", "")), "")
            r["pmid"] = pm
            r["abs"] = bool(pm and info.get(pm, {}).get("abs"))
        done += 1
        if done % 100 == 0:
            log(f"  progress: {done}/{len(todo)} PI lists")
            save_json(pubs_cache_path(), pubs)
    if linked_todo:
        try:
            info = _pubmed_flags_for([str(linked[c]["pmid"]) for c in linked_todo], [], stats)
            for c in linked_todo:
                linked[c]["abs"] = bool(info.get(str(linked[c]["pmid"]), {}).get("abs"))
        except Exception as exc:
            log(f"  abstract flags failed for linked papers: {exc}")
    save_json(pubs_cache_path(), pubs)
    save_json(linked_cache_path(), linked)
    yes = sum(1 for v in pubs.values() if isinstance(v, dict)
              for r in v.get("rows") or [] if r.get("abs"))
    log(f"abstract flags: done ({done} lists this run, {stats['net']} PubMed calls, "
        f"{yes} papers with abstracts overall)")


def _pub_status(entry, university, pubs, linked):
    """('linked'|'recent'|'none'|'pending', slug-or-None, match)"""
    core = entry.get("core") or ""
    lk = linked.get(core) if core else None
    if lk and lk.get("pmid"):
        return "linked", _slug(f"linked|{core}"), "exact"
    key = f"{entry.get('pi', '')}|{university}".lower()
    pc = pubs.get(key)
    if pc is None:
        return "pending", None, ""
    if pc.get("rows"):
        return "recent", _slug(key), pc.get("match", "")
    return "none", None, pc.get("match", "")


def write_pub_files(data, pubs, linked) -> int:
    """One small JSON per popup under docs/pubs/. Returns the count."""
    out_dir = DOCS_DIR / "pubs"
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    written = set()
    for e, uni in _top_entries(data):
        kind, slug, match = _pub_status(e, uni, pubs, linked)
        if not slug or slug in written:
            continue
        written.add(slug)
        if kind == "linked":
            lk = linked[e["core"]]
            payload = {"pi": e.get("pi", ""), "university": uni, "linked": True,
                       "match": "exact",
                       "rows": [{"year": lk.get("year", 0),
                                 "position": lk.get("position", "unknown"),
                                 "title": lk.get("title", ""),
                                 "url": lk.get("url", ""),
                                 "pmid": str(lk.get("pmid") or ""),
                                 "abs": bool(lk.get("abs"))}]}
        else:
            pc = pubs[f"{e.get('pi', '')}|{uni}".lower()]
            payload = {"pi": e.get("pi", ""), "university": uni, "linked": False,
                       "match": pc.get("match", ""), "rows": pc.get("rows", [])}
        (out_dir / f"{slug}.json").write_text(json.dumps(payload))
        n += 1
    return n


# ----------------------------------------------------------------------------
# NIH fetch: awards NOTICED during the target week
# ----------------------------------------------------------------------------

def make_award(key, university, department, amount, url, funder, pi,
               pi_email="", core="", notice=""):
    return {
        "key": key, "university": university, "department": department,
        "funder": funder, "amount": int(amount), "url": url, "pi": pi,
        "pi_email": pi_email, "core": core, "notice": notice,
    }


_DIAG_LOGGED = [False]


def fetch_nih_week(week_start: date, week_end: date):
    """(awards, pending): the week's matching awards, and how many otherwise
    matching awards NIH lists without a dollar amount yet."""
    awards, offset, pending = [], 0, set()
    while offset <= 9500:
        payload = {
            "criteria": {
                "award_notice_date": {"from_date": week_start.isoformat(),
                                      "to_date": week_end.isoformat()},
                "org_countries": ["UNITED STATES"],
                "exclude_subprojects": True,
            },
            "include_fields": [
                "ApplId", "ProjectNum", "ProjectTitle", "AwardAmount",
                "Organization", "AwardNoticeDate", "ProjectDetailUrl",
                "PrefTerms", "AgencyIcAdmin", "ContactPiName",
                "PrincipalInvestigators", "CoreProjectNum",
            ],
            "limit": 500,
            "offset": offset,
        }
        # every week is re-checked now, so ride out a brief NIH hiccup instead of
        # failing the whole run (a failed run changes nothing; it never counts as quiet)
        for attempt in range(3):
            try:
                r = SESSION.post("https://api.reporter.nih.gov/v2/projects/search",
                                 json=payload, timeout=TIMEOUT)
                r.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == 2:
                    raise
                log(f"  NIH search failed ({e}); retrying")
                time.sleep(10 * (attempt + 1))
        results = r.json().get("results", [])
        if offset == 0 and results and not _DIAG_LOGGED[0]:
            # One-time diagnostic: does the public API carry PI emails at all?
            _DIAG_LOGGED[0] = True
            p0 = results[0]
            pis0 = p0.get("principal_investigators") or []
            log(f"  NIH PI object keys: "
                f"{sorted(pis0[0].keys()) if pis0 else 'no PI array returned'}")
            found = EMAIL_RE.findall(json.dumps(p0))
            log(f"  emails inside the API record: {found[:2] if found else 'NONE'}")
        for p in results:
            amt = p.get("award_amount") or 0
            org = p.get("organization") or {}
            dept = (org.get("dept_type") or "").strip().upper()
            name = (org.get("org_name") or "").strip()
            text = (p.get("project_title") or "") + " " + (p.get("pref_terms") or "")
            if not name:
                continue
            if dept in NIH_INCLUDE_DEPTS:
                if dept == "ENGINEERING (ALL TYPES)" and not has_bio_keyword(text):
                    continue
                dept_label = nice_name(dept)
            elif (dept in ("", "NONE", "NO CODE ASSIGNED")
                  and has_bio_keyword(text)
                  and looks_like_university(name)):
                dept_label = NO_DEPT_LABEL  # NIH reported no department
            else:
                continue  # named clinical dept, company, or off-topic
            appl = p.get("appl_id")
            if amt < 1:
                pending.add(appl)  # matches, but NIH hasn't posted a dollar amount yet
                continue
            ic = (p.get("agency_ic_admin") or {}).get("abbreviation") or ""
            funder = (ic if ic in NON_NIH_AGENCIES
                      else f"NIH ({ic})" if ic else "NIH")
            url = p.get("project_detail_url") or \
                f"https://reporter.nih.gov/project-details/{appl}"
            # If the API itself carries an email, take it - free and exact.
            api_email = ""
            pis = p.get("principal_investigators") or []
            for pi_obj in pis:
                if not isinstance(pi_obj, dict):
                    continue
                cand = _walk_for_email(pi_obj)
                if cand and (pi_obj.get("is_contact_pi") or not api_email):
                    api_email = cand
                    if pi_obj.get("is_contact_pi"):
                        break
            awards.append(make_award(
                key=f"NIH:{appl}", university=nice_name(name),
                department=dept_label, amount=amt, url=url, funder=funder,
                pi=flip_name(p.get("contact_pi_name") or ""),
                pi_email=api_email, core=(p.get("core_project_num") or ""),
                notice=(p.get("award_notice_date") or "")[:10],
            ))
        if len(results) < 500:
            break
        offset += 500
        time.sleep(1)
    return awards, len(pending)


# ----------------------------------------------------------------------------
# LEDGER
# ----------------------------------------------------------------------------

def row_key(a) -> str:
    return f"{a['university'].lower()}||{a['department'].lower()}||{a['funder'].lower()}"


def history_path() -> Path:
    return DATA_DIR / "added_history.json"


def _appl_id(entry) -> str:
    key = entry.get("key") or ""
    if key.startswith("NIH:"):
        return key[4:]
    m = re.search(r"(\d+)\s*$", entry.get("url", ""))
    return m.group(1) if m else ""


def apply_added_history(data) -> None:
    """Awards recorded before 'added' dates were kept get them here, from
    data/added_history.json (rebuilt from the repo's commit history): the day
    each week was first checked, and the awards that arrived after that."""
    hist = load_json(history_path(), {})
    first = hist.get("week_first_checked") or {}
    late = {str(i): d for d, ids in (hist.get("late") or {}).items() for i in ids}
    weeks = data.setdefault("meta", {}).setdefault("weeks", {})
    for w in data.get("dates", []):
        if w not in weeks and w in first:
            weeks[w] = {"first_checked": first[w], "quiet": 0}
    for row in data["rows"].values():
        for w, entries in row["cells"].items():
            for e in entries:
                if "added" in e:
                    continue
                appl = _appl_id(e)
                if appl in late:
                    e["added"], e["late"] = late[appl], True
                else:
                    e["added"] = (weeks.get(w) or {}).get("first_checked", "")
                    e["late"] = False


def nih_emails_path() -> Path:
    return DATA_DIR / "nih_emails.csv"


def load_nih_emails() -> dict:
    """{RePORTER appl id: email} from data/nih_emails.csv - the lines the email
    helper copies (appl_id,email,pi,date). A header, blank lines and anything
    that isn't an id plus a valid email are skipped; a later line for the same
    project wins. Emails typed into data/nih_links.csv are added too."""
    import csv
    path = nih_emails_path()
    out = {}
    if path.exists():
        for rec in csv.reader(path.read_text(encoding="utf-8").splitlines()):
            if len(rec) >= 2:
                appl, email = rec[0].strip(), rec[1].strip()
                if appl.isdigit() and EMAIL_RE.fullmatch(email):
                    out[appl] = email
    # emails typed by hand into the links list's nih_email column
    for appl, note in read_nih_links_notes().items():
        if EMAIL_RE.fullmatch(note):
            out[appl] = note
    return out


def nih_links_path() -> Path:
    return DATA_DIR / "nih_links.csv"


def nih_links_md_path() -> Path:
    return DATA_DIR / "nih_links.md"


NIH_LINKS_URL = "https://github.com/BrettS600/funding-tracker/blob/main/data/nih_links.md"
NIH_LINKS_EDIT_URL = "https://github.com/BrettS600/funding-tracker/edit/main/data/nih_links.csv"
NIH_LINKS_HEADER = ["appl_id", "pi", "university", "department", "funder",
                    "notice_date", "amount", "nih_link", "nih_email"]


def read_nih_links_notes() -> dict:
    """{appl id: whatever is typed in the nih_email column} from data/nih_links.csv,
    kept exactly as typed (an email, or a note like 'not available')."""
    import csv
    path = nih_links_path()
    out = {}
    if not path.exists():
        return out
    for rec in csv.DictReader(path.read_text(encoding="utf-8").splitlines()):
        appl = (rec.get("appl_id") or "").strip()
        note = (rec.get("nih_email") or "").strip()
        if appl.isdigit() and note:
            out[appl] = note
    return out


def write_nih_links(data) -> int:
    """Rebuilds data/nih_links.csv: one row per PI (their newest award), newest first,
    with the link that opens NIH's project page at the View Email button. Anything
    already typed in a PI's nih_email column is kept, even when their newest award
    changes; otherwise the column shows an NIH email from data/nih_emails.csv.
    Run after apply_nih_emails. Returns the number of rows."""
    import csv
    import io
    notes = read_nih_links_notes()
    best = {}
    for row in data["rows"].values():
        for week, entries in row["cells"].items():
            for e in entries:
                appl, pi = _appl_id(e), (e.get("pi") or "").strip()
                if not appl or not pi:
                    continue
                key = f"{pi}|{row['university']}".lower()
                g = best.setdefault(key, {"note": "", "nih": ""})
                g["note"] = g["note"] or notes.get(appl, "")
                g["nih"] = g["nih"] or e.get("nih_email", "")
                rank = (e.get("notice") or week, int(appl))
                if rank > g.get("rank", ("", 0)):
                    g.update(rank=rank, appl=appl, e=e, row=row)
    table = []
    for g in sorted(best.values(), key=lambda g: (g["rank"][0], g["e"]["pi"]), reverse=True):
        e, row = g["e"], g["row"]
        table.append([g["appl"], e["pi"], row["university"], row["department"], row["funder"],
                      g["rank"][0], e.get("label") or fmt_money(e["amount"]),
                      f"https://reporter.nih.gov/project-details/{g['appl']}#details",
                      g["note"] or g["nih"]])
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(NIH_LINKS_HEADER)
    w.writerows(table)
    nih_links_path().parent.mkdir(parents=True, exist_ok=True)
    nih_links_path().write_text(out.getvalue(), encoding="utf-8")

    # the same list as a page GitHub shows with clickable links (its CSV view
    # shows links as plain text)
    def md(v):
        # \$ stops GitHub reading dollar amounts as math
        return (str(v).replace("\\", "\\\\").replace("|", "\\|").replace("$", "\\$")
                .replace("<", "&lt;").strip())

    done = sum(1 for r in table if r[8])
    lines = [
        "# NIH View Email links, one per PI",
        "",
        f"Rebuilt every run from the tracker: {len(table):,} PIs, newest award first. "
        f"Email recorded for {done:,}.",
        "",
        "- **View email** opens the PI's NIH project page at the View Email button "
        "(Ctrl-click or Cmd-click opens it in a new tab). With the Chrome email helper "
        "installed, the button is pressed for you.",
        f"- Record an email in [nih_links.csv]({NIH_LINKS_EDIT_URL}): row **#N** here is "
        "line **N+1** in the editor. Type the email at the end of that line (after the "
        "last comma), then Commit changes. The tracker shows it with the NIH tag on its "
        "next update, and every run keeps what you typed.",
        "",
        "| # | PI | University | Notice date | Amount | NIH | Email recorded |",
        "|---:|---|---|---|---:|---|---|",
    ]
    for i, r in enumerate(table, start=1):
        lines.append(f"| {i} | {md(r[1])} | {md(r[2])} | {r[5]} | {md(r[6])} | "
                     f"[View email]({r[7]}) | {md(r[8])} |")
    nih_links_md_path().write_text("\n".join(lines) + "\n", encoding="utf-8")
    return len(best)


def apply_nih_emails(data) -> int:
    """Puts each NIH email on its award and on the same PI's other awards at
    the same university (entry["nih_email"], used ahead of paper-found
    emails). Recomputed on every render, so removing a line removes it.
    Returns the number of awards with one."""
    nih = load_nih_emails()
    by_pi = {}
    for row in data["rows"].values():
        for entries in row["cells"].values():
            for e in entries:
                e.pop("nih_email", None)
                email = nih.get(_appl_id(e))
                if email:
                    e["nih_email"] = email
                    if e.get("pi"):
                        by_pi[f"{e['pi']}|{row['university']}".lower()] = email
    n = 0
    for row in data["rows"].values():
        for entries in row["cells"].values():
            for e in entries:
                if "nih_email" not in e and e.get("pi"):
                    email = by_pi.get(f"{e['pi']}|{row['university']}".lower())
                    if email:
                        e["nih_email"] = email
                n += "nih_email" in e
    return n


def ingest(awards, week_start_iso: str, today: date | None = None):
    today = today or date.today()
    data = load_json(DATA_FILE, {"dates": [], "rows": {}, "meta": {}})
    seen = load_json(SEEN_FILE, {})

    # Automatic fresh start when the ledger predates the weekly NIH-only
    # format (old daily columns can't be re-bucketed into award-date weeks).
    meta = data.setdefault("meta", {})
    if meta.get("schema") != SCHEMA:
        if data.get("rows"):
            log("ledger schema changed -> starting the weekly NIH-only "
                "ledger fresh (old daily data cleared automatically)")
        data = {"dates": [], "rows": {}, "meta": {"schema": SCHEMA}}
        seen = {}
    apply_added_history(data)

    if week_start_iso not in data["dates"]:
        data["dates"].append(week_start_iso)
        data["dates"].sort()
    # an award is "late" when it turns up after its week was first checked
    weeks = data["meta"].setdefault("weeks", {})
    if week_start_iso not in weeks:
        weeks[week_start_iso] = {"first_checked": today.isoformat(), "quiet": 0}
    late = weeks[week_start_iso]["first_checked"] < today.isoformat()

    # awards recorded before notice dates were kept get theirs on the next check
    by_appl = {_appl_id(e): e for row in data["rows"].values()
               for e in row["cells"].get(week_start_iso, [])}

    new_count = 0
    for a in awards:
        if a["key"] in seen:
            e = by_appl.get(a["key"][4:])
            if e is not None and a.get("notice") and not e.get("notice"):
                e["notice"] = a["notice"]
            continue
        seen[a["key"]] = {"week": week_start_iso, "amount": a["amount"]}
        row = data["rows"].setdefault(row_key(a), {
            "university": a["university"],
            "department": a["department"],
            "funder": a["funder"],
            "cells": {},
        })
        row["cells"].setdefault(week_start_iso, []).append({
            "amount": a["amount"], "label": fmt_money(a["amount"]),
            "url": a["url"], "pi": a["pi"], "pi_email": a["pi_email"],
            "core": a.get("core", ""), "key": a["key"],
            "notice": a.get("notice", ""), "added": today.isoformat(), "late": late,
        })
        new_count += 1

    save_json(DATA_FILE, data)
    save_json(SEEN_FILE, seen)
    log(f"ingested {new_count} new award(s); ledger now has "
        f"{len(data['rows'])} rows x {len(data['dates'])} week(s)")
    return data, new_count


# ----------------------------------------------------------------------------
# HTML RENDER
# ----------------------------------------------------------------------------

def _cell_html(entries, institution="", pubs=None, linked=None):
    if not entries:
        return '<td class="zero" data-v="0">0</td>'
    total = sum(e["amount"] for e in entries)

    def aline(e):
        notice, late = e.get("notice") or "", bool(e.get("late"))
        meta = []
        if notice:
            meta.append(f"Award Notice Date: {short_date(notice)}")
        if late and e.get("added"):
            meta.append(f'<span class="late" title="NIH posted this award after its week '
                        f'was first checked">Added: {short_date(e["added"])}</span>')
        sub = f'<div class="ameta">{" ".join(meta)}</div>' if meta else ""
        return (f'<div class="aline" data-a="{e["amount"]}" data-n="{html_lib.escape(notice)}" '
                f'data-late="{1 if late else 0}"><a href="{html_lib.escape(e["url"], quote=True)}" '
                f'target="_blank" rel="noopener">{html_lib.escape(e["label"])}</a>{sub}</div>')

    lines = "".join(aline(e) for e in entries)
    if len(entries) > 1:
        lines += f'<div class="tot">= {fmt_money(total)}</div>'
    top = max(entries, key=lambda e: e["amount"])
    pi_name = html_lib.escape(top.get("pi") or "--")
    nih_email = (top.get("nih_email") or "").strip()
    email = nih_email or (top.get("pi_email") or "").strip()
    if email:
        tag = ('<span class="esrc nih" title="Revealed on NIH&rsquo;s project page (View Email)">NIH</span>'
               if nih_email else
               '<span class="esrc paper" title="Found in a published paper, not from NIH: it may '
               'belong to someone with a similar name">from a paper</span>')
        pi_mail = (f'<a href="mailto:{html_lib.escape(email, quote=True)}">'
                   f"{html_lib.escape(email)}</a> {tag}")
    else:
        pi_mail = "--"
    appl = _appl_id(top)
    if appl and not nih_email:
        # opens the PI's NIH page at the View Email button (the email helper presses it)
        pi_mail += (f' <a class="nihlink" href="https://reporter.nih.gov/project-details/{appl}#details" '
                    f'target="_blank" rel="noopener">View email on NIH</a>')
    inst = html_lib.escape(institution.strip() or "--")
    kind, slug, match = _pub_status(top, institution, pubs or {}, linked or {})
    if kind == "linked":
        pub_line = (f'<a class="publink" data-pub="{slug}" href="#">'
                    f'Publication Linked To Grant</a>')
    elif kind == "recent":
        pub_line = (f'<a class="publink" data-pub="{slug}" href="#">'
                    f'Publications Within The Past 5 Years? Yes</a>'
                    + (' <span class="unc" title="name match uncertain - check '
                       'affiliations in the popup">?</span>' if match == "uncertain" else ""))
    elif kind == "none":
        pub_line = "Publications Within The Past 5 Years? No"
    else:
        pub_line = '<span class="pend">Publications: not checked yet</span>'
    # one line each; cards hide the Institution line because the card's title names it
    lines += (f'<div class="pib"><div>PI Name: {pi_name}</div><div>PI Email: {pi_mail}</div>'
              f'<div class="pinst">Institution: {inst}</div><div>{pub_line}</div></div>')
    # per-cell flags: the checkboxes and the count apply to the selected week
    flags = (f' data-he="{1 if email else 0}" data-nih="{1 if nih_email else 0}"'
             f' data-hn="{1 if (top.get("pi") or "").strip() else 0}"'
             f' data-hi="{1 if institution.strip() else 0}"'
             f' data-pl="{1 if kind == "linked" else 0}"'
             f' data-pr="{1 if kind == "recent" else 0}"')
    return f'<td data-v="{total}"{flags}>{lines}</td>'


POPUP_CSS = """
.publink { color:var(--grow); border-bottom:1px solid #BFE0CC; cursor:pointer; }
.pend { color:var(--mut); }
.unc { color:var(--caution, #A5670A); font-weight:600; cursor:help; }
#pubmodal { position:fixed; inset:0; background:rgba(16,20,24,.45); z-index:50;
  display:flex; align-items:center; justify-content:center; padding:24px; }
#pubmodal[hidden] { display:none; }
.pubbox { background:#fff; color:var(--ink); border-radius:10px; width:min(1040px,100%);
  max-height:90vh; display:flex; flex-direction:column; box-shadow:0 20px 60px rgba(0,0,0,.25); }
.pubhead { display:flex; justify-content:space-between; align-items:flex-start;
  gap:16px; padding:18px 22px 10px; border-bottom:1px solid var(--line); }
.pubhead h2 { margin:0 0 4px; font-size:18px; font-weight:600; }
.pubhead .sub { font-size:13px; color:var(--mut); }
.pubhead .sub b { color:var(--ink); font-weight:600; }
.pubclose { border:0; background:none; font-size:22px; line-height:1; cursor:pointer;
  color:var(--mut); padding:2px 6px; }
.pubclose:hover { color:var(--ink); }
.pubscroll { overflow:auto; padding:0 22px 18px; }
#pubtable { table-layout:fixed; border-collapse:separate; border-spacing:0;
  width:100%; min-width:640px; }
#pubtable th, #pubtable td { padding:7px 10px; border-bottom:1px solid var(--grid);
  border-right:1px solid var(--grid); text-align:left; font-size:13.5px;
  vertical-align:top; white-space:normal; overflow-wrap:anywhere;
  position:static; }
#pubtable th:last-child, #pubtable td:last-child { border-right:0; }
#pubtable th { position:sticky; top:0; background:#fff; z-index:2; font-weight:600;
  user-select:none; white-space:nowrap; border-bottom:2px solid var(--gridstrong); }
#pubtable th.sortable { cursor:pointer; }
#pubtable th.sortable .thwrap::after { content:"⇅"; margin-left:6px;
  font-size:11px; color:var(--mut); }
#pubtable th.sorted.asc .thwrap::after { content:"▲"; color:var(--grow); }
#pubtable th.sorted.desc .thwrap::after { content:"▼"; color:var(--grow); }
#pubtable th.sorted.c-first .thwrap::after { content:"■"; color:#A32D2D; }
#pubtable th.sorted.c-middle .thwrap::after { content:"■"; color:#8A6A00; }
#pubtable th.sorted.c-last .thwrap::after { content:"■"; color:#1B7A43; }
#pubtable td.num { color:var(--mut); text-align:right; }
#pubtable td.year { font-variant-numeric:tabular-nums; }
#pubtable td.pos-first { background:#F9DAD8; color:#7A1F1F; }
#pubtable td.pos-middle { background:#FBEFC4; color:#5C4A00; }
#pubtable td.pos-last { background:#DCEFE2; color:#124D2B; }
#pubtable td.pos-unknown { background:#F1F2F4; color:#5A6068; }
.rz { position:absolute; top:0; right:0; width:7px; height:100%; cursor:col-resize; }
#pubtable th { position:sticky; }
#pubtable th .thwrap { position:relative; display:block; padding-right:8px; }
.pubempty { padding:24px; color:var(--mut); }
#pubtable td.absc a { color:var(--grow); border-bottom:1px solid #BFE0CC; cursor:pointer; }
#pubtable td.absc.no { color:var(--mut); }
#absmodal { position:fixed; inset:0; background:rgba(16,20,24,.35); z-index:60;
  display:flex; align-items:center; justify-content:center; padding:24px; }
#absmodal[hidden] { display:none; }
.absbox { background:#fff; color:var(--ink); border-radius:10px; width:min(760px,100%);
  max-height:85vh; display:flex; flex-direction:column;
  box-shadow:0 20px 60px rgba(0,0,0,.3); border:2px solid #BFE0CC; }
.abshead { display:flex; justify-content:space-between; align-items:flex-start;
  gap:16px; padding:16px 20px 10px; border-bottom:1px solid var(--grid); }
.abshead h2 { margin:0 0 4px; font-size:16px; font-weight:600; line-height:1.35; }
.abshead .sub { font-size:12.5px; color:var(--mut); }
.absbody { overflow:auto; padding:14px 20px; font-size:14px; line-height:1.6; }
.absbody p { margin:0 0 10px; }
.absbody .lbl { font-weight:600; color:var(--ink); display:block; margin-top:8px; }
.absbody .wait, .absbody .err { color:var(--mut); }
.absfoot { padding:10px 20px 14px; border-top:1px solid var(--grid); font-size:12.5px;
  color:var(--mut); display:flex; justify-content:space-between; gap:12px; flex-wrap:wrap; }
"""

BASE_CSS = """
:root {
  --bg:#FBFBFD; --ink:#101418; --line:#CDD3DA; --mut:#5B636B;
  --grow:#1B7A43; --broth:#EAF6EF; --grid:#C3CAD2; --gridstrong:#8F98A3;
  --caution:#A5670A; --card:#D5DAE0; --soft:#E6E9ED;
}
* { box-sizing:border-box; }
[hidden] { display:none !important; }
body { margin:0; background:var(--bg); color:var(--ink);
  font:15px/1.45 "IBM Plex Sans", system-ui, sans-serif; }
a { color:var(--grow); text-decoration:none; border-bottom:1px solid #BFE0CC; }
a:hover { border-bottom-color:var(--grow); }
.mast { display:flex; flex-wrap:wrap; gap:24px; align-items:flex-end;
  justify-content:space-between; padding:28px 32px 18px; }
.mast h1 { margin:0; font-size:22px; font-weight:600; letter-spacing:-.01em; }
.mast .sub { color:var(--mut); font-size:13px; margin-top:4px; }
.todaybox { text-align:right; }
.todaybox .fig { font-size:28px; font-weight:600; color:var(--grow);
  font-variant-numeric:tabular-nums; }
.todaybox .cap { font-size:12.5px; color:var(--mut); }

/* toolbar: weeks menu, search, filters, sort, active-filter chips */
.toolbar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; padding:0 32px 18px; }
.tbtn { height:44px; display:inline-flex; align-items:center; gap:8px; padding:0 14px;
  border:1px solid var(--grid); border-radius:8px; background:#fff; font:inherit;
  font-size:14px; color:var(--ink); cursor:pointer; white-space:nowrap; }
.tbtn:hover { border-color:var(--gridstrong); }
.tbtn .lab { color:var(--mut); }
.tbtn svg { flex:none; }
.badge { min-width:20px; height:20px; padding:0 6px; border-radius:10px; background:var(--grow);
  color:#fff; font-size:12px; font-weight:600; display:inline-flex; align-items:center;
  justify-content:center; }
.search { height:44px; display:flex; align-items:center; gap:8px; padding:0 12px;
  border:1px solid var(--grid); border-radius:8px; background:#fff; width:340px; max-width:100%; }
.search:focus-within { border-color:var(--grow); outline:2px solid var(--broth); }
.search input { border:0; outline:0; background:transparent; font:inherit; font-size:14px;
  color:var(--ink); width:100%; }
.sortwrap select { border:0; outline:0; background:transparent; font:inherit; font-size:14px;
  color:var(--ink); cursor:pointer; }
.chips { display:flex; flex-wrap:wrap; gap:8px; }
.chip { display:inline-flex; align-items:center; gap:2px; height:36px; padding:0 2px 0 12px;
  border-radius:18px; background:var(--broth); color:#124D2B; font-size:13px; }
.chip button { width:32px; height:32px; border:0; border-radius:16px; background:transparent;
  color:#124D2B; display:inline-flex; align-items:center; justify-content:center;
  cursor:pointer; font:inherit; font-size:17px; line-height:1; }
.chip button:hover { background:#D3ECDD; }

/* weeks menu */
.menuwrap { position:relative; }
.menu { position:absolute; top:calc(100% + 6px); left:0; z-index:40; width:380px;
  max-width:calc(100vw - 28px); background:#fff; border:1px solid var(--grid);
  border-radius:10px; box-shadow:0 16px 40px rgba(16,20,24,.18);
  display:flex; flex-direction:column; }
.quick { display:flex; flex-wrap:wrap; gap:8px; padding:14px 14px 12px;
  border-bottom:1px solid var(--soft); }
.quick button { height:36px; padding:0 14px; border:1px solid var(--grid); border-radius:18px;
  background:#fff; font:inherit; font-size:13.5px; color:var(--ink); cursor:pointer; }
.quick button.on { border-color:var(--grow); background:var(--broth); color:#124D2B;
  font-weight:500; }
.wklist { display:flex; flex-direction:column; padding:6px 0; max-height:min(420px, 60vh);
  overflow:auto; }
.wk { display:flex; align-items:center; gap:12px; min-height:44px; padding:0 16px;
  cursor:pointer; font-size:14px; }
.wk:hover { background:#F3F7F4; }
.wk input, .fopt input { width:18px; height:18px; margin:0; accent-color:var(--grow);
  cursor:pointer; flex:none; }
.wk .wkl { flex:1; }
.wk .wkn { font-size:12.5px; color:var(--mut); font-variant-numeric:tabular-nums; }
.wk .wkn .ok { color:var(--grow); }
.range { display:flex; flex-direction:column; gap:8px; padding:12px 14px;
  border-bottom:1px solid var(--soft); }
.rangehead { font-size:12.5px; font-weight:600; letter-spacing:.04em; text-transform:uppercase;
  color:var(--mut); }
.rangerow { display:flex; align-items:center; gap:8px; }
.rangerow label { display:flex; flex-direction:column; gap:3px; flex:1; font-size:12.5px;
  color:var(--mut); }
.rangerow select { height:40px; padding:0 8px; border:1px solid var(--grid); border-radius:8px;
  background:#fff; font:inherit; font-size:14px; color:var(--ink); }
.rangerow select:focus { border-color:var(--grow); outline:2px solid var(--broth); }
.rangenote { font-size:12.5px; color:var(--mut); line-height:1.45; }
.menufoot { display:flex; justify-content:space-between; align-items:center; gap:12px;
  padding:10px 14px; border-top:1px solid var(--soft); font-size:12.5px; color:var(--mut); }
.primary { height:40px; padding:0 18px; border:0; border-radius:8px; background:var(--grow);
  color:#fff; font:inherit; font-size:14px; font-weight:500; cursor:pointer; }
.linkbtn { height:44px; padding:0 4px; border:0; background:transparent; font:inherit;
  font-size:14px; color:var(--grow); cursor:pointer; }
.iconbtn { width:44px; height:44px; border:0; border-radius:22px; background:transparent;
  display:inline-flex; align-items:center; justify-content:center; cursor:pointer;
  color:var(--mut); }
.iconbtn:hover { background:#F1F2F4; color:var(--ink); }

/* filters popup */
#filtermodal { position:fixed; inset:0; background:rgba(16,20,24,.45); z-index:50;
  display:flex; align-items:flex-start; justify-content:center; padding:84px 16px 16px; }
.fbox { background:#fff; color:var(--ink); border-radius:12px; width:min(520px,100%);
  max-height:calc(100vh - 100px); overflow:auto; box-shadow:0 20px 60px rgba(0,0,0,.25);
  display:flex; flex-direction:column; }
.fhead { display:flex; justify-content:space-between; align-items:flex-start; gap:16px;
  padding:18px 14px 12px 22px; border-bottom:1px solid var(--soft); }
.fhead h2 { margin:0; font-size:18px; font-weight:600; }
.fhead .sub { font-size:12.5px; color:var(--mut); }
.fbody { display:flex; flex-direction:column; padding:0 22px; }
.fswitch { display:flex; align-items:flex-start; justify-content:space-between; gap:20px;
  padding:16px 0; border-bottom:1px solid var(--soft); cursor:pointer; }
.fswitch .t { display:block; font-size:15px; font-weight:500; }
.fswitch .d { display:block; margin-top:3px; font-size:12.5px; color:var(--mut); line-height:1.45; }
.switch { position:relative; flex:none; width:44px; height:26px; margin-top:2px; }
.switch input { position:absolute; inset:0; width:44px; height:26px; margin:0; opacity:0;
  cursor:pointer; z-index:1; }
.switch .track { position:absolute; inset:0; border-radius:13px; background:#8F98A3; }
.switch .knob { position:absolute; top:3px; left:3px; width:20px; height:20px;
  border-radius:10px; background:#fff; }
.switch input:checked ~ .track { background:var(--grow); }
.switch input:checked ~ .knob { left:21px; }
.switch input:focus-visible ~ .track { outline:2px solid var(--grow); outline-offset:2px; }
.fgroup { display:flex; flex-direction:column; padding:14px 0 8px; border-bottom:1px solid var(--soft); }
.fgroup:last-child { border-bottom:0; padding-bottom:12px; }
.fgroup h3 { margin:0 0 2px; font-size:12.5px; font-weight:600; letter-spacing:.04em;
  text-transform:uppercase; color:var(--mut); }
.fopt { display:flex; align-items:center; gap:12px; min-height:44px; cursor:pointer; font-size:14.5px; }
.fhint { font-size:12.5px; color:var(--mut); }
.ffoot { display:flex; justify-content:space-between; align-items:center; gap:12px;
  padding:12px 22px 16px; border-top:1px solid var(--soft); }
.ffoot .saved { font-size:12.5px; color:var(--mut); }
.ffoot .right { display:flex; align-items:center; gap:14px; }

/* card view: each selected week is its own list of funded institutions */
.tabs { display:none; }
.cards { display:grid; grid-auto-flow:column; grid-auto-columns:minmax(280px, 1fr); gap:16px;
  padding:0 32px 32px; overflow-x:auto; align-items:start; }
.col { display:flex; flex-direction:column; gap:10px; min-width:0; }
.colhead { display:flex; flex-direction:column; gap:2px; padding:0 4px 10px;
  border-bottom:2px solid var(--grow); }
.colhead h2 { margin:0; font-size:15px; font-weight:600; }
.colhead span { font-size:12.5px; color:var(--mut); font-variant-numeric:tabular-nums; }
.colhead .wstat { color:var(--mut); }
.colhead .wstat.done { color:var(--grow); font-weight:500; }
.cbody .ameta { margin-top:1px; }
.lateline { margin-top:6px; font-size:13px; color:#7A4300; display:flex; flex-wrap:wrap;
  align-items:center; gap:4px 8px; }
.lateline .linkbtn { height:32px; color:#7A4300; text-decoration:underline; font-size:13px; }
.card { background:#fff; border:1px solid var(--card); border-radius:8px; padding:14px 16px;
  display:flex; flex-direction:column; gap:10px; }
.chead { display:flex; gap:10px; align-items:baseline; }
.cnum { font-size:12px; color:var(--mut); min-width:16px; font-variant-numeric:tabular-nums; }
.ctitle { display:flex; flex-direction:column; gap:2px; min-width:0; }
.ctitle h3 { margin:0; font-size:14.5px; font-weight:600; line-height:1.3; }
.cmeta { font-size:12.5px; color:var(--mut); }
.cbody { padding-left:26px; font-variant-numeric:tabular-nums; }
.cbody .aline a { font-size:15px; font-weight:500; }
.cbody .tot { font-size:15px; }
.cbody .pib { border-top:1px solid var(--soft); margin-top:10px; padding-top:10px;
  font-size:12.5px; overflow-wrap:anywhere; }
.cbody .pinst { display:none; }
.empty { border:1px dashed var(--grid); border-radius:8px; padding:18px 16px;
  display:flex; flex-direction:column; gap:4px; }
.empty b { font-size:14px; font-weight:500; }
.empty span { font-size:12.5px; color:var(--mut); line-height:1.45; }
.more { height:44px; border:1px solid var(--card); border-radius:8px; background:#fff;
  font:inherit; font-size:13.5px; color:var(--grow); cursor:pointer; }

/* table view (rows with no award shown): every rule is scoped to #t so none of it
   leaks into the popup table */
.wrap { overflow:auto; max-height:calc(100vh - 150px); border-top:1px solid var(--line); }
#t { border-collapse:separate; border-spacing:0; min-width:100%;
  font-variant-numeric:tabular-nums; }
#t th, #t td { padding:9px 14px; border-bottom:1px solid var(--line);
  white-space:nowrap; text-align:right; font-size:14px; vertical-align:top; }
.aline { display:block; line-height:1.6; }
.aline + .aline { margin-top:4px; }
.ameta { font-size:12px; line-height:1.4; color:var(--mut); font-weight:400; }
.late { display:inline-block; padding:0 6px; border-radius:4px; background:#FDF0DC;
  color:#7A4300; font-size:11.5px; font-weight:500; line-height:18px; }
.ameta .late { margin-left:4px; }
.tot { display:inline-block; border-top:1px solid var(--ink); margin-top:3px;
  padding-top:3px; font-weight:600; }
.pib { margin-top:7px; font-size:12px; color:var(--mut); font-weight:400; line-height:1.5; }
.pib a { color:var(--grow); border-bottom-color:#BFE0CC; }
.esrc { display:inline-block; padding:0 5px; border-radius:4px; font-size:11px; line-height:17px;
  font-weight:500; }
.esrc.nih { background:var(--broth); color:#124D2B; }
.esrc.paper { background:#F1F2F4; color:var(--mut); font-weight:400; }
.pib a.nihlink { white-space:nowrap; font-weight:500; }
.nihline { margin-top:4px; font-size:13px; color:var(--mut); }
#t th { position:sticky; top:0; background:var(--bg); z-index:3; font-weight:500; }
.thsub { font-size:11.5px; font-weight:400; color:var(--mut); }
.thsub.done { color:var(--grow); }
#t th:nth-child(1), #t td.rownum { position:sticky; left:0; background:var(--bg);
  text-align:right; min-width:48px; max-width:48px; z-index:2;
  color:var(--mut); font-size:12.5px; }
#t th:nth-child(2), #t td.uni { position:sticky; left:48px; background:var(--bg);
  text-align:left; min-width:220px; max-width:220px; white-space:normal;
  z-index:2; font-weight:500; }
#t th:nth-child(3), #t td.dept { position:sticky; left:268px; background:var(--bg);
  text-align:left; min-width:200px; max-width:200px; white-space:normal;
  z-index:2; color:#3C444C; }
#t th:nth-child(4), #t td.funder { position:sticky; left:468px; background:var(--bg);
  text-align:left; min-width:130px; max-width:170px; white-space:normal;
  z-index:2; box-shadow:2px 0 0 var(--line); }
#t th:nth-child(-n+4) { z-index:4; }
#t td[data-v]:not(.zero) { background:#fff; }
#t tr:hover td { background:#F3F7F4; }
.zero { color:var(--mut); }
.legend { padding:12px 32px 40px; color:var(--mut); font-size:12.5px; max-width:1100px; }
"""

MOBILE_CSS = """
/* ---- phones ---- */
@media (max-width: 700px) {
  .mast { padding:16px 14px 12px; gap:10px; }
  .mast h1 { font-size:19px; }
  .todaybox { text-align:left; width:100%; }
  .todaybox .fig { font-size:24px; }
  .toolbar { padding:0 14px 12px; gap:8px; }
  .menuwrap, #filtersbtn { flex:1 1 calc(50% - 4px); }
  .menuwrap .tbtn, #filtersbtn { width:100%; justify-content:center; }
  #weeksbtn .lab { display:none; }
  .search { order:3; width:100%; }
  .search input { font-size:16px; }  /* 16px stops iOS zooming in on focus */
  .sortwrap { order:4; flex:1 1 100%; }
  .chips { order:5; width:100%; }
  .menu { position:fixed; top:auto; left:8px; right:8px; bottom:8px; width:auto;
    max-width:none; max-height:85vh; z-index:60; }
  .wklist { max-height:50vh; }
  #filtermodal { align-items:flex-end; padding:8px; }
  .fbox { max-height:88vh; }
  .tabs { display:flex; gap:8px; overflow-x:auto; padding:0 14px 12px; margin-bottom:12px;
    border-bottom:1px solid var(--card); }
  .tabs button { flex:none; height:40px; padding:0 14px; border:1px solid var(--grid);
    border-radius:20px; background:#fff; font:inherit; font-size:13.5px; color:var(--ink);
    white-space:nowrap; cursor:pointer; }
  .tabs button.on { background:var(--grow); border-color:var(--grow); color:#fff; font-weight:500; }
  .cards { display:block; padding:0 14px 24px; overflow:visible; }
  .col { display:none; }
  .col.on { display:flex; }
  .colhead { border-bottom:0; padding:0 0 4px; }
  .rangerow select { font-size:16px; }
  .ameta { white-space:normal; }
  .card { padding:14px; }
  .cbody { padding-left:24px; }
  .legend { padding:12px 14px 32px; }
  .wrap { max-height:calc(100vh - 16px); max-height:calc(100svh - 16px); }
  #t th, #t td { padding:7px 8px; font-size:13px; }
  #t th:nth-child(1), #t td.rownum { width:30px; min-width:30px; max-width:30px;
    font-size:11.5px; }
  #t th:nth-child(2), #t td.uni { left:30px; width:118px; min-width:118px; max-width:118px;
    font-size:12.5px; box-shadow:2px 0 0 var(--line); }
  #t th:nth-child(3), #t th:nth-child(4) { left:auto; }
  #t td.dept, #t td.funder { position:static; }
  #t th:nth-child(3), #t td.dept { width:112px; min-width:112px; max-width:112px;
    font-size:12.5px; }
  #t th:nth-child(4), #t td.funder { width:88px; min-width:88px; max-width:88px;
    font-size:12.5px; box-shadow:none; }
  #t td[data-v] { white-space:normal; width:172px; min-width:172px; max-width:172px; }
  .aline, .tot { white-space:nowrap; }
  .pib { font-size:11.5px; overflow-wrap:anywhere; }
  #pubmodal, #absmodal { padding:8px; }
  .pubhead { padding:12px 12px 8px; }
  .pubscroll { padding:0 8px 12px; }
  #pubtable { min-width:0; }
  #pubtable col:nth-child(1) { width:28px !important; }
  #pubtable col:nth-child(2) { width:46px !important; }
  #pubtable col:nth-child(3) { width:70px !important; }
  #pubtable col:nth-child(5) { width:68px !important; }
  #pubtable th, #pubtable td { padding:6px 5px; font-size:12.5px; }
  #pubtable th { white-space:normal; overflow-wrap:normal; font-size:12px; }
  .abshead { padding:12px 12px 8px; }
  .absbody { padding:10px 12px; }
  .absfoot { padding:8px 12px 10px; }
}
"""

LEDGER_JS = """
// ---- the ledger: weeks menu, filters popup, card view and table view ----
(function () {
  const $ = id => document.getElementById(id);
  const table = $('t');
  const tbody = table.tBodies[0];
  const ths = [...table.tHead.rows[0].cells];
  const WEEKS = ths.filter(th => th.dataset.w).map(th => ({
    iso: th.dataset.w, end: th.dataset.end, label: th.dataset.label, short: th.dataset.short,
    done: th.dataset.status === 'complete', quiet: +th.dataset.quiet || 0,
    need: +th.dataset.need || 8, pending: +th.dataset.pending || 0,
    i: ths.indexOf(th), th: th, funded: 0 }));
  const NEWEST = WEEKS.slice().reverse();
  // every row's text is read once here; reading it inside a loop that also hides
  // rows forces a page re-layout per row (seconds per keystroke)
  const ROWS = [...tbody.rows].map(tr => {
    const inst = tr.cells[1].textContent, dept = tr.cells[2].textContent,
          funder = tr.cells[3].textContent;
    return { tr, inst, dept, funder, hay: (inst + ' ' + dept + ' ' + funder).toLowerCase() };
  });
  const val = (r, w) => +r.tr.cells[w.i].dataset.v || 0;
  WEEKS.forEach(w => { w.funded = ROWS.filter(r => val(r, w) > 0).length; });

  const q = $('q'), sortSel = $('sort'), cardsEl = $('cards'), tabsEl = $('tabs');
  const tableWrap = $('tablewrap'), hideBox = $('hidezero');
  const rf = $('rangefrom'), rt = $('rangeto');
  // the days that have awards, newest first, with their labels ("Sep 16")
  const NOTICE = rf ? [...rf.options].map(o => o.value).filter(Boolean) : [];
  const NLABEL = {};
  if (rf) [...rf.options].forEach(o => { if (o.value) NLABEL[o.value] = o.textContent; });
  const BOX = { email: $('onlyemail'), name: $('onlyname'), inst: $('onlyinst'),
                linked: $('onlylinked'), recent: $('onlyrecent'), late: $('onlylate'),
                nonih: $('onlynonih') };
  const PAGE = 25;                          // cards per week before "Show all"
  const KEY = 'funding-ledger-view-v1';     // choices remembered on this device
  const DEFAULTS = { mode: 'last4', weeks: [], from: '', to: '', hide: true, sort: 'amount',
    f: { email: false, name: false, inst: false, linked: false, recent: false, late: false,
         nonih: false } };
  let S = load();
  let expanded = {};
  let activeTab = null;

  function load() {
    const out = JSON.parse(JSON.stringify(DEFAULTS));
    let s = null;
    try { s = JSON.parse(localStorage.getItem(KEY) || 'null'); } catch (e) {}
    if (s && typeof s === 'object') {
      if (['latest', 'last4', 'all', 'custom', 'range'].includes(s.mode)) out.mode = s.mode;
      if (Array.isArray(s.weeks)) out.weeks = s.weeks.filter(x => WEEKS.some(w => w.iso === x));
      if (NOTICE.includes(s.from)) out.from = s.from;
      if (NOTICE.includes(s.to)) out.to = s.to;
      if (typeof s.hide === 'boolean') out.hide = s.hide;
      if (s.sort === 'name' || s.sort === 'amount') out.sort = s.sort;
      if (s.f && typeof s.f === 'object') for (const k in out.f) out.f[k] = !!s.f[k];
    }
    if (out.mode === 'custom' && !out.weeks.length) out.mode = 'last4';
    if (out.mode === 'range' && !(out.from && out.to)) out.mode = 'last4';
    if (out.mode !== 'range') out.from = out.to = '';
    return out;
  }
  function save() { try { localStorage.setItem(KEY, JSON.stringify(S)); } catch (e) {} }

  function selected() {                     // the weeks shown, newest first
    if (S.mode === 'latest') return NEWEST.slice(0, 1);
    if (S.mode === 'last4') return NEWEST.slice(0, 4);
    if (S.mode === 'all') return NEWEST.slice();
    if (S.mode === 'range') return NEWEST.filter(w => w.iso <= S.to && w.end >= S.from);
    return NEWEST.filter(w => S.weeks.includes(w.iso));
  }
  const rangeText = () => S.from === S.to ? NLABEL[S.from]
    : NLABEL[S.from] + ' – ' + NLABEL[S.to];
  function anyCellFilter() {
    const f = S.f;
    return f.email || f.name || f.inst || f.linked || f.recent || f.nonih;
  }
  // the date range and "added late" pick out single awards inside a cell
  const awardFilterOn = () => S.mode === 'range' || S.f.late;
  function passes(c) {
    // email / name / institution must all hold; the two publication boxes combine as OR
    const f = S.f, d = c.dataset;
    if (f.email && d.he !== '1') return false;
    if (f.name && d.hn !== '1') return false;
    if (f.inst && d.hi !== '1') return false;
    if (f.nonih && d.nih === '1') return false;
    if ((f.linked || f.recent) && !((f.linked && d.pl === '1') || (f.recent && d.pr === '1'))) return false;
    return true;
  }
  function lineOk(line, w) {
    if (S.mode === 'range') {
      const n = line.dataset.n || w.iso;
      if (n < S.from || n > S.to) return false;
    }
    if (S.f.late && line.dataset.late !== '1') return false;
    return true;
  }
  const money = n => '$' + Math.round(n).toLocaleString('en-US');
  const count = n => n.toLocaleString('en-US');
  const el = (tag, cls, text) => {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined) e.textContent = text;
    return e;
  };

  // Hides the award lines the range / "added late" filters leave out and fixes the
  // cell's "= total"; returns what is left. Cards copy the cell's markup, so they match.
  function trim(cell, w) {
    if (!cell._lines) {
      cell._lines = [...cell.querySelectorAll('.aline')];
      cell._tot = cell.querySelector('.tot');
      cell._pib = cell.querySelector('.pib');
      if (cell._tot) cell._tot.dataset.orig = cell._tot.textContent;
    }
    if (!cell._lines.length) return { sum: 0, n: 0 };
    const on = awardFilterOn();
    let sum = 0, n = 0;
    cell._lines.forEach(l => {
      const ok = !on || lineOk(l, w);
      l.style.display = ok ? '' : 'none';
      if (ok) { sum += +l.dataset.a || 0; n++; }
    });
    if (cell._tot) {
      cell._tot.textContent = on ? '= ' + money(sum) : cell._tot.dataset.orig;
      cell._tot.style.display = n > 1 ? '' : 'none';
    }
    if (cell._pib) cell._pib.style.display = n ? '' : 'none';
    return { sum, n };
  }
  const eff = (r, w) => r.tr.cells[w.i]._eff || { sum: 0, n: 0 };

  function render() {
    const sel = selected(), qv = q.value.trim().toLowerCase();
    sel.forEach(w => ROWS.forEach(r => { const c = r.tr.cells[w.i]; c._eff = trim(c, w); }));
    const res = S.hide ? renderCards(sel, qv) : renderTable(sel, qv);
    cardsEl.hidden = !S.hide;
    tabsEl.hidden = !S.hide;
    tableWrap.hidden = S.hide;
    $('fig').textContent = money(res.grand);
    const what = S.mode === 'range' ? 'with notice dates ' + rangeText()
      : 'in the ' + (sel.length === 1 ? 'week' : sel.length + ' weeks') + ' shown';
    $('cap').textContent = 'granted ' + what + ' · ' + count(res.funded)
      + (res.funded === 1 ? ' funded row' : ' funded rows');
    renderControls(sel);
  }

  function status(w) {
    if (w.done) {
      const s = el('span', 'wstat done', '✓ Complete');
      s.title = 'Nothing new in ' + w.need + ' weekly checks in a row; still checked once a month';
      return s;
    }
    let t = 'Still checking · ' + Math.min(w.quiet, w.need) + ' of ' + w.need + ' quiet weeks';
    if (w.pending) t += ' · ' + count(w.pending) + ' waiting for amounts';
    const s = el('span', 'wstat', t);
    s.title = 'Checked every Monday; complete after ' + w.need
      + ' checks in a row find nothing new and nothing waiting for an amount';
    return s;
  }

  function renderCards(sel, qv) {
    cardsEl.textContent = '';
    tabsEl.textContent = '';
    if (!sel.some(w => w.iso === activeTab)) activeTab = sel.length ? sel[0].iso : null;
    let grand = 0, funded = 0;
    sel.forEach(w => {
      const list = ROWS.filter(r => eff(r, w).n > 0 && r.hay.includes(qv) && passes(r.tr.cells[w.i]));
      if (S.sort === 'name') list.sort((a, b) => a.inst.localeCompare(b.inst));
      else list.sort((a, b) => eff(b, w).sum - eff(a, w).sum || a.inst.localeCompare(b.inst));
      const sum = list.reduce((s, r) => s + eff(r, w).sum, 0);
      grand += sum;
      funded += list.length;

      const col = el('section', w.iso === activeTab ? 'col on' : 'col');
      const head = el('div', 'colhead');
      head.append(el('h2', '', w.label),
                  el('span', 'csub', 'Notice week · ' + count(list.length) + ' funded'
                     + (list.length ? ' · ' + money(sum) : '')),
                  status(w));
      col.append(head);
      if (!list.length) {
        const e = el('div', 'empty');
        if (w.funded) e.append(el('b', '', 'No rows match'),
                               el('span', '', 'Nothing this week fits the search or filters.'));
        else e.append(el('b', '', 'No awards recorded for this week'),
                      el('span', '', 'Every Monday each week is checked again until it’s complete, so awards NIH posts late are still added.'));
        col.append(e);
      }
      const show = expanded[w.iso] ? list : list.slice(0, PAGE);
      show.forEach((r, k) => col.append(card(r, r.tr.cells[w.i], k + 1)));
      if (list.length > show.length) {
        const more = el('button', 'more', 'Show all ' + count(list.length));
        more.type = 'button';
        more.addEventListener('click', () => { expanded[w.iso] = true; render(); });
        col.append(more);
      }
      cardsEl.append(col);

      const tab = el('button', w.iso === activeTab ? 'on' : '',
                     w.short + (w.done ? ' ✓' : '') + ' · ' + count(list.length));
      tab.type = 'button';
      tab.setAttribute('aria-pressed', w.iso === activeTab ? 'true' : 'false');
      tab.addEventListener('click', () => { activeTab = w.iso; render(); });
      tabsEl.append(tab);
    });
    return { grand, funded };
  }

  function card(r, cell, n) {
    const a = el('article', 'card');
    const head = el('div', 'chead');
    const title = el('div', 'ctitle');
    title.append(el('h3', '', r.inst),
                 el('div', 'cmeta', (r.dept === '--' ? 'No department listed' : r.dept) + ' · ' + r.funder));
    head.append(el('span', 'cnum', String(n)), title);
    const body = el('div', 'cbody');
    body.innerHTML = cell.innerHTML;   // the cell's own markup: amounts, total, PI lines, publications
    a.append(head, body);
    return a;
  }

  function renderTable(sel, qv) {
    const on = new Set(sel.map(w => w.iso));
    WEEKS.forEach(w => {
      const d = on.has(w.iso) ? '' : 'none';
      w.th.style.display = d;
      ROWS.forEach(r => { r.tr.cells[w.i].style.display = d; });
    });
    const filt = anyCellFilter() || awardFilterOn();
    const keep = (r, w) => eff(r, w).n > 0 && passes(r.tr.cells[w.i]);
    ROWS.forEach(r => {
      const ok = r.hay.includes(qv) && (!filt || sel.some(w => keep(r, w)));
      r.tr.style.display = ok ? '' : 'none';
    });
    const tot = r => sel.reduce((s, w) => s + eff(r, w).sum, 0);
    const order = ROWS.slice();
    if (S.sort === 'name') order.sort((a, b) => a.inst.localeCompare(b.inst));
    else order.sort((a, b) => tot(b) - tot(a) || a.inst.localeCompare(b.inst));
    order.forEach(r => tbody.appendChild(r.tr));
    let n = 0, grand = 0, funded = 0;
    order.forEach(r => {
      if (r.tr.style.display === 'none') return;
      r.tr.cells[0].textContent = ++n;
      sel.forEach(w => {
        if (eff(r, w).n > 0 && (!filt || passes(r.tr.cells[w.i]))) { grand += eff(r, w).sum; funded++; }
      });
    });
    return { grand, funded };
  }

  const NAMES = { email: 'Has a PI email', name: 'Has a PI name', inst: 'Has an institution',
                  linked: 'Publication linked to grant', recent: 'Publications in last 5 years',
                  late: 'Only awards added late', nonih: 'No NIH email yet' };
  function renderControls(sel) {
    const labels = { latest: 'Latest week', last4: 'Last 4 weeks', all: 'All weeks' };
    $('weekslabel').textContent = S.mode === 'range' ? rangeText() : labels[S.mode]
      || (sel.length === 1 ? sel[0].label : sel.length + ' picked');
    document.querySelectorAll('#weeksmenu [data-quick]').forEach(b => {
      b.classList.toggle('on', b.dataset.quick === S.mode);
      b.setAttribute('aria-pressed', b.dataset.quick === S.mode ? 'true' : 'false');
    });
    const on = new Set(sel.map(w => w.iso));
    document.querySelectorAll('#weeksmenu input[type=checkbox]').forEach(cb => { cb.checked = on.has(cb.value); });
    $('wkcount').textContent = sel.length + (sel.length === 1 ? ' week' : ' weeks') + ' selected';
    if (rf) { rf.value = S.mode === 'range' ? S.from : ''; rt.value = S.mode === 'range' ? S.to : ''; }
    hideBox.checked = S.hide;
    for (const k in BOX) if (BOX[k]) BOX[k].checked = S.f[k];
    sortSel.value = S.sort;

    const chips = [];
    if (S.mode === 'range') chips.push(['Notice dates ' + rangeText(),
      () => { S.mode = 'last4'; S.from = S.to = ''; }]);
    if (S.hide) chips.push(['Rows with no award hidden', () => { S.hide = false; }]);
    for (const k in NAMES) if (S.f[k]) chips.push([NAMES[k], () => { S.f[k] = false; }]);
    const badge = $('fbadge');          // counts only what the Filters popup sets
    const nf = chips.length - (S.mode === 'range' ? 1 : 0);
    badge.textContent = nf;
    badge.hidden = !nf;
    const box = $('chips');
    box.textContent = '';
    chips.forEach(([text, off]) => {
      const chip = el('span', 'chip');
      const x = el('button', '', '×');
      x.type = 'button';
      x.setAttribute('aria-label', 'Remove filter: ' + text);
      x.addEventListener('click', () => { off(); expanded = {}; changed(); });
      chip.append(el('span', '', text), x);
      box.append(chip);
    });
  }
  function changed() { save(); render(); }

  // weeks menu
  const wbtn = $('weeksbtn'), wmenu = $('weeksmenu');
  function openWeeks(open) {
    wmenu.hidden = !open;
    wbtn.setAttribute('aria-expanded', open ? 'true' : 'false');
  }
  wbtn.addEventListener('click', e => { e.stopPropagation(); openWeeks(wmenu.hidden); });
  wmenu.addEventListener('click', e => e.stopPropagation());
  document.addEventListener('click', () => { if (!wmenu.hidden) openWeeks(false); });
  $('weeksdone').addEventListener('click', () => openWeeks(false));
  wmenu.querySelectorAll('[data-quick]').forEach(b => b.addEventListener('click', () => {
    S.mode = b.dataset.quick; S.weeks = []; S.from = S.to = ''; expanded = {}; changed();
  }));
  wmenu.querySelectorAll('input[type=checkbox]').forEach(cb => cb.addEventListener('change', () => {
    const cur = new Set(selected().map(w => w.iso));
    if (cb.checked) cur.add(cb.value); else cur.delete(cb.value);
    if (!cur.size) { cb.checked = true; return; }   // always keep at least one week
    S.mode = 'custom'; S.weeks = [...cur]; S.from = S.to = ''; changed();
  }));
  // custom range: picking one end fills the other with the oldest / newest day
  function rangeChanged() {
    let a = rf.value, b = rt.value;
    if (!a && !b) {
      if (S.mode === 'range') S.mode = 'last4';
      S.from = S.to = ''; expanded = {}; changed(); return;
    }
    if (!a) a = NOTICE[NOTICE.length - 1];
    if (!b) b = NOTICE[0];
    if (a > b) [a, b] = [b, a];
    S.mode = 'range'; S.from = a; S.to = b; S.weeks = []; expanded = {}; changed();
  }
  if (rf) { rf.addEventListener('change', rangeChanged); rt.addEventListener('change', rangeChanged); }

  // filters popup
  const fmodal = $('filtermodal');
  const closeFilters = () => { fmodal.hidden = true; };
  $('filtersbtn').addEventListener('click', () => { openWeeks(false); fmodal.hidden = false; });
  $('filterclose').addEventListener('click', closeFilters);
  $('filterdone').addEventListener('click', closeFilters);
  fmodal.addEventListener('click', e => { if (e.target === fmodal) closeFilters(); });
  hideBox.addEventListener('change', () => { S.hide = hideBox.checked; changed(); });
  for (const k in BOX) if (BOX[k]) BOX[k].addEventListener('change', () => {
    S.f[k] = BOX[k].checked; expanded = {}; changed();
  });
  $('filterreset').addEventListener('click', () => {
    S.hide = true;
    for (const k in S.f) S.f[k] = false;
    changed();
  });
  // header line: "N awards were added late ... Show them"
  const showLate = $('showlate');
  if (showLate) showLate.addEventListener('click', () => {
    S.f.late = true; S.hide = true;
    if (S.mode !== 'range') { S.mode = 'all'; S.weeks = []; }
    expanded = {}; changed();
  });

  sortSel.addEventListener('change', () => { S.sort = sortSel.value; changed(); });
  let qTimer;
  q.addEventListener('input', () => { clearTimeout(qTimer); qTimer = setTimeout(render, 150); });
  document.addEventListener('keydown', e => {
    if (e.key !== 'Escape') return;
    if (!wmenu.hidden) openWeeks(false);
    else if (!fmodal.hidden) closeFilters();
  });

  render();
})();
"""

POPUP_HTML = """
<div id="pubmodal" hidden>
  <div class="pubbox" role="dialog" aria-modal="true" aria-labelledby="pubtitle">
    <div class="pubhead">
      <div>
        <h2 id="pubtitle">Publications</h2>
        <div class="sub" id="pubsub"></div>
      </div>
      <button class="pubclose" id="pubclose" aria-label="Close">&times;</button>
    </div>
    <div class="pubscroll">
      <table id="pubtable">
        <colgroup>
          <col style="width:56px"><col style="width:92px"><col style="width:148px"><col><col style="width:126px">
        </colgroup>
        <thead><tr>
          <th><span class="thwrap">#<span class="rz"></span></span></th>
          <th class="sortable" data-k="year"><span class="thwrap">Year<span class="rz"></span></span></th>
          <th class="sortable" data-k="pos"><span class="thwrap">Author Order<span class="rz"></span></span></th>
          <th class="sortable" data-k="title"><span class="thwrap">Publication Title<span class="rz"></span></span></th>
          <th class="sortable" data-k="abs"><span class="thwrap">Abstract (Yes/No)</span></th>
        </tr></thead>
        <tbody></tbody>
      </table>
      <div class="pubempty" id="pubempty" hidden>No publications found.</div>
    </div>
  </div>
</div>
<div id="absmodal" hidden>
  <div class="absbox" role="dialog" aria-modal="true" aria-labelledby="abstitle">
    <div class="abshead">
      <div>
        <h2 id="abstitle">Abstract</h2>
        <div class="sub" id="absmeta"></div>
      </div>
      <button class="pubclose" id="absclose" aria-label="Close">&times;</button>
    </div>
    <div class="absbody" id="absbody"></div>
    <div class="absfoot" id="absfoot"></div>
  </div>
</div>
"""

POPUP_JS = """
// ---- publication popup ----
(function () {
  const modal = document.getElementById('pubmodal');
  const tbl = document.getElementById('pubtable');
  const tb = tbl.tBodies[0];
  const cols = tbl.querySelectorAll('colgroup col');
  const sub = document.getElementById('pubsub');
  const empty = document.getElementById('pubempty');
  const POS = { first: 'First Author', middle: 'Middle Author', last: 'Last Author', unknown: 'Unknown' };
  const CYCLE = [['last', 'middle', 'first'], ['middle', 'first', 'last'], ['first', 'last', 'middle']];
  let rows = [], sortState = { k: null, dir: 1, cyc: -1 };

  function renumber() {
    let n = 0;
    [...tb.rows].forEach(r => { r.cells[0].textContent = ++n; });
  }
  function draw(list) {
    tb.innerHTML = '';
    for (const r of list) {
      const tr = document.createElement('tr');
      const title = r.url
        ? `<a href="${r.url.replace(/"/g, '&quot;')}" target="_blank" rel="noopener">${esc(r.title)}</a>`
        : esc(r.title);
      const absCell = r.abs
        ? `<td class="absc"><a class="abslink" data-pmid="${esc(r.pmid || '')}" href="#">Yes</a></td>`
        : `<td class="absc no">No</td>`;
      tr.innerHTML = `<td class="num"></td><td class="year">${r.year || ''}</td>` +
        `<td class="pos-${r.position}">${POS[r.position] || r.position}</td><td>${title}</td>` + absCell;
      tr._row = r;
      tb.appendChild(tr);
    }
    renumber();
  }
  function esc(s) { return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
  function open(slug) {
    modal.hidden = false;
    document.getElementById('pubtitle').textContent = 'Loading…';
    sub.textContent = ''; tb.innerHTML = ''; empty.hidden = true;
    fetch('pubs/' + slug + '.json').then(r => r.json()).then(d => {
      rows = d.rows || [];
      document.getElementById('pubtitle').textContent = d.pi || 'Publications';
      sub.innerHTML = `Publication Linked To Grant (Yes/No): <b>${d.linked ? 'Yes' : 'No'}</b>` +
        (d.linked ? '' : ` &middot; journal articles, last 5 years &middot; <b>${rows.length}</b> found`) +
        (d.match === 'uncertain' ? ' &middot; <b style="color:#A5670A">match uncertain</b> — verify affiliation' : '') +
        (d.match === 'merged' ? ' &middot; merged from two OpenAlex profiles' : '');
      sortState = { k: null, dir: 1, cyc: -1 };
      tbl.querySelectorAll('th').forEach(h => h.className = h.className.replace(/\\bsorted\\b|\\basc\\b|\\bdesc\\b|\\bc-\\w+/g, '').trim());
      draw(rows);
      empty.hidden = rows.length > 0;
    }).catch(() => {
      document.getElementById('pubtitle').textContent = 'Could not load publications';
    });
  }
  function close() { modal.hidden = true; }

  document.addEventListener('click', e => {
    const a = e.target.closest('a.publink');
    if (a) { e.preventDefault(); open(a.dataset.pub); }
  });
  document.getElementById('pubclose').addEventListener('click', close);
  modal.addEventListener('click', e => { if (e.target === modal) close(); });
  document.addEventListener('keydown', e => {
    if (e.key !== 'Escape') return;
    if (!absModal.hidden) closeAbs(); else if (!modal.hidden) close();
  });

  // ---- abstract window: text is fetched live from Europe PMC / PubMed on click,
  // ---- never stored on this site
  const absModal = document.getElementById('absmodal');
  const absBody = document.getElementById('absbody');
  const absMeta = document.getElementById('absmeta');
  const absFoot = document.getElementById('absfoot');
  function closeAbs() { absModal.hidden = true; }
  document.getElementById('absclose').addEventListener('click', closeAbs);
  absModal.addEventListener('click', e => { if (e.target === absModal) closeAbs(); });

  function renderParts(parts) {
    absBody.innerHTML = '';
    for (const p of parts) {
      if (p.label) { const l = document.createElement('span'); l.className = 'lbl'; l.textContent = p.label; absBody.appendChild(l); }
      const para = document.createElement('p'); para.textContent = p.text; absBody.appendChild(para);
    }
  }
  async function fromEuropePmc(pmid) {
    const r = await fetch(`https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=EXT_ID:${pmid}%20AND%20SRC:MED&resultType=core&format=json`);
    if (!r.ok) return null;
    const j = await r.json();
    const t = (((j.resultList || {}).result || [])[0] || {}).abstractText;
    if (!t) return null;
    // structured abstracts arrive as <h4>Label</h4><p>text</p>; parse into a detached document
    const doc = new DOMParser().parseFromString(t, 'text/html');
    const parts = []; let label = null;
    doc.body.childNodes.forEach(n => {
      if (n.nodeName === 'H4' || n.nodeName === 'H3' || n.nodeName === 'STRONG') { label = n.textContent.trim(); return; }
      const txt = n.textContent.trim();
      if (txt) { parts.push({ label, text: txt }); label = null; }
    });
    return parts.length ? parts : [{ label: null, text: doc.body.textContent.trim() }];
  }
  async function fromPubMed(pmid) {
    const r = await fetch(`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id=${pmid}&retmode=xml`);
    if (!r.ok) return null;
    const xml = new DOMParser().parseFromString(await r.text(), 'text/xml');
    const parts = [...xml.querySelectorAll('Abstract AbstractText')]
      .map(el => ({ label: el.getAttribute('Label'), text: el.textContent.trim() }))
      .filter(p => p.text);
    return parts.length ? parts : null;
  }
  async function openAbs(row) {
    absModal.hidden = false;
    document.getElementById('abstitle').textContent = row.title || 'Abstract';
    absMeta.textContent = [row.year || '', POS[row.position] || ''].filter(Boolean).join(' · ');
    absBody.innerHTML = '<p class="wait">Loading abstract…</p>';
    const paper = row.url || (row.pmid ? `https://pubmed.ncbi.nlm.nih.gov/${row.pmid}/` : '');
    absFoot.innerHTML =
      (paper ? `<a href="${paper.replace(/"/g, '&quot;')}" target="_blank" rel="noopener">Open paper ↗</a>` : '<span></span>') +
      `<span>Abstract shown live from PubMed / Europe PMC (PMID ${esc(row.pmid || '')})</span>`;
    let parts = null;
    try { parts = await fromEuropePmc(row.pmid); } catch (e) {}
    if (!parts) { try { parts = await fromPubMed(row.pmid); } catch (e) {} }
    if (parts) renderParts(parts);
    else absBody.innerHTML = '<p class="err">Could not load the abstract right now. Use the paper link above.</p>';
  }
  tb.addEventListener('click', e => {
    const a = e.target.closest('a.abslink');
    if (!a) return;
    e.preventDefault();
    const tr = a.closest('tr');
    if (tr && tr._row) openAbs(tr._row);
  });

  // sorting: year (desc/asc), title (A-Z/Z-A), contribution (green -> yellow -> red first)
  tbl.querySelectorAll('th.sortable').forEach(th => th.addEventListener('click', e => {
    if (e.target.classList.contains('rz')) return;
    const k = th.dataset.k;
    tbl.querySelectorAll('th').forEach(h => h.className = h.className.replace(/\\bsorted\\b|\\basc\\b|\\bdesc\\b|\\bc-\\w+/g, '').trim());
    let list = [...rows];
    if (k === 'pos') {
      sortState.cyc = (sortState.k === 'pos') ? (sortState.cyc + 1) % 3 : 0;
      const order = CYCLE[sortState.cyc];
      const rank = p => { const i = order.indexOf(p); return i < 0 ? 9 : i; };
      list.sort((a, b) => rank(a.position) - rank(b.position) || (b.year - a.year));
      th.classList.add('sorted', 'c-' + order[0]);
    } else if (k === 'abs') {
      // Yes first, click again for No first
      sortState.dir = (sortState.k === k) ? -sortState.dir : 1;
      list.sort((a, b) => ((b.abs ? 1 : 0) - (a.abs ? 1 : 0)) * sortState.dir || (b.year - a.year));
      th.classList.add('sorted', sortState.dir === 1 ? 'asc' : 'desc');
    } else {
      sortState.dir = (sortState.k === k) ? -sortState.dir : (k === 'year' ? -1 : 1);
      if (k === 'year') list.sort((a, b) => ((a.year || 0) - (b.year || 0)) * sortState.dir);
      else list.sort((a, b) => a.title.localeCompare(b.title) * sortState.dir);
      th.classList.add('sorted', sortState.dir === 1 ? 'asc' : 'desc');
    }
    sortState.k = k;
    draw(list);
  }));

  // manual column resizing: drag the right edge of a header
  tbl.querySelectorAll('th .rz').forEach((h, i) => {
    h.addEventListener('mousedown', e => {
      e.preventDefault(); e.stopPropagation();
      const col = cols[i], startX = e.clientX, startW = col.getBoundingClientRect().width;
      const move = ev => { col.style.width = Math.max(40, startW + ev.clientX - startX) + 'px'; };
      const up = () => { document.removeEventListener('mousemove', move); document.removeEventListener('mouseup', up); };
      document.addEventListener('mousemove', move); document.addEventListener('mouseup', up);
    });
  });
})();
"""


ICON_CAL = ('<svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" '
            'stroke="#5B636B" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
            '<rect x="3" y="5" width="18" height="16" rx="2"></rect>'
            '<path d="M3 10h18M8 3v4M16 3v4"></path></svg>')
ICON_CHEVRON = ('<svg aria-hidden="true" width="16" height="16" viewBox="0 0 24 24" fill="none" '
                'stroke="#5B636B" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
                '<path d="M6 9l6 6 6-6"></path></svg>')
ICON_SEARCH = ('<svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" '
               'stroke="#5B636B" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
               '<circle cx="11" cy="11" r="7"></circle><path d="M20 20l-3.5-3.5"></path></svg>')
ICON_SLIDERS = ('<svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" '
                'stroke="#5B636B" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
                '<path d="M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12"></path><circle cx="16" cy="6" r="2">'
                '</circle><circle cx="10" cy="12" r="2"></circle><circle cx="18" cy="18" r="2"></circle></svg>')
ICON_CLOSE = ('<svg aria-hidden="true" width="18" height="18" viewBox="0 0 24 24" fill="none" '
              'stroke="currentColor" stroke-width="2" stroke-linecap="round">'
              '<path d="M6 6l12 12M18 6L6 18"></path></svg>')


def render_html(data, pubs=None, linked=None) -> str:
    pubs, linked = pubs or {}, linked or {}
    weeks = data["dates"]
    rows = sorted(
        data["rows"].values(),
        key=lambda r: (r["university"].lower(), r.get("funder", "").lower(),
                       r["department"].lower()),
    )

    week_totals = {
        w: sum(e["amount"] for r in rows for e in r["cells"].get(w, []))
        for w in weeks}
    week_funded = {w: sum(1 for r in rows if r["cells"].get(w)) for w in weeks}
    n_awards = sum(len(v) for r in rows for v in r["cells"].values())

    # each week's check-off status (see update_week_status)
    today = date.today()
    wmeta = data.get("meta", {}).get("weeks", {})

    def wstatus(w):
        st = wmeta.get(w) or {}
        young = (today - date.fromisoformat(w)).days < PENDING_GRACE_DAYS
        return (bool(st.get("complete")), int(st.get("quiet", 0)),
                int(st.get("pending", 0)) if young else 0)

    def head_cell(w):
        done, quiet, pend = wstatus(w)
        end = (date.fromisoformat(w) + timedelta(days=6)).isoformat()
        sub = ("✓ Complete" if done else
               f"Checking · {min(quiet, QUIET_CHECKS_TO_COMPLETE)}/{QUIET_CHECKS_TO_COMPLETE} quiet")
        return (f'<th class="num" data-w="{w}" data-end="{end}" data-total="{week_totals[w]}" '
                f'data-status="{"complete" if done else "open"}" data-quiet="{quiet}" '
                f'data-need="{QUIET_CHECKS_TO_COMPLETE}" data-pending="{pend}" '
                f'data-label="{wlabel(w)}" data-short="{wshort(w)}" title="{w}">{wlabel(w)}'
                f'<div class="thsub{" done" if done else ""}">{sub}</div></th>')

    head_cells = "".join(head_cell(w) for w in weeks)
    # the weeks menu lists the newest week first
    week_items = "".join(
        f'<label class="wk"><input type="checkbox" value="{w}">'
        f'<span class="wkl">{wlabel(w)}</span>'
        f'<span class="wkn">{week_funded[w]:,} funded'
        + (' · <span class="ok">✓ complete</span>' if wstatus(w)[0] else "")
        + "</span></label>"
        for w in reversed(weeks))

    # custom range: From / To list only the days that have awards, newest first
    entries = [e for r in rows for v in r["cells"].values() for e in v]
    notices = sorted({e["notice"] for e in entries if e.get("notice")}, reverse=True)
    multi_year = len({n[:4] for n in notices}) > 1

    def nlabel(n):
        return short_date(n) + (f", {n[:4]}" if multi_year else "")

    if notices:
        opts = '<option value="">—</option>' + "".join(
            f'<option value="{n}">{nlabel(n)}</option>' for n in notices)
        range_html = f"""<div class="range" role="group" aria-labelledby="rangehead">
        <div class="rangehead" id="rangehead">Custom range · award notice dates</div>
        <div class="rangerow"><label>From<select id="rangefrom">{opts}</select></label>
          <label>To<select id="rangeto">{opts}</select></label></div>
        <div class="rangenote">Shows the weeks these dates fall in, with only the awards
          whose notice date is in the range.</div></div>"""
    else:
        range_html = """<div class="range"><div class="rangehead">Custom range · award notice
        dates</div><div class="rangenote">Notice dates appear after the next Monday
        update.</div></div>"""

    # awards NIH posted after their week was first checked
    late = [e for e in entries if e.get("late")]
    if late:
        recent = max(e.get("added") or "" for e in late)
        one = len(late) == 1
        late_html = (
            f'<div class="lateline" id="lateline"><span>{len(late):,} '
            f'{"award was" if one else "awards were"} added late, after '
            f'{"its" if one else "their"} week was first checked'
            + (f" (most recent: {short_date(recent)})" if recent else "")
            + f'.</span><button type="button" class="linkbtn" id="showlate">'
            f'{"Show it" if one else "Show them"}</button></div>')
    else:
        late_html = ""

    # NIH emails collected so far (data/nih_emails.csv), counted per PI
    pis, pis_nih = set(), set()
    for r in rows:
        for v in r["cells"].values():
            for e in v:
                if e.get("pi"):
                    k = f"{e['pi']}|{r['university']}".lower()
                    pis.add(k)
                    if e.get("nih_email"):
                        pis_nih.add(k)
    nih_html = (f'<div class="nihline">NIH emails collected for {len(pis_nih):,} of {len(pis):,} PIs '
                f'&middot; <a href="{NIH_LINKS_URL}" target="_blank" rel="noopener">'
                f'List of every PI&rsquo;s NIH link</a> '
                f'&middot; <a href="nih-email-helper.user.js">Get the Chrome email helper</a></div>')

    body = []
    for i, r in enumerate(rows, start=1):
        cells = "".join(_cell_html(r["cells"].get(w, []), r.get("university", ""),
                                   pubs, linked)
                        for w in weeks)
        body.append(
            "<tr>"
            f'<td class="rownum">{i}</td>'
            f'<td class="uni">{html_lib.escape(r["university"])}</td>'
            f'<td class="dept">{html_lib.escape(r["department"])}</td>'
            f'<td class="funder">{html_lib.escape(r.get("funder", "—"))}</td>'
            f"{cells}</tr>"
        )

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Biotech department funding ledger</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
{BASE_CSS}
{POPUP_CSS}
{MOBILE_CSS}
</style></head><body>

<div class="mast">
  <div>
    <h1>Biotech department funding ledger</h1>
    <div class="sub">NIH awards granted each week to biotech-adjacent
      university departments &middot; updated {generated} &middot;
      {len(rows)} rows &middot; {n_awards} awards tracked</div>
    {late_html}
    {nih_html}
  </div>
  <div class="todaybox">
    <div class="fig" id="fig">$0</div>
    <div class="cap" id="cap">granted in the weeks shown</div>
  </div>
</div>

<div class="toolbar">
  <div class="menuwrap">
    <button type="button" class="tbtn" id="weeksbtn" aria-haspopup="dialog" aria-expanded="false"
      aria-controls="weeksmenu">{ICON_CAL}<span><span class="lab">Weeks:</span>
      <span id="weekslabel">Last 4 weeks</span></span>{ICON_CHEVRON}</button>
    <div class="menu" id="weeksmenu" role="dialog" aria-label="Choose weeks" hidden>
      <div class="quick">
        <button type="button" data-quick="latest">Latest week</button>
        <button type="button" data-quick="last4">Last 4 weeks</button>
        <button type="button" data-quick="all">All weeks</button>
      </div>
      {range_html}
      <div class="wklist" role="group" aria-label="Weeks">{week_items}</div>
      <div class="menufoot"><span id="wkcount"></span>
        <button type="button" class="primary" id="weeksdone">Done</button></div>
    </div>
  </div>
  <label class="search">{ICON_SEARCH}<input id="q" type="search"
    placeholder="Search institution, dept or funder" aria-label="Search rows"></label>
  <button type="button" class="tbtn" id="filtersbtn" aria-haspopup="dialog">{ICON_SLIDERS}
    <span>Filters</span><span class="badge" id="fbadge" hidden>0</span></button>
  <label class="tbtn sortwrap"><span class="lab">Sort:</span>
    <select id="sort" aria-label="Sort">
      <option value="amount">Largest award first</option>
      <option value="name">Institution A–Z</option>
    </select></label>
  <div class="chips" id="chips"></div>
</div>

<div class="tabs" id="tabs" role="group" aria-label="Week shown"></div>
<div class="cards" id="cards"></div>

<div class="wrap" id="tablewrap" hidden><table id="t">
<thead><tr>
  <th>#</th>
  <th data-t="text">Institution</th>
  <th data-t="text">Department</th>
  <th data-t="text">Funder</th>
  {head_cells}
</tr></thead>
<tbody>
{chr(10).join(body)}
</tbody></table></div>

<div class="legend">Each week is one completed Sunday&ndash;Saturday week of NIH award
notice dates. An award appears once, in the week of its notice date, so amounts are
never double-counted. NIH often posts an award, or its dollar amount, weeks late, so
every Monday each week is checked again. A week is marked <b>&check; Complete</b> after
{QUIET_CHECKS_TO_COMPLETE} weekly checks in a row find nothing new and no matching award
still waiting for an amount; complete weeks are still checked once a month and reopen
if anything turns up. Every award shows its <b>Award Notice Date</b>; awards that arrived
after their week was first checked carry an amber <span class="late">Added: date</span>
tag. Cards with several awards list each one and end with a ruled <b>= total</b>. &ldquo;No department listed&rdquo; (-- in the table) means
NIH reported none for that keyword-matched award. PI lines show the contact PI of
the largest award. Emails tagged <span class="esrc nih">NIH</span> were revealed on
NIH&rsquo;s own project page and saved in data/nih_emails.csv; emails tagged
<span class="esrc paper">from a paper</span> were found in the PI&rsquo;s recent
publications (PubMed and Europe PMC) and may belong to someone with a similar name.
&ldquo;View email on NIH&rdquo; opens the PI&rsquo;s NIH page at its View Email button;
the <a href="nih-email-helper.user.js">Chrome email helper</a> (needs the Tampermonkey
extension) presses it once on the page you open and keeps a list to paste into
data/nih_emails.csv. The same links, one per PI, are listed in
<a href="{NIH_LINKS_URL}" target="_blank" rel="noopener">data/nih_links.md</a>
(rebuilt every run); an email typed into the nih_email column of data/nih_links.csv
is kept and shown here with the NIH tag. Source: NIH RePORTER (refreshes
Sunday nights; this page updates every Monday morning).</div>

<div id="filtermodal" hidden>
  <div class="fbox" role="dialog" aria-modal="true" aria-labelledby="ftitle">
    <div class="fhead">
      <div><h2 id="ftitle">Filters</h2><div class="sub">Applies to every week shown</div></div>
      <button type="button" class="iconbtn" id="filterclose" aria-label="Close filters">{ICON_CLOSE}</button>
    </div>
    <div class="fbody">
      <label class="fswitch">
        <span><span class="t">Hide rows with no award</span>
          <span class="d">Each week lists only the institutions it funded. Turn off to see
          the full table, with every row lined up across weeks.</span></span>
        <span class="switch"><input type="checkbox" role="switch" id="hidezero">
          <span class="track" aria-hidden="true"></span><span class="knob" aria-hidden="true"></span></span>
      </label>
      <div class="fgroup" role="group" aria-labelledby="fonly">
        <h3 id="fonly">Only show rows with</h3>
        <label class="fopt"><input type="checkbox" id="onlyemail">A PI email</label>
        <label class="fopt"><input type="checkbox" id="onlyname">A PI name</label>
        <label class="fopt"><input type="checkbox" id="onlyinst">An institution</label>
      </div>
      <div class="fgroup" role="group" aria-labelledby="fnih">
        <h3 id="fnih">NIH email</h3>
        <label class="fopt"><input type="checkbox" id="onlynonih">No NIH email yet</label>
        <span class="fhint">The PIs still to collect with the email helper.</span>
      </div>
      <div class="fgroup" role="group" aria-labelledby="fpubs">
        <h3 id="fpubs">Publications</h3>
        <label class="fopt"><input type="checkbox" id="onlylinked">Publication linked to the grant</label>
        <label class="fopt"><input type="checkbox" id="onlyrecent">Publications in the last 5 years</label>
        <span class="fhint">Check both to see rows with either.</span>
      </div>
      <div class="fgroup" role="group" aria-labelledby="flate">
        <h3 id="flate">Added late</h3>
        <label class="fopt"><input type="checkbox" id="onlylate">Only awards added late</label>
        <span class="fhint">Awards NIH posted after their week was first checked. They carry
          an <span class="late">Added: date</span> tag.</span>
      </div>
    </div>
    <div class="ffoot">
      <button type="button" class="linkbtn" id="filterreset">Reset to default</button>
      <span class="right"><span class="saved">Saved on this device</span>
        <button type="button" class="primary" id="filterdone">Done</button></span>
    </div>
  </div>
</div>

{POPUP_HTML}

<script>
{LEDGER_JS}
{POPUP_JS}
</script>
</body></html>"""


def write_site(data) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    n_nih = apply_nih_emails(data)
    n_links = write_nih_links(data)
    log(f"NIH emails on {n_nih} award(s); data/nih_links.csv lists {n_links} PI(s)")
    pubs = load_json(pubs_cache_path(), {})
    linked = load_json(linked_cache_path(), {})
    n = write_pub_files(data, pubs, linked)
    (DOCS_DIR / "index.html").write_text(render_html(data, pubs, linked))
    (DOCS_DIR / ".nojekyll").write_text("")
    log(f"wrote {DOCS_DIR/'index.html'} (+{n} publication popup file(s))")


# ----------------------------------------------------------------------------
# Self-test (offline)
# ----------------------------------------------------------------------------

def selftest() -> int:
    import tempfile
    global HOME, DATA_DIR, DOCS_DIR, DATA_FILE, SEEN_FILE
    tmp = Path(tempfile.mkdtemp(prefix="tracker_test_"))
    HOME, DATA_DIR, DOCS_DIR = tmp, tmp / "data", tmp / "docs"
    DATA_FILE, SEEN_FILE = DATA_DIR / "tracker_data.json", DATA_DIR / "seen_awards.json"

    # week math + labels
    assert last_completed_week(date(2026, 9, 16)) == (date(2026, 9, 6), date(2026, 9, 12))
    assert last_completed_week(date(2026, 9, 21)) == (date(2026, 9, 13), date(2026, 9, 19))
    assert last_completed_week(date(2026, 9, 20)) == (date(2026, 9, 13), date(2026, 9, 19))
    assert wlabel("2026-09-06") == "Sep 6 – Sep 12"
    assert wlabel("2026-09-27") == "Sep 27 – Oct 3"
    assert wshort("2026-09-20") == "Sep 20–26" and wshort("2026-09-27") == "Sep 27–Oct 3"
    assert short_date("2026-09-08") == "Sep 8"

    # every week is checked until 8 quiet weekly checks in a row; then monthly
    led = {"dates": ["2026-09-06", "2026-09-20"], "rows": {}, "meta": {}}
    due = weeks_to_check(led, date(2026, 11, 3))
    assert [s0 for s0, _ in due][0] == date(2026, 9, 6) and len(due) == 8, \
        "all weeks from the first through the last completed one, gaps included"
    assert due[-1] == (date(2026, 10, 25), date(2026, 10, 31)), "newest = last completed week"
    day = date(2026, 11, 2)
    for k in range(7):
        st = update_week_status(led, "2026-09-06", 0, 0, day + timedelta(days=7 * k))
    assert st["quiet"] == 7 and not st.get("complete"), "7 quiet checks: still open"
    st = update_week_status(led, "2026-09-06", 0, 0, day + timedelta(days=44))
    assert st["quiet"] == 7, "a second run in the same week must not count twice"
    st = update_week_status(led, "2026-09-06", 0, 0, day + timedelta(days=49))
    assert st["quiet"] == 8 and st["complete"], "8th quiet week completes it"
    mid = {"dates": [], "rows": {}, "meta": {}}
    update_week_status(mid, "2026-09-06", 0, 0, date(2026, 11, 4))   # a Wednesday run
    st = update_week_status(mid, "2026-09-06", 0, 0, date(2026, 11, 9))
    assert st["quiet"] == 2, "a mid-week run and the next Monday each see a new NIH refresh"
    later = day + timedelta(days=49 + 7)
    assert date(2026, 9, 6) not in [s0 for s0, _ in weeks_to_check(led, later)], \
        "complete weeks skip the weekly check"
    assert date(2026, 9, 6) in [s0 for s0, _ in weeks_to_check(led, day + timedelta(days=49 + 28))], \
        "complete weeks get the monthly check"
    st = update_week_status(led, "2026-09-06", 2, 0, later)
    assert not st["complete"] and st["quiet"] == 0 and st["reopened"], "new awards reopen it"
    young = date(2026, 9, 20)
    st = update_week_status(led, "2026-09-20", 0, 0, day)
    st = update_week_status(led, "2026-09-20", 0, 3, day + timedelta(days=7))
    assert st["quiet"] == 0 and st["pending"] == 3, "a $0 listing keeps a young week open"
    old = young + timedelta(days=PENDING_GRACE_DAYS + 7)
    st = update_week_status(led, "2026-09-20", 0, 3, old)
    assert st["quiet"] == 1, "after the grace period a $0 listing no longer holds it open"

    # automatic fresh start from old daily-format data
    save_json(DATA_FILE, {"dates": ["2026-09-16"], "rows": {
        "x||y||z": {"university": "X", "department": "Y", "funder": "Z",
                    "cells": {"2026-09-16": [{"amount": 1, "label": "$1",
                                              "url": "u", "pi": "",
                                              "pi_email": ""}]}}}})
    save_json(SEEN_FILE, {"NIH:1": {}})

    wk1 = [
        make_award("NIH:111", "Brown University", "Biomedical Engineering",
                   4_600_000, "https://reporter.nih.gov/project-details/111",
                   "NIH (NIGMS)", "Jane Big"),
        make_award("NIH:112", "Brown University", "Biomedical Engineering",
                   500_000, "https://reporter.nih.gov/project-details/112",
                   "NIH (NIGMS)", "John Small"),
        make_award("NIH:222", "Tufts University", NO_DEPT_LABEL,
                   750_000, "https://reporter.nih.gov/project-details/222",
                   "NIH (NIAID)", "Ada Lovelace"),
    ]
    data, n1 = ingest(wk1, "2026-09-06")
    assert data["meta"]["schema"] == SCHEMA and n1 == 3, "fresh start failed"
    assert len(data["rows"]) == 2, "old-format rows were not cleared"
    assert "NIH:1" not in load_json(SEEN_FILE, {}), "seen not reset"

    data, n2 = ingest([wk1[0], make_award(
        "NIH:333", "Brown University", "Biomedical Engineering", 1_000_000,
        "https://reporter.nih.gov/project-details/333", "NIH (NCI)",
        "Amy New")], "2026-09-13")
    assert n2 == 1, "dedup failed"
    assert data["dates"] == ["2026-09-06", "2026-09-13"]
    bme = data["rows"]["brown university||biomedical engineering||nih (nigms)"]
    assert "2026-09-13" not in bme["cells"], "duplicate re-emitted"
    assert [e["amount"] for e in bme["cells"]["2026-09-06"]] == [4_600_000, 500_000]

    # reporter payload email extraction
    payload = {"contact_pi": {"full_name": "MILOSAVLJEVIC, ALEKSANDAR",
                              "contact_pi_email": "amilosav@bcm.edu"},
               "other": [{"email_notes": "none"}]}
    assert _walk_for_email(payload) == "amilosav@bcm.edu"
    assert _walk_for_email({"foo": "bar"}) == ""

    # pubmed extraction still guarded by name match
    xml = ("<PubmedArticleSet><PubmedArticle><MedlineCitation><Article>"
           "<AuthorList><Author><LastName>Big</LastName><ForeName>Jane</ForeName>"
           "<AffiliationInfo><Affiliation>Brown University. jane_big@brown.edu"
           "</Affiliation></AffiliationInfo></Author></AuthorList>"
           "</Article></MedlineCitation></PubmedArticle></PubmedArticleSet>")
    assert _extract_email(xml, [("Jane", "Big")]) == "jane_big@brown.edu"
    assert _extract_email(xml, [("Zed", "Nowhere")]) == ""

    # .org / .gov institutional addresses accepted, free-mail rejected
    assert _institutional("jmarkwelch@forsyth.org")
    assert _institutional("someone@nih.gov")
    assert not _institutional("someone@gmail.com")

    # compound surnames: both forms tried, PubMed's "Mark Welch" matches
    forms = _name_forms("Jessica Leigh Mark Welch")
    assert ("Jessica", "Welch") in forms and ("Jessica", "Mark Welch") in forms
    xml2 = ("<PubmedArticleSet><PubmedArticle><MedlineCitation><Article>"
            "<AuthorList><Author><LastName>Mark Welch</LastName>"
            "<ForeName>Jessica L</ForeName><AffiliationInfo><Affiliation>"
            "ADA Forsyth Institute, Somerville MA. jmarkwelch@forsyth.org"
            "</Affiliation></AffiliationInfo></Author></AuthorList>"
            "</Article></MedlineCitation><PubmedData><ArticleIdList>"
            "<ArticleId IdType=\"pmc\">PMC9999999</ArticleId>"
            "</ArticleIdList></PubmedData></PubmedArticle></PubmedArticleSet>")
    assert _extract_email(xml2, forms) == "jmarkwelch@forsyth.org", \
        "compound surname + .org email should match"
    assert _pmcids_in(xml2) == ["PMC9999999"], "PMC id extraction"

    # affiliation token picks the distinctive word
    assert _uni_affil_terms("Ada Forsyth Institute, Inc.") == "Forsyth"
    assert _uni_affil_terms("Johns Hopkins University") == "Hopkins"
    assert _uni_affil_terms("University of Minnesota") == "Minnesota"

    # email sweep with stubbed lookup (no network) + cache-flush check
    save_json(email_cache_path(), {
        "old hit|somewhere": {"email": "keep@x.edu", "src": "PubMed",
                              "checked": "2026-09-01"},
        "poisoned miss|somewhere": {"email": "", "src": "",
                                    "checked": "2026-09-01"},
    })
    orig_lookup = globals()["lookup_pi_email"]
    orig_batch = globals()["_batch_grant_pmids"]
    calls = []
    globals()["_batch_grant_pmids"] = lambda cores: {}
    globals()["lookup_pi_email"] = (
        lambda pi, uni, linked, stats, verbose=False:
        (calls.append(pi) or (("jbig@brown.edu", "PubMed") if pi == "Jane Big"
                              else ("", ""))))
    data = sweep_pi_emails(data)
    assert data["rows"]["brown university||biomedical engineering||nih (nigms)"][
        "cells"]["2026-09-06"][0]["pi_email"] == "jbig@brown.edu"
    flushed = load_json(email_cache_path(), {})
    assert "old hit|somewhere" in flushed, "legacy flush must keep confirmed hits"
    assert "poisoned miss|somewhere" not in flushed, "legacy flush must drop misses"
    assert flushed.get("_meta", {}).get("v") == CACHE_VERSION
    assert len(calls) == 4, f"expected 4 fresh lookups, got {len(calls)}"

    # re-run: every PI is now cached (hit or fresh miss) -> ZERO lookups
    calls.clear()
    data = sweep_pi_emails(data)
    assert calls == [], f"re-run must not query anything, but queried {calls}"
    globals()["lookup_pi_email"] = orig_lookup
    globals()["_batch_grant_pmids"] = orig_batch

    # awards recorded before notice dates were kept get theirs on the next check
    filled = dict(wk1[0], notice="2026-09-08")
    data, n_fill = ingest([filled], "2026-09-06")
    assert n_fill == 0, "a notice-date fill is not a new award"
    big = data["rows"]["brown university||biomedical engineering||nih (nigms)"]["cells"]["2026-09-06"][0]
    assert big["notice"] == "2026-09-08" and big["late"] is False and big["added"]

    # an award that turns up after its week was first checked is tagged late
    later_day = date.today() + timedelta(days=7)
    data, n_late = ingest([make_award(
        "NIH:555", "Tufts University", NO_DEPT_LABEL, 90_000,
        "https://reporter.nih.gov/project-details/555", "NIH (NIAID)", "Ada Lovelace",
        notice="2026-09-10")], "2026-09-06", later_day)
    assert n_late == 1
    e555 = [e for e in data["rows"]["tufts university||--||nih (niaid)"]["cells"]["2026-09-06"]
            if e["url"].endswith("/555")][0]
    assert e555["late"] is True and e555["added"] == later_day.isoformat(), "late flag"

    # added dates for awards recorded before this build come from the repo history
    hist_led = {"dates": ["2026-09-06"], "meta": {}, "rows": {"r": {"cells": {"2026-09-06": [
        {"amount": 1, "url": "https://reporter.nih.gov/project-details/901"},
        {"amount": 2, "key": "NIH:902", "url": "x"}]}}}}
    save_json(history_path(), {"week_first_checked": {"2026-09-06": "2026-09-16"},
                               "late": {"2026-11-03": [902]}})
    apply_added_history(hist_led)
    h1, h2 = hist_led["rows"]["r"]["cells"]["2026-09-06"]
    assert h1["added"] == "2026-09-16" and h1["late"] is False, "on-time award from history"
    assert h2["added"] == "2026-11-03" and h2["late"] is True, "late award from history"
    assert hist_led["meta"]["weeks"]["2026-09-06"]["first_checked"] == "2026-09-16"
    history_path().unlink()

    # emails revealed on NIH's pages (data/nih_emails.csv) win over paper-found ones;
    # junk lines are ignored
    nih_emails_path().write_text(
        'appl_id,email,pi,date\n222,ada@tufts.edu,"LOVELACE, ADA",2026-11-04\n'
        'not-an-id,x@y.edu\n111,not an email\n\n', encoding="utf-8")
    assert load_nih_emails() == {"222": "ada@tufts.edu"}
    # emails (and notes) typed by hand into the links list
    nih_links_path().write_text(
        ",".join(NIH_LINKS_HEADER) + "\n"
        '333,Amy New,Brown University,x,x,2026-09-13,"$1,000,000",u,not available\n'
        '112,John Small,Brown University,x,x,2026-09-06,"$500,000",u, jsmall@brown.edu \n',
        encoding="utf-8")
    assert load_nih_emails() == {"222": "ada@tufts.edu", "112": "jsmall@brown.edu"}

    write_site(data)
    import csv
    links = list(csv.DictReader(nih_links_path().read_text(encoding="utf-8").splitlines()))
    by_pi = {r["pi"]: r for r in links}
    assert len(links) == 4 and set(by_pi) == {"Jane Big", "John Small", "Ada Lovelace", "Amy New"}, \
        "one row per PI"
    assert links[0]["pi"] == "Amy New", "newest award first"
    assert by_pi["Jane Big"]["nih_link"] == "https://reporter.nih.gov/project-details/111#details" \
        and by_pi["Jane Big"]["nih_email"] == "", "link opens at View Email; email blank to fill in"
    assert by_pi["John Small"]["nih_email"] == "jsmall@brown.edu", "typed email kept"
    assert by_pi["Amy New"]["nih_email"] == "not available", "typed note kept as typed"
    small = [e for e in data["rows"]["brown university||biomedical engineering||nih (nigms)"][
        "cells"]["2026-09-06"] if e["pi"] == "John Small"][0]
    assert small["nih_email"] == "jsmall@brown.edu", "a typed email reaches the award"
    assert by_pi["Ada Lovelace"]["appl_id"] == "555" and by_pi["Ada Lovelace"]["nih_email"] \
        == "ada@tufts.edu", "a PI's newest award, with the email recorded on an older one"
    assert by_pi["Jane Big"]["amount"] == "$4,600,000" and by_pi["Jane Big"]["notice_date"] == "2026-09-08"
    mdl = nih_links_md_path().read_text(encoding="utf-8")
    assert "| 1 | Amy New | Brown University | 2026-09-13 | \\$1,000,000 | " \
        "[View email](https://reporter.nih.gov/project-details/333#details) | not available |" in mdl
    assert "[View email](https://reporter.nih.gov/project-details/111#details) |  |" in mdl, \
        "blank email cell"
    assert "4 PIs, newest award first. Email recorded for 3." in mdl
    assert "line **N+1**" in mdl and NIH_LINKS_EDIT_URL in mdl
    page = (DOCS_DIR / "index.html").read_text()
    main_head = page.split('<table id="t">')[1].split("</thead>")[0]
    assert main_head.count("<th ") == 5, "expected 3 label ths + 2 week ths (# th has no attrs)"
    assert "Sep 6 – Sep 12" in page and "Sep 13 – Sep 19" in page
    assert '<td class="rownum">1</td>' in page and '<td class="rownum">3</td>' in page
    assert 'id="onlyemail"' in page and 'data-he="1"' in page and 'data-he="0"' in page
    assert 'id="onlyname"' in page and 'id="onlyinst"' in page, "new checkboxes"
    assert 'id="onlylinked"' in page and 'id="onlyrecent"' in page, "publication checkboxes"
    assert 'id="weeksmenu"' in page and 'data-quick="last4"' in page, "weeks menu"
    assert page.index('value="2026-09-13"') < page.index('value="2026-09-06"'), \
        "weeks menu lists the newest week first"
    assert 'id="filtermodal"' in page and 'id="hidezero"' in page, "filters popup"
    assert 'id="rangefrom"' in page and 'id="rangeto"' in page, "notice-date range"
    assert '<option value="2026-09-10">Sep 10</option>' in page, "range lists days with awards"
    assert 'id="onlylate"' in page and 'id="showlate"' in page, "late filter + header line"
    assert "1 award was added late" in page, "header late line"
    assert 'data-status="open"' in page and 'data-end="2026-09-12"' in page, "week status attrs"
    assert "Award Notice Date: Sep 8" in page and 'data-n="2026-09-08"' in page, "notice date"
    assert "Added: " in page and 'data-late="1"' in page, "late tag"
    assert 'id="cards"' in page and 'id="tablewrap"' in page, "card view + table view"
    assert ">Author Order<" in page and "Author Contribution" not in page, "renamed header"
    assert '<th data-t="text">Institution</th>' in page, "Institution column"
    assert "⇅" in page and "1C5" not in page, "popup arrows must be real glyphs"
    css = page.split("<style>")[1].split("</style>")[0]
    leaks = [ln.strip() for ln in css.splitlines()
             if re.match(r"^(table|th|td|tr)[\s,{:.\[]", ln.strip())]
    assert not leaks, f"unscoped table CSS would leak into the popup: {leaks}"
    js = page.split("<script>")[1]
    assert "((f.linked && d.pl === '1') || (f.recent && d.pr === '1'))" in js, \
        "the two publication filters must combine as OR"
    assert "f.email && d.he !== '1'" in js and "f.name && d.hn !== '1'" in js \
        and "f.inst && d.hi !== '1'" in js, "email / name / institution filters"
    assert "mode: 'last4'" in js and "hide: true" in js, \
        "defaults: last 4 weeks, rows with no award hidden"
    assert "localStorage" in js and "PAGE = 25" in js, "remembered choices, top 25 per week"
    assert "S.mode === 'range'" in js and "S.f.late" in js, "range + late filters in the script"
    ledger_js = js.split("// ---- publication popup ----")[0]
    assert "innerText" not in ledger_js, "row text must be read once (innerText per row froze the page)"
    assert "<tr data-he=" not in page and ' data-pl="' in page.split("<tbody>")[1] \
        and ' data-he="1"' in page, "flags must live on cells, not rows"
    demo = _cell_html([{"amount": 1, "label": "$1", "url": "u", "pi": "Amy New",
                        "pi_email": "", "core": "R01Y"}], "Tufts University",
                      {"amy new|tufts university": {"match": "exact", "rows": [
                          {"year": 2024, "position": "first", "title": "R", "url": ""}],
                          "checked": "2026-09-20"}},
                      {"R01Y": {"pmid": None, "checked": "2026-09-20"}})
    assert 'href="#">Publications Within The Past 5 Years? Yes</a>' in demo, \
        "whole 5-year line must be the popup link"
    demo2 = _cell_html([{"amount": 5, "label": "$5", "url": "u", "notice": "2026-09-16",
                         "added": "2026-11-03", "late": True}], "X")
    assert "Award Notice Date: Sep 16" in demo2 and "Added: Nov 3" in demo2 \
        and 'data-late="1"' in demo2 and 'data-a="5"' in demo2, "award line dates"
    for need in ["Abstract (Yes/No)", 'class="abslink"', 'id="absmodal"', "k === 'abs'",
                 "europepmc/webservices/rest/search"]:
        assert need in page, f"abstract feature markup missing: {need}"
    fx = ("<PubmedArticleSet><PubmedArticle><MedlineCitation><PMID>9</PMID><Article>"
          "<Abstract><AbstractText>t</AbstractText></Abstract></Article></MedlineCitation>"
          "<PubmedData><ArticleIdList><ArticleId IdType=\"doi\">10.1/X</ArticleId>"
          "</ArticleIdList></PubmedData></PubmedArticle></PubmedArticleSet>")
    assert _parse_pubmed_flags(fx) == {"9": {"doi": "10.1/x", "abs": True}}
    assert _pmid_from_ids({"ids": {"pmid": "https://pubmed.ncbi.nlm.nih.gov/77"}}) == "77"
    assert ' data-pr="1"' in demo and ' data-pl="0"' in demo, "cell flags for a recent cell"
    assert 'data-hn="1"' in page and 'data-hi="1"' in page, "row flags"
    assert '<div class="pinst">Institution: Brown University</div>' in page, "institution line"
    assert '<div class="tot">= $5,100,000</div>' in page
    assert "PI Name: Jane Big" in page and 'href="mailto:jbig@brown.edu"' in page
    assert 'href="mailto:jbig@brown.edu">jbig@brown.edu</a> <span class="esrc paper"' in page, \
        "paper-found emails are labelled"
    assert 'href="mailto:ada@tufts.edu">ada@tufts.edu</a> <span class="esrc nih"' in page, \
        "an NIH email wins and is tagged NIH"
    assert 'href="https://reporter.nih.gov/project-details/111#details"' in page, \
        "PIs without an NIH email get the View email on NIH link"
    assert "project-details/222#details" not in page, "no NIH link once the NIH email is in"
    assert 'data-nih="1"' in page and 'data-nih="0"' in page and 'id="onlynonih"' in page
    assert "NIH emails collected for 2 of 4 PIs" in page, "progress line"
    assert "project-details/333#details" in page, "a note that isn't an email keeps the link"
    assert f'href="{NIH_LINKS_URL}"' in page, "header links to the clickable list"
    assert all(e.get("nih_email") == "ada@tufts.edu"
               for e in data["rows"]["tufts university||--||nih (niaid)"]["cells"]["2026-09-06"]), \
        "the PI's other award at the same university gets the NIH email too"
    assert "f.nonih && d.nih === '1'" in js, "No NIH email yet filter"
    assert "PI Email: --" in page
    assert 'data-v="0">0<' in page
    assert "r.tr.cells[0].textContent = ++n" in js, "table rows renumber after sort/filter"
    assert 'data-total="5940000"' in page, \
        "week header must carry its own total (4.6M + 0.5M + 0.75M + the late 90K)"
    assert 'data-label="Sep 6 – Sep 12"' in page, "week header label missing"
    assert 'id="fig"' in page and 'id="cap"' in page, "summary ids missing"
    assert page.count('<th class="num" data-w="') == 2, "one header per week"
    assert 'data-short="Sep 6–12"' in page, "short week label for phone tabs"
    assert "@media (max-width: 700px)" in page and "#t td.dept, #t td.funder { position:static; }" in page, \
        "phone layout must unpin Department and Funder"

    data, n3 = ingest([wk1[0]], "2026-09-13")
    assert n3 == 0 and data["dates"].count("2026-09-13") == 1, "rerun broke"

    # re-check: an award NIH posts late lands in its own (older) week, once
    late = make_award("NIH:444", "Tufts University", NO_DEPT_LABEL, 250_000,
                      "https://reporter.nih.gov/project-details/444", "NIH (NIAID)",
                      "Ada Lovelace")
    data, n4 = ingest([wk1[0], late], "2026-09-06")
    assert n4 == 1, "only the late award is new"
    tufts = data["rows"]["tufts university||--||nih (niaid)"]["cells"]["2026-09-06"]
    assert [e["amount"] for e in tufts] == [750_000, 90_000, 250_000], "late award joins its own week"
    assert data["dates"] == ["2026-09-06", "2026-09-13"], "no extra columns"
    data, n5 = ingest([late], "2026-09-06")
    assert n5 == 0, "re-checking the same week again adds nothing"

    print(f"SELFTEST PASSED  (artifacts in {tmp})")
    return 0


# ----------------------------------------------------------------------------

def main() -> int:
    global ENABLE_EMAIL_LOOKUP
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--skip-emails", action="store_true",
                    help="update the table only; skip the PI-email sweep")
    ap.add_argument("--emails-only", action="store_true",
                    help="skip the NIH fetch; run the enrichment stage only "
                         "(PI emails, then publications) and re-render")
    ap.add_argument("--skip-pubs", action="store_true",
                    help="skip the OpenAlex publications stage")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if args.skip_emails:
        ENABLE_EMAIL_LOOKUP = False
    if args.emails_only:  # the enrichment stage: emails, then publications
        data = load_json(DATA_FILE, {"dates": [], "rows": {}, "meta": {}})
        data = sweep_pi_emails(data)
        if not args.skip_pubs:
            data = sweep_publications(data)
        write_site(data)
        return 0

    today = date.today()
    data = load_json(DATA_FILE, {"dates": [], "rows": {}, "meta": {}})
    due = weeks_to_check(data, today)
    log(f"checking {len(due)} week(s) (awards by NIH award-notice date; "
        f"ones already recorded are skipped; complete weeks wait for their monthly check)")
    added = 0
    for start, end in due:
        awards, pending = fetch_nih_week(start, end)
        data, new = ingest(awards, start.isoformat(), today)
        st = update_week_status(data, start.isoformat(), new, pending, today)
        save_json(DATA_FILE, data)
        added += new
        log(f"  week of {start}: {len(awards)} matched, {new} new, {pending} waiting for "
            f"an amount - " + ("complete" if st.get("complete") else
                               f"quiet {st.get('quiet', 0)} of {QUIET_CHECKS_TO_COMPLETE}"))
        time.sleep(1)
    log(f"NIH RePORTER: {added} new award(s) across {len(due)} week(s)")
    data = sweep_pi_emails(data)
    if ENABLE_EMAIL_LOOKUP and not args.skip_pubs:
        data = sweep_publications(data)
    write_site(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
