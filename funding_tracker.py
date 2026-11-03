#!/usr/bin/env python3
"""
Biotech Department Funding Tracker  (weekly, NIH-only)
======================================================
Runs once a week (GitHub Actions, Monday morning, right after NIH RePORTER's
Sunday-night refresh) and maintains a ledger of NIH awards GRANTED during the
just-completed week to biotech-adjacent university departments.

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


def wlabel(week_start_iso: str) -> str:
    s = datetime.strptime(week_start_iso, "%Y-%m-%d").date()
    e = s + timedelta(days=6)
    return f"{s:%b} {s.day} – {e:%b} {e.day}"


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
               pi_email="", core=""):
    return {
        "key": key, "university": university, "department": department,
        "funder": funder, "amount": int(amount), "url": url, "pi": pi,
        "pi_email": pi_email, "core": core,
    }


def fetch_nih_week(week_start: date, week_end: date):
    awards, offset = [], 0
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
        r = SESSION.post("https://api.reporter.nih.gov/v2/projects/search",
                         json=payload, timeout=TIMEOUT)
        r.raise_for_status()
        results = r.json().get("results", [])
        if offset == 0 and results:
            # One-time diagnostic: does the public API carry PI emails at all?
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
            if amt < 1 or not name:
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
            ic = (p.get("agency_ic_admin") or {}).get("abbreviation") or ""
            funder = (ic if ic in NON_NIH_AGENCIES
                      else f"NIH ({ic})" if ic else "NIH")
            appl = p.get("appl_id")
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
            ))
        if len(results) < 500:
            break
        offset += 500
        time.sleep(1)
    return awards


# ----------------------------------------------------------------------------
# LEDGER
# ----------------------------------------------------------------------------

def row_key(a) -> str:
    return f"{a['university'].lower()}||{a['department'].lower()}||{a['funder'].lower()}"


def ingest(awards, week_start_iso: str):
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

    if week_start_iso not in data["dates"]:
        data["dates"].append(week_start_iso)
        data["dates"].sort()

    new_count = 0
    for a in awards:
        if a["key"] in seen:
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
            "core": a.get("core", ""),
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
    lines = "".join(
        f'<div class="aline"><a href="{html_lib.escape(e["url"], quote=True)}" '
        f'target="_blank" rel="noopener">{html_lib.escape(e["label"])}</a></div>'
        for e in entries
    )
    if len(entries) > 1:
        lines += f'<div class="tot">= {fmt_money(total)}</div>'
    top = max(entries, key=lambda e: e["amount"])
    pi_name = html_lib.escape(top.get("pi") or "--")
    email = (top.get("pi_email") or "").strip()
    if email:
        via = html_lib.escape(top.get("pi_email_via") or "")
        t = f' title="source: {via}"' if via else ""
        pi_mail = (f'<a href="mailto:{html_lib.escape(email, quote=True)}"{t}>'
                   f"{html_lib.escape(email)}</a>")
    else:
        pi_mail = "--"
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
    lines += (f'<div class="pib">PI Name: {pi_name}<br>PI Email: {pi_mail}'
              f'<br>Institution: {inst}<br>{pub_line}</div>')
    # per-cell flags: the checkboxes and the count apply to the selected week
    flags = (f' data-he="{1 if email else 0}"'
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

MOBILE_CSS = """
/* ---- phones: pin only # + Institution so the week columns have room ---- */
@media (max-width: 700px) {
  .mast { padding:16px 14px 12px; gap:12px; }
  .mast h1 { font-size:19px; }
  .todaybox { text-align:left; width:100%; }
  .todaybox .fig { font-size:24px; }
  .emailchk { justify-content:flex-start; margin-top:2px; padding:4px 0; font-size:13.5px; }
  .emailchk:first-of-type { margin-top:6px; }
  .emailchk input { width:18px; height:18px; flex:none; }
  .controls { padding:0 14px 10px; gap:6px; }
  .controls input { width:100%; font-size:16px; }  /* 16px stops iOS zooming in on focus */
  .controls span { font-size:12px; }
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
  .legend { padding:12px 14px 32px; }
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


def render_html(data, pubs=None, linked=None) -> str:
    pubs, linked = pubs or {}, linked or {}
    weeks = data["dates"]
    latest = weeks[-1] if weeks else None
    rows = sorted(
        data["rows"].values(),
        key=lambda r: (r["university"].lower(), r.get("funder", "").lower(),
                       r["department"].lower()),
    )

    week_totals = {
        w: sum(e["amount"] for r in rows for e in r["cells"].get(w, []))
        for w in weeks}
    total_latest = week_totals.get(latest, 0)
    n_awards = sum(len(v) for r in rows for v in r["cells"].values())

    head_cells = "".join(
        f'<th class="num{" sel" if w == latest else ""}" data-t="num" '
        f'data-total="{week_totals[w]}" data-label="{wlabel(w)}" '
        f'title="{w}">{wlabel(w)}</th>' for w in weeks)

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
:root {{
  --bg:#FBFBFD; --ink:#101418; --line:#CDD3DA; --mut:#8A939C;
  --grow:#1B7A43; --broth:#EAF6EF; --grid:#C3CAD2; --gridstrong:#8F98A3;
  --caution:#A5670A;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink);
  font:15px/1.45 "IBM Plex Sans", system-ui, sans-serif; }}
a {{ color:var(--grow); text-decoration:none; border-bottom:1px solid #BFE0CC; }}
a:hover {{ border-bottom-color:var(--grow); }}
.mast {{ display:flex; flex-wrap:wrap; gap:24px; align-items:flex-end;
  justify-content:space-between; padding:28px 32px 18px; }}
.mast h1 {{ margin:0; font-size:22px; font-weight:600; letter-spacing:-.01em; }}
.mast .sub {{ color:var(--mut); font-size:13px; margin-top:4px; }}
.todaybox {{ text-align:right; }}
.todaybox .fig {{ font-size:26px; font-weight:600; color:var(--grow);
  font-variant-numeric:tabular-nums; }}
.todaybox .cap {{ font-size:12px; color:var(--mut); }}
.emailchk {{ display:flex; gap:7px; align-items:center; justify-content:flex-end;
  margin-top:6px; font-size:13px; color:var(--ink); cursor:pointer;
  user-select:none; }}
.emailchk:first-of-type {{ margin-top:9px; }}
.colcount {{ margin-top:10px; padding-top:8px; font-size:13px; color:var(--mut);
  border-top:1px solid var(--line); }}
.colcount b {{ color:var(--ink); font-variant-numeric:tabular-nums; }}
.colhint {{ font-size:11.5px; color:var(--mut); margin-top:3px; }}
.emailchk input {{ width:15px; height:15px; accent-color:var(--grow);
  cursor:pointer; }}
.controls {{ display:flex; gap:16px; align-items:center; flex-wrap:wrap;
  padding:0 32px 14px; font-size:13px; color:var(--mut); }}
.controls input {{ font:inherit; color:var(--ink); padding:7px 10px;
  border:1px solid var(--line); border-radius:6px; background:#fff; width:260px; }}
.controls input:focus {{ outline:2px solid var(--broth); border-color:var(--grow); }}
.wrap {{ overflow:auto; max-height:calc(100vh - 150px);
  border-top:1px solid var(--line); }}
/* every rule below is scoped to #t so none of it leaks into the popup table */
#t {{ border-collapse:separate; border-spacing:0; min-width:100%;
  font-variant-numeric:tabular-nums; }}
#t th, #t td {{ padding:9px 14px; border-bottom:1px solid var(--line);
  white-space:nowrap; text-align:right; font-size:14px; vertical-align:top; }}
.aline {{ display:block; line-height:1.6; }}
.tot {{ display:inline-block; border-top:1px solid var(--ink); margin-top:3px;
  padding-top:3px; font-weight:600; }}
.pib {{ margin-top:7px; font-size:12px; color:var(--mut); font-weight:400;
  line-height:1.5; }}
.pib a {{ color:var(--grow); border-bottom-color:#BFE0CC; }}
#t th {{ position:sticky; top:0; background:var(--bg); z-index:3; cursor:pointer;
  font-weight:500; user-select:none; }}
#t th::after {{ content:"⇅"; margin-left:6px; font-size:11px; color:var(--mut); }}
#t th:first-child {{ cursor:default; }}
#t th:first-child::after {{ content:""; }}
#t th.sorted {{ box-shadow:inset 0 -2px 0 var(--grow); color:var(--grow); }}
#t th.sorted.asc::after {{ content:"▲"; color:var(--grow); }}
#t th.sorted.desc::after {{ content:"▼"; color:var(--grow); }}
#t th:hover {{ color:var(--grow); }}
#t th.sel {{ background:var(--broth); }}
#t th:nth-child(1), #t td.rownum {{ position:sticky; left:0; background:var(--bg);
  text-align:right; min-width:48px; max-width:48px; z-index:2;
  color:var(--mut); font-size:12.5px; }}
#t th:nth-child(2), #t td.uni {{ position:sticky; left:48px; background:var(--bg);
  text-align:left; min-width:220px; max-width:220px; white-space:normal;
  z-index:2; font-weight:500; }}
#t th:nth-child(3), #t td.dept {{ position:sticky; left:268px; background:var(--bg);
  text-align:left; min-width:200px; max-width:200px; white-space:normal;
  z-index:2; color:#3C444C; }}
#t th:nth-child(4), #t td.funder {{ position:sticky; left:468px; background:var(--bg);
  text-align:left; min-width:130px; max-width:170px; white-space:normal;
  z-index:2; box-shadow:2px 0 0 var(--line); }}
#t th:nth-child(-n+4) {{ z-index:4; }}
#t td[data-v]:not(.zero) {{ background:#fff; }}
#t tr:hover td {{ background:#F3F7F4; }}
.zero {{ color:var(--mut); }}
.legend {{ padding:12px 32px 40px; color:var(--mut); font-size:12.5px; }}
{POPUP_CSS}
{MOBILE_CSS}
</style></head><body>

<div class="mast">
  <div>
    <h1>Biotech department funding ledger</h1>
    <div class="sub">NIH awards granted each week to biotech-adjacent
      university departments &middot; updated {generated} &middot;
      {len(rows)} rows &middot; {n_awards} awards tracked</div>
  </div>
  <div class="todaybox">
    <div class="fig" id="fig">{fmt_money(total_latest)}</div>
    <div class="cap">granted <span id="cap">{wlabel(latest) if latest else "-"}</span></div>
    <label class="emailchk"><input type="checkbox" id="onlyemail">
      Only show rows with a PI email</label>
    <label class="emailchk"><input type="checkbox" id="onlyname">
      Only show rows with a PI name</label>
    <label class="emailchk"><input type="checkbox" id="onlyinst">
      Only show rows with an institution</label>
    <label class="emailchk"><input type="checkbox" id="onlylinked">
      Only rows with publication linked to grant</label>
    <label class="emailchk"><input type="checkbox" id="onlyrecent">
      Only rows with publications within the last 5 years</label>
    <div class="colcount">Total Count For Column = <b id="colcount">0</b>
      <div class="colhint">Checkboxes and count apply to the highlighted week
        &mdash; click a week header to switch</div></div>
  </div>
</div>

<div class="controls">
  <input id="q" type="search" placeholder="Filter by institution, department or funder"
    aria-label="Filter rows">
  <span>The ⇅ arrows mean a column is sortable &mdash; click to sort,
    click again to reverse</span>
</div>

<div class="wrap"><table id="t">
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

<div class="legend">Each column is one completed Sunday&ndash;Saturday week;
an award appears in the week of its NIH award-notice date, once, so amounts
are never double-counted. 0 = no award granted to that row that week. Cells
with several awards list each one and end with a ruled, unlinked
<b>= total</b>. A <b>--</b> Department means NIH reported no department for
that keyword-matched award. PI lines show the contact PI of the cell's
largest award; emails come from NIH's own project record when available,
otherwise from the PI's recent PubMed publications (hover an email to see
which), and -- means neither source had one yet. Row numbers always follow
the current sort and filter. Source: NIH RePORTER (refreshes Sunday nights;
this page updates every Monday morning).</div>

{POPUP_HTML}

<script>
const table = document.getElementById('t');
const tbody = table.tBodies[0];
const ths = [...table.tHead.rows[0].cells];
const q = document.getElementById('q');
const only = document.getElementById('onlyemail');
const onlyName = document.getElementById('onlyname');
const onlyInst = document.getElementById('onlyinst');
const onlyLinked = document.getElementById('onlylinked');
const onlyRecent = document.getElementById('onlyrecent');
const countEl = document.getElementById('colcount');
let cur = {{ i:-1, dir:1 }};
// search text per row, read once: reading innerText inside the filter loop
// forced a full page re-layout for every row (seconds per keystroke)
const hayOf = new Map([...tbody.rows].map(r => [r,
  (r.cells[1].textContent + ' ' + r.cells[2].textContent + ' '
   + r.cells[3].textContent).toLowerCase()]));

function renumber() {{
  let n = 0;
  [...tbody.rows].forEach(r => {{
    if (r.style.display !== 'none') r.cells[0].textContent = ++n;
  }});
}}
function updateCount() {{
  // cells with funding in the currently selected week, among visible rows
  const sel = ths.findIndex(h => h.classList.contains('sel'));
  let n = 0;
  if (sel >= 0) {{
    [...tbody.rows].forEach(r => {{
      if (r.style.display === 'none') return;
      const c = r.cells[sel];
      if (c && +c.dataset.v > 0) n++;
    }});
  }}
  countEl.textContent = n.toLocaleString('en-US');
}}
function applyFilter() {{
  // every checkbox is evaluated against the SELECTED (highlighted) week column
  const v = q.value.toLowerCase();
  const sel = ths.findIndex(h => h.classList.contains('sel'));
  const anyPub = onlyLinked.checked || onlyRecent.checked;
  [...tbody.rows].forEach(r => {{
    const hay = hayOf.get(r);
    const c = sel >= 0 ? r.cells[sel] : null;
    const has = k => !!c && c.dataset[k] === '1';
    const ok = hay.includes(v)
      && (!only.checked || has('he'))
      && (!onlyName.checked || has('hn'))
      && (!onlyInst.checked || has('hi'))
      // linked + recent combine as OR, so both boxes = linked count + recent count
      && (!anyPub || (onlyLinked.checked && has('pl')) || (onlyRecent.checked && has('pr')));
    r.style.display = ok ? '' : 'none';
  }});
  renumber();
  updateCount();
}}
ths.forEach((th, i) => {{
  if (i === 0) return; // the # column reflects position, it never sorts
  th.addEventListener('click', () => {{
    const num = th.dataset.t === 'num';
    cur.dir = (cur.i === i) ? -cur.dir : (num ? -1 : 1);
    cur.i = i;
    ths.forEach(h => h.classList.remove('sorted', 'asc', 'desc'));
    th.classList.add('sorted', cur.dir === 1 ? 'asc' : 'desc');
    if (num && th.dataset.total !== undefined) {{
      ths.forEach(h => h.classList.remove('sel'));
      th.classList.add('sel');
      document.getElementById('fig').textContent =
        '$' + (+th.dataset.total).toLocaleString('en-US');
      document.getElementById('cap').textContent = th.dataset.label;
    }}
    const rows = [...tbody.rows];
    rows.sort((a, b) => {{
      if (num) {{
        return ((+a.cells[i].dataset.v || 0) - (+b.cells[i].dataset.v || 0)) * cur.dir;
      }}
      return a.cells[i].innerText.localeCompare(b.cells[i].innerText) * cur.dir;
    }});
    rows.forEach(r => tbody.appendChild(r));
    renumber();
    if (num) applyFilter(); else updateCount();
  }});
}});
let qTimer;
q.addEventListener('input', () => {{ clearTimeout(qTimer); qTimer = setTimeout(applyFilter, 150); }});
only.addEventListener('change', applyFilter);
onlyName.addEventListener('change', applyFilter);
onlyInst.addEventListener('change', applyFilter);
onlyLinked.addEventListener('change', applyFilter);
onlyRecent.addEventListener('change', applyFilter);
applyFilter();
// phones: open with the highlighted (newest) week next to the pinned Institution column
if (window.matchMedia('(max-width: 700px)').matches) {{
  const sel = ths.find(h => h.classList.contains('sel'));
  if (sel) document.querySelector('.wrap').scrollLeft +=
    sel.getBoundingClientRect().left - ths[1].getBoundingClientRect().right;
}}
{POPUP_JS}
</script>
</body></html>"""


def write_site(data) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
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

    write_site(data)
    page = (DOCS_DIR / "index.html").read_text()
    main_head = page.split('<table id="t">')[1].split("</thead>")[0]
    assert main_head.count("<th ") == 5, "expected 3 label ths + 2 week ths (# th has no attrs)"
    assert "Sep 6 – Sep 12" in page and "Sep 13 – Sep 19" in page
    assert '<td class="rownum">1</td>' in page and '<td class="rownum">3</td>' in page
    assert 'id="onlyemail"' in page and 'data-he="1"' in page and 'data-he="0"' in page
    assert 'id="onlyname"' in page and 'id="onlyinst"' in page, "new checkboxes"
    assert 'id="onlylinked"' in page and 'id="onlyrecent"' in page, "publication checkboxes"
    assert 'Total Count For Column = <b id="colcount">' in page, "column count"
    assert ">Author Order<" in page and "Author Contribution" not in page, "renamed header"
    assert '<th data-t="text">Institution</th>' in page, "Institution column"
    assert "⇅" in page and "1C5" not in page, "popup arrows must be real glyphs"
    css = page.split("<style>")[1].split("</style>")[0]
    leaks = [ln.strip() for ln in css.splitlines()
             if re.match(r"^(table|th|td|tr)[\s,{:.\[]", ln.strip())]
    assert not leaks, f"unscoped table CSS would leak into the popup: {leaks}"
    js = page.split("<script>")[1]
    assert "(onlyLinked.checked && has('pl')) || (onlyRecent.checked && has('pr'))" in js, \
        "the two publication filters must combine as OR"
    assert "ths.findIndex(h => h.classList.contains('sel'))" in js.split("function applyFilter")[1], \
        "filters must be evaluated against the selected week column"
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
    assert "Institution: Brown University" in page, "institution line"
    assert "onlyName.checked" in page and "onlyInst.checked" in page, "combined filter"
    assert '<div class="tot">= $5,100,000</div>' in page
    assert "PI Name: Jane Big" in page and 'href="mailto:jbig@brown.edu"' in page
    assert 'title="source: PubMed"' in page
    assert "PI Email: --" in page
    assert 'data-v="0">0<' in page
    assert "renumber" in page and "r.cells[0].textContent" in page
    assert 'data-total="5850000"' in page, \
        "week header must carry its own total (4.6M + 0.5M + 0.75M)"
    assert 'data-label="Sep 6 – Sep 12"' in page, "week header label missing"
    assert 'id="fig"' in page and 'id="cap"' in page, "summary ids missing"
    assert page.count('class="num sel"') == 1, "exactly one week highlighted"
    assert "@media (max-width: 700px)" in page and "#t td.dept, #t td.funder { position:static; }" in page, \
        "phone layout must unpin Department and Funder"
    assert "hayOf.get(r)" in js and "innerText + ' ' + r.cells[2]" not in js, \
        "filter must use cached row text (innerText per row froze the page)"

    data, n3 = ingest([wk1[0]], "2026-09-13")
    assert n3 == 0 and data["dates"].count("2026-09-13") == 1, "rerun broke"

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

    start, end = last_completed_week(date.today())
    log(f"target week: {start} .. {end} (awards by NIH award-notice date)")
    awards = fetch_nih_week(start, end)
    log(f"NIH RePORTER: {len(awards)} award(s) matched filters")
    data, _ = ingest(awards, start.isoformat())
    data = sweep_pi_emails(data)
    if ENABLE_EMAIL_LOOKUP and not args.skip_pubs:
        data = sweep_publications(data)
    write_site(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
