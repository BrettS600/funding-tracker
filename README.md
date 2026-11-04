# Biotech Department Funding Tracker

A self-updating ledger of NIH awards granted each week to biotech-adjacent
university departments, with the contact PI's email, institution, and
recent publications. Runs automatically **every Monday morning** on
GitHub's servers (right after NIH RePORTER's Sunday-night refresh) and
publishes a sortable, filterable page to GitHub Pages — nothing to install
or run on your own computer.

**Live page:** https://biotech-funding-ledger.github.io/funding-tracker/

Personal project built from public data sources (NIH RePORTER, PubMed,
Europe PMC, OpenAlex). Not affiliated with or endorsed by any employer.

## What the page shows

**One column per completed Sunday–Saturday week** ("Sep 13 – Sep 19"). An
award lands in the week of its NIH award-notice date, exactly once — never
double-counted. Each Monday run adds the just-finished week.

**Fixed columns:** # (position number that always follows the current
sort and filter), Institution (the awardee organization NIH lists),
Department (NIH's standardized name, or `--` when NIH reported none and the
award matched the bio keywords), Funder (the NIH institute, e.g.
*NIH (NIGMS)*).

**Each funding cell** stacks the week's awards (each amount linked to its
RePORTER project page) and ends with a ruled unlinked **= total** when there
are several. Every award, not just the largest, shows its own contact PI:

- **PI Name** — the contact PI NIH lists for that award.
- **NIH PI Email** — the email revealed by the View Email button on the
  award's NIH project page, once you've recorded it in `data/nih_links.csv`
  (or `data/nih_emails.csv`). One recorded email covers every award that PI
  has at the same institution.
- **Predicted Email** — shown until an NIH email is recorded: an address
  found in the PI's own publications (PubMed affiliation lines, then Europe
  PMC open-access full text), or `--` when none was found. It may belong to
  someone with a similar name, so verify before sending. **View email on
  NIH** next to it opens the project page at the View Email button.
- **Publication line** — one of three:
  - **Publication Linked To Grant** (clickable) when NIH links a paper to
    the grant: opens a one-row popup with that paper (the most recent, any
    year).
  - **Publications Within The Past 5 Years? Yes** (whole line clickable)
    when no paper is linked but OpenAlex finds the PI's recent journal
    articles. A **?** marks an uncertain name match — check the affiliations
    in the popup.
  - **Publications Within The Past 5 Years? No** (plain) when neither
    exists, or *not checked yet* until the PI's first lookup.

Below the awards, the table view also shows **Institution** (the awardee
organization); cards leave it out because the card title names it.

**The publications popup** lists #, Year, Author Order (first = red,
middle = yellow, last = green — in biomedicine the last author is usually
the lab head; the first author is the student or postdoc who did the work),
and the title (linked when a link is known). Every column sorts (Author
Order cycles green → yellow → red first), row numbers follow the sort, and
column edges drag to resize. The header shows the PI, whether the paper is
grant-linked, and a warning if the OpenAlex match was uncertain or merged
from two profiles.

## Controls

- **Click any week header** to select that week. The big total and the
  "granted" caption switch to it, and the highlight follows.
- **Search box** filters by institution, department, or funder.
- **Checkboxes** (top right) — every one is evaluated against the cell in
  the *selected* week, not the row as a whole:
  - Only show rows with a PI email
  - Only show rows with a PI name
  - Only show rows with an institution
  - Only rows with publication linked to grant
  - Only rows with publications within the last 5 years

  The first three combine as AND. The two publication boxes combine as OR,
  so checking both gives linked count + recent count exactly.
- **Total Count For Column = X** — visible rows with funding in the
  selected week, after filters. It updates as you click weeks or boxes.
- **Column headers** sort: Institution / Department / Funder alphabetically,
  week columns by amount (descending first). ⇅ means sortable; ▲/▼ shows
  the active direction.

## How the data is gathered

1. **NIH RePORTER** (official API) — the week's awards, filtered to
   biotech-adjacent departments. Clinical/medical departments and
   companies (SBIR/STTR) are excluded; the include-list and keywords are
   in the CONFIG block at the top of `funding_tracker.py`.
2. **PI emails** — papers NIH links to the grant first, then a PubMed
   author search, then Europe PMC full text. Only institutional addresses
   (no free-mail). Every lookup is cached in `data/pi_email_cache.json`;
   misses retry after 45 days; transient errors retry next run.
3. **Publications** — grant-linked papers via RePORTER (batched), otherwise
   the PI's journal articles from OpenAlex, matched by name plus affiliation
   history (so PIs who moved institutions still match). Cached in
   `data/pubs_cache.json` and `data/linked_pubs_cache.json`; each popup is a
   small file under `docs/pubs/` loaded on click.

## Setup (all in the browser)

1. **Secrets** — repo → Settings → Secrets and variables → Actions:
   - `OPENALEX_API_KEY` (required for publications; free at
     openalex.org/settings/api, $1/day of usage included)
   - `NCBI_API_KEY` (optional; free from your NCBI account; triples the
     PubMed lookup rate)
2. **Files** — `funding_tracker.py` and `requirements.txt` at the repo root;
   the workflow at `.github/workflows/update.yml`. To update a file, open
   it → pencil icon → select all → paste → *Commit directly to main*.
   (Uploading a file with the same name from your Downloads folder is where
   stale copies sneak in — check the file after committing.)
3. **Pages** — Settings → Pages → Deploy from a branch → `main` / `docs`.
4. **Run** — Actions → *Weekly funding tracker* → Run workflow. After that,
   Mondays are automatic.

## Run time and what the log says

The workflow runs in **two stages**, each ending in its own commit:

- **Stage 1 (~2 min):** pull the week's awards and publish the table.
- **Stage 2:** emails, then publications, for PIs not seen before; publish
  again. On a normal Monday this is 5–15 minutes. Re-running on the same
  week is quick, because every PI already has a cached result.

Log lines worth reading: `emails: found=X, none=Y`, `grant-linked papers:
X found among Y grant(s)`, and `publications: N looked up - A with recent
papers, B without, C uncertain match`. `OpenAlex daily budget reached` is
normal on a large backfill — the rest continues on the next run.

## Notes

- Weekly commits keep the schedule alive (GitHub disables schedules only
  after 60 days with no commits).
- `python funding_tracker.py --selftest` runs an offline test of the
  ledger, rendering, and lookup logic on any machine with Python.
- Amounts are NIH award amounts for the notice date, so a week's total is
  award value granted that week, not money spent that week.
