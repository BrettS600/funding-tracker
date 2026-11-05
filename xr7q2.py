# Loaded by funding_tracker.py.
import html as html_lib

_G = {}


def _use(name):
    return lambda *a, **k: _G[name](*a, **k)


short_date, fmt_money, _appl_id, _pub_status = (
    _use(n) for n in ("short_date", "fmt_money", "_appl_id", "_pub_status"))


def b(g):
    global _G
    _G = g
    g["_award_entries"] = _award_entries
    g["_cell_html"] = _cell_html


def _award_entries(data):
    """(entry, university) for every award: each one shows its own contact PI,
    email and publications line, so each needs its publication lookup."""
    for row in data["rows"].values():
        for entries in row["cells"].values():
            for e in entries:
                yield e, row["university"]


def _mailto(addr: str, title: str) -> str:
    return (f'<a href="mailto:{html_lib.escape(addr, quote=True)}" title="{title}">'
            f"{html_lib.escape(addr)}</a>")


def _email_line(e) -> str:
    """'NIH PI Email: ...' when an email from NIH's View Email button is recorded
    for this award's PI; otherwise 'Predicted Email: ...' (the address found in
    the PI's papers, or --) plus the link that opens NIH at the View Email button."""
    nih_email = (e.get("nih_email") or "").strip()
    if nih_email:
        return "NIH PI Email: " + _mailto(
            nih_email, "Revealed on NIH&rsquo;s project page (View Email)")
    predicted = (e.get("pi_email") or "").strip()
    line = "Predicted Email: " + (
        _mailto(predicted, "Found in a published paper, not from NIH: it may belong "
                           "to someone with a similar name") if predicted else "--")
    appl = _appl_id(e)
    if appl:
        # opens the PI's NIH page at the View Email button (the email helper presses it)
        line += (f' <a class="nihlink" href="https://reporter.nih.gov/project-details/{appl}#details" '
                 f'target="_blank" rel="noopener">View email on NIH</a>')
    return line


def _pub_line(e, institution, pubs, linked):
    """(html, kind) for an award's publications line."""
    kind, slug, match = _pub_status(e, institution, pubs, linked)
    if kind == "linked":
        html = (f'<a class="publink" data-pub="{slug}" href="#">'
                f'Publication Linked To Grant</a>')
    elif kind == "recent":
        html = (f'<a class="publink" data-pub="{slug}" href="#">'
                f'Publications Within The Past 5 Years? Yes</a>'
                + (' <span class="unc" title="name match uncertain - check '
                   'affiliations in the popup">?</span>' if match == "uncertain" else ""))
    elif kind == "none":
        html = "Publications Within The Past 5 Years? No"
    else:
        html = '<span class="pend">Publications: not checked yet</span>'
    return html, kind


def _cell_html(entries, institution="", pubs=None, linked=None):
    if not entries:
        return '<td class="zero" data-v="0">0</td>'
    pubs, linked = pubs or {}, linked or {}
    total = sum(e["amount"] for e in entries)
    kinds = []

    def aline(e):
        """One award: amount, dates, then its own contact PI, email and publications."""
        notice, late = e.get("notice") or "", bool(e.get("late"))
        meta = []
        if notice:
            meta.append(f"Award Notice Date: {short_date(notice)}")
        if late and e.get("added"):
            meta.append(f'<span class="late" title="NIH posted this award after its week '
                        f'was first checked">Added: {short_date(e["added"])}</span>')
        sub = f'<div class="ameta">{" ".join(meta)}</div>' if meta else ""
        pub, kind = _pub_line(e, institution, pubs, linked)
        kinds.append(kind)
        pib = (f'<div class="pib"><div>PI Name: {html_lib.escape(e.get("pi") or "--")}</div>'
               f'<div>{_email_line(e)}</div><div>{pub}</div></div>')
        return (f'<div class="aline" data-a="{e["amount"]}" data-n="{html_lib.escape(notice)}" '
                f'data-late="{1 if late else 0}"><a href="{html_lib.escape(e["url"], quote=True)}" '
                f'target="_blank" rel="noopener">{html_lib.escape(e["label"])}</a>{sub}{pib}</div>')

    lines = "".join(aline(e) for e in entries)
    if len(entries) > 1:
        lines += f'<div class="tot">= {fmt_money(total)}</div>'
    # cards hide the Institution line because the card's title names it
    lines += f'<div class="pinst">Institution: {html_lib.escape(institution.strip() or "--")}</div>'
    # per-cell flags for the checkboxes (they apply to the selected week): a cell
    # qualifies when any of its awards does; "No NIH email yet" keeps a cell until
    # every award's PI has an NIH email
    has = lambda f: 1 if any(f(e) for e in entries) else 0
    flags = (f' data-he="{has(lambda e: (e.get("nih_email") or e.get("pi_email") or "").strip())}"'
             f' data-nih="{1 if all((e.get("nih_email") or "").strip() for e in entries) else 0}"'
             f' data-hn="{has(lambda e: (e.get("pi") or "").strip())}"'
             f' data-hi="{1 if institution.strip() else 0}"'
             f' data-pl="{1 if "linked" in kinds else 0}"'
             f' data-pr="{1 if "recent" in kinds else 0}"')
    return f'<td data-v="{total}"{flags}>{lines}</td>'


def t(page):
    assert 'Predicted Email: <a href="mailto:jbig@brown.edu"' in page, \
        "paper-found emails are shown as Predicted Email"
    assert 'NIH PI Email: <a href="mailto:ada@tufts.edu"' in page, \
        "an NIH email wins and is shown as NIH PI Email"
    assert "PI Email: <a" not in page.replace("NIH PI Email: <a", ""), "old PI Email label gone"
    # every award shows its own PI, not just the cell's largest award
    bme_cell = [tr for tr in page.split("<tr>") if "PI Name: Jane Big" in tr][0]
    assert "PI Name: Jane Big" in bme_cell and "PI Name: John Small" in bme_cell, \
        "the smaller award's PI is shown too"
    assert 'NIH PI Email: <a href="mailto:jsmall@brown.edu"' in bme_cell, \
        "the smaller award's PI gets their own email line"
    assert bme_cell.index("$4,600,000") < bme_cell.index("PI Name: Jane Big") \
        < bme_cell.index("$500,000") < bme_cell.index("PI Name: John Small"), \
        "each PI sits under their own award"
    assert ' data-nih="0"' in bme_cell, "No NIH email yet keeps a cell until every award has one"
    assert "Predicted Email: --" in page
