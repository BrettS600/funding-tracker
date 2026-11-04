// ==UserScript==
// @name         Funding tracker: NIH email helper
// @namespace    https://bretts600.github.io/funding-tracker/
// @version      1.0
// @description  On the NIH RePORTER project page you open from the funding tracker, presses the contact PI's View Email button once, copies the email, and keeps a list to paste into the tracker.
// @match        https://reporter.nih.gov/project-details/*
// @run-at       document-idle
// @noframes
// @grant        GM_setClipboard
// @grant        GM_getValue
// @grant        GM_setValue
// @updateURL    https://bretts600.github.io/funding-tracker/nih-email-helper.user.js
// @downloadURL  https://bretts600.github.io/funding-tracker/nih-email-helper.user.js
// ==/UserScript==

// How it works, and what it deliberately does not do:
// - It only acts on the one project page you have open. It never opens pages,
//   never moves on to the next PI, and never runs in the background.
// - On a page opened from the tracker (its link ends in #details) it presses the
//   Contact PI's "View Email" button once, the same as you clicking it. If NIH's
//   robot check shows a picture puzzle, you complete it; the helper just waits.
// - On any other project page it does nothing until you press View Email
//   yourself, then it copies the email the same way.
// - Collected emails stay in this browser until you copy them into the tracker
//   (data/nih_emails.csv on GitHub) or send them to Claude.

(function () {
  'use strict';

  const KEY = 'collected';   // { applId: { email, pi, date } }
  const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/;
  const EDIT_URL = 'https://github.com/BrettS600/funding-tracker/edit/main/data/nih_emails.csv';

  let currentId = '';        // project shown right now
  let pressed = false;       // View Email pressed for this page view
  let saved = false;         // email for this page view already saved

  const applId = () => (location.pathname.match(/project-details\/(\d+)/) || [])[1] || '';
  const load = () => GM_getValue(KEY, {}) || {};

  function contactSection() {
    const head = [...document.querySelectorAll('.data-header')]
      .find(h => /^\s*Contact PI/i.test(h.textContent));
    return head ? head.parentElement : null;
  }
  function piName(sec) {
    const info = sec.querySelector('.data-info');
    return info ? info.textContent.replace(/\s+/g, ' ').trim() : '';
  }
  function readEmail(sec) {
    const a = sec.querySelector('a[href^="mailto:" i]');
    if (a && EMAIL.test(a.textContent)) return a.textContent.trim();
    if (/Email not available/i.test(sec.textContent)) return 'unavailable';
    return '';
  }

  // ---- small panel, bottom-left (NIH's reCAPTCHA badge sits bottom-right) ----
  let panel = null;
  function show(html) {
    if (!panel) {
      panel = document.createElement('div');
      panel.style.cssText = 'position:fixed;left:16px;bottom:16px;z-index:2147483647;max-width:380px;' +
        'background:#fff;color:#101418;border:2px solid #1B7A43;border-radius:10px;padding:12px 14px;' +
        'box-shadow:0 10px 30px rgba(0,0,0,.25);font:14px/1.45 system-ui,sans-serif;';
      document.body.appendChild(panel);
    }
    const n = Object.keys(load()).length;
    panel.innerHTML = '<div style="font-weight:600;margin-bottom:4px">Tracker email helper</div>' +
      '<div>' + html + '</div>' +
      '<div style="margin-top:8px;color:#5B636B;font-size:12.5px">' + n + (n === 1 ? ' email' : ' emails') +
      ' collected in this browser</div>' +
      '<div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap">' +
      '<button data-act="copyall" style="' + BTN + '">Copy all for tracker</button>' +
      '<button data-act="clear" style="' + BTN2 + '">Clear list</button>' +
      '<button data-act="hide" style="' + BTN2 + '">Hide</button></div>';
  }
  const BTN = 'border:0;border-radius:6px;background:#1B7A43;color:#fff;padding:6px 10px;font:inherit;font-size:13px;cursor:pointer';
  const BTN2 = 'border:1px solid #C3CAD2;border-radius:6px;background:#fff;color:#101418;padding:5px 10px;font:inherit;font-size:13px;cursor:pointer';
  const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const csv = s => /[",\n]/.test(s) ? '"' + String(s).replace(/"/g, '""') + '"' : String(s);

  document.addEventListener('click', e => {
    const b = e.target.closest && e.target.closest('button[data-act]');
    if (!b || !panel || !panel.contains(b)) return;
    const all = load();
    const ids = Object.keys(all);
    if (b.dataset.act === 'copyall') {
      if (!ids.length) { show('Nothing collected yet.'); return; }
      const lines = ids.map(id => [id, all[id].email, all[id].pi, all[id].date].map(csv).join(','));
      GM_setClipboard(lines.join('\n') + '\n', 'text');
      show('Copied ' + ids.length + ' line' + (ids.length === 1 ? '' : 's') + '. Paste them at the end of ' +
        '<a href="' + EDIT_URL + '" target="_blank" rel="noopener">nih_emails.csv on GitHub</a> ' +
        'and commit, or send them to Claude. Then press Clear list.');
    } else if (b.dataset.act === 'clear') {
      if (confirm('Clear the ' + ids.length + ' collected emails from this browser? Copy them first if you have not.')) {
        GM_setValue(KEY, {});
        show('List cleared.');
      }
    } else if (b.dataset.act === 'hide') {
      panel.remove(); panel = null;
    }
  });

  function save(id, email, pi) {
    const all = load();
    all[id] = { email, pi, date: new Date().toISOString().slice(0, 10) };
    GM_setValue(KEY, all);
    GM_setClipboard(email, 'text');
    show('Copied <b>' + esc(email) + '</b><br><span style="color:#5B636B">' + esc(pi) + ' · project ' + id + '</span>');
  }

  // ---- one check per second for the page you have open ----
  function tick() {
    const id = applId();
    if (id !== currentId) {              // new page view (RePORTER changes pages without reloading)
      currentId = id; pressed = false; saved = false;
    }
    if (!id || saved) return;
    const sec = contactSection();
    if (!sec) return;
    const email = readEmail(sec);
    if (email === 'unavailable') { saved = true; show('NIH has no email on file for this PI.'); return; }
    if (email) { saved = true; save(id, email, piName(sec)); return; }
    if (!pressed && location.hash === '#details') {
      const btn = [...sec.querySelectorAll('button')].find(x => /View Email/i.test(x.textContent));
      if (btn) {
        pressed = true;
        btn.click();
        show('Getting the email… if a picture check appears, complete it and the email will be copied.');
      }
    }
  }
  setInterval(tick, 1000);
})();
