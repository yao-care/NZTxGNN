---
layout: default
title: Privacy Policy
nav_order: 95
permalink: /privacy-policy/
description: "NZTxGNN privacy policy: a static site with no accounts or forms; it uses Google Analytics 4 analytics cookies for usage statistics."
---

# Privacy Policy

<div class="key-takeaway">
This is a static site — no registration and no forms. It uses Google Analytics 4 analytics cookies to compile usage statistics.
</div>

---

## What we collect

<p class="key-answer" data-question="Does NZTxGNN collect personal data?">
<strong>Only usage statistics, through Google Analytics 4.</strong> This site is hosted on GitHub Pages as a purely static site.
There is no account system and there are no forms, and we do not collect names, email addresses or medical
records. The site uses Google Analytics 4 analytics cookies to compile usage statistics (pages viewed, referrer,
scroll depth and time on page). The cookies are set when a page loads; search terms, form contents and URL
query strings are not sent.
</p>

---

## Third-party services

The site uses the following third-party resources, which may log access according to their own
policies:

| Service | Purpose | Privacy policy |
|---------|---------|----------------|
| GitHub Pages | Site hosting | [GitHub Privacy Statement](https://docs.github.com/site-policy/privacy-policies/github-privacy-statement) |
| Google Analytics 4 (www.googletagmanager.com, google-analytics.com) | Visit statistics (pages viewed, referrer); uses analytics cookies. Search terms, form contents and URL query strings are not sent. | [Google Privacy Policy](https://policies.google.com/privacy) |
| cdn.jsdelivr.net | Script libraries: d3 charts on every page; Fuse.js name matching and the SMART on FHIR client on the SMART pages | [jsDelivr Privacy Policy](https://www.jsdelivr.com/terms/privacy-policy-jsdelivr-net) |
| clinicaltrials.gov | Clinical-trial lookup on the SMART pages: only when you ask for trials, the drug and indication names are sent to the ClinicalTrials.gov API | [NLM Web Policies](https://www.nlm.nih.gov/web_policies.html) |
| rxnav.nlm.nih.gov | SMART app: the RxNorm codes of the medications read from your EHR are sent to the NLM RxNav API to look up ingredient names (no patient identifiers) | [NLM Web Policies](https://www.nlm.nih.gov/web_policies.html) |

When you actively launch the SMART on FHIR app, it requests medication data from your electronic
health record system. Patient identity and EHR data are <strong>not sent to this site's servers</strong> (this is a
static site with no backend) and are not stored by this site. To match drug names, the app sends the
medications' RxNorm codes to the US National Library of Medicine's RxNav service; these codes contain no
patient identifiers. When you ask for clinical trials, the drug and indication names are sent to ClinicalTrials.gov.

---

## Data subject rights

This site has no accounts and keeps no personal records on servers of its own (it has none). Usage statistics
are collected by Google Analytics 4; you can block or delete its cookies in your browser settings.
For questions about this policy, contact service@yao.care.

---

## Changes to this policy

Any changes will be published directly on this page.

---

## About the Developer

This platform is developed and operated by **藥提醒科技有限公司** (yao.care). See the
<a href="{{ '/about/' | relative_url }}">About</a> page for the full company and methodology background.

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>
