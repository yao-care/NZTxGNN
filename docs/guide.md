---
layout: default
title: User Guide
nav_order: 92
permalink: /guide/
description: "NZTxGNN user guide: how to look up drugs, read evidence levels and interpret recommendations."
---

# User Guide

<div class="key-takeaway">
Check the evidence level first, then the recommendation, then read the source literature.
</div>

---

<!-- site-data:start（母目錄 .claude/gen_page_data_20261116.py 產生，勿手改）-->
## New Zealand: data behind this site

This site publishes reports for **{{ site.data.drug_stats.total_drugs }}** medicines on the [Pharmac Community Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) (New Zealand).

Dataset: [Pharmac Community Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/), downloaded and compared automatically every week (Mondays). Details: [Data Sources]({{ '/sources/' | relative_url }}).

Reports by evidence level:

| Evidence level | Reports | Share |
|---|---|---|
| L1 | {{ site.data.drug_stats.level_counts.L1 }} | {{ site.data.drug_stats.level_counts.L1 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L2 | {{ site.data.drug_stats.level_counts.L2 }} | {{ site.data.drug_stats.level_counts.L2 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L3 | {{ site.data.drug_stats.level_counts.L3 }} | {{ site.data.drug_stats.level_counts.L3 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L4 | {{ site.data.drug_stats.level_counts.L4 }} | {{ site.data.drug_stats.level_counts.L4 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
| L5 | {{ site.data.drug_stats.level_counts.L5 }} | {{ site.data.drug_stats.level_counts.L5 | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |
{% assign na = site.data.drug_stats.total_drugs | minus: site.data.drug_stats.level_counts.L1 | minus: site.data.drug_stats.level_counts.L2 | minus: site.data.drug_stats.level_counts.L3 | minus: site.data.drug_stats.level_counts.L4 | minus: site.data.drug_stats.level_counts.L5 %}{% if na > 0 %}| N/A | {{ na }} | {{ na | times: 100.0 | divided_by: site.data.drug_stats.total_drugs | round: 1 }}% |{% endif %}

2 predicted drug–disease pairs on this site appear on no other of the 30 TxGNN sites.

<!-- site-data:end -->

## Looking up a drug

<ol class="actionable-steps">
<li>Use the search box at the top of the page (generic ingredient names match better than brand names).</li>
<li>Or browse the full list on <a href="{{ '/drugs/' | relative_url }}">All Drugs</a>.</li>
<li>You can also browse by evidence level: <a href="{{ '/evidence-high/' | relative_url }}">high</a>, <a href="{{ '/evidence-medium/' | relative_url }}">moderate</a>, <a href="{{ '/evidence-low/' | relative_url }}">model prediction only</a>.</li>
</ol>

---

## Reading a report

<p class="key-answer" data-question="What do evidence levels L1 to L5 mean?">
Each drug report lists predicted new indications, and each indication carries an L1&ndash;L5 evidence
level. <strong>L1 means multiple Phase 3 randomised controlled trials already support it; L5 means
model prediction only, with no human evidence.</strong> Full criteria are on the
<a href="{{ '/methodology/' | relative_url }}">Methodology</a> page.
</p>

| If you see | It means | Suggested action |
|-----------|----------|------------------|
| L1 / L2 | Clinical trial evidence exists | Review the source NCT and PMID records |
| L3 / L4 | Observational or preclinical evidence | Treat as a research lead |
| L5 | Model prediction only | Hypothesis generation only; not for clinical reference |

---

## Citation and traceability

Every piece of evidence in a report carries a traceable identifier:

- **NCT number**: links to the ClinicalTrials.gov registration
- **PMID**: links to the PubMed record
- **DrugBank ID**: links to drug and target data

Please read the source literature to confirm context before citing any conclusion from this platform.

---

## Frequently asked questions

<p class="key-answer" data-question="Can predictions be used clinically?">
<strong>No.</strong> Predictions on this platform are research leads, not clinical advice. Any
clinical application of drug repurposing must go through full clinical trial validation and
regulatory review.
</p>

<p class="key-answer" data-question="Why can't I find a particular drug?">
An ingredient must map to the DrugBank vocabulary to be included in prediction. Herbal extracts,
vaccines, excipients and other items not catalogued by DrugBank do not appear on this platform.
</p>

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
