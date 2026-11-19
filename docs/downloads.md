---
layout: default
title: Downloads
nav_order: 94
permalink: /downloads/
description: "NZTxGNN open data downloads: FHIR resources, prediction results and search index."
---

# Downloads

<div class="key-takeaway">
Predictions are published in FHIR R4 format, ready to integrate with EHR systems.
</div>

---

<!-- local-facts:start（母目錄 .claude/gen_local_pages_20261119.py 產生，勿手改）-->
## New Zealand: what the files contain now

| File | Contents now |
|---|---|
| `/data/search-index.json` | {{ site.data.local_facts.files.search_index }} medicines with their predicted indications |
| `/data/drugs.json` | {{ site.data.local_facts.files.drugs_json }} medicines with evidence level and original indication |
| `/fhir/MedicationKnowledge/` | {{ site.data.local_facts.fhir.medication_knowledge }} MedicationKnowledge resources (one per medicine) |
| `/fhir/ClinicalUseDefinition/` | {{ site.data.local_facts.fhir.clinical_use_definition }} ClinicalUseDefinition resources (one per predicted indication) |

Reuse of the local dataset: [Pharmac Community Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/), published by Pharmac; reuse is governed by the publisher's terms, and this site does not grant any further rights.

{% if site.data.local_facts.top_licensed.size > 0 %}Medicines with the most current Pharmac Schedule listings: {% for r in site.data.local_facts.top_licensed limit: 5 offset: 3 %}[{{ r.t }}]({{ r.u | relative_url }}) ({{ r.lv }}){% unless forloop.last %}, {% endunless %}{% endfor %}{% endif %}

{% if site.data.local_facts.recent_reviews.size > 0 %}Latest review-log corrections: {% for r in site.data.local_facts.recent_reviews limit: 8 offset: 16 %}[{{ r.t }}]({{ r.u | relative_url }}) ({{ r.d }}): {{ r.s }}{% unless forloop.last %}; {% endunless %}{% endfor %}{% endif %}

<!-- local-facts:end -->

<!-- local-condensed:start（母目錄 .claude/gen_local_pages_20261119.py；原共用段落縮寫，勿手改）-->
## FHIR, search index and reuse

The FHIR R4 capability statement is at `/fhir/metadata`, all predictions are bundled in `/fhir/Bundle/all-predictions.json`, and `/data/search-index.json` holds the medicines and indications behind the site search. Data here are for research reference only; credit the site and cite the TxGNN paper, and check each source's licence on [Data Sources]({{ '/sources/' | relative_url }}).
<!-- local-condensed:end -->

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
