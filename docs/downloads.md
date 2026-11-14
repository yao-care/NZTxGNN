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

## FHIR resources

This site publishes predictions as FHIR R4 resources, consumable directly by SMART on FHIR apps:

| Resource | Path | Description |
|----------|------|-------------|
| CapabilityStatement | `/fhir/metadata` | FHIR server capability statement |
| MedicationKnowledge | `/fhir/MedicationKnowledge/` | Drug resources |
| ClinicalUseDefinition | `/fhir/ClinicalUseDefinition/` | Predicted indications |
| Bundle | `/fhir/Bundle/all-predictions.json` | All predictions bundled |

---

## Search index

`/data/search-index.json` provides a search index of drugs and indications for building your own
query interface.

---

## Terms of use

<ol class="actionable-steps">
<li>Data on this site is <strong>for research reference only</strong> and must not be used as a basis for medical decisions.</li>
<li>When citing, credit NZTxGNN (藥提醒科技有限公司) and cite the original TxGNN paper.</li>
<li>Downstream data remains subject to the licence terms of each original source (see <a href="{{ '/sources/' | relative_url }}">Data Sources</a>).</li>
</ol>

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
