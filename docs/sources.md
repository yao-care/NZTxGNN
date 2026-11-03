---
layout: default
title: Data Sources
nav_order: 93
permalink: /sources/
description: "Data sources behind NZTxGNN: the Pharmac Community Pharmaceutical Schedule, TxGNN, ClinicalTrials.gov, PubMed and DrugBank."
---

# Data Sources

<div class="key-takeaway">
Every conclusion traces back to a public data source — nothing is a black box.
</div>

---

## Source overview

<table class="comparison-table">
<thead>
<tr><th>Type</th><th>Source</th><th>Used for</th></tr>
</thead>
<tbody>
<tr><td>Local medicine list</td><td><a href="https://schedule.pharmac.govt.nz/">Pharmac</a></td><td>Community Pharmaceutical Schedule: medicines listed in New Zealand, their ingredients and subsidy status</td></tr>
<tr><td>Prediction model</td><td><a href="https://zitniklab.hms.harvard.edu/projects/TxGNN/">TxGNN</a></td><td>Drug&ndash;disease association prediction</td></tr>
<tr><td>Clinical trials</td><td><a href="https://clinicaltrials.gov/">ClinicalTrials.gov</a></td><td>Evidence grading (NCT)</td></tr>
<tr><td>Literature</td><td><a href="https://pubmed.ncbi.nlm.nih.gov/">PubMed</a></td><td>Evidence grading (PMID)</td></tr>
<tr><td>Drug information</td><td><a href="https://go.drugbank.com/">DrugBank</a></td><td>Ingredient mapping and target data</td></tr>
<tr><td>Interactions</td><td><a href="https://ddinter2.scbdd.com/">DDInter</a></td><td>Drug&ndash;drug interaction data</td></tr>
</tbody>
</table>

---

## New Zealand medicine data

| Item | Detail |
|------|--------|
| Dataset | Pharmac Community Pharmaceutical Schedule (CPSReporting.xlsx, "Community Medicines" sheet) |
| Publisher | Pharmac |
| Download | <https://schedule.pharmac.govt.nz/latest/CPSReporting.xlsx> |
| Nature of the data | A funding schedule, not a licence register: Medsafe licence status is not included |
| Update | Downloaded again every Monday by an automated workflow; listing status in drug reports is updated from the new file |

---

## Licensing

Each source has its own licence — please check before citing:

- **TxGNN**: academic use; cite Huang et al. (2024)
- **ClinicalTrials.gov / PubMed**: US NIH public data
- **DrugBank**: non-commercial use subject to its licence terms
- **Pharmac Pharmaceutical Schedule**: published by Pharmac; check its terms of use before reuse

---

## Update frequency

| Data | Frequency |
|------|-----------|
| Pharmac Schedule | Weekly (every Monday, automated) |
| Trial / literature evidence | Re-collected periodically |
| Interaction data | Reviewed quarterly |

---

## Academic citation

> Huang, K., et al. (2024). A foundation model for clinician-centered drug repurposing. *Nature Medicine*.
> [DOI: 10.1038/s41591-024-03233-x](https://doi.org/10.1038/s41591-024-03233-x)

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
