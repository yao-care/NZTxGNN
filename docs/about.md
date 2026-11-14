---
layout: default
title: About
nav_order: 90
permalink: /about/
description: "NZTxGNN is a drug repurposing prediction platform developed by 藥提醒科技有限公司 (yao.care), built on the Harvard TxGNN model, covering medicines on the Pharmac Community Pharmaceutical Schedule in New Zealand."
---

# About

<div class="key-takeaway">
Accelerating drug repurposing evidence validation with AI — from prediction to evidence at a glance.
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

## Background

<p class="key-answer" data-question="What is NZTxGNN?">
<strong>NZTxGNN</strong> is a research-support platform for drug repurposing, built on the TxGNN
model published in <em>Nature Medicine</em> by the Zitnik Lab at Harvard University. It predicts
indication expansion for medicines on the Pharmac Community Pharmaceutical Schedule in New Zealand. Beyond AI prediction scores,
the platform integrates clinical evidence from ClinicalTrials.gov and PubMed so researchers can
quickly assess how credible each prediction is.
</p>

---

## About the Developer

This platform is developed and operated by **藥提醒科技有限公司** (yao.care, company registration
number 83620786, 12F, No. 220, Sec. 2, Taiwan Blvd., West Dist., Taichung City, Taiwan).

NZTxGNN is the New Zealand site of the company's "TxGNN Drug Repurposing" product line.
The same system is deployed across 30 countries and regions, each named `{CC}TxGNN`
(JpTxGNN, UsTxGNN, DETxGNN, and so on) at `{cc}txgnn.yao.care`.
Product overview: <https://www.yao.care/medical/txgnn/>.

The TxGNN model itself was developed by the Zitnik Lab at Harvard Medical School and published
in *Nature Medicine*. This platform is the production system 藥提醒科技有限公司 built on top of that
model, covering local drug-list data integration, deep-learning prediction that excludes uses already recorded in the knowledge graph, PubMed / ClinicalTrials evidence grading, and SMART on FHIR
electronic health record integration.

---

## What is drug repurposing?

<p class="key-answer" data-question="What is drug repurposing?">
<strong>Drug repurposing</strong> means finding new therapeutic uses for existing medicines.
Compared with developing a new drug from scratch — 10 to 15 years and USD 1&ndash;2 billion —
repurposing takes 3 to 5 years and USD 100&ndash;300 million, and human safety data already exists,
so the risk of failure is lower.
</p>

<table class="comparison-table">
<thead>
<tr><th>Aspect</th><th>New drug development</th><th>Drug repurposing</th></tr>
</thead>
<tbody>
<tr><td>Time</td><td>10&ndash;15 years</td><td>3&ndash;5 years</td></tr>
<tr><td>Cost</td><td>USD 1&ndash;2 billion</td><td>USD 100&ndash;300 million</td></tr>
<tr><td>Safety data</td><td>Must be established</td><td>Human data already available</td></tr>
<tr><td>Risk of failure</td><td>Very high (&gt;90%)</td><td>Lower</td></tr>
</tbody>
</table>

---

## What is TxGNN?

<p class="key-answer" data-question="What is TxGNN?">
<a href="https://www.nature.com/articles/s41591-024-03233-x">TxGNN</a> is a deep learning model
developed by the Zitnik Lab at Harvard Medical School and published in <em>Nature Medicine</em>.
It predicts novel drug&ndash;disease associations and is the first foundation model for drug
repurposing designed specifically for clinicians.
</p>

<blockquote class="expert-quote">
"TxGNN integrates a knowledge graph covering 17,080 diseases and uses graph neural networks
to learn complex relationships between nodes, predicting potential efficacy of drugs against
rare diseases."
<cite>&mdash; Huang et al., Nature Medicine (2024)</cite>
</blockquote>

---

## Data sources

Predictions combine TxGNN, ClinicalTrials.gov, PubMed and DrugBank with New Zealand medicine data from the
Pharmac Community Pharmaceutical Schedule. Datasets, download links, licences and update frequency are listed on the
<a href="{{ '/sources/' | relative_url }}">Data Sources</a> page.

---

## Academic basis

> Huang, K., et al. (2024). A foundation model for clinician-centered drug repurposing. *Nature Medicine*.
> [DOI: 10.1038/s41591-024-03233-x](https://doi.org/10.1038/s41591-024-03233-x)

---

## Scale

| Item | Value |
|------|-------|
| Drug reports | {{ site.data.drug_stats.total_drugs }} |
| Local medicine list | Pharmac Community Pharmaceutical Schedule |
| Deployed sites | 30 countries / regions |

---

## Contact

- **GitHub Issues**: <https://github.com/yao-care/NZTxGNN/issues>
- **Developer**: 藥提醒科技有限公司 (<https://www.yao.care>, service@yao.care)
- **Product overview**: <https://www.yao.care/medical/txgnn/>

---

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>
