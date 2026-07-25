---
layout: default
title: About
nav_order: 90
permalink: /about/
description: "NZTxGNN is a drug repurposing prediction platform developed by 藥提醒科技有限公司 (yao.care), built on the Harvard TxGNN model, covering Medsafe-approved medicines in New Zealand."
---

# About

<div class="key-takeaway">
Accelerating drug repurposing evidence validation with AI — from prediction to evidence at a glance.
</div>

---

## Background

<p class="key-answer" data-question="What is NZTxGNN?">
<strong>NZTxGNN</strong> is a research-support platform for drug repurposing, built on the TxGNN
model published in <em>Nature Medicine</em> by the Zitnik Lab at Harvard University. It predicts
indication expansion for medicines approved by Medsafe in New Zealand. Beyond AI prediction scores,
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
model, covering national drug-registration data integration, dual knowledge-graph and
deep-learning prediction, PubMed / ClinicalTrials evidence grading, and SMART on FHIR
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
<a href="https://www.nature.com/articles/s41591-023-02233-x">TxGNN</a> is a deep learning model
developed by the Zitnik Lab at Harvard Medical School and published in <em>Nature Medicine</em>.
It predicts novel drug&ndash;disease associations and is the first foundation model for drug
repurposing designed specifically for clinicians.
</p>

<blockquote class="expert-quote">
"TxGNN integrates a knowledge graph of 17,080 biomedical entities and uses graph neural networks
to learn complex relationships between nodes, predicting potential efficacy of drugs against
rare diseases."
<cite>&mdash; Huang et al., Nature Medicine (2023)</cite>
</blockquote>

---

## Data sources

<table class="comparison-table">
<thead>
<tr><th>Type</th><th>Source</th><th>Description</th></tr>
</thead>
<tbody>
<tr><td>AI prediction</td><td><a href="https://zitniklab.hms.harvard.edu/projects/TxGNN/">TxGNN</a></td><td>Harvard knowledge graph prediction model</td></tr>
<tr><td>Clinical trials</td><td><a href="https://clinicaltrials.gov/">ClinicalTrials.gov</a></td><td>Global clinical trial registry</td></tr>
<tr><td>Literature</td><td><a href="https://pubmed.ncbi.nlm.nih.gov/">PubMed</a></td><td>Biomedical literature database</td></tr>
<tr><td>Drug information</td><td><a href="https://go.drugbank.com/">DrugBank</a></td><td>Drug and target database</td></tr>
<tr><td>Registration data</td><td><a href="https://www.medsafe.govt.nz/">Medsafe</a></td><td>Drug approval data for New Zealand</td></tr>
</tbody>
</table>

---

## Academic basis

> Huang, K., et al. (2023). A foundation model for clinician-centered drug repurposing. *Nature Medicine*.
> [DOI: 10.1038/s41591-023-02233-x](https://doi.org/10.1038/s41591-023-02233-x)

---

## Scale

| Item | Value |
|------|-------|
| Drug reports | 581 |
| Regulatory authority | Medsafe |
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
