---
layout: default
title: Methodology
nav_order: 91
permalink: /methodology/
description: "How NZTxGNN produces and validates predictions: TxGNN knowledge graph prediction, evidence collection, L1-L5 grading, and decision recommendations."
---

# Methodology

<div class="key-takeaway">
From AI prediction to evidence grading — every candidate has a traceable basis for its rating.
</div>

---

## Overall pipeline

<p class="key-answer" data-question="How does NZTxGNN produce its predictions?">
The platform uses a four-stage pipeline: the TxGNN knowledge graph model predicts potential
drug&ndash;disease associations, evidence is then collected automatically for each predicted pair,
the evidence is graded from L1 to L5, and finally a decision recommendation is issued.
</p>

<ol class="actionable-steps">
<li><strong>TxGNN prediction</strong>: drug&ndash;disease relationships predicted with a knowledge graph combined with graph neural networks.</li>
<li><strong>Evidence collection</strong>: for each predicted pair, evidence is gathered from ClinicalTrials.gov, PubMed, DrugBank and Medsafe.</li>
<li><strong>Evidence grading</strong>: graded L1 to L5, where L1 is strongest (multiple Phase 3 RCTs) and L5 is model prediction only.</li>
<li><strong>Decision recommendation</strong>: Go, Proceed, Consider, Explore or Hold, based on the evidence level.</li>
</ol>

---

## Evidence grading criteria

<table class="comparison-table">
<thead>
<tr><th>Level</th><th>Definition</th><th>Clinical meaning</th></tr>
</thead>
<tbody>
<tr><td><strong>L1</strong></td><td>Multiple Phase 3 RCTs / systematic reviews</td><td>Strong support; clinical use may be considered</td></tr>
<tr><td><strong>L2</strong></td><td>Single RCT or multiple Phase 2 trials</td><td>Moderate support; validation trials can be designed</td></tr>
<tr><td><strong>L3</strong></td><td>Observational studies / large case series</td><td>Preliminary support; needs further validation</td></tr>
<tr><td><strong>L4</strong></td><td>Preclinical / mechanistic studies</td><td>Theoretical support; far from clinical use</td></tr>
<tr><td><strong>L5</strong></td><td>Model prediction only</td><td>Hypothesis stage; no human evidence yet</td></tr>
</tbody>
</table>

---

## How predictions are made

TxGNN's deep learning (DL) model scores drug–disease pairs. Indications and contraindications already recorded for the drug in the TxGNN knowledge graph (KG) are then removed, so drug pages list only new predicted indications with a score of at least 0.99 (up to 10 per drug).

| Step | Role |
|------|------|
| Deep learning (DL) | The TxGNN graph neural network scores each drug–disease pair |
| Known-use check (KG) | Removes indications and contraindications already recorded in the knowledge graph; these are not counted as new predictions |
| Evidence collection | Searches ClinicalTrials.gov and PubMed for each remaining prediction and assigns an evidence level (L1–L5) |

---

## Regulatory data integration

Drug approval data for New Zealand comes from Medsafe. Ingredient names are mapped to the
DrugBank vocabulary; ingredients that cannot be mapped — herbal extracts, vaccines, excipients
and others not catalogued by DrugBank — are excluded from prediction.

---

## Limitations

<ol class="actionable-steps">
<li>Predictions are statistical associations and <strong>do not imply causation or clinical efficacy</strong>.</li>
<li>An L5 rating means model prediction only, with no supporting human evidence.</li>
<li>Evidence collection depends on public databases; unpublished or unindexed studies are not captured.</li>
<li>Ingredient mapping may miss items because of naming differences.</li>
</ol>

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

<div class="disclaimer">
<strong>Disclaimer</strong><br>
This report is for academic research reference only and <strong>does not constitute medical advice</strong>. Always follow your physician's instructions; never adjust medication on your own. Any drug repurposing decision requires full clinical validation and regulatory review.
<br><br>
<small>Reviewed by: 藥提醒科技有限公司 (yao.care)</small>
</div>
