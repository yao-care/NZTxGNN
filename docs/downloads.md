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
