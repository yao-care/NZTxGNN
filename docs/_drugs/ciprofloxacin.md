---
layout: default
title: Ciprofloxacin
parent: 僅模型預測 (L5)
nav_order: 73
evidence_level: L5
indication_count: 10
---

# Ciprofloxacin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Ciprofloxacin: From Bacterial Infections to Diffuse Scleroderma

## One-Sentence Summary

Ciprofloxacin is a broad-spectrum fluoroquinolone antibiotic widely used for treating a range of bacterial infections, including respiratory, urinary tract, and skin infections.
The TxGNN model predicts it may be effective for **Diffuse Scleroderma**,
with **0 clinical trials** and **2 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Broad-spectrum bacterial infections (fluoroquinolone antibiotic) |
| Predicted New Indication | Diffuse Scleroderma |
| TxGNN Prediction Score | 99.87% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Ciprofloxacin is a fluoroquinolone antibiotic whose primary mechanism targets bacterial DNA gyrase (GyrA/GyrB) and Topoisomerase IV, enzymes essential for DNA replication — leading to rapid bactericidal activity. Beyond this antibacterial role, emerging in vitro and small-scale clinical data suggest ciprofloxacin may possess antifibrotic properties, potentially through inhibition of TGF-β–induced fibroblast proliferation and collagen synthesis. This secondary mechanism forms the biological basis of the TxGNN prediction.

Diffuse scleroderma (diffuse cutaneous systemic sclerosis) is an autoimmune connective tissue disease characterized by progressive skin and visceral fibrosis, microvascular injury, and immune dysregulation — conditions for which no disease-modifying pharmacological treatment currently exists. Two mechanistic pathways connect ciprofloxacin to this indication: (1) direct suppression of fibroblast activation and collagen deposition, potentially attenuating skin thickening; and (2) treatment of small intestinal bacterial overgrowth (SIBO), a common and debilitating gastrointestinal complication in systemic sclerosis patients that contributes to malnutrition and symptom burden.

However, this prediction requires cautious interpretation. The antifibrotic evidence remains confined to preclinical models and a single small-cohort clinical study, with no registered trials specifically designed to evaluate ciprofloxacin as a disease-modifying agent in scleroderma. The signal is biologically plausible but clinically unvalidated.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [20507401](https://pubmed.ncbi.nlm.nih.gov/20507401/) | 2010 | Small cohort (controlled, double-blind design) | The Journal of Dermatology | Evaluated oral ciprofloxacin as an antifibrotic agent in scleroderma patients; primary study directly investigating this repurposing hypothesis |
| [7728404](https://pubmed.ncbi.nlm.nih.gov/7728404/) | 1995 | Observational | British Journal of Rheumatology | Investigated small bowel bacterial overgrowth (SIBO) in 24 systemic sclerosis patients (6 with diffuse form); ciprofloxacin was among antibiotic treatments used, demonstrating indirect benefit via SIBO management |

---

## New Zealand Market Information

Ciprofloxacin is not currently approved or marketed in New Zealand (0 authorizations on record). Regulatory approval would be required before any clinical use.

---

## Safety Considerations

> ⚠️ **Important safety note relevant to this indication:** Ciprofloxacin (fluoroquinolone class) carries an **FDA Black Box Warning** for risk of peripheral neuropathy, which may be irreversible. This is particularly relevant as one of the TxGNN's lower-ranked predictions (rank 9) involves a hematological disease with acquired peripheral neuropathy — a direction that should be **excluded** as the drug may cause rather than treat this condition.

For full safety information, please refer to the package insert warnings and precautions.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The current evidence base for ciprofloxacin in diffuse scleroderma is limited to two small observational-level studies (L4), with no registered clinical trials. While the antifibrotic and SIBO-treatment hypotheses offer a coherent mechanistic framework, the evidence is insufficient to support advancing beyond an exploratory research stage.

**To proceed, the following is needed:**
- Confirmatory mechanistic studies establishing ciprofloxacin's antifibrotic activity in scleroderma-relevant fibroblast models (TGF-β pathway quantification)
- A prospective pilot clinical trial (Phase 1/2) in diffuse scleroderma patients with pre-specified endpoints (e.g., modified Rodnan Skin Score, SIBO eradication rate)
- Long-term safety data for ciprofloxacin use in autoimmune/fibrotic disease populations, given fluoroquinolone-class risks (tendinopathy, peripheral neuropathy, QT prolongation)
- New Zealand regulatory assessment for this new indication prior to any clinical development pathway

> **Note on ranked predictions:** While diffuse scleroderma is the top-ranked TxGNN prediction by model score, **septicemic plague (rank 10)** carries the strongest clinical evidence in this Evidence Pack (L2, Phase 2 completed RCT, FDA-recognized indication). Ciprofloxacin is already established by US CDC/FDA as a first-line treatment and post-exposure prophylaxis agent for plague — this finding reflects the model correctly identifying a validated clinical use, reinforcing confidence in the TxGNN knowledge graph.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

