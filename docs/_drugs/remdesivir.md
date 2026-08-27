---
layout: default
title: Remdesivir
parent: 僅模型預測 (L5)
nav_order: 301
evidence_level: L5
indication_count: 6
---

# Remdesivir
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

# Remdesivir: From COVID-19 to Multiple Endocrine Neoplasia

## One-Sentence Summary

Remdesivir is a nucleotide analogue prodrug developed as a broad-spectrum RNA-dependent RNA polymerase (RdRp) inhibitor, originally studied and approved for COVID-19 (and earlier investigated for Ebola virus disease). The TxGNN model predicts potential efficacy for **Multiple Endocrine Neoplasia (MEN)**, but this prediction is currently supported by **zero clinical trials** and **zero publications** — it is a model-score-only signal that the evidence pack itself flags as mechanistically implausible.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | COVID-19 (not confirmed via New Zealand regulatory data — no licenses on file; historically also investigated for Ebola virus disease) |
| Predicted New Indication | Multiple Endocrine Neoplasia |
| TxGNN Prediction Score | 99.50% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed, verified mechanism-of-action data from DrugBank is not currently available (flagged as a High-severity data gap, DG002). Based on information captured in this evidence pack, remdesivir is a nucleotide analogue prodrug that inhibits the RNA-dependent RNA polymerase (RdRp) of RNA viruses such as SARS-CoV-2 and Ebola virus — an antiviral mechanism with no known biological pathway overlapping endocrine tumour syndromes.

Multiple Endocrine Neoplasia (MEN) is driven by germline mutations in genes such as *RET* or *MEN1*, causing hormone-secreting tumours across multiple endocrine glands. This is a genetically driven oncogenic/endocrine process, not a viral infection, and it does not depend on RdRp activity or any other target known to be modulated by remdesivir.

The evidence pack's own mechanistic assessment concludes that this prediction lacks mechanistic plausibility and that the high TxGNN score most likely reflects a spurious correlation at the embedding level rather than a genuine pharmacological relationship. No clinical trials, no preclinical studies, and no literature currently exist connecting remdesivir to MEN — this is a pure model-prediction signal (L5) with no corroborating evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Remdesivir is not currently marketed in New Zealand (0 authorizations on file), so no product/dosage-form/indication table can be produced.

---

## Safety Considerations

Please refer to the package insert for safety information. (No structured warnings, contraindications, or drug-interaction data are currently available in this evidence pack — DG001, TFDA package insert retrieval, is flagged as a Blocking gap preventing formal S1 safety review.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The MEN prediction has no supporting clinical trials or literature and the proposed mechanistic link is explicitly assessed as biologically implausible — the high TxGNN score alone is insufficient to justify any further review stage.

**To proceed, the following is needed:**
- Resolve DG001 (Blocking): obtain and parse the TFDA/official package insert for warnings, contraindications, and DDI data before any safety evaluation can begin
- Resolve DG002 (High): confirm verified MOA data via DrugBank API rather than the narrative summary in this evidence pack
- Any preclinical or mechanistic rationale specifically linking RdRp inhibition to MEN pathophysiology, if it is to be pursued further
- Note for portfolio triage: the rank-2 candidate ("HIV infectious disease") in this same evidence pack shows a disease-label mismatch — all 23 linked trials and effectively all 20 linked publications are actually COVID-19/Ebola studies, not HIV studies. This suggests the underlying evidence-matching pipeline may need review before other remdesivir candidates in this pack are trusted at face value.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

