---
layout: default
title: Palbociclib
parent: 僅模型預測 (L5)
nav_order: 262
evidence_level: L5
indication_count: 4
---

# Palbociclib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Palbociclib: From Breast Cancer to Hyperthyroidism

## One-Sentence Summary

Palbociclib is a CDK4/6 inhibitor whose evidence-pack literature consistently describes it as a treatment for HR+/HER2- metastatic breast cancer (the drug's own structured original-indication and MOA fields are data gaps). The TxGNN model's top prediction is **Hyperthyroidism**, but this ranking is supported by **zero clinical trials and zero publications** — it is a pure model output with no identifiable mechanistic rationale between CDK4/6 inhibition and thyroid hormone pathways.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in structured data (TFDA/NZ license text and `original_indications` are data gaps — DG001/DG002). Literature within this evidence pack repeatedly identifies palbociclib as a therapy for HR+/HER2- metastatic breast cancer. |
| Predicted New Indication | Hyperthyroidism |
| TxGNN Prediction Score | 99.44% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap DG002). Based on the literature captured elsewhere in this evidence pack, palbociclib is a CDK4/6 inhibitor used for HR+/HER2- metastatic breast cancer, acting on the G1/S cell-cycle checkpoint.

For the top-ranked prediction, hyperthyroidism, the evidence pack's own rationale states there is **no identifiable mechanistic link**: CDK4/6 inhibition acts on cell-cycle regulation, with no known intersection with thyroid hormone synthesis, release, or receptor signaling. No clinical trials or publications support this candidate — it is a model-only association (TxGNN score 99.44%, evidence level L5).

This is a case where a high TxGNN score is not corroborated by mechanistic or literature evidence. By contrast, the pack's other ranked candidates carry more supporting information and are worth flagging for the record: **rheumatoid arthritis** (rank 2, L4) has four supporting publications, including a case report of RA improvement during palbociclib treatment and preclinical evidence of CDK6-dependent synovial hyperplasia; and **thrombotic disease** (rank 3, L4) is actually a *safety signal*, not a therapeutic lead — pharmacovigilance and cohort studies show CDK4/6 inhibitors are associated with an **increased** risk of thromboembolic events, the opposite of a treatment effect.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Palbociclib is not currently marketed in New Zealand (0 authorizations on file; no license records available).

---

## Cytotoxicity

Palbociclib is an antineoplastic agent (CDK4/6 inhibitor used in breast cancer, per the literature captured in this evidence pack).

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (CDK4/6 inhibitor) |
| Myelosuppression Risk | High — the evidence pack's own literature notes palbociclib-induced myelosuppression (PMID 39940918) and describes bone marrow suppression as a common adverse event for this drug class (PMID 37994878) |
| Emetogenicity Classification | Not established in current evidence pack; refer to package insert |
| Monitoring Items | CBC with differential (neutropenia monitoring), liver and renal function |
| Handling Protection | Please refer to the package insert warnings and precautions; no TFDA/NZ label data available (DG001) |

---

## Safety Considerations

Please refer to the package insert for safety information (key warnings, contraindications, and DDI data are all data gaps in this evidence pack).

**Note from evidence review:** although not part of the structured safety fields, the literature evidence gathered for the "thrombotic disease" candidate (rank 3) indicates CDK4/6 inhibitors, including palbociclib, are associated with an **increased** risk of thromboembolic events in real-world and pharmacovigilance studies (PMID 35300061, 36794339, 39123221, 39083396, 41496429). This should be treated as a safety consideration for any repurposing pathway, not a therapeutic signal.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (hyperthyroidism) has no supporting clinical trials, no supporting literature, and no identifiable mechanistic link — it is evidence level L5, a model-only association. There is insufficient basis to advance this indication.

**To proceed, the following is needed:**
- Original indication and MOA data for palbociclib (DrugBank API lookup — DG002)
- TFDA/NZ package insert warnings and contraindications (DG001)
- If pursuing repurposing further, prioritize the rheumatoid arthritis candidate (rank 2, L4, 4 supporting publications) over hyperthyroidism, which currently has no evidentiary basis
- Treat the thromboembolism signal surfaced under "thrombotic disease" (rank 3) as a safety monitoring item, not a repurposing candidate
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

