---
layout: default
title: Nintedanib
parent: 僅模型預測 (L5)
nav_order: 243
evidence_level: L5
indication_count: 3
---

# Nintedanib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

Using the evidence pack (NINTEDANIB, DB09079), here is the repurposing evaluation report. Note upfront: `original_moa`, `original_indications`, and all `safety` fields are explicitly marked as data gaps in the pack, so I have not fabricated values for them — this is called out where relevant rather than guessed.

---

# Nintedanib: From Angiokinase Inhibition to a New Role in Dermatofibrosarcoma Protuberans

*(Original approved indication is not populated in this evidence pack — see Data Gaps below. Nintedanib is presented here by its known pharmacological class, a triple angiokinase inhibitor, pending confirmation of its labeled indication.)*

## One-Sentence Summary

> Nintedanib's original approved indication is not documented in the current evidence pack (data gap). The TxGNN model predicts it may be effective for **Dermatofibrosarcoma Protuberans (DFSP)**, based on its known activity as a VEGFR/FGFR/PDGFR-targeting tyrosine kinase inhibitor. This direction is currently supported by **0 clinical trials** and **1 review publication** — evidence is mechanistic/theoretical only, not yet clinically tested.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in evidence pack (data gap — DrugBank/TFDA license extraction pending) |
| Predicted New Indication | Dermatofibrosarcoma Protuberans |
| TxGNN Prediction Score | 99.15% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data (`original_moa`) is not available as a standalone field. However, the repurposing rationale in this evidence pack describes nintedanib as a **triple angiokinase inhibitor**, with activity against VEGFR, FGFR, and PDGFR. This is consistent with its known public profile as a multi-target tyrosine kinase inhibitor.

Dermatofibrosarcoma protuberans (DFSP) is a rare soft-tissue sarcoma with a well-characterized molecular driver: the **COL1A1-PDGFB gene fusion**, which causes constitutive activation of PDGFRβ signaling and drives tumor growth. This mechanism is already clinically validated — imatinib, a PDGFR-targeted tyrosine kinase inhibitor, is an approved treatment for DFSP. Since nintedanib's PDGFR-inhibitory activity overlaps directly with this validated disease mechanism, there is a plausible biological rationale for its potential efficacy in DFSP.

That said, the supporting evidence in this pack is limited to a single review article discussing PDGFR inhibitors as a drug class in oncology — it does not report primary data on nintedanib specifically in DFSP, nor is there any clinical trial evidence. The mechanistic logic is sound, but the evidence strength remains indirect (class-level, not drug-specific).

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29408302](https://pubmed.ncbi.nlm.nih.gov/29408302/) | 2018 | Review | Pharmacological Research | Reviews the role of small-molecule PDGFR inhibitors as a drug class in treating neoplastic disorders driven by PDGF signaling; discusses PDGFR inhibition as a therapeutic strategy relevant to PDGFR-fusion-driven tumors such as DFSP, but does not report nintedanib-specific trial or case data. |

---

## New Zealand Market Information

Nintedanib is currently **not marketed** in New Zealand under this evidence pack (`total_licenses: 0`), and no product authorizations are on file. No authorization table can be generated at this time.

---

## Safety Considerations

Please refer to the package insert for safety information. *(All safety fields — key warnings, contraindications, and drug interactions — are unpopulated in this evidence pack; DDI query returned "not_found.")*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- Evidence level is L4 (mechanistic/class-level rationale only) with a single non-drug-specific review article and zero clinical trials for the lead candidate indication (DFSP). While the PDGFR-mechanism argument is biologically credible given the precedent of imatinib in DFSP, there is no direct evidence — preclinical or clinical — for nintedanib itself in this indication, and core safety/regulatory data for the drug are also missing.

**To proceed, the following is needed:**
- Confirmed original indication and mechanism of action (DG002 — DrugBank API lookup)
- TFDA/package-insert warnings, contraindications, and DDI data (DG001 — currently Blocking; required before any S1 safety pre-assessment)
- Nintedanib-specific preclinical evidence (e.g., PDGFRβ-fusion cell line or xenograft models) supporting activity in DFSP
- A drug-specific literature or case-report search (current single hit is a general PDGFR-inhibitor class review, not nintedanib-specific)

---

### Note: Additional Lower-Priority Candidate Indications

Two further indications were predicted by TxGNN but currently have **no clinical trial or literature evidence at all** and are scored L5 (model prediction only):

| Predicted Indication | TxGNN Score | Evidence Level | Recommendation |
|---|---|---|---|
| Liposarcoma | 99.13% | L5 | Hold |
| Ovarian Myxoid Liposarcoma | 99.12% | L5 | Hold |

Both are held pending any supporting mechanistic or clinical data; no action is recommended on these at this time.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

