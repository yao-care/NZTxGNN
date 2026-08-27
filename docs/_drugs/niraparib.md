---
layout: default
title: Niraparib
parent: 僅模型預測 (L5)
nav_order: 244
evidence_level: L5
indication_count: 10
---

# Niraparib
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

Using the drug-repurposing evaluation report template to structure this Evidence Pack for Niraparib (DB11793).

A few things stand out in this Evidence Pack that shape how the report should read: `predicted_indications[0]` (rank 1, "epiglottis neoplasm") has zero supporting trials/literature despite the highest TxGNN score, `original_moa` is flagged `[Data Gap]`, and `taiwan_regulatory` shows the drug is not marketed in New Zealand (empty license list). I'll build the report strictly from the template's instructed field (`predicted_indications[0]`), state data gaps honestly instead of fabricating outside knowledge, and only use context that is actually present in the JSON (e.g., trial/literature text that mentions niraparib's known ovarian cancer use) to fill the MOA/original-indication fallback language.

---

# Niraparib: From Ovarian Cancer to Epiglottis Neoplasm

## One-Sentence Summary

> Niraparib is a PARP inhibitor whose established use — referenced within this evidence pack's own trial descriptions — is maintenance therapy for platinum-sensitive recurrent ovarian, fallopian tube, or primary peritoneal cancer. The TxGNN model's top-ranked prediction is **Epiglottis Neoplasm**, but currently **0 clinical trials** and **0 publications** support this specific prediction — it is a model-score-only signal with no corroborating evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from Taiwan/NZ regulatory data (drug not marketed). Based on trial descriptions within this evidence pack, niraparib's established use is maintenance treatment of recurrent epithelial ovarian, fallopian tube, or primary peritoneal cancer |
| Predicted New Indication | Epiglottis Neoplasm |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data for niraparib is not available (flagged as a High-severity data gap, DG002). Based on information contained elsewhere in this evidence pack (literature evidence under a different predicted indication, PMID 31466953), niraparib is described as "a poly adenosine diphosphate ribose polymerase inhibitor which uses the concept of synthetic lethality in the presence of a mutation in the breast cancer susceptibility gene (BRCA)," and is recommended as maintenance treatment for platinum-sensitive relapse of ovarian cancer. This PARP1/2 inhibition and synthetic-lethality mechanism is well established for BRCA-mutated, homologous-recombination-deficient (HRD) malignancies.

For the top-ranked prediction, **epiglottis neoplasm**, no mechanistic, trial, or literature evidence exists anywhere in this evidence pack. Per the model's own rationale field: epiglottis neoplasms are predominantly squamous cell carcinomas, and there is no publicly established or evidenced link between this tumor type and the BRCA/HRD pathway that underlies niraparib's synthetic-lethality mechanism. The prediction likely reflects the TxGNN model's similarity linkage to broad "neoplasm" nodes in the knowledge graph rather than a genuine, biologically grounded mechanistic signal.

It is worth noting that a lower-ranked prediction in this same evidence pack, **cystic neoplasm** (rank 2, score 99.99%, evidence level L2), is substantially better supported — it is anchored to high-grade serous carcinoma/BRCA-HRD biology with an active Phase 2 trial (NCT04716686, n=83, recruiting) and nine literature references. This suggests the overall TxGNN score alone is not a reliable proxy for evidence strength across ranked predictions, and epiglottis neoplasm specifically should not be advanced without independent validation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Niraparib is currently **not marketed** in New Zealand (0 authorizations on file). No license records are available in this evidence pack.

---

## Cytotoxicity

Niraparib is an antineoplastic agent (PARP inhibitor used in oncology, per trial/literature context in this evidence pack), so this section applies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (PARP inhibitor) — based on mechanism described in literature referenced elsewhere in this evidence pack |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information. No key warnings, contraindications, or drug-drug interaction data are currently available in this evidence pack (DDI query returned "not found"; TFDA package insert warnings/contraindications are flagged as a **Blocking** data gap, DG001).

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (epiglottis neoplasm) has zero supporting clinical trials or literature — this is a decision-stage S0, model-prediction-only signal (L5) with no biological or clinical corroboration. Combined with a Blocking-severity data gap on TFDA safety warnings/contraindications, this prediction cannot proceed past initial screening.

**To proceed, the following is needed:**
- Resolve DG001 (Blocking): obtain TFDA/manufacturer package insert warnings and contraindications before any safety pre-assessment (S1) can begin
- Resolve DG002 (High): obtain confirmed mechanism-of-action data from DrugBank or primary literature specific to niraparib
- Independent hypothesis generation or targeted literature/trial search specifically for niraparib in epiglottis or head-and-neck neoplasms, since none currently exists
- Given the disparity in evidence strength, consider prioritizing evaluation of the rank-2 candidate (**cystic neoplasm**, evidence level L2, active Phase 2 trial NCT04716686) instead of, or ahead of, epiglottis neoplasm
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

