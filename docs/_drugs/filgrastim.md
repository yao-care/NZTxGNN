---
layout: default
title: Filgrastim
parent: 僅模型預測 (L5)
nav_order: 151
evidence_level: L5
indication_count: 10
---

# Filgrastim
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

Using the drug-repurposing evaluation report template to produce the requested report from the given Evidence Pack. Below is the completed report.

---

# Filgrastim: From Neutrophil Growth Factor Therapy to Primary Platelet Release Disorder

## One-Sentence Summary

Filgrastim (DB00099) is a recombinant human G-CSF whose formal original indication and regulatory profile are not documented in this evidence pack (both `original_indications` and `original_moa` are flagged as data gaps). The TxGNN model predicts it may be effective for **Primary Release Disorder of Platelets**, but the **14 clinical trials** and **1 publication** retrieved are almost entirely indirect — G-CSF appears only as a supportive stem-cell mobilization agent in unrelated hematopoietic transplant protocols, not as a direct treatment for platelet release/storage pool disease.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no license or approved-indication text on file (drug not marketed in this jurisdiction) |
| Predicted New Indication | Primary Release Disorder of Platelets |
| TxGNN Prediction Score | 99.9976% |
| Evidence Level | L4 (mechanistic/indirect trial evidence only; no study directly targets the predicted disease) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action and original-indication data for filgrastim are not available in this evidence pack (flagged as data gaps DG001/DG002). Based on the mechanistic notes embedded in the evidence pack itself, filgrastim (recombinant G-CSF) acts on the CSF3R receptor on myeloid granulocyte precursor cells in the bone marrow, promoting neutrophil production and mobilizing hematopoietic stem cells into peripheral blood — this is a granulocyte-lineage / stem-cell-mobilization mechanism, not a platelet-specific one.

The predicted indication, primary release disorder of platelets, is a storage pool disease caused by defective platelet dense-granule/α-granule secretion machinery — a distinct pathway from granulocyte colony stimulation. The evidence pack's own mechanistic assessment explicitly states there is **no known direct pathway** connecting the G-CSF/CSF3R axis to platelet granule secretion defects.

Despite the very high TxGNN score, the supporting evidence is weak: the single literature citation is a cohort study on lymphocyte (not platelet) mobilization in stem cell donors, and the majority of the 14 clinical trials are hematopoietic stem cell transplant (HSCT) studies in which filgrastim is used only as a routine donor-mobilization or supportive-care agent for unrelated hematologic malignancies — none investigates platelet release disorders directly. This pattern is consistent with the score reflecting proximity between "bone marrow / hematopoietic system" nodes in the knowledge graph rather than a substantiated pharmacological relationship, and should be treated as a low-confidence signal pending mechanistic validation.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00281879](https://clinicaltrials.gov/study/NCT00281879) | Phase 2 | Terminated | 200 | Unrelated donor HSCT for hematologic malignancies; G-CSF role limited to stem-cell mobilization/supportive care, not disease-specific |
| [NCT00043979](https://clinicaltrials.gov/study/NCT00043979) | Phase 2 | Completed | 60 | Allogeneic/syngeneic blood stem cell transplant in pediatric sarcomas — G-CSF used as donor mobilizer (Grade B: supportive-use relevance only) |
| [NCT00354172](https://clinicaltrials.gov/study/NCT00354172) | Phase 2 | Terminated | 16 | Umbilical cord blood transplant for myeloid leukemia not in CR |
| [NCT00923364](https://clinicaltrials.gov/study/NCT00923364) | Phase 2 | Completed | 19 | Reduced-intensity HSCT for patients with GATA2 mutations |
| [NCT02646098](https://clinicaltrials.gov/study/NCT02646098) | Phase 2 | Completed | 64 | CD34+ selected vs. unselected autologous transplant in mantle cell/DLBCL lymphoma (Grade C: unrelated disease entity, likely KG mismatch) |
| [NCT05436418](https://clinicaltrials.gov/study/NCT05436418) | Phase 1/2 | Recruiting | 260 | Post-transplant cyclophosphamide dose-finding for GVHD prophylaxis after PBSC transplant (Grade B: G-CSF as adjunct mobilizer) |
| [NCT05170828](https://clinicaltrials.gov/study/NCT05170828) | Phase 1 | Withdrawn | 0 | Cryopreserved HLA-mismatched unrelated donor bone marrow transplant with PTCy |
| [NCT00076752](https://clinicaltrials.gov/study/NCT00076752) | Phase 2 | Completed | 9 | Intensified lymphodepletion + autologous HSCT for severe systemic lupus erythematosus |
| [NCT04540120](https://clinicaltrials.gov/study/NCT04540120) | Phase 2 | Terminated | 49 | Oral dapansutrile for COVID-19 with early cytokine release syndrome (Grade C: no clear filgrastim/platelet link, likely search noise) |
| [NCT06859424](https://clinicaltrials.gov/study/NCT06859424) | Phase 2 | Recruiting | 358 | Platform trial of PTCy-based GVHD prophylaxis after mismatched unrelated donor PBSC transplant |

*4 additional trials were retrieved (NCT04047628, NCT01335932, NCT01503918, NCT00245037) but are omitted here as lower-relevance transplant/infection-prophylaxis studies with no direct link to platelet release disorders.*

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29770133](https://pubmed.ncbi.nlm.nih.gov/29770133/) | 2018 | Cohort | Frontiers in Immunology | G-CSF mobilization in healthy donors preferentially mobilizes lymphocyte subsets; does not address platelet granule release function |

## New Zealand Market Information

Filgrastim currently has **no market authorization on record** in this jurisdiction (market status: Not Marketed; total licenses: 0). No product/dosage-form data is available to summarize.

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug-interaction data are all flagged as data gaps in this evidence pack; DDI query returned no results.)

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- No clinical trial or publication directly studies filgrastim in primary platelet release disorder — all retrieved evidence is indirect (G-CSF used only as a supportive stem-cell mobilization agent in unrelated HSCT protocols).
- The evidence pack's own mechanistic analysis concludes there is no established biological pathway linking the G-CSF/CSF3R granulocyte axis to platelet dense-granule secretion defects; the high TxGNN score likely reflects knowledge-graph node proximity rather than genuine pharmacology.
- A Blocking-severity data gap (DG001: TFDA/regulatory package insert safety data) prevents even a preliminary safety (S1) assessment.
- The drug is not currently marketed in this jurisdiction (0 authorizations), adding a regulatory barrier on top of the weak evidence base.
- Note: all 9 other TxGNN-predicted indications for filgrastim in this pack (pseudo-von Willebrand disease, Glanzmann thrombasthenia, Scott syndrome, etc.) are similarly rated L4/L5 with "Hold" recommendations and equally weak or absent mechanistic overlap — this candidate profile shows a systematic pattern of high-score/low-evidence predictions rather than an isolated exception.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data to resolve the blocking safety data gap (DG001)
- Confirmed original indication and mechanism-of-action documentation (DG002)
- A dedicated preclinical/mechanistic study directly testing G-CSF's effect on platelet dense-granule secretion or a storage pool disease model
- Manual re-review of the trials still graded "pending" to rule out further knowledge-graph keyword-matching noise
- If pursued, a regulatory pathway assessment given filgrastim's current unmarketed status in this jurisdiction
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

