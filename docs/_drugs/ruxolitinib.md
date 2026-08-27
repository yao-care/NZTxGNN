---
layout: default
title: Ruxolitinib
parent: 僅模型預測 (L5)
nav_order: 315
evidence_level: L5
indication_count: 10
---

# Ruxolitinib
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

# Ruxolitinib: From Myelofibrosis/GvHD to Infection-Associated Hemophagocytic Syndrome

> **Note on scope:** This evidence pack lists 10 TxGNN-predicted indications for ruxolitinib. Nine of them (uterine corpus PEComa, benign PEComa, lymphangiomyoma, LAM, liposarcoma, familial rhabdoid tumor, lung PEComa, ovarian myxoid liposarcoma, malignancy-associated HLH) have TxGNN scores >99% but **zero** supporting clinical trials or literature (Evidence Level L5, recommendation "Hold"). Only **rank 9 — "hemophagocytic syndrome associated with an infection"** — has actual clinical trial and literature support, so this report focuses on that candidate as the actionable one.

## One-Sentence Summary

Ruxolitinib is a JAK1/2 inhibitor internationally approved for myelofibrosis, polycythemia vera, and steroid-refractory graft-versus-host disease (GvHD), though it currently holds no New Zealand market authorization in this dataset. The TxGNN model predicts it may be effective for **infection-associated hemophagocytic syndrome (HLH)**, and this is the one candidate among ten backed by real-world data: **2 clinical trials** and **20 publications**, including several cohort studies of ruxolitinib used as salvage or first-line HLH therapy.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this evidence pack (0 New Zealand licenses on file); internationally, ruxolitinib is approved for myelofibrosis, polycythemia vera, and steroid-refractory acute/chronic GvHD |
| Predicted New Indication | Hemophagocytic syndrome associated with an infection |
| TxGNN Prediction Score | 99.32% (graph rank 5646) |
| Evidence Level | L3 (observational cohort studies; no completed Phase 2/3 RCT yet — the one Phase 3 trial is status UNKNOWN, and the Phase 1 trial has not started recruiting) |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed structured MOA data was not populated in this evidence pack (flagged as a High-severity data gap). Based on established pharmacology, ruxolitinib is a small-molecule inhibitor of Janus kinase 1 and 2 (JAK1/2), blocking downstream JAK-STAT signaling triggered by pro-inflammatory cytokines such as IFN-γ, IL-6, and IL-2.

Hemophagocytic lymphohistiocytosis (HLH) is a hyperinflammatory cytokine-storm syndrome — in infection-triggered HLH, excessive IFN-γ and IL-6 signaling drives macrophage/T-cell activation and multi-organ damage. Because JAK1/2 sits directly downstream of these cytokines, blocking this pathway is mechanistically well-matched to the disease, distinct from the tenuous "graph co-occurrence" links seen in the other nine PEComa/sarcoma-family predictions (which are mTOR- or SWI/SNF-driven, not JAK-STAT-driven, and have no supporting evidence at all).

This mechanistic plausibility is reinforced by ruxolitinib's approved use in steroid-refractory GvHD — another cytokine-storm-driven inflammatory condition — showing the drug is already used clinically for JAK-STAT-mediated hyperinflammation, not just myeloproliferative disease.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04424056](https://clinicaltrials.gov/study/NCT04424056) | Phase 3 | Unknown | 216 | Randomized trial of Anakinra/Tocilizumab alone or combined with ruxolitinib for severe COVID-19-associated hyperinflammatory disease (stage 2b/3); status not confirmed as recruiting/completed since last update |
| [NCT07424222](https://clinicaltrials.gov/study/NCT07424222) | Phase 1 | Not yet recruiting | 16 | Pilot study of ruxolitinib for CAR-T-associated Immune Effector Cell-Associated HLH-like Syndrome (IEC-HS); aims to determine optimal treatment duration and identify response biomarkers |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34605776](https://pubmed.ncbi.nlm.nih.gov/34605776/) | 2022 | Guideline/Consensus | Critical Care Medicine | Consensus guideline on recognition, diagnosis, and management of HLH in critically ill children and adults |
| [35344583](https://pubmed.ncbi.nlm.nih.gov/35344583/) | 2022 | Cohort | Blood | Ruxolitinib as a response-stratified first-line agent in a prospective cohort of pediatric HLH (n=50+) |
| [37787838](https://pubmed.ncbi.nlm.nih.gov/37787838/) | 2023 | Cohort (compassionate use) | Annals of Hematology | Sintilimab + ruxolitinib as compassionate therapy in 12 adults with EBV-associated HLH |
| [40665481](https://pubmed.ncbi.nlm.nih.gov/40665481/) | 2025 | Cohort | British Journal of Haematology | Retrospective comparison: ruxolitinib-based regimen (n=53) vs. adjusted HLH-94 chemotherapy (n=42) in pediatric EBV-HLH |
| [31943120](https://pubmed.ncbi.nlm.nih.gov/31943120/) | 2020 | Review | QJM | Review of adult HLH diagnosis and treatment landscape |
| [31015190](https://pubmed.ncbi.nlm.nih.gov/31015190/) | 2019 | Mechanistic study | Blood | Foundational murine model study establishing ruxolitinib's mechanism of action in HLH (dampens T-cell activation, reduces IFN-γ-driven inflammation) |
| [31879790](https://pubmed.ncbi.nlm.nih.gov/31879790/) | 2020 | Cohort | Annals of Hematology | Ruxolitinib for steroid-refractory acute GvHD in HSCT patients with concurrent EBV-HLH (n=12) — bridges original approved GvHD use and the HLH prediction |
| [38691058](https://pubmed.ncbi.nlm.nih.gov/38691058/) | 2024 | Case Series | J Pediatr Hematol Oncol | Emapalumab + ruxolitinib + dexamethasone for EBV-HLH with multiorgan damage and severe infection |
| [36263041](https://pubmed.ncbi.nlm.nih.gov/36263041/) | 2022 | Case Report | Frontiers in Immunology | Ruxolitinib as first-line therapy for secondary HLH in AIDS patients |
| [37702780](https://pubmed.ncbi.nlm.nih.gov/37702780/) | 2023 | Review | Innere Medizin | Review of HLH management in the ICU setting, including targeted JAK inhibition |

## New Zealand Market Information

Ruxolitinib currently has no marketed products or authorizations on file in this evidence pack (market status: Not marketed; 0 licenses).

## Safety Considerations

Please refer to the package insert for safety information. (No structured warnings, contraindications, or drug-interaction data were available in this evidence pack — this is flagged as a **Blocking** data gap that must be resolved before any safety assessment.)

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple cohort studies, a foundational mechanism-of-action study, and case series consistently support ruxolitinib's use in infection/EBV-associated HLH as salvage or first-line therapy, and the JAK-STAT mechanism is directly relevant to this cytokine-storm disease. However, no completed randomized trial yet confirms efficacy (the one Phase 3 trial is status-unknown and the Phase 1 trial hasn't started), and the drug is unmarketed in New Zealand — this keeps the evidence at L3 rather than higher.

**To proceed, the following is needed:**
- **Blocking:** TFDA/Medsafe package insert (warnings, contraindications) — safety evaluation (S1) cannot proceed without this
- Confirmed MOA and DrugBank drug-category data (currently a data gap)
- Drug-drug interaction (DDI) data — current query returned no results
- Follow-up on NCT04424056 (status UNKNOWN) and NCT07424222 (not yet recruiting) for updated results
- A New Zealand regulatory pathway assessment, given zero current local licenses
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

