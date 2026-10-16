---
layout: default
title: Cyclophosphamide
parent: Model Prediction Only (L5)
nav_order: 125
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Cyclophosphamide: predicted new indication Myeloid leukemia (evidence level L5). Approved use: Mycosis fungoides and cutaneous T-cell lymphoma, plasma cell myeloma…"
---

# Cyclophosphamide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Cyclophosphamide: From Lymphoma and Breast Cancer to Myeloid Leukemia

## One-Sentence Summary

Cyclophosphamide is an alkylating chemotherapy agent originally used for cancers such as lymphoma, multiple myeloma, neuroblastoma, breast carcinoma and retinoblastoma.
The TxGNN model predicts it may be effective for **myeloid leukemia**,
with **35 clinical trials** and **20 publications** currently supporting this direction, almost all of them using cyclophosphamide as part of combination or transplant regimens rather than on its own.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Mycosis fungoides and cutaneous T-cell lymphoma, plasma cell myeloma, neuroblastoma, lymphoma, breast carcinoma, retinoblastoma (the New Zealand licence records contain no approved indication text) |
| Predicted New Indication | Myeloid leukemia |
| TxGNN Prediction Score | 99.47% |
| Evidence Level | L1 (as assigned in the data compiled for this report; see the note under Clinical Trial Evidence) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, cyclophosphamide is an alkylating agent: its active metabolites crosslink DNA and give direct cytotoxicity against rapidly dividing cells, including leukemic cells.

Its original indications (lymphoma, myeloma, and other haematological and solid cancers) share this cytotoxic and immunomodulatory mechanism. In leukemia it is an established part of conditioning regimens before stem cell transplantation (for example busulfan-cyclophosphamide). It is also used as post-transplant cyclophosphamide (PTCy) to prevent graft-versus-host disease, where it selectively depletes alloreactive T cells while sparing regulatory T cells and stem cells.

An important caveat is that the evidence is strong and consistent but mostly describes cyclophosphamide inside combination or transplant regimens. It does not show leukemia-specific efficacy as a single agent.

## Clinical Trial Evidence

The table below lists the most relevant trials among those retrieved for myeloid leukemia. Most are small, early-phase transplant studies. Only one Phase 2/3 trial (NCT00003187, n=19) and one Phase 3 trial (NCT02724163, pediatric AML, n=700) appear in the list, and neither tests cyclophosphamide specifically. The L1 level assigned in the data therefore likely overstates the strength of leukemia-specific evidence, and I would treat it as closer to moderate.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00003187](https://clinicaltrials.gov/study/NCT00003187) | Phase 2/3 | Completed | 19 | Unrelated-donor marrow transplantation, conventional vs T-cell-depleted; mixed haematological malignancies, so cyclophosphamide attribution is limited |
| [NCT00390793](https://clinicaltrials.gov/study/NCT00390793) | Phase 2 | Completed | 107 | Hyper-CVAD (includes cyclophosphamide) plus dasatinib in Ph+ ALL; mainly lymphoid, so only partly relevant to myeloid leukemia |
| [NCT04473911](https://clinicaltrials.gov/study/NCT04473911) | Phase 1 | Completed | 25 | Haploidentical transplant with post-transplant cyclophosphamide and RGI-2001 for GVHD prevention |
| [NCT00315705](https://clinicaltrials.gov/study/NCT00315705) | Phase 1/2 | Completed | 50 | Clofarabine with etoposide and cyclophosphamide in pediatric relapsed or refractory acute leukemia |
| [NCT00939653](https://clinicaltrials.gov/study/NCT00939653) | Phase 2 | Terminated | 6 | Clofarabine, etoposide and cyclophosphamide in relapsed or refractory AML |
| [NCT04904588](https://clinicaltrials.gov/study/NCT04904588) | Phase 2 | Active, not recruiting | 300 | HLA-mismatched unrelated donor transplant with PTCy, tacrolimus and MMF in haematological malignancies |
| [NCT00309842](https://clinicaltrials.gov/study/NCT00309842) | Phase 2 | Completed | 213 | Cord blood transplant after cyclophosphamide/fludarabine/TBI myeloablative conditioning |
| [NCT02724163](https://clinicaltrials.gov/study/NCT02724163) | Phase 3 | Active, not recruiting | 700 | Pediatric AML induction chemotherapy with gemtuzumab ozogamicin; cyclophosphamide is not the variable under test |
| [NCT01350245](https://clinicaltrials.gov/study/NCT01350245) | Phase 2 | Completed | 28 | Two-step haploidentical stem cell transplant for haematological malignancies in remission |
| [NCT02057770](https://clinicaltrials.gov/study/NCT02057770) | Phase 1 | Terminated | 25 | Myeloablative allogeneic or haploidentical transplant with high-dose PTCy in relapsed or refractory AML |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36357773](https://pubmed.ncbi.nlm.nih.gov/36357773/) | 2023 | Systematic review / network meta-analysis | Bone Marrow Transplant | Compares myeloablative conditioning regimens, including Bu/Cy, in adult AML in complete remission |
| [38758817](https://pubmed.ncbi.nlm.nih.gov/38758817/) | 2024 | Registry study | Cancer | PTCy vs ATG vs combination for GVHD prevention in haploidentical transplant for adult AML |
| [40905088](https://pubmed.ncbi.nlm.nih.gov/40905088/) | 2026 | Cohort | Haematologica | 217 AML patients in remission after transplant with PTCy: 2-year OS 77%, EFS 72% |
| [41046309](https://pubmed.ncbi.nlm.nih.gov/41046309/) | 2026 | Cohort (registry) | Bone Marrow Transplant | Single-unit cord blood transplant vs haploidentical transplant with PTCy in AML in first remission |
| [42373850](https://pubmed.ncbi.nlm.nih.gov/42373850/) | 2026 | Cohort (registry) | Bone Marrow Transplant | Haploidentical vs mismatched unrelated transplant with PTCy in secondary AML |
| [38466265](https://pubmed.ncbi.nlm.nih.gov/38466265/) | 2024 | Cohort | Cytotherapy | Prognostic factors in haploidentical transplant with PTCy for AML |
| [40434956](https://pubmed.ncbi.nlm.nih.gov/40434956/) | 2025 | Cohort | Future Oncol | Busulfan-cyclophosphamide vs fludarabine-busulfan conditioning in AML |
| [25345651](https://pubmed.ncbi.nlm.nih.gov/25345651/) | 2015 | Cohort | Am J Hematol | Cyclophosphamide/fludarabine nonmyeloablative vs myeloablative allotransplant in 165 AML patients |
| [14662323](https://pubmed.ncbi.nlm.nih.gov/14662323/) | 2003 | Review | Exp Hematol | Busulfan-cyclophosphamide vs TBI-cyclophosphamide conditioning in AML |
| [35955881](https://pubmed.ncbi.nlm.nih.gov/35955881/) | 2022 | Cohort | Int J Mol Sci | PTCy after matched donor transplant in pediatric AML |

## New Zealand Market Information

All six licences on record have been cancelled, and none is currently valid. No approved indication text is recorded for any of them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2725614 | Cyclophosphamide Seacross | Inj 2 g vial | Not recorded (cancelled) |
| 2034220 | Endoxan | Inj 2 g vial | Not recorded (cancelled) |
| 2611597 | Cyclonex | Tab 50 mg | Not recorded (cancelled) |
| 785318 | Cytoxan | Inj 1 g vial | Not recorded (cancelled) |
| 2034212 | Endoxan | Inj 1 g vial | Not recorded (cancelled) |
| 2204266 | Baxter | Inj 1 mg for ECP | Not recorded (cancelled) |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (alkylating agent, oxazaphosphorine class) |
| Myelosuppression Risk | High (neutropenia is the main dose-limiting effect, particularly at conditioning doses) |
| Emetogenicity Classification | Moderate to high, dose-dependent |
| Monitoring Items | CBC with differential, liver and renal function, electrolytes, urinalysis for haematuria |
| Handling Protection | Must follow cytotoxic drug handling regulations |

Please refer to the package insert warnings and precautions for full details.

## Safety Considerations

- **Drug Interactions**: The interaction screening returned 100 interactions in total. The first 20 retrieved are mostly Moderate, with a few Minor. Main groups:
  - Amphotericin B products (conventional, lipid complex, cholesteryl sulfate, liposomal): Moderate
  - Sulfonylureas and related antidiabetics (chlorpropamide, acetohexamide, glimepiride, glipizide, glyburide, tolazamide, tolbutamide, nateglinide, repaglinide, troglitazone): Moderate
  - Others: bupropion, aprepitant, metronidazole, nitisinone (Moderate); levofloxacin and ondansetron (Minor)

Package insert warnings and contraindications for cyclophosphamide were not available in the data compiled for this report; please consult the product information before any use.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Cyclophosphamide is firmly established in transplant conditioning and PTCy regimens for myeloid leukemia, with many trials and cohort studies supporting this role. However, that evidence is regimen-level and mostly early-phase or observational, and the drug has no valid New Zealand licence at present. Use should be limited to defined transplant protocols rather than treated as a new stand-alone indication.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Confirmation of regulatory pathway, since all New Zealand licences are cancelled
- Leukemia-specific efficacy data separating cyclophosphamide's contribution from the rest of each regimen
- A safety and monitoring plan for myelosuppression, haemorrhagic cystitis and drug interactions in transplant patients

*Note on other predictions:* Lymph node cancer (rank 5) is supported by large Phase 3 breast cancer trials but largely overlaps existing indications. Ganglioneuroblastoma (rank 2) rests on case reports only. Retroperitoneal neoplasm (rank 4) and the vertebral anomalies/T-cell dysfunction syndrome (rank 3) are not supported well enough to proceed.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

