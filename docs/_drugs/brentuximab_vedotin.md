---
layout: default
title: Brentuximab Vedotin
parent: 僅模型預測 (L5)
nav_order: 52
evidence_level: L5
indication_count: 10
---

# Brentuximab Vedotin
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

# Brentuximab Vedotin: From Hodgkin Lymphoma / sALCL to Follicular Lymphoma

## One-Sentence Summary

Brentuximab vedotin (BV) is an anti-CD30 antibody–drug conjugate approved for relapsed/refractory classical Hodgkin lymphoma (cHL) and systemic anaplastic large-cell lymphoma (sALCL), where CD30 is universally or highly expressed on tumour cells.
The TxGNN model predicts it may be effective for **Follicular Lymphoma (FL)**, with **6 clinical trials** and **20 publications** currently informing this direction — though the majority are indirect or non-FL-specific.
The mechanistic basis is biologically weak (CD30 expression in FL is low and unstable in < 10% of cases), and the sole active direct trial (NCT04587687) is still recruiting; a decision to proceed should await its results.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Classical Hodgkin Lymphoma; Systemic Anaplastic Large Cell Lymphoma (sALCL) |
| Predicted New Indication | Follicular Lymphoma |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed (0 authorisations) |
| Number of Authorisations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the data source. Based on contextual information from the clinical trial records in this Evidence Pack, brentuximab vedotin is an antibody–drug conjugate (ADC) composed of an anti-CD30 monoclonal antibody (brentuximab) linked to the microtubule-disrupting cytotoxin MMAE (monomethyl auristatin E). Upon binding to CD30-expressing cells, the conjugate is internalised, MMAE is released intracellularly, and it arrests cell division by disrupting the tubulin network — ultimately triggering apoptosis.

The mechanistic bridge to follicular lymphoma rests on whether FL cells express CD30. Unlike cHL (near-universal CD30 expression on Reed–Sternberg cells) or sALCL (CD30 universally expressed), FL cells show CD30 positivity in fewer than 10% of cases, and expression is typically low-level and heterogeneous. This fundamentally limits the selectivity of BV's antibody-targeting mechanism in unselected FL patients. PMID 32476657 documents a single case where Grade I FL underwent histological transformation to CD30+ ALCL, after which BV produced a complete response — but this represents transformed disease, not de novo FL, and cannot be generalised to FL as an entity.

The TxGNN model likely identified the shared lymphoma network topology (FL is a B-cell non-Hodgkin lymphoma with overlapping biological pathways to CD30-positive subtypes) as the basis for this prediction. While the prediction score is high (99.89%), it reflects graph-structural similarity rather than confirmed CD30 biology in FL. The currently recruiting Phase 2 trial NCT04587687 is directly testing BV + Bendamustine in relapsed/refractory FL and represents the key piece of evidence that could either validate or refute this hypothesis.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|-----------|--------------|
| [NCT04587687](https://clinicaltrials.gov/study/NCT04587687) | Phase 2 | Recruiting | 23 | BV + Bendamustine specifically in R/R FL; the only active, directly relevant trial — results will determine feasibility of this combination |
| [NCT02594163](https://clinicaltrials.gov/study/NCT02594163) | Phase 2 | Terminated | 25 | Rituximab + Bendamustine ± BV in R/R CD30+ DLBCL; early termination limits conclusions, but provides limited safety data in the B-cell NHL setting |
| [NCT01805037](https://clinicaltrials.gov/study/NCT01805037) | Phase 1/2 | Terminated | 20 | BV + Rituximab as frontline therapy for CD30+ and/or EBV+ lymphomas (including FL); CD30 selection criterion is an important design precedent for FL sub-group identification |
| [NCT04138875](https://clinicaltrials.gov/study/NCT04138875) | Phase 2 | Withdrawn | 0 | Risk-stratified sequential treatment with Rituximab-BV-Bendamustine (RBvB) for newly diagnosed PTLD with CD20/CD30 expression; withdrawn before enrolment — no data generated |
| [NCT02623920](https://clinicaltrials.gov/study/NCT02623920) | Phase 2 | Withdrawn | 0 | BV + Bendamustine + Rituximab for R/R CD30+ B-cell NHL (including FL); withdrawn before enrolment — no data generated |
| [NCT04795869](https://clinicaltrials.gov/study/NCT04795869) | Phase 2 | Withdrawn | 0 | BV + Pembrolizumab for recurrent PTCL; broad NHL indication, not FL-specific, withdrawn before enrolment |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [32476657](https://pubmed.ncbi.nlm.nih.gov/32476657/) | 2020 | Case Report | Gulf J Oncol | Grade I FL transforming to CD30+ ALK1− ALCL; BV + high-dose methotrexate achieved complete response — demonstrates BV activity in FL transformation context, not primary FL |
| [35663281](https://pubmed.ncbi.nlm.nih.gov/35663281/) | 2022 | Review | Leuk Res Rep | Immunotherapy in indolent NHL including FL; reviews monoclonal antibody landscape — contextual background for BV positioning in iNHL |
| [28967896](https://pubmed.ncbi.nlm.nih.gov/28967896/) | 2018 | Review | Bone Marrow Transplant | Post-ASCT maintenance in lymphoma; mentions FL maintenance rituximab and BV post-ASCT consolidation in HL — contextual, not FL-BV direct evidence |
| [40758949](https://pubmed.ncbi.nlm.nih.gov/40758949/) | 2025 | Phase 2 Trial | Blood Adv | BV + Gemcitabine followed by BV maintenance in R/R PTCL (≥5% CD30+); ORR and PFS reported — PTCL, not FL, but establishes BV combination activity in non-HL lymphoma |
| [38306597](https://pubmed.ncbi.nlm.nih.gov/38306597/) | 2024 | Review | Blood | Current and upcoming treatment for common PTCL subtypes including PTCL-NOS, ALCL, TFH lymphomas; BV + CHP as frontline for CD30+ PTCL — relevant as background on BV in NHL, not FL |

> **Note:** The majority of the 20 retrieved publications relate to PTCL, HL, or DLBCL rather than FL specifically. Dedicated FL–BV literature is sparse, reflecting the early-stage nature of this repurposing hypothesis.

---

## New Zealand Market Information

Brentuximab vedotin currently holds **no regulatory authorisations** in New Zealand. No product licences, approved indications, or marketed formulations are on record.

---

## Cytotoxicity

Brentuximab vedotin qualifies as an antineoplastic agent: it is an ADC with MMAE (a cytotoxic microtubule-disrupting agent) as its payload, and it is used exclusively for malignant haematological conditions.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted cytotoxic — Antibody–Drug Conjugate (anti-CD30 × MMAE, auristatin class) |
| Myelosuppression Risk | Moderate to High — neutropenia is the most common haematological toxicity reported across cHL/sALCL trials; febrile neutropenia documented |
| Emetogenicity Classification | Low to Moderate (MMAE payload; IV infusion every 3 weeks) |
| Monitoring Items | CBC with differential (each cycle), liver function tests, renal function, peripheral neuropathy assessment, serum uric acid (tumour lysis risk in lymphoma) |
| Handling Protection | Must be handled according to cytotoxic drug handling regulations; MMAE payload has vesicant potential — closed-system drug transfer devices recommended |

---

## Safety Considerations

Please refer to the package insert for safety information. Detailed warnings, contraindications, and drug interaction data were not available in this Evidence Pack (data gaps DG001 and DG002 are flagged as Blocking/High severity). Key areas to investigate prior to any clinical use include: peripheral neuropathy (a class effect of MMAE-based ADCs), infusion-related reactions, pulmonary toxicity, and opportunistic infections.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The biological basis for BV in follicular lymphoma is weak — CD30 expression occurs in fewer than 10% of FL cases, meaning the majority of FL patients would not have the drug's target. Only one active Phase 2 trial (NCT04587687, BV + Bendamustine, n=23, recruiting) is directly evaluating this combination in R/R FL; all other trials in this Evidence Pack were terminated or withdrawn without generating data. The evidence level (L3) reflects indirect and observational support rather than completed controlled trials in FL.

**To proceed, the following is needed:**

- Results from NCT04587687 (BV + Bendamustine in R/R FL; expected completion December 2026) — this trial is the decision gate
- CD30 expression profiling data in FL patients to identify the CD30-positive sub-population that could plausibly benefit
- Mechanism of action documentation (DrugBank MOA query — data gap DG002)
- New Zealand-specific regulatory pathway assessment: BV is not currently approved in NZ for any indication; global approval status (FDA/EMA) for cHL/sALCL should be documented as part of the regulatory bridge strategy
- Safety profile review from the package insert (data gap DG001 — TFDA warnings and contraindications required for safety pre-screening)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

