---
layout: default
title: Minoxidil
parent: 僅模型預測 (L5)
nav_order: 228
evidence_level: L5
indication_count: 10
---

# Minoxidil
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

# Minoxidil: From Hypertension to Hypotrichosis Simplex of the Scalp

## One-Sentence Summary

Minoxidil is known in the evidence base as "the antihypertensive vasodilator" (per PMID 832355), originally used as a vasodilator antihypertensive and later repurposed for hair-growth indications such as androgenetic alopecia. The TxGNN model predicts it may also be effective for **Hypotrichosis Simplex of the Scalp** (HSS), a rare hereditary hair-loss disorder, with a prediction score of **~99.9999%**, though this direction is currently supported only by **3 case reports/series** and **no registered clinical trials**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Antihypertensive vasodilator (per literature reference, PMID 832355); not documented in New Zealand regulatory records — drug is unmarketed there |
| Predicted New Indication | Hypotrichosis Simplex of the Scalp |
| TxGNN Prediction Score | 99.9999% |
| Evidence Level | L4 (preclinical/mechanistic + case-report level only) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

A formal DrugBank mechanism-of-action record is not currently available (Data Gap DG002). However, the literature captured in this evidence pack (PMID 34159872, Gupta et al. 2022) describes minoxidil as acting through multiple pathways relevant to hair growth: it is a prodrug metabolized by follicular sulfotransferase into minoxidil sulfate, an ATP-sensitive potassium (K+ ATP) channel opener that produces vasodilation; it also has anti-inflammatory activity, induces the Wnt/β-catenin signalling pathway, has weak anti-androgenic activity, and prolongs the anagen (growth) phase while shortening telogen (resting) phase of the hair cycle.

Hypotrichosis simplex of the scalp is a rare monogenic autosomal-dominant disorder linked to variants in *CDSN* (corneodesmosin), which disrupts normal hair follicle cycling and leads to progressive, diffuse thinning without an underlying inflammatory or scarring process. Because the follicles in HSS remain structurally present (unlike scarring alopecias), the anagen-prolongation and follicular blood-flow stimulation mechanisms already established for minoxidil in androgenetic alopecia represent a plausible, if indirect, extrapolation.

That said, HSS is a monogenic structural/developmental defect of the hair follicle rather than the androgen- or vascular-driven process minoxidil classically targets, so the mechanistic extrapolation is of **moderate strength** rather than a direct match — consistent with the rationale captured in this evidence pack.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39902296](https://pubmed.ncbi.nlm.nih.gov/39902296/) | 2024 | Case report/series | Frontiers in Genetics | Familial case of HSS in an 8-year-old boy with a *CDSN* mutation, treated with a combination of botanical extracts and minoxidil; underscores the lack of a definitive effective therapy for HSS. |
| [36651821](https://pubmed.ncbi.nlm.nih.gov/36651821/) | 2023 | Case report/series | The Journal of Dermatological Treatment | 14-year-old patient with hereditary hypotrichosis simplex successfully treated with combined platelet-rich plasma injection and topical minoxidil 2%. |
| [35761391](https://pubmed.ncbi.nlm.nih.gov/35761391/) | 2022 | Case report/series | Dermatologic Therapy | Describes treatment of hereditary hypotrichosis simplex of the scalp with oral minoxidil combined with growth factors. |

---

## New Zealand Market Information

Minoxidil currently holds no marketing authorizations in New Zealand (market status: not marketed; 0 licenses on file), so no product/dosage-form table is available.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug-interaction data are all currently unavailable in this evidence pack — see Data Gap DG001, Blocking severity, below.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for this specific indication is limited to three low-tier case reports/series with no registered clinical trials, and a Blocking-severity data gap (missing package insert / TFDA-equivalent safety data, DG001) means an initial safety assessment (S1) cannot yet be completed.

**To proceed, the following is needed:**
- Package insert / regulatory safety data (key warnings, contraindications) — closes Blocking gap DG001
- Formal DrugBank mechanism-of-action documentation — closes High-severity gap DG002
- Prospective or larger-scale clinical evidence specifically in *CDSN*-associated hypotrichosis simplex (current data are pediatric-heavy single/small case reports)
- Route/formulation and dosing assessment (oral vs. topical) appropriate for this population, including pediatric patients
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

