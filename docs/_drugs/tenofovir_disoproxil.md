---
layout: default
title: Tenofovir Disoproxil
parent: 僅模型預測 (L5)
nav_order: 333
evidence_level: L5
indication_count: 4
---

# Tenofovir Disoproxil
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

# Tenofovir Disoproxil: From HIV-1 Infection to Feline Acquired Immunodeficiency Syndrome (FIV)

## One-Sentence Summary

Tenofovir disoproxil is a nucleotide reverse transcriptase inhibitor whose established use, referenced within this evidence pack, is HIV-1 antiretroviral therapy (formal New Zealand label text is unavailable — the drug is not marketed there). The TxGNN model's top prediction is **feline acquired immunodeficiency syndrome (FIV/feline AIDS)**, a veterinary indication, supported by **4 clinical trials** (all flagged as human-HIV disease-term mismatches) and **2 animal literature studies** that are directly relevant.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not on file for New Zealand (0 licenses, not marketed). Evidence context (see rank‑2 rationale) references tenofovir's established, already-approved HIV‑1 antiretroviral use. |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome (FIV) |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data for tenofovir disoproxil is not available in this evidence pack (data gap DG002). Based on the evidence collected, tenofovir disoproxil is the oral prodrug of PMPA (tenofovir), a nucleotide reverse transcriptase inhibitor (NRTI) whose antiretroviral activity against HIV-1 is well established — this is corroborated by the clinical and literature evidence attached to the related rank‑2 prediction in this pack.

Feline immunodeficiency virus (FIV), the causative agent of feline AIDS, belongs to the same *Retroviridae* family as HIV and depends on an analogous reverse transcriptase enzyme. This shared enzymatic target is the pharmacological basis for the TxGNN prediction: PMPA can, in principle, inhibit FIV reverse transcriptase the same way it inhibits HIV reverse transcriptase.

However, this mechanistic plausibility does not translate into an actionable human drug-repurposing candidate. Feline AIDS is a veterinary indication, outside the target patient population (humans) for this pipeline. Three of the four retrieved clinical trials are human HIV-1 trials of unrelated drugs (dolutegravir, darunavir) matched to this prediction only because of a disease-term collision between "immunodeficiency syndrome" in cats and "HIV/AIDS" in humans — they provide no support for the FIV use case. Only the two animal literature studies (PMID 37112803, PMID 24782459) are genuinely on-topic. This candidate should therefore be treated primarily as a TxGNN disease-ontology artifact rather than a viable human repurposing opportunity.

---

## Clinical Trial Evidence

*Note: All four trials below were retrieved for the term "feline acquired immunodeficiency syndrome" but are human HIV-1 trials of other drugs, mismatched by disease-ontology term collision (each internally graded "C" relevance). None directly studies tenofovir in FIV.*

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01263015](https://clinicaltrials.gov/study/NCT01263015) | Phase 3 | Completed | 844 | Dolutegravir+abacavir/lamivudine vs. Atripla (efavirenz/emtricitabine/tenofovir DF) in ART-naive HIV-1 adults. Human HIV trial, not FIV-specific. |
| [NCT00951015](https://clinicaltrials.gov/study/NCT00951015) | Phase 2 | Completed | 208 | Dolutegravir dose-selection with abacavir/lamivudine or tenofovir/emtricitabine in ART-naive HIV-1 adults. Human HIV trial, not FIV-specific. |
| [NCT02770508](https://clinicaltrials.gov/study/NCT02770508) | Phase 4 | Completed | 145 | Boosted darunavir + lamivudine vs. + emtricitabine/tenofovir or lamivudine/tenofovir in ART-naive HIV-1 patients. Human HIV trial, not FIV-specific. |
| [NCT01227824](https://clinicaltrials.gov/study/NCT01227824) | Phase 3 | Completed | 828 | Dolutegravir vs. raltegravir with dual-NRTI backbone (incl. tenofovir/emtricitabine option) in ART-naive HIV-1 adults. Human HIV trial, not FIV-specific. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37112803](https://pubmed.ncbi.nlm.nih.gov/37112803/) | 2023 | Animal/Cohort | Viruses | Combination antiretroviral therapy (dolutegravir + tenofovir 20 mg/kg + emtricitabine) evaluated for pharmacokinetics and immunophenotype outcomes in FIV-infected specific-pathogen-free cats. |
| [24782459](https://pubmed.ncbi.nlm.nih.gov/24782459/) | 2015 | Animal/Preclinical | J Feline Med Surg | Six naturally FIV-infected cats treated with PMEDAP (a related acyclic nucleoside phosphonate); notes no antiviral compound is currently registered for FIV, and human antivirals used experimentally have caused serious adverse effects. |

---

## New Zealand Market Information

Currently no marketing authorization on file. Tenofovir disoproxil is not marketed in New Zealand (0 licenses recorded).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (feline AIDS) is a veterinary indication outside this pipeline's human-repurposing scope, and its supporting clinical trial evidence is a disease-ontology mismatch rather than genuine support. Only two on-topic animal studies exist, and no relevant human population, dosing, or safety data are available for this candidate. Related lower-ranked predictions in the same evidence pack (simian immunodeficiency virus infection, an obsolete disease term, and an unrelated rare neurodevelopmental disorder) are separately assessed as reflecting tenofovir's already-approved HIV indication or as likely false positives — none constitutes a novel human repurposing signal at this time.

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (DG001, Blocking) — required before any S1 safety assessment
- Verified mechanism-of-action data (DG002)
- Re-run or re-filter the TxGNN prediction against a human-only disease ontology to exclude veterinary and non-human-primate disease terms
- If a genuine repurposing signal is sought, prioritize disease terms distinct from tenofovir's existing HIV indication rather than the FIV/SIV predictions in this pack
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

