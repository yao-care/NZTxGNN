---
layout: default
title: Lamivudine
parent: 僅模型預測 (L5)
nav_order: 192
evidence_level: L5
indication_count: 5
---

# Lamivudine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Lamivudine: From HIV-1/Chronic Hepatitis B to Feline Acquired Immunodeficiency Syndrome

## One-Sentence Summary

> Lamivudine (3TC, DB00709) is a nucleoside reverse transcriptase inhibitor established for HIV-1 and chronic Hepatitis B infection in humans. The TxGNN model's top-ranked prediction is **Feline Acquired Immunodeficiency Syndrome (FIV in cats)** — a veterinary, not human, indication — supported by **5 clinical trials** (all human HIV trials, graded as species-mismatched) and **5 veterinary publications**. The signal appears to be a keyword-driven artifact ("immunodeficiency") rather than a genuine human repurposing opportunity.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV-1 infection / Chronic Hepatitis B (based on known drug class; not marketed in New Zealand, so no local label text is available) |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome (FIV) |
| TxGNN Prediction Score | 99.93% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is flagged as a data gap in this evidence pack (DG002). Based on known information, Lamivudine is a cytidine nucleoside analogue reverse transcriptase inhibitor (NRTI) used in combination antiretroviral regimens for HIV-1 and as monotherapy/combination therapy for chronic Hepatitis B; its efficacy in these indications is well established, and its mechanism (competitive inhibition of viral reverse transcriptase) is not disease-specific to human hosts.

Feline Immunodeficiency Virus (FIV) is a lentivirus in the same family as HIV, and both depend on reverse transcriptase for replication, so there is a genuine molecular rationale for cross-species antiviral activity — this is corroborated by veterinary literature dating back over two decades. However, this predicted indication is a **veterinary disease in cats**, not a human indication, which falls outside the scope of a human drug repurposing evaluation. Additionally, all five associated clinical trials are human HIV-1 studies evaluating dolutegravir- or darunavir-based regimens with abacavir/lamivudine or lamivudine/tenofovir combinations — they were almost certainly retrieved by keyword overlap on "immunodeficiency" rather than genuine relevance to feline disease, and are graded "C" (species mismatch) by the evidence pipeline itself. The only substantively relevant evidence is a small body of independent veterinary literature (in vitro and small-cohort studies in cats), which supports biological plausibility but does not constitute human clinical evidence.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01499199](https://clinicaltrials.gov/study/NCT01499199) | Phase 3 | Completed | 13 | Dolutegravir + abacavir/lamivudine PK/safety study in HIV-1 ART-naïve adults (Grade C — species mismatch, human HIV trial, not related to feline disease) |
| [NCT01263015](https://clinicaltrials.gov/study/NCT01263015) | Phase 3 | Completed | 844 | Dolutegravir + abacavir/lamivudine vs Atripla in HIV-1 ART-naïve adults (Grade C — species mismatch) |
| [NCT00951015](https://clinicaltrials.gov/study/NCT00951015) | Phase 2 | Completed | 208 | Dolutegravir dose-selection with abacavir/lamivudine or tenofovir/emtricitabine in HIV-1 adults (Grade C — species mismatch) |
| [NCT02770508](https://clinicaltrials.gov/study/NCT02770508) | Phase 4 | Completed | 145 | Boosted darunavir + lamivudine vs tenofovir-based regimens in HIV-1 adults (Grade C — species mismatch) |
| [NCT01227824](https://clinicaltrials.gov/study/NCT01227824) | Phase 3 | Completed | 828 | Dolutegravir vs raltegravir with dual NRTI (incl. abacavir/lamivudine) in HIV-1 adults (Grade C — species mismatch) |

**Note:** All 5 trials are human HIV-1 studies with no relevance to feline disease; they were most likely captured due to keyword overlap on "immunodeficiency virus" and lamivudine's presence in the regimen.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25855689](https://pubmed.ncbi.nlm.nih.gov/25855689/) | 2016 | Cohort | Journal of feline medicine and surgery | Long-term antiretroviral therapy follow-up (initially zidovudine, ART) in FIV-infected cats |
| [22816032](https://pubmed.ncbi.nlm.nih.gov/22816032/) | 2012 | Cohort | Viruses | Compared zidovudine, zidovudine + interferon-α, zidovudine + lamivudine, and zidovudine + valproic acid in naturally FIV-infected cats over 1 year |
| [11943320](https://pubmed.ncbi.nlm.nih.gov/11943320/) | 2002 | Review | Veterinary immunology and immunopathology | AZT/3TC combination showed additive-to-synergistic anti-FIV activity in primary PBMCs, but not in chronically infected cells |
| [11327469](https://pubmed.ncbi.nlm.nih.gov/11327469/) | 2001 | In-vitro | American journal of veterinary research | Characterized 3TC-resistant FIV pol gene mutants and replication kinetics vs wild-type FIV |
| [11684314](https://pubmed.ncbi.nlm.nih.gov/11684314/) | 2002 | Preclinical/Animal | Antiviral research | Combined zidovudine + lamivudine + abacavir suppressed FIV replication in vitro |

## New Zealand Market Information

Lamivudine is currently **not marketed** in New Zealand under this evidence pack (0 authorizations on file), so no product/authorization table is available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction is a veterinary indication (feline FIV), not a human disease, and falls outside the scope of human drug repurposing. The associated "clinical trial" evidence consists entirely of human HIV-1 trials misattributed via keyword matching, and the only genuinely relevant evidence is limited to small, older veterinary studies. Note also that the remaining ranked candidates for this drug are similarly weak: simian immunodeficiency virus infection (rank 2, L4) reflects existing preclinical HIV drug-development models rather than a new indication; the rare neurodevelopmental disorder (rank 3, L5) and "obsolete" familial combined hyperlipidemia (rank 4, L5) have zero supporting trials or literature and no plausible mechanism; and chronic Hepatitis C (rank 5, L4) is mechanistically implausible since lamivudine only inhibits reverse-transcribing viruses, with the cited evidence largely mislabeled Hepatitis B data.

**To proceed, the following is needed:**
- Confirmed mechanism of action (MOA) data from DrugBank (DG002)
- TFDA/label warnings and contraindications (DG001, currently blocking S1 safety screening)
- A re-run of the TxGNN indication list restricted to human-disease ontology terms, to filter out veterinary/animal-model entries and disease-ontology mismatches (e.g., HBV/HCV mislabeling)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

