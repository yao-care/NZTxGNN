---
layout: default
title: Tenoxicam
parent: Model Prediction Only (L5)
nav_order: 442
evidence_level: L5
indication_count: 8
monitor_inn: "TENOXICAM"
predicted_indications: ["brachydactyly-syndactyly syndrome", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "headache disorder", "hypotrichosis simplex of the scalp", "congenital hypotrichosis milia", "diffuse alopecia areata", "trigeminal autonomic cephalalgia", "alopecia"]
last_modified_at: 2026-10-30
description: "Tenoxicam: predicted new indication Brachydactyly-syndactyly syndrome (evidence level L5). Approved use: Arthritic joint disease and arthropathy"
---

# Tenoxicam
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Tenoxicam: From Arthritic Joint Disease to Brachydactyly-Syndactyly Syndrome
{: .text-alpha }

## One-Sentence Summary

Tenoxicam is a long-acting NSAID (oxicam class). Scores on this page come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.
The TxGNN model's top-ranked prediction is **brachydactyly-syndactyly syndrome**, a congenital limb malformation, but **no clinical trials and no publications** support it.
The only candidate in the list with real supporting evidence is headache disorder (rank 3), which is discussed in the notes below.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Arthritic joint disease and arthropathy (relations recorded in the knowledge graph; no approved indication text is available in the local licence records) |
| Predicted New Indication | Brachydactyly-syndactyly syndrome |
| TxGNN Prediction Score | 0.9980 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Tenoxicam is an NSAID of the oxicam class, which acts by inhibiting cyclooxygenase (COX) and reducing prostaglandin synthesis. This provides anti-inflammatory and analgesic activity relevant to joint disease.

Brachydactyly-syndactyly syndrome is a congenital limb malformation with a genetic basis. COX inhibition is not expected to influence its pathogenesis, and no plausible mechanistic link has been identified. The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. It should not be read as a repurposing signal.

**Other predictions in the list (for context):**

| Rank | Predicted Disease | Score | Evidence Level | Assessment |
|---|---|---|---|---|
| 1 | Brachydactyly-syndactyly syndrome | 0.9980 | L5 | No mechanism, no evidence |
| 2 | Colobomatous microphthalmia-rhizomelic dysplasia syndrome | 0.9980 | L5 | Congenital disorder; no mechanism, no evidence |
| 3 | Headache disorder | 0.9968 | See note below | Plausible mechanism; one Phase 4 trial under way |
| 4 | Hypotrichosis simplex of the scalp | 0.9966 | L5 | No mechanism; alopecia is a reported adverse effect |
| 5 | Congenital hypotrichosis milia | 0.9966 | L5 | No mechanism, no evidence |
| 6 | Diffuse alopecia areata | 0.9961 | L5 | Autoimmune; NSAIDs do not target the pathways |
| 7 | Trigeminal autonomic cephalalgia | 0.9961 | L5 | Indirect evidence only; no tenoxicam-specific data |
| 8 | Alopecia | 0.9960 | L5 | Only a case report of tenoxicam-induced hair loss (a safety signal) |

**Note on headache disorder (rank 3):** NSAIDs reduce prostaglandin-mediated pain signalling in migraine, so this prediction is biologically plausible. A Phase 4 trial (NCT06786650) directly tests tenoxicam in acute migraine, but it has no reported results. The two publications listed are a comparative/observational migraine treatment study and a retrospective emergency-centre chart review; neither provides tenoxicam-specific efficacy data. Because no study yet reports results for headache with tenoxicam, it is treated conservatively as a lead for follow-up rather than as established evidence.

## Clinical Trial Evidence

For the top-ranked prediction (brachydactyly-syndactyly syndrome): currently no related clinical trials registered.

For reference, the trial relevant to the rank-3 candidate (headache disorder):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06786650](https://clinicaltrials.gov/study/NCT06786650) | Phase 4 | Active, not recruiting | 100 | IV tenoxicam vs IV ibuprofen for analgesia in acute migraine attack; no results available yet |

## Literature Evidence

For the top-ranked prediction (brachydactyly-syndactyly syndrome): currently no related literature available.

For reference, literature for other candidates:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27813032](https://pubmed.ncbi.nlm.nih.gov/27813032/) | 2016 | Cohort | Agri | Compared triptans, NSAIDs and their combination in migraine attacks (headache disorder) |
| [18345416](https://pubmed.ncbi.nlm.nih.gov/18345416/) | 2007 | Cohort | Arq Neuropsiquiatr | Chart review of 109 patients with acute primary headaches at an emergency centre |
| [9303679](https://pubmed.ncbi.nlm.nih.gov/9303679/) | 1997 | Case report | Atencion Primaria | Alopecia reported as a possible adverse effect of tenoxicam (safety signal, not benefit) |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2307022 | AFT | Inj 20 mg vial | Not available in the compiled records |
| 2520559 | Tilcotil | Tab 20 mg | Not available in the compiled records |

Both licences are valid, single-ingredient products. The injection and tablet forms are both available.

## Safety Considerations

Please refer to the package insert for safety information.

For context, the literature includes a case report of alopecia associated with tenoxicam. If the headache lead is pursued, standard NSAID precautions (gastrointestinal, renal and cardiovascular risk, and limiting analgesic frequency to avoid medication-overuse headache) would apply.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, brachydactyly-syndactyly syndrome, is a congenital genetic malformation with no plausible link to COX inhibition and no supporting trials or publications. Its score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. Most other high-scoring predictions (congenital and hair disorders) are similar. Only headache disorder has a plausible rationale, and that remains unproven until the Phase 4 trial reports.

**To proceed, the following is needed:**
- Do not pursue brachydactyly-syndactyly syndrome unless new mechanistic or clinical evidence emerges.
- Results from NCT06786650 to evaluate headache/migraine as a possible lead; it should be re-evaluated as its own candidate at that point.
- Mechanism of action data and the Medsafe package insert (warnings and contraindications) to enable safety screening.
- Approved indication text for the two New Zealand licences.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

