---
layout: default
title: Teriflunomide
parent: 僅模型預測 (L5)
nav_order: 335
evidence_level: L5
indication_count: 1
---

# Teriflunomide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Teriflunomide: From Relapsing Multiple Sclerosis (Established Global Indication) to Relapsing-Remitting Multiple Sclerosis (TxGNN-Predicted)

## One-Sentence Summary

Teriflunomide is an oral dihydroorotate dehydrogenase (DHODH) inhibitor already marketed globally (as Aubagio®) for relapsing forms of multiple sclerosis, though it is not currently registered in New Zealand. The TxGNN model predicts efficacy in **Relapsing-Remitting Multiple Sclerosis**, a prediction that is **confirmatory rather than novel** — it matches the drug's already-documented worldwide indication — and is backed by **28 clinical trials** and **19 publications**, including several completed Phase 3 RCTs.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not on file for New Zealand (drug unregistered); literature confirms teriflunomide (Aubagio®) is EU/globally approved for relapsing multiple sclerosis since 2013 |
| Predicted New Indication | Relapsing-Remitting Multiple Sclerosis |
| TxGNN Prediction Score | 99.24% |
| Evidence Level | L1 (≥2 completed Phase 3 RCTs) |
| New Zealand Market Status | Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed DrugBank mechanism-of-action data was not returned in this evidence pack, but the literature evidence collected alongside the prediction fills the gap directly: teriflunomide "selectively and reversibly inhibits the mitochondrial enzyme dihydro-orotate dehydrogenase, with consequent inhibition of de novo pyrimidine synthesis and reduced lymphocyte proliferation" (PMID 31098896). This anti-proliferative effect on activated T- and B-lymphocytes is the accepted basis for its disease-modifying activity in autoimmune demyelinating disease.

Critically, the same literature set shows this is not an exploratory repurposing signal in the usual sense: PMID 26758290 states teriflunomide (Aubagio®) "has been licensed in the EU since August 2013 for the treatment of adult patients with relapsing-remitting multiple sclerosis (RRMS)." In other words, the TxGNN model's top prediction reproduces the drug's already-established, globally approved indication rather than identifying a genuinely new therapeutic use. This explains the unusually high score (99.24%) and the depth of supporting evidence — the model is correctly recovering known pharmacology, which is a useful sanity check on model validity, but should not be presented to stakeholders as a novel repurposing opportunity without that caveat.

The practical gap here is regulatory, not mechanistic: teriflunomide has zero New Zealand market authorizations on file, so the open question for this jurisdiction is registration and safety-label availability rather than efficacy rationale.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00134563](https://clinicaltrials.gov/study/NCT00134563) | Phase 3 | Completed | 1088 | Pivotal RCT: teriflunomide reduced relapse frequency and delayed disability accumulation (EDSS, MRI) vs. placebo |
| [NCT00883337](https://clinicaltrials.gov/study/NCT00883337) | Phase 3 | Completed | 324 | TENERE: teriflunomide vs. interferon beta-1a — effectiveness (time to failure), relapse frequency, fatigue, safety |
| [NCT00803049](https://clinicaltrials.gov/study/NCT00803049) | Phase 3 | Completed | 742 | Long-term extension documenting safety/tolerability of teriflunomide 7 mg and 14 mg, plus disability/relapse/MRI outcomes |
| [NCT00228163](https://clinicaltrials.gov/study/NCT00228163) | Phase 2 | Completed | 147 | Long-term safety extension study; secondary long-term efficacy assessment |
| [NCT02490982](https://clinicaltrials.gov/study/NCT02490982) | N/A | Completed | 106 | Real-world observational effectiveness study in RRMS patients over ≥2 years |
| [NCT03302442](https://clinicaltrials.gov/study/NCT03302442) | N/A | Completed | 3000 | Real-world comparison of dimethyl fumarate vs. teriflunomide (French MS cohort), clinical + MRI outcomes |
| [NCT03464448](https://clinicaltrials.gov/study/NCT03464448) | N/A | Completed | 30 | Mechanistic study: regulatory B lymphocytes as mediators of teriflunomide's therapeutic effect |
| [NCT02833714](https://clinicaltrials.gov/study/NCT02833714) | N/A | Terminated | 26 | Mechanistic study on teriflunomide's effect on B-cell activation markers and cytokine secretion |
| [NCT04129736](https://clinicaltrials.gov/study/NCT04129736) | Phase 4 | Completed | 12 | Pharmacokinetics: teriflunomide concentration in serum and cerebrospinal fluid |
| [NCT03561402](https://clinicaltrials.gov/study/NCT03561402) | N/A | Completed | 24 | Biomarkers associated with disease activity in teriflunomide-treated patients |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32757523](https://pubmed.ncbi.nlm.nih.gov/32757523/) | 2020 | RCT | NEJM | Ofatumumab vs. teriflunomide head-to-head trial in relapsing MS |
| [40202623](https://pubmed.ncbi.nlm.nih.gov/40202623/) | 2025 | RCT | NEJM | Tolebrutinib (BTK inhibitor) vs. teriflunomide in relapsing MS |
| [36001711](https://pubmed.ncbi.nlm.nih.gov/36001711/) | 2022 | RCT | NEJM | Ublituximab vs. teriflunomide in relapsing MS |
| [39307151](https://pubmed.ncbi.nlm.nih.gov/39307151/) | 2024 | RCT | Lancet Neurology | evolutionRMS1/2: evobrutinib vs. teriflunomide, phase 3 active-comparator trials |
| [33779698](https://pubmed.ncbi.nlm.nih.gov/33779698/) | 2021 | RCT | JAMA Neurology | OPTIMUM: ponesimod vs. teriflunomide, first phase 3 head-to-head oral DMT comparison |
| [38174776](https://pubmed.ncbi.nlm.nih.gov/38174776/) | 2024 | Systematic Review / Network Meta-analysis | Cochrane Database Syst Rev | Comparative efficacy of immunomodulators/immunosuppressants (incl. teriflunomide) for RRMS |
| [31098896](https://pubmed.ncbi.nlm.nih.gov/31098896/) | 2019 | Review | Drugs | Comprehensive review of teriflunomide MOA (DHODH inhibition) and RCT/real-world efficacy in RRMS |
| [26758290](https://pubmed.ncbi.nlm.nih.gov/26758290/) | 2016 | Review | CNS Drugs | Review of EU label, key efficacy/safety outcomes, and prescribing considerations for teriflunomide |
| [33620411](https://pubmed.ncbi.nlm.nih.gov/33620411/) | 2021 | Review | JAMA | General MS diagnosis and treatment review, situates teriflunomide among DMTs |
| [37691530](https://pubmed.ncbi.nlm.nih.gov/37691530/) | 2023 | Open-Label Extension | Mult Scler | ALITHIOS 4-year extension: ofatumumab shows superior efficacy/safety vs. teriflunomide over 2.5 years |

---

## New Zealand Market Information

Teriflunomide currently has **no market authorizations on file in New Zealand** (0 licenses; market status: not marketed). No product listing table can be produced from this evidence pack.

---

## Safety Considerations

Please refer to the package insert for safety information. No warnings, contraindications, or drug interaction data were retrievable in this evidence pack — notably, TFDA package insert warnings/contraindications (DG001) are flagged as a **blocking** data gap that must be resolved before any safety pre-assessment (S1) can proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Clinical evidence is strong (L1, multiple completed Phase 3 RCTs) but two issues block progression: (1) the TFDA/Medsafe package insert with warnings and contraindications is a blocking data gap, so no safety pre-assessment can be completed; and (2) teriflunomide is unregistered in New Zealand (0 authorizations), and the predicted indication mirrors the drug's already-established global label rather than representing a genuinely novel repurposing target — this needs to be clarified before framing it as a repurposing candidate.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) to unblock S1 safety evaluation
- Drug interaction (DDI) data — current query returned no results
- Confirmation of New Zealand registration/import pathway and timeline for market entry
- Explicit scoping decision on whether this candidate should be tracked as "novel repurposing" or reclassified as "market-access gap for an already-approved indication"
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

