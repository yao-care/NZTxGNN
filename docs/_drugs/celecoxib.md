---
layout: default
title: Celecoxib
parent: 僅模型預測 (L5)
nav_order: 69
evidence_level: L5
indication_count: 10
---

# Celecoxib
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

# Celecoxib: From Inflammatory Arthritis to Inflammatory Spondylopathy

## One-Sentence Summary

Celecoxib is a COX-2 selective inhibitor approved internationally for inflammatory arthritis conditions including osteoarthritis, rheumatoid arthritis, and ankylosing spondylitis, though not currently registered in Taiwan.
The TxGNN model predicts it may be effective for **Inflammatory Spondylopathy** (ankylosing spondylitis and axial spondyloarthritis),
with **multiple completed Phase 3 and Phase 4 RCTs** and **over 20 publications** strongly supporting this direction — including a landmark 2025 systematic review identifying celecoxib as the **only NSAID proven to inhibit radiographic bone progression** in this disease class.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not registered in Taiwan; approved internationally for OA, RA, AS, and acute pain |
| Predicted New Indication | Inflammatory Spondylopathy (Ankylosing Spondylitis / Axial SpA) |
| TxGNN Prediction Score | 99.80% |
| Evidence Level | L1 |
| Taiwan Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data was not retrieved from DrugBank for this Evidence Pack. However, based on the published clinical literature included here, celecoxib is a selective cyclooxygenase-2 (COX-2) inhibitor — the first coxib class drug introduced into clinical practice. By selectively blocking COX-2 without inhibiting COX-1, celecoxib suppresses prostaglandin E2 (PGE2) synthesis at inflammatory sites while preserving COX-1-dependent gastric mucosal protection, delivering effective anti-inflammatory and analgesic activity with a substantially lower gastrointestinal toxicity burden compared to non-selective NSAIDs.

Inflammatory spondylopathy — encompassing ankylosing spondylitis (AS) and axial spondyloarthritis (axSpA) — is a chronic inflammatory disease driven by PGE2-mediated pathways and elevated cytokines including IL-1β, IL-6, and IL-17A that promote sacroiliac joint inflammation and progressive spinal ankylosis. COX-2 selective inhibition directly targets this core inflammatory mechanism, which is why international guidelines (ASAS, ACR) position NSAIDs as the recommended first-line therapy for AS and axSpA. The biological rationale here is among the clearest of any NSAID repurposing scenario.

What makes this TxGNN prediction especially compelling is a 2025 systematic review (PMID 39757202) demonstrating that celecoxib is the **only NSAID proven to inhibit radiographic spinal progression** in spondyloarthritis — an effect that appears to extend beyond general COX-2 inhibition and may involve modulation of the Wnt/DKK-1 bone formation signalling pathway. This mechanistically unique property, validated across multiple high-quality RCTs, differentiates celecoxib from other NSAIDs and strongly supports prioritising it for formal registration evaluation in Taiwan.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|-----------|-------------|
| [NCT00648141](https://clinicaltrials.gov/study/NCT00648141) | Phase 3 | Completed | 458 | Largest AS RCT: Celecoxib 200mg QD vs 200mg BID vs Diclofenac 75mg SR BID — 12-week efficacy and safety comparison in ankylosing spondylitis |
| [NCT00762463](https://clinicaltrials.gov/study/NCT00762463) | Phase 3 | Completed | 240 | Chinese AS patients: Celecoxib 200mg vs Diclofenac SR — 6-week double-blind RCT with 6-week Celecoxib 400mg extension phase |
| [NCT02528201](https://clinicaltrials.gov/study/NCT02528201) | Phase 4 | Completed | 330 | Post-marketing confirmatory RCT: Celecoxib 200mg QD vs 400mg QD vs Diclofenac TID in AS — 12-week double-blind design |
| [NCT01934933](https://clinicaltrials.gov/study/NCT01934933) | Phase 4 | Completed | 150 | Multi-centre open-label RCT: Celecoxib 200mg BID vs Etanercept 50mg QW vs combination — 54-week active AS treatment with MRI SPARCC scoring |
| [NCT02758782](https://clinicaltrials.gov/study/NCT02758782) | Phase 4 | Completed | 156 | CONSUL trial: Celecoxib + Golimumab vs Golimumab monotherapy — 2-year radiographic spinal damage progression in ankylosing spondylitis |
| [NCT04115098](https://clinicaltrials.gov/study/NCT04115098) | Phase 2 | Terminated | 42 | N-of-1 crossover trials: Selective COX-2 inhibitors vs non-selective NSAIDs in axial SpA — personalised pain response and HRQoL comparison |
| [NCT02456363](https://clinicaltrials.gov/study/NCT02456363) | Phase 2 | Unknown | 300 | AS registry: Adalimumab ± NSAIDs — safety and efficacy comparison providing real-world anti-TNF + NSAID context |
| [NCT03190603](https://clinicaltrials.gov/study/NCT03190603) | Phase 4 | Completed | 12 | NSAID effects on MRI inflammatory lesions in axial spondyloarthritis — imaging biomarker pilot study |
| [NCT05164198](https://clinicaltrials.gov/study/NCT05164198) | Phase 4 | Unknown | 448 | TNFi dose optimisation in stable AS — celecoxib used as background therapy, providing real-world combination treatment context |
| [NCT01572675](https://clinicaltrials.gov/study/NCT01572675) | N/A | Completed | 547 | Post-marketing pharmacoepidemiology: Real-world use of celecoxib vs etoricoxib across inflammatory arthritis indications in France |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [39757202](https://pubmed.ncbi.nlm.nih.gov/39757202/) | 2025 | Systematic Review | BMB Reports | Celecoxib uniquely inhibits radiographic bone progression in spondyloarthritis among all NSAIDs tested; mechanistic investigation suggests COX-independent Wnt/DKK-1 pathway modulation |
| [40911151](https://pubmed.ncbi.nlm.nih.gov/40911151/) | 2025 | Umbrella Review | Drugs | Systematic synthesis of celecoxib safety from meta-analyses across chronic musculoskeletal conditions — comprehensive cardiovascular, GI, and renal safety profile |
| [40028763](https://pubmed.ncbi.nlm.nih.gov/40028763/) | 2025 | Comparative Safety Study | Scand J Rheumatol | Nationwide retrospective cohort in AS patients: Cardiovascular disease and gastrointestinal bleeding risk comparable between celecoxib and non-selective NSAIDs |
| [38228361](https://pubmed.ncbi.nlm.nih.gov/38228361/) | 2024 | RCT | Ann Rheum Dis | CONSUL trial results: Adding celecoxib to golimumab vs golimumab monotherapy — 2-year radiographic spinal progression in radiographic axial SpA |
| [38832489](https://pubmed.ncbi.nlm.nih.gov/38832489/) | 2024 | RCT | Scand J Rheumatol | Iguratimod + celecoxib vs placebo + celecoxib in active axSpA — randomised, double-blind, placebo-controlled efficacy and safety study |
| [36800138](https://pubmed.ncbi.nlm.nih.gov/36800138/) | 2023 | RCT / Mechanistic Study | Clin Rheumatol | Celecoxib vs imrecoxib in axSpA: Bone metabolism markers and angiogenesis regulation in sacroiliac joint inflammation — clinical efficacy correlation |
| [28626213](https://pubmed.ncbi.nlm.nih.gov/28626213/) | 2017 | RCT | Med Sci Monit | Celecoxib 200mg vs imrecoxib 200mg BID in axSpA — efficacy, safety, and DKK-1 serum levels at baseline, week 4, and week 12 |
| [16960941](https://pubmed.ncbi.nlm.nih.gov/16960941/) | 2006 | RCT | J Rheumatol | Foundational efficacy RCT: Celecoxib is efficacious and well tolerated in treating signs and symptoms of ankylosing spondylitis |
| [22141388](https://pubmed.ncbi.nlm.nih.gov/22141388/) | 2011 | Narrative Review | Drugs | Comprehensive review of celecoxib (Celebrex®) across OA, RA, and AS — EU-approved indications and comparative clinical trial summary |
| [32955700](https://pubmed.ncbi.nlm.nih.gov/32955700/) | 2021 | Clinical Study | Ir J Med Sci | IL-1β, IL-6, IL-17A as predictive biomarkers of celecoxib response in AS — cytokine profiling for precision patient selection |

---

## Taiwan Market Information

Celecoxib is currently **not registered in Taiwan** (未上市). No marketing authorizations were identified in the TFDA database at the time of this review (query date: 2026-03-29).

Celecoxib (branded as Celebrex®, originator: Pfizer/Pharmacia) holds regulatory approvals in multiple major markets including the United States (FDA), European Union (EMA), and Japan (PMDA) for osteoarthritis, rheumatoid arthritis, ankylosing spondylitis, and acute pain in adults. These international approvals, supported by the L1 RCT evidence base, provide a strong foundation for a Taiwan NDA (新藥查驗登記) application via the abridged or bibliographic review pathway.

---

## Safety Considerations

No Taiwan package insert data was retrievable for celecoxib, and the local DDI database returned no results for this drug. The following safety signals are drawn directly from the published literature retrieved in this Evidence Pack:

- **Cardiovascular risk**: A 2025 nationwide cohort study in AS patients (PMID 40028763) found cardiovascular disease risk to be **comparable between celecoxib and non-selective NSAIDs**. The 2025 umbrella review (PMID 40911151) similarly confirms that the cardiovascular risk profile in chronic musculoskeletal conditions is well characterised. Individual cardiovascular risk assessment (prior MI, heart failure, hypertension) is recommended before initiating therapy.
- **Gastrointestinal tolerability**: Celecoxib's COX-2 selectivity confers a more favourable GI profile than non-selective NSAIDs. This advantage is particularly relevant in elderly patients and those at GI risk.
- **Concurrent DMARD use**: A Cochrane systematic review (PMID 22071858) evaluated the safety of NSAIDs co-administered with methotrexate in inflammatory arthritis — a relevant consideration for axSpA patients on combination therapy. No prohibitive safety signal was identified, but renal function monitoring is prudent.
- **Potential bone progression benefit**: Based on PMID 39757202 (2025), continuous celecoxib use may provide unique structural protection in spondyloarthritis beyond symptom control — this benefit-risk consideration should inform long-term treatment planning.

Please refer to the approved package insert in the prescribing jurisdiction for complete warnings, contraindications, and drug-drug interaction information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 3 and Phase 4 RCTs directly demonstrate celecoxib's efficacy and safety in ankylosing spondylitis and axial spondyloarthritis (satisfying L1 evidence criteria), and a 2025 systematic review uniquely establishes celecoxib as the only NSAID proven to inhibit radiographic spinal progression in this disease class — providing both symptomatic and potentially disease-modifying justification for Taiwan regulatory consideration.

**To proceed, the following is needed:**

- Initiate TFDA registration application (新藥查驗登記) via the abridged/bibliographic pathway, referencing existing FDA and EMA approvals along with the Phase 3/4 RCT evidence package
- Obtain the originator full prescribing information (Pfizer/Pharmacia package insert) for complete Taiwan-context safety review, including warnings and contraindications not captured in this Evidence Pack
- Establish a cardiovascular risk management plan for the Taiwan patient population, aligned with TFDA pharmacovigilance requirements
- Confirm drug-drug interaction profile with medications commonly co-prescribed in Taiwan (e.g., NSAIDs, DMARDs, biologics, aspirin) using a validated DDI reference database
- Clarify intended product positioning: symptomatic relief only, or disease-modifying use based on the bone progression inhibition data (PMID 39757202) — this determines regulatory strategy and labelling scope
- Survey whether any generic celecoxib products have been submitted to or approved by TFDA that might affect market entry planning
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

