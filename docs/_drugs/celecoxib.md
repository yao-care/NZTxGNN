---
layout: default
title: Celecoxib
parent: Model Prediction Only (L5)
nav_order: 91
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Celecoxib: predicted new indication Inflammatory spondylopathy (evidence level L5). Approved use: Rheumatoid arthritis, ankylosing spondylitis, osteoarthritis…"
monitor_inn: "CELECOXIB"
predicted_indications: ["acromesomelic dysplasia, Hunter-Thompson type", "brachyolmia-amelogenesis imperfecta syndrome", "rheumatoid vasculitis", "myosclerosis", "hypermobility of coccyx", "brachyolmia", "rheumatoid nodulosis", "rheumatoid factor-positive polyarticular juvenile idiopathic arthritis", "inflammatory spondylopathy", "WHIM syndrome"]
---

# Celecoxib
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Celecoxib: From Rheumatoid Arthritis to Inflammatory Spondylopathy

## One-Sentence Summary

Celecoxib is a COX-2-selective NSAID, originally used for inflammatory and degenerative joint diseases such as rheumatoid arthritis, osteoarthritis and ankylosing spondylitis.
The TxGNN model's top-ranked prediction is a rare skeletal dysplasia with no supporting evidence, so this report focuses on the best-supported candidate, **inflammatory spondylopathy** (rank 9), which has **11 registered clinical trials** and **20 publications**.
Because this indication overlaps with celecoxib's existing uses (ankylosing spondylitis and spondyloarthropathy), it is effectively a confirmation of an existing use rather than a novel repurposing.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Rheumatoid arthritis, ankylosing spondylitis, osteoarthritis, juvenile idiopathic arthritis (from the knowledge graph; no approved-indication text is available for the New Zealand licences) |
| Predicted New Indication | Inflammatory spondylopathy (best-supported of the top 10 predictions; rank 1 is acromesomelic dysplasia, Hunter-Thompson type, with no evidence) |
| TxGNN Prediction Score | 99.80% (rank 1 candidate: 99.88%) |
| Evidence Level | L1 (the data show at least two completed Phase 3 trials, NCT00648141 and NCT00762463) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

**Note on the other top-10 predictions:** Seven of them (acromesomelic dysplasia Hunter-Thompson type, brachyolmia-amelogenesis imperfecta syndrome, myosclerosis, hypermobility of coccyx, brachyolmia, rheumatoid nodulosis and WHIM syndrome) have no clinical trials or literature and are rated L5 / Hold. Rheumatoid vasculitis (L4, Hold) has only a single case report with no demonstrated celecoxib benefit. Rheumatoid factor-positive polyarticular JIA (L3) has one safety registry publication (PMID 25057265) but no subtype-specific efficacy data.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the compiled data. Based on known pharmacology, celecoxib selectively inhibits COX-2, reducing prostaglandin synthesis and thereby relieving inflammatory pain and stiffness. Its efficacy in rheumatoid arthritis, osteoarthritis and ankylosing spondylitis is well established.

Inflammatory spondylopathy covers inflammatory diseases of the spine, including ankylosing spondylitis and axial spondyloarthritis. NSAIDs are first-line therapy for these conditions, and celecoxib is already among the original indications (ankylosing spondylitis, spondyloarthropathy). The model's prediction therefore reflects a genuine pharmacological link.

Recent literature also suggests a possible additional benefit: celecoxib may inhibit radiographic bone progression in spondyloarthritis, possibly through effects partly independent of COX inhibition (PMID 39757202; PMID 36800138). This still needs confirmation, as the CONSUL trial publication (PMID 38228361) addresses structural progression when celecoxib is added to anti-TNF therapy.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00648141](https://clinicaltrials.gov/study/NCT00648141) | Phase 3 | Completed | 458 | 12-week comparison of celecoxib 200 mg QD, 200 mg BID and diclofenac SR in ankylosing spondylitis |
| [NCT00762463](https://clinicaltrials.gov/study/NCT00762463) | Phase 3 | Completed | 240 | Celecoxib vs diclofenac SR in Chinese ankylosing spondylitis patients, with 6-week extension |
| [NCT02528201](https://clinicaltrials.gov/study/NCT02528201) | Phase 4 | Completed | 330 | Randomized double-blind 12-week study of two celecoxib doses vs diclofenac in ankylosing spondylitis |
| [NCT01934933](https://clinicaltrials.gov/study/NCT01934933) | Phase 4 | Completed | 150 | Etanercept and celecoxib, alone or combined, in active ankylosing spondylitis |
| [NCT02758782](https://clinicaltrials.gov/study/NCT02758782) | Phase 4 | Completed | 156 | CONSUL: NSAID (celecoxib) added to golimumab vs golimumab alone on spinal structural progression over 2 years |
| [NCT04115098](https://clinicaltrials.gov/study/NCT04115098) | Phase 2 | Terminated | 42 | N-of-1 trials comparing selective COX-2 and non-selective COX inhibitors in axial spondyloarthritis |
| [NCT03473665](https://clinicaltrials.gov/study/NCT03473665) | Phase 4 | Terminated | 9 | Pilot comparison of four NSAIDs in axial spondyloarthritis; too small to be informative |
| [NCT03190603](https://clinicaltrials.gov/study/NCT03190603) | Phase 4 | Completed | 12 | NSAID effects on inflammatory lesions of axial spondyloarthritis on MRI |
| [NCT02638896](https://clinicaltrials.gov/study/NCT02638896) | Phase 4 | Unknown | 100 | Etanercept dose reduction in ankylosing spondylitis; celecoxib is not the focus |
| [NCT00813241](https://clinicaltrials.gov/study/NCT00813241) | Phase 1 | Completed | 16 | Pharmacokinetic study of enhanced-bioavailability celecoxib formulations in healthy volunteers |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38228361](https://pubmed.ncbi.nlm.nih.gov/38228361/) | 2024 | RCT (CONSUL) | Ann Rheum Dis | Effect of adding celecoxib to golimumab on 2-year radiographic spinal progression in r-axSpA |
| [40911151](https://pubmed.ncbi.nlm.nih.gov/40911151/) | 2025 | Umbrella review | Drugs | Synthesis of meta-analyses on celecoxib safety in chronic musculoskeletal conditions |
| [39757202](https://pubmed.ncbi.nlm.nih.gov/39757202/) | 2025 | Comparative/meta-analysis | BMB Rep | Celecoxib reported as the only NSAID to inhibit bone progression in spondyloarthritis |
| [40676732](https://pubmed.ncbi.nlm.nih.gov/40676732/) | 2025 | N-of-1 trials | Rheumatology (Oxford) | Bayesian N-of-1 comparison of celecoxib, meloxicam and naproxen in axSpA |
| [36800138](https://pubmed.ncbi.nlm.nih.gov/36800138/) | 2023 | Clinical study | Clin Rheumatol | Imrecoxib and celecoxib affect sacroiliac joint inflammation via bone metabolism and angiogenesis |
| [16960941](https://pubmed.ncbi.nlm.nih.gov/16960941/) | 2006 | Clinical trial | J Rheumatol | Celecoxib efficacious and well tolerated for signs and symptoms of ankylosing spondylitis |
| [40028763](https://pubmed.ncbi.nlm.nih.gov/40028763/) | 2025 | Retrospective cohort | Scand J Rheumatol | Cardiovascular and GI bleeding risk comparable between celecoxib and non-selective NSAIDs in AS |
| [18613804](https://pubmed.ncbi.nlm.nih.gov/18613804/) | 2008 | Review/safety | Expert Opin Drug Saf | Safety of celecoxib in ankylosing spondylitis treatment |
| [17196469](https://pubmed.ncbi.nlm.nih.gov/17196469/) | 2007 | Meta-analysis of RCTs | Am J Cardiol | Cardiovascular event risk in patients receiving celecoxib |
| [22141388](https://pubmed.ncbi.nlm.nih.gov/22141388/) | 2011 | Review | Drugs | Celecoxib for symptomatic relief in OA, RA and ankylosing spondylitis |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2468840 | Celostea | Cap 200 mg | Not available (cancelled) |
| 2642433 | Celebrex | Cap 100 mg | Not available (cancelled) |
| 2642441 | Celebrex | Cap 200 mg | Not available (cancelled) |
| 2521172 | Celecoxib Pfizer | Cap 200 mg | Not available (cancelled) |

All four licences are cancelled, so there is currently no valid celecoxib product on the New Zealand register in the data compiled for this report.

## Safety Considerations

- **Contraindication-related conditions (knowledge graph):** The knowledge graph links celecoxib to contraindications or cautions in cardiovascular disease (hypertension, congestive heart failure, myocardial infarction, coronary thrombosis, stroke and other cerebrovascular disorders), peptic ulcer disease (including active), kidney disease, liver disease, gallbladder disease, asthma (intrinsic and allergic), anaemia and haemoglobinopathy, and substance abuse/dependence. These come from knowledge graph edges, not label text.
- **Drug Interactions:** 100 interactions were recorded. One is rated Major (eliglustat). Many are rated Moderate, including hydrocortisone, metformin and sulfonylureas (glimepiride, glipizide, glyburide, chlorpropamide, acetohexamide), nateglinide, exenatide, cimetidine, levofloxacin, kanamycin, metronidazole, naltrexone, and the aminosalicylates (mesalazine, balsalazide, olsalazine).
- **Literature signals:** Cardiovascular and gastrointestinal risks of celecoxib are discussed in PMID 17196469 and PMID 40028763, which reported cardiovascular and GI bleeding risk comparable to non-selective NSAIDs in ankylosing spondylitis.

Package insert warnings and contraindications could not be retrieved; please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two completed Phase 3 trials and several Phase 4 trials, together with an RCT publication, support celecoxib in ankylosing spondylitis and axial spondyloarthritis, which overlap with its existing indications. This is effectively confirmation of an existing use rather than novel repurposing. Cardiovascular, gastrointestinal and renal risks and the large number of interactions call for patient selection and monitoring. The other nine top-10 predictions are not supported by adequate evidence and should remain on Hold.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- Clarification of the regulatory position, as all New Zealand licences are cancelled and no valid product is marketed
- Confirmation of the NSAID identity in NCT02758782's title (celecoxib is specified in its summary) and verification of the Phase 3 trial results
- A patient-selection and monitoring plan covering cardiovascular, gastrointestinal and renal risk and the major interaction with eliglustat
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

