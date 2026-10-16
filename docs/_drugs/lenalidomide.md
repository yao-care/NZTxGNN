---
layout: default
title: Lenalidomide
parent: Model Prediction Only (L5)
nav_order: 264
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Lenalidomide: predicted new indication Myeloid leukemia (evidence level L5). Approved use: Plasma cell myeloma, mantle cell lymphoma, B-cell neoplasm…"
monitor_inn: "LENALIDOMIDE"
predicted_indications: ["myeloid leukemia", "unclassified myelodysplastic syndrome", "refractory cytopenia of childhood", "aregenerative anemia", "severe congenital hypochromic anemia with ringed sideroblasts", "partial deletion of the long arm of chromosome 5"]
---

# Lenalidomide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

# Lenalidomide: From Plasma Cell Myeloma to Myeloid Leukemia

## One-Sentence Summary

Lenalidomide is an immunomodulatory anticancer drug originally used for plasma cell myeloma, mantle cell lymphoma and myelodysplastic syndrome (MDS).
The TxGNN model predicts it may be effective for **myeloid leukemia**,
with **44 registered clinical trials** and **20 publications** compiled for this direction. Most trials are small, early-phase or single-arm studies.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Plasma cell myeloma, mantle cell lymphoma, B-cell neoplasm, lymphosarcoma, myelodysplastic syndrome (from the knowledge-graph indication list; no approved-indication text is available from local licences) |
| Predicted New Indication | Myeloid leukemia |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L2 (no completed Phase 3 RCT; Phase 2 evidence only) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the compiled data. Based on known pharmacology, lenalidomide binds cereblon (CRBN) and redirects the CRL4-CRBN E3 ligase to degrade the transcription factors IKZF1 and IKZF3. In del(5q) cells it also degrades CK1alpha. It additionally activates NK and T cells and alters the bone marrow microenvironment.

Lenalidomide is already established in MDS, particularly del(5q) MDS, which is biologically close to acute myeloid leukemia (AML), since MDS can transform into AML. These effects plausibly extend to myeloid malignancies. A 2025 laboratory study also found that CRBN levels influence the sensitivity of AML cells to lenalidomide.

The caveat is that the link is inferred rather than demonstrated. The benefit seen in del(5q) MDS depends on a specific genetic vulnerability, and that may not apply to AML in general.

---

## Clinical Trial Evidence

The compiled data contains 44 trials. The table lists the 10 most relevant (those graded A or B for relevance, plus the largest randomized study).

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00831766](https://clinicaltrials.gov/study/NCT00831766) | Phase 1/2 | Completed | 51 | Sequential idarubicin + cytarabine followed by lenalidomide in MDS (RAEB-2) or untreated AML; tests safety and response |
| [NCT00352001](https://clinicaltrials.gov/study/NCT00352001) | Phase 1/2 | Completed | 37 | Lenalidomide + azacitidine in advanced MDS; combination safety and preliminary activity |
| [NCT02126553](https://clinicaltrials.gov/study/NCT02126553) | Phase 2 | Completed | 29 | Lenalidomide maintenance in high-risk AML in remission; single-arm, does not establish efficacy |
| [NCT00360672](https://clinicaltrials.gov/study/NCT00360672) | Phase 2 | Completed | 27 | Lenalidomide in relapsed/refractory AML or high-risk MDS with chromosome 5 abnormalities; no randomized control |
| [NCT01681537](https://clinicaltrials.gov/study/NCT01681537) | Phase 1 | Completed | 36 | Lenalidomide plus MEC chemotherapy for AML reinduction; dose-finding and safety |
| [NCT01578954](https://clinicaltrials.gov/study/NCT01578954) | Phase 1 | Completed | 20 | Dose-finding of lenalidomide reinduction/consolidation and maintenance in AML patients aged 60 or older |
| [NCT01254578](https://clinicaltrials.gov/study/NCT01254578) | Phase 1 | Completed | 17 | Lenalidomide maintenance after allogeneic transplant in high-risk haematological malignancies |
| [NCT01522976](https://clinicaltrials.gov/study/NCT01522976) | Phase 2 | Active, not recruiting | 282 | Randomized: azacitidine alone vs. with lenalidomide vs. with vorinostat in higher-risk MDS and CMML |
| [NCT00843882](https://clinicaltrials.gov/study/NCT00843882) | Phase 3 | Active, not recruiting | 247 | Lenalidomide with or without epoetin alfa in low/intermediate-1 risk MDS with anemia (MDS, not AML) |
| [NCT01358734](https://clinicaltrials.gov/study/NCT01358734) | Phase 2 | Completed | 88 | Randomized: lenalidomide, azacitidine plus lenalidomide, or azacitidine alone in older patients with newly diagnosed AML |

The compiled data records no completed Phase 3 trial in myeloid leukemia itself. One further Phase 3 trial (NCT04490707, azacitidine plus lenalidomide maintenance in elderly or unfit AML, n=60) has unknown status.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23316859](https://pubmed.ncbi.nlm.nih.gov/23316859/) | 2013 | Review | Expert Opin Investig Drugs | Lenalidomide as a novel AML treatment; approved for del(5q) MDS and myeloma, with trials in higher-risk MDS and AML |
| [32899586](https://pubmed.ncbi.nlm.nih.gov/32899586/) | 2020 | Review | Cancers | Immunomodulatory drugs (IMiDs) in AML treatment, covering immunomodulatory, antiangiogenic and proapoptotic properties |
| [39881283](https://pubmed.ncbi.nlm.nih.gov/39881283/) | 2025 | Laboratory study | Cell Mol Biol Lett | KDM5C stabilizes CRBN and raises the sensitivity of AML cells to lenalidomide |
| [37288607](https://pubmed.ncbi.nlm.nih.gov/37288607/) | 2023 | Review | Am J Hematol | 2023 update on MDS diagnosis, risk stratification and management |
| [42400116](https://pubmed.ncbi.nlm.nih.gov/42400116/) | 2026 | Review | Am J Hematol | 2026 update on MDS diagnosis, risk stratification and management |
| [37874917](https://pubmed.ncbi.nlm.nih.gov/37874917/) | 2023 | Review | Blood | Clinical decision-making and treatment in MDS |
| [29795386](https://pubmed.ncbi.nlm.nih.gov/29795386/) | 2018 | Review | Blood Cancer J | MDS treatment algorithm 2018 |
| [17488990](https://pubmed.ncbi.nlm.nih.gov/17488990/) | 2007 | Review | J Clin Oncol | AML and MDS in older patients; poor outcomes with standard regimens suggest a role for investigational therapy |
| [22483153](https://pubmed.ncbi.nlm.nih.gov/22483153/) | 2012 | Review | Curr Cancer Drug Targets | Novel and emerging drugs for AML |
| [24656536](https://pubmed.ncbi.nlm.nih.gov/24656536/) | 2014 | Review | Lancet | Overview of myelodysplastic syndromes, including progression to AML |

No randomized controlled trial publication specific to lenalidomide in myeloid leukemia was retrieved. Several of the retrieved papers are general MDS or AML reviews rather than lenalidomide-specific evidence.

---

## New Zealand Market Information

All six licences are cancelled, so lenalidomide is not currently marketed in New Zealand under these authorizations. Approved-indication text is not available for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2707543 | Lenalidomide Viatris | Capsule 15 mg | Cancelled |
| 2707527 | Lenalidomide Viatris | Capsule 5 mg | Cancelled |
| 2673568 | Lenalidomide Viatris | Capsule 25 mg | Cancelled |
| 2707551 | Lenalidomide Viatris | Capsule 25 mg | Cancelled |
| 2673541 | Lenalidomide Viatris | Capsule 15 mg | Cancelled |

A sixth licence (2707535, Capsule 10 mg) is also cancelled.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Immunomodulatory agent (thalidomide analogue), not a conventional cytotoxic |
| Myelosuppression Risk | High. Neutropenia and thrombocytopenia are listed as knowledge-graph contraindication associations |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count with differential; renal function (renal impairment is a listed association); signs of venous thromboembolism |
| Handling Protection | Please refer to the package insert warnings and precautions. As a thalidomide analogue with embryo-related concerns, strict handling and pregnancy-prevention precautions are expected |

---

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** thrombocytopenia, neutropenia, agranulocytosis, pulmonary embolism, impaired renal function, cancer, embryoma.
- **Drug Interactions:** 100 interactions were found in total. Major-level interactions include **rosuvastatin** and **simvastatin**. Moderate-level interactions include **eliglustat** and **naltrexone**. Many others (for example metformin, prednisone, omeprazole, morphine) are of unknown severity.

Please refer to the package insert for authoritative warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is very high (99.49%) and the mechanism is plausible, but the evidence for myeloid leukemia consists mostly of small Phase 1 and Phase 2 studies, many single-arm, with no completed Phase 3 RCT. In addition, no licence is currently valid in New Zealand and safety label data are missing. The package-insert data gap blocks safety screening.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (blocking gap)
- Formal mechanism-of-action data from DrugBank
- Published results and outcomes of the larger randomized trials (NCT01358734, NCT01522976)
- Subgroup analysis, particularly del(5q) and chromosome 5 abnormalities, where benefit is most plausible
- A haematological and thromboembolic monitoring plan, and a regulatory pathway given that all New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

