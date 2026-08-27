---
layout: default
title: Folic Acid
parent: 僅模型預測 (L5)
nav_order: 159
evidence_level: L5
indication_count: 1
---

# Folic Acid
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

# Folic Acid: From Folate Deficiency to Biotin Metabolic Disease

## One-Sentence Summary

> Folic acid (Vitamin B9, DB00158) is a water-soluble B-vitamin classically used to treat folate-deficiency states such as megaloblastic anemia; it currently holds no marketing authorization in New Zealand and no confirmed original-indication label in the available regulatory data.
> The TxGNN model predicts a possible link to **Biotin Metabolic Disease** (e.g., biotinidase deficiency, holocarboxylase synthetase deficiency) with a very high raw score (**99.49%**), but this is supported by only **13 loosely-relevant clinical trials** and **20 mostly background-review publications**, and the model's own rationale flags the signal as likely a knowledge-graph artifact rather than a genuine pharmacological link.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in available Taiwan/NZ regulatory data (no licenses on record); generically, folic acid is indicated for folate-deficiency anemia and prevention of neural tube defects |
| Predicted New Indication | Biotin Metabolic Disease |
| TxGNN Prediction Score | 99.49% (raw score 0.9949, rank 4584) |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism-of-action data for folic acid in this context is not available (Data Gap DG002). Based on known pharmacology, folic acid (Vitamin B9) is a coenzyme precursor required for one-carbon metabolism, purine/pyrimidine synthesis, and methionine regeneration from homocysteine — a pathway that is biochemically distinct from biotin (Vitamin B7) metabolism, which centers on carboxylase enzyme cofactor activity.

Biotin metabolic diseases (biotinidase deficiency, holocarboxylase synthetase deficiency) are treated with biotin supplementation, not folic acid. The evidence pack's own repurposing rationale is explicit on this point: the high TxGNN score likely arises from a **"vitamin" node clustering effect** in the knowledge graph — folic acid and biotin frequently co-occur in multivitamin/B-complex supplementation studies and are jointly classified under "vitamin-responsive inborn errors of metabolism" in review literature (e.g., PMID 23622402, PMID 30557456). This is a **co-classification pattern, not a demonstrated biochemical mechanism** by which folic acid could compensate for or substitute in the biotin-dependent carboxylase pathway.

No study identified in this evidence pack proposes a biochemical hypothesis for folic acid activity in biotin metabolic disease specifically. Consequently, while the raw prediction score is high, the mechanistic plausibility underlying it is weak, and this should be treated as a hypothesis-generating signal only, not as evidence of therapeutic potential.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07350538](https://clinicaltrials.gov/study/NCT07350538) | N/A | Active, not recruiting | 20 | Gut microbiome/prebiotic pilot study in alcohol addiction recovery — not disease-specific (Grade C) |
| [NCT03360435](https://clinicaltrials.gov/study/NCT03360435) | N/A | Completed | 99 | Transdermal vitamin absorption after bariatric surgery — general micronutrient deficiency study, not disease-specific (Grade C) |
| [NCT00572741](https://clinicaltrials.gov/study/NCT00572741) | N/A | Completed | 39 | Oxidative stress/metabolic pathology in autism — different mechanism from biotin metabolism (Grade C) |
| [NCT01558193](https://clinicaltrials.gov/study/NCT01558193) | N/A | Completed | 202 | Multivitamin/mineral + fatty acid supplementation on impulsivity/aggression — unrelated to metabolic disease treatment (Grade C) |
| [NCT04067921](https://clinicaltrials.gov/study/NCT04067921) | N/A | Unknown | 1963 | General nutrition/health clinical trials platform — not disease-specific (Grade C) |
| [NCT05687474](https://clinicaltrials.gov/study/NCT05687474) | N/A | Completed | 6824 | Universal newborn genomic screening panel (may include biotinidase deficiency screening) — a screening platform, not a treatment trial (Grade C) |
| [NCT01643187](https://clinicaltrials.gov/study/NCT01643187) | Phase 2 | Unknown | 1000 | Fortified food vs. milk in malnourished children; folic acid was one of several micronutrients monitored, not biotin-disease specific (Grade C) |
| [NCT01173315](https://clinicaltrials.gov/study/NCT01173315) | Phase 2 | Completed | 75 | Vitamin/mineral supplementation for neuropathy/nephropathy in Type 2 diabetes — no direct link to biotin metabolic disease (Grade C) |
| [NCT04586348](https://clinicaltrials.gov/study/NCT04586348) | Phase 4 | Active, not recruiting | 794 | Prenatal iodine supplementation and neurodevelopment — unrelated to biotin metabolic disease (Grade C) |
| [NCT03444155](https://clinicaltrials.gov/study/NCT03444155) | N/A | Completed | 30 | Natural vs. synthetic Vitamin B-complex pilot (includes biotin and folic acid) — general bioavailability comparison, not disease-targeted (Grade C) |

*3 additional trials (NCT02302729, NCT01474486, NCT04312152) are general micronutrient/multivitamin studies with relevance grading still pending; none were disease-specific to biotin metabolic disease.*

**None of the trials above directly study folic acid as a treatment for biotin metabolic disease** — all are graded low relevance (C) as general vitamin/nutrition studies.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23622402](https://pubmed.ncbi.nlm.nih.gov/23622402/) | 2013 | Review | Handbook of Clinical Neurology | Discusses cobalamin, folate, and biotin deficiencies as related but biochemically distinct "vitamin-responsive" inborn errors of metabolism |
| [30557456](https://pubmed.ncbi.nlm.nih.gov/30557456/) | 2019 | Review | Movement Disorders | Reviews treatable inborn errors of metabolism, including biotin- and folate-responsive conditions, without proposing cross-mechanism substitution |
| [38203763](https://pubmed.ncbi.nlm.nih.gov/38203763/) | 2024 | Review | Int J Mol Sciences | Notes biotin and folic acid are both cofactors converging on Vitamin B12-dependent pathways, but does not support folic acid activity in biotin-specific disease |
| [29173522](https://pubmed.ncbi.nlm.nih.gov/29173522/) | 2017 | Review | Gastroenterology Clinics of North America | Reviews vitamin/mineral deficiencies in IBD; general micronutrient monitoring context only |
| [37123774](https://pubmed.ncbi.nlm.nih.gov/37123774/) | 2023 | Review | Cureus | Notes biotin levels are lower in diabetic patients alongside other B-vitamins; no biotin metabolic disease mechanism discussed |
| [25388747](https://pubmed.ncbi.nlm.nih.gov/25388747/) | 2015 | Review | Endocr Metab Immune Disord Drug Targets | Similar review of B-vitamin status (including biotin) in Type 2 diabetes |
| [41692080](https://pubmed.ncbi.nlm.nih.gov/41692080/) | 2026 | Review | Clinics in Dermatology | General overview of B-vitamin group physiology, including biotin and folate, in dermatologic context |
| [7027768](https://pubmed.ncbi.nlm.nih.gov/7027768/) | 1981 | Review | Acta Vitaminol Enzymol | General review of vitamin-dependent metabolic disease mechanisms across multiple vitamins |
| [1368195](https://pubmed.ncbi.nlm.nih.gov/1368195/) | 1992 | Review | J Chem Technol Biotechnol | Background review on industrial production of vitamins/coenzymes; not clinically relevant |
| [36197290](https://pubmed.ncbi.nlm.nih.gov/36197290/) | 2022 | Cohort | Microbiology Spectrum | Gut microbiome/metabolomics changes in seafarers; tangential, no disease-specific relevance |

**Across the 10 most relevant publications, folic acid and biotin are consistently discussed as parallel members of the B-vitamin family or co-listed under "vitamin-responsive metabolic disorders" — no publication proposes a mechanistic pathway by which folic acid would treat biotin metabolic disease.**

---

## New Zealand Market Information

Folic acid currently has no marketing authorization on record in New Zealand (0 licenses; market status: Not Marketed). No product-level data (dosage form, approved indication text) is available to populate this table.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Key warnings, contraindications, and drug-interaction data are not currently available in this evidence pack — Data Gap DG001, classified as Blocking for safety pre-screening.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but the evidence pack's own mechanistic analysis attributes this to knowledge-graph clustering around "vitamin" nodes rather than a genuine biochemical pathway linking folic acid to biotin metabolic disease. Supporting clinical trial and literature evidence is uniformly low-relevance (general B-vitamin/nutrition studies, not disease-targeted), and two Blocking/High-severity data gaps remain: TFDA/package-insert safety data (DG001, Blocking) and drug mechanism-of-action confirmation (DG002, High). This candidate remains at the initial screening stage (S0) and does not meet the threshold to proceed.

**To proceed, the following is needed:**
- Package insert warnings/contraindications data (source: TFDA/Medsafe official site) to clear the Blocking safety gap
- Confirmed mechanism-of-action data from DrugBank or primary pharmacology literature
- A targeted biochemical or preclinical study specifically testing folic acid's effect on biotin-dependent carboxylase pathways, rather than relying on co-occurrence in general B-vitamin literature
- Reassessment of the TxGNN prediction after correcting for the suspected "vitamin" node clustering artifact in the knowledge graph
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

