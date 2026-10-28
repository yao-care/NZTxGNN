---
layout: default
title: Probenecid
parent: Moderate Evidence (L3-L4)
nav_order: 384
evidence_level: L4
indication_count: 3
last_modified_at: 2026-10-15
description: "Probenecid: predicted new indication hypouricemia, renal (evidence level L4). Approved use: Gout"
monitor_inn: "PROBENECID"
predicted_indications: ["hypouricemia, renal", "Lesch-Nyhan syndrome", "hypoxanthine guanine phosphoribosyltransferase partial deficiency"]
---

# Probenecid
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **3** 
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

## Probenecid: From Gout to hypouricemia, renal
{: .text-alpha }

## One-Sentence Summary

Probenecid is a uricosuric drug originally used to treat gout and hyperuricemia.
The TxGNN model predicts it may be effective for **renal hypouricemia**, but the data compiled for this report show **0 clinical trials** and **20 publications** that describe the disease only. None of them shows probenecid being used as a treatment.
The mechanism also points the wrong way: probenecid would likely worsen this condition rather than treat it.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gout; hyperuricemia |
| Predicted New Indication | Renal hypouricemia |
| TxGNN Prediction Score | 0.9973 |
| Evidence Level | L4 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

*Note: the New Zealand license record has no approved indication text, so the original indications above are not taken from approved labelling.*

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, probenecid is a uricosuric. It inhibits renal urate reabsorption through the URAT1 (SLC22A12) and OAT transporters, which lowers serum uric acid. That is why it works in gout.

The predicted indication, however, is hard to justify. Renal hypouricemia is caused mainly by loss-of-function variants in URAT1 or GLUT9. These variants already cause excessive urinary urate excretion and low serum urate. Additional uricosuric action from probenecid would be expected to **worsen** the underlying defect. It could increase the risk of exercise-induced acute kidney injury and urolithiasis.

The high TxGNN score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. In the literature, probenecid appears only as a **diagnostic probe** of tubular urate handling. Several case reports describe patients whose urate excretion barely responded to it. No publication reports it as a treatment.

Two lower-ranked predictions were also reviewed: Lesch-Nyhan syndrome (score 0.9939) and partial HPRT deficiency (score 0.9937). Both are purine-overproduction disorders. Probenecid could plausibly lower serum urate there, but it would raise the urinary urate load and the risk of stones and urate nephropathy. Xanthine oxidase inhibitors such as allopurinol remain the preferred approach. For Lesch-Nyhan syndrome, lowering urate would also not address the neurobehavioral manifestations. Neither prediction has clinical trials or supporting clinical literature.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Of the 20 publications retrieved, the following are the most relevant. All are reviews, observational studies or case reports, and none tests probenecid as a therapy.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clin Rheumatol | Update on hypouricemia (serum urate < 2 mg/dL) for rheumatologists, covering its causes |
| [16678460](https://pubmed.ncbi.nlm.nih.gov/16678460/) | 2006 | Review | Mol Genet Metab | Hereditary renal hypouricemia is mostly caused by loss-of-function mutations in SLC22A12 (URAT1) |
| [14694169](https://pubmed.ncbi.nlm.nih.gov/14694169/) | 2004 | Cohort | J Am Soc Nephrol | Clinical and genetic analysis of 32 Japanese patients with renal hypouricemia; effect of the URAT1 gene on urate excretion |
| [7771493](https://pubmed.ncbi.nlm.nih.gov/7771493/) | 1995 | Review/Case report | Am J Kidney Dis | Exercise-induced acute renal failure in renal hypouricemia, with a literature review |
| [854144](https://pubmed.ncbi.nlm.nih.gov/854144/) | 1977 | Case report | Nephron | Familial hypouricemia with a blunted uric acid response to probenecid and pyrazinamide, indicating a tubular reabsorption defect |
| [8341392](https://pubmed.ncbi.nlm.nih.gov/8341392/) | 1993 | Case report | Nephron | Novel type of renal hypouricemia with no urate response to probenecid or pyrazinamide |
| [8533596](https://pubmed.ncbi.nlm.nih.gov/8533596/) | 1995 | Case report | Acta Paediatr Jpn | Acute renal failure after exercise in a child; probenecid and pyrazinamide tests showed a total defect of uric acid reabsorption |
| [2709741](https://pubmed.ncbi.nlm.nih.gov/2709741/) | 1989 | Case report | Klin Wochenschr | Acute renal failure from uric acid nephropathy; urate excretion was enhanced by probenecid, used as a diagnostic test |
| [14747372](https://pubmed.ncbi.nlm.nih.gov/14747372/) | 2004 | Basic research | J Am Soc Nephrol | Function and localization of URAT1 in mouse kidney |
| [24078686](https://pubmed.ncbi.nlm.nih.gov/24078686/) | 2013 | Computational study | Bioinformatics | Fuzzy optimization method to detect enzyme targets in uric acid metabolism (retrieved for Lesch-Nyhan syndrome) |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2012499 | Probenecid-AFT | Tab 500 mg | Not recorded (authorization valid) |

---

## Safety Considerations

- **Conditions flagged as contraindications** (not package insert text): liver failure, kidney disease and impaired renal function, blood disorders (thrombocytopenia, leukopenia, agranulocytosis, neutropenia and congenital neutropenia syndromes), peptic ulcer disease, muscle disorders (myopathy, rhabdomyolysis, familial periodic paralysis), and diarrhoeal or colitis conditions (including *Clostridium difficile* colitis).
- **Drug interactions:** 71 interactions were identified. The main ones are:
  - **Major:** phenylbutyric acid and glycerol phenylbutyrate.
  - **Moderate:** acetylsalicylic acid (aspirin), and the sulfonylurea and glinide antidiabetics (chlorpropamide, glimepiride, glipizide, glyburide, tolazamide, tolbutamide, acetohexamide, repaglinide, nateglinide).
  - **Minor:** famotidine, amoxicillin, levofloxacin and eluxadoline.
- **Disease-specific concern:** in renal hypouricemia, further urate excretion could increase the risk of exercise-induced acute kidney injury and urolithiasis.

Please refer to the package insert for authoritative warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but there are no clinical trials and no literature showing a therapeutic benefit. The mechanism argues against benefit in renal hypouricemia, because probenecid would add to the urate loss that already defines the disease. The product is also no longer authorized in New Zealand. The other two predictions (Lesch-Nyhan syndrome and partial HPRT deficiency) are no better supported, and xanthine oxidase inhibitors are the preferred treatment for both.

**To proceed, the following is needed:**
- Package insert warnings and contraindications, which are the blocking data gap for safety screening
- Detailed mechanism of action data
- Direct evidence, from non-clinical or clinical studies, that probenecid has any therapeutic benefit in renal hypouricemia
- A risk assessment of nephrolithiasis and exercise-induced acute kidney injury in this population
- Confirmation of whether a currently authorized probenecid product exists in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

