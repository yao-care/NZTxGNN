---
layout: default
title: Benzbromarone
parent: Model Prediction Only (L5)
nav_order: 62
evidence_level: L5
indication_count: 1
monitor_inn: "BENZBROMARONE"
predicted_indications: ["hypouricemia, renal"]
last_modified_at: 2026-10-30
description: "Benzbromarone: predicted new indication Renal hypouricemia (hypouricemia, renal) (evidence level L5). Approved use: Not recorded in the local data"
---

# Benzbromarone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Benzbromarone: URAT1 inhibitor uricosuric → Renal hypouricemia (hypouricemia, renal)
{: .text-alpha }

## One-Sentence Summary

Benzbromarone is a uricosuric drug that lowers serum uric acid by blocking renal urate reabsorption.
The TxGNN model predicts it for **renal hypouricemia (hypouricemia, renal)**, but **no clinical trials** and **18 publications** were found, and none of them studies benzbromarone as a treatment for this condition. The prediction is most likely a false positive.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Renal hypouricemia (hypouricemia, renal) |
| TxGNN Prediction Score | 0.9907 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From general pharmacology, benzbromarone is a potent uricosuric that inhibits URAT1 (SLC22A12), reducing renal urate reabsorption and lowering serum uric acid.

Renal hypouricemia is characterised by excessive urinary urate loss, most often caused by loss-of-function variants in URAT1 (SLC22A12) or GLUT9 (SLC2A9). Benzbromarone's pharmacology would therefore reproduce or worsen the underlying defect rather than correct it. The high score of 0.9907 most likely reflects a close association between the drug and the urate transport pathway, not a therapeutic relationship.

In the publications, benzbromarone appears as a **diagnostic probe**. It is used in urate clearance tests, together with pyrazinamide, to classify the type of tubular defect. It is not used as a treatment. The disease also carries risks such as exercise-induced acute renal failure and kidney stones, which a uricosuric could aggravate.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

None of the publications tests benzbromarone as therapy for renal hypouricemia. The selection below covers the most relevant ones, with review articles prioritised.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clinical Rheumatology | Narrative update on hypouricemia (serum urate < 2 mg/dL), its causes and clinical relevance |
| [8976099](https://pubmed.ncbi.nlm.nih.gov/8976099/) | 1996 | Review | Nihon Rinsho | Concept and classification of urate metabolism abnormalities |
| [14694169](https://pubmed.ncbi.nlm.nih.gov/14694169/) | 2004 | Cohort/Genetic analysis | J Am Soc Nephrol | Renal hypouricemia in 32 Japanese patients, linked to SLC22A12 (URAT1) mutations |
| [10879667](https://pubmed.ncbi.nlm.nih.gov/10879667/) | 2000 | Case series | Clin Nephrol | Renal hypouricemia with exercise-induced acute renal failure and chronic renal dysfunction |
| [4009341](https://pubmed.ncbi.nlm.nih.gov/4009341/) | 1985 | Case series | J Pediatr | In four children with hereditary renal hypouricemia, benzbromarone did not change urate clearance ratios |
| [14655203](https://pubmed.ncbi.nlm.nih.gov/14655203/) | 2003 | Case report | Am J Kidney Dis | Two brothers with hereditary renal hypouricemia and exercise-induced acute renal failure |
| [8863890](https://pubmed.ncbi.nlm.nih.gov/8863890/) | 1996 | Case report | Acta Paediatr | Recurrent exercise-induced renal failure; a benzbromarone/pyrazinamide test used to locate the defect |
| [3380222](https://pubmed.ncbi.nlm.nih.gov/3380222/) | 1988 | Case report | Nephron | Isolated renal urate transport defect; urate clearance rose further after benzbromarone |
| [9144014](https://pubmed.ncbi.nlm.nih.gov/9144014/) | 1997 | Case report | Intern Med | Two cases with kidney stones; benzbromarone suppression test used for classification |
| [8893184](https://pubmed.ncbi.nlm.nih.gov/8893184/) | 1996 | Case report | Nephron | Uric acid transport in Fanconi syndrome with renal hypouricemia, analysed with pyrazinamide and benzbromarone |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2583194 | Narcaricin mite | Tab 50 mg | Not recorded in the local data |

## Safety Considerations

Please refer to the package insert for safety information.

The publications do indicate that renal hypouricemia is associated with exercise-induced acute renal failure and kidney stones. A uricosuric such as benzbromarone could plausibly aggravate these risks.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The evidence is at L5, with only a model prediction and no study showing a benefit of benzbromarone in this condition. The drug's mechanism, increasing urinary urate excretion, points in the opposite direction to what the disease would need, so the prediction is probably an artefact of the drug's link to the URAT1 pathway.

**To proceed, the following is needed:**
- Mechanism of action data confirmed from DrugBank
- Safety information from the Medsafe package insert (warnings and contraindications)
- Any new evidence showing a therapeutic rationale; without it, this direction should not be pursued
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

