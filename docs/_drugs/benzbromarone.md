---
layout: default
title: Benzbromarone
parent: Model Prediction Only (L5)
nav_order: 62
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Benzbromarone: predicted new indication Renal hypouricaemia (evidence level L5). Approved use: Gout"
monitor_inn: "BENZBROMARONE"
predicted_indications: ["hypouricemia, renal"]
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

# Benzbromarone: From Gout and Hyperuricaemia to Renal Hypouricaemia

## One-Sentence Summary

Benzbromarone is a uricosuric drug originally used to treat gout and hyperuricaemia.
The TxGNN model predicts it may be effective for **renal hypouricaemia**, but there are **no registered clinical trials** and **18 publications**, none of which show a therapeutic benefit. The drug's mechanism points in the opposite direction to the disease, so this prediction is most likely a false positive.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gout; hyperuricaemia (from the knowledge graph; no approved indication text is available from the New Zealand licence) |
| Predicted New Indication | Renal hypouricaemia |
| TxGNN Prediction Score | 99.07% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, benzbromarone is a potent uricosuric that inhibits the urate transporter URAT1 (SLC22A12), reducing renal urate reabsorption and lowering serum uric acid. Its efficacy in gout and hyperuricaemia comes from this action.

Renal hypouricaemia is the opposite problem: excessive urate loss through the kidneys, typically caused by loss-of-function variants in URAT1 (SLC22A12) or GLUT9 (SLC2A9). Benzbromarone would therefore reproduce or worsen the underlying defect rather than correct it. The disease is effectively a genetic version of URAT1 inhibition. It also carries risks of exercise-induced acute renal failure and nephrolithiasis, which a uricosuric could aggravate.

The high TxGNN score most likely reflects a network association between benzbromarone and the URAT1/uric acid transport axis, not a therapeutic relationship. In the literature, benzbromarone appears as a **diagnostic probe** (together with pyrazinamide) used to classify the type of tubular urate transport defect, not as a treatment.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

The publications retrieved are mostly reviews, case reports and case series. None is a controlled study of benzbromarone as a treatment for renal hypouricaemia. The 10 most relevant are listed below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clinical Rheumatology | Narrative update on the causes, diagnosis and clinical handling of hypouricaemia (serum urate < 2 mg/dL) |
| [14694169](https://pubmed.ncbi.nlm.nih.gov/14694169/) | 2004 | Cohort/Genetic analysis | J Am Soc Nephrol | In 32 patients with renal hypouricaemia, SLC22A12 (URAT1) mutations were linked to increased urate clearance |
| [8976099](https://pubmed.ncbi.nlm.nih.gov/8976099/) | 1996 | Review | Nihon Rinsho | Concept and classification of urate metabolism abnormalities (hyperuricaemia vs hypouricaemia) |
| [14655203](https://pubmed.ncbi.nlm.nih.gov/14655203/) | 2003 | Case report | Am J Kidney Dis | Two brothers with hereditary renal hypouricaemia and exercise-induced acute renal failure; full recovery with conservative care |
| [8863890](https://pubmed.ncbi.nlm.nih.gov/8863890/) | 1996 | Case report | Acta Paediatr | Recurrent exercise-induced acute renal failure; a benzbromarone/pyrazinamide test suggested a presecretory reabsorption defect |
| [4009341](https://pubmed.ncbi.nlm.nih.gov/4009341/) | 1985 | Case series | J Pediatr | In children with hereditary renal hypouricaemia, benzbromarone did not change urate clearance ratios |
| [8893184](https://pubmed.ncbi.nlm.nih.gov/8893184/) | 1996 | Case report/Physiological study | Nephron | Benzbromarone and pyrazinamide used to analyse urate transport in Fanconi syndrome with renal hypouricaemia |
| [3380222](https://pubmed.ncbi.nlm.nih.gov/3380222/) | 1988 | Case report/Physiological study | Nephron | Urate clearance increased further after benzbromarone in a patient with an isolated renal urate transport defect |
| [8302413](https://pubmed.ncbi.nlm.nih.gov/8302413/) | 1993 | Case report | Nephron | Benzbromarone markedly increased urate clearance in renal hypouricaemia with urolithiasis; the urolithiasis was treated by urine alkalisation |
| [9144014](https://pubmed.ncbi.nlm.nih.gov/9144014/) | 1997 | Case report | Intern Med | Benzbromarone suppression test used to classify urate transport defects in two cases with nephrolithiasis |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2583194 (cancelled) | Narcaricin mite | Tab 50 mg | Not recorded |

## Cytotoxicity

Not applicable. Benzbromarone is not an antineoplastic drug, so this section is omitted.

## Safety Considerations

No usable warning, contraindication or drug interaction data were available for this report. Please refer to the package insert for safety information.

Pharmacological concerns specific to this prediction:
- A uricosuric could further increase urinary urate excretion in a disease already defined by renal urate wasting.
- Patients with renal hypouricaemia are already at risk of exercise-induced acute renal failure and nephrolithiasis.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high (99.07%), but the evidence level is L5: there are no clinical trials and no published studies showing benefit. The mechanism (URAT1 inhibition) would mimic or worsen the disease rather than treat it, so the prediction is likely a false positive caused by the direction of effect. The drug also has no currently valid New Zealand licence.

**To proceed, the following is needed:**
- A credible mechanistic rationale showing how URAT1 inhibition could benefit this condition (none is currently apparent)
- Mechanism of action data confirmed from DrugBank
- Package insert warnings and contraindications from Medsafe
- Safety assessment in patients with renal hypouricaemia, especially for acute renal failure and nephrolithiasis
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

