---
layout: default
title: Amiloride
parent: Model Prediction Only (L5)
nav_order: 32
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Amiloride: predicted new indication Malignant renovascular hypertension (evidence level L5). Approved use: Hypertension, hypertensive disorder, congestive heart failure"
monitor_inn: "Amiloride"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome", "chronic pulmonary heart disease"]
---

# Amiloride
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Amiloride: From Hypertension and Congestive Heart Failure to Malignant Renovascular Hypertension
{: .text-alpha }

## One-Sentence Summary

Amiloride is a potassium-sparing diuretic, originally used for hypertension and congestive heart failure.
The TxGNN model predicts it may be effective for **malignant renovascular hypertension**,
but **no clinical trials** and only **1 publication** (a review on a different type of hypertension) support this prediction, so it rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, hypertensive disorder, congestive heart failure (from the knowledge graph; no approved indication text is available for the New Zealand licences) |
| Predicted New Indication | Malignant renovascular hypertension |
| TxGNN Prediction Score | 99.82% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, amiloride blocks the epithelial sodium channel (ENaC) and is an established antihypertensive and potassium-sparing diuretic. Its efficacy in hypertension and heart failure is established, so a conceptual link to severe hypertension exists.

However, malignant renovascular hypertension is driven mainly by activation of the renin-angiotensin system from renal artery stenosis, and amiloride is not a primary therapy for it. The only retrieved paper (PMID 12929904) concerns mineralocorticoid hypertension and does not address this condition. The high prediction score most likely reflects the drug's existing hypertension indication rather than new evidence.

Other predictions in the same model run were also reviewed:
- **Chronic pulmonary heart disease** (score 99.68%, evidence level L3) has the most supporting literature. Human hemodynamic and double-blind studies in congestive heart failure support amiloride's cardiac use, but these studies concern left-sided heart failure, so applicability is by extrapolation.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (score 99.81%, evidence level L4) has preclinical support only. Hypoxia downregulates alveolar ENaC, and ASIC1 is implicated in hypoxic pulmonary hypertension. Amiloride's ASIC inhibition is nonspecific, and clinical relevance is unproven.
- **Malignant hypertensive renal disease, unclear multifactorial pulmonary hypertension and Braddock syndrome** have no supporting evidence and appear to inherit their scores from the general hypertension association.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12929904](https://pubmed.ncbi.nlm.nih.gov/12929904/) | 2003 | Review | Journal of Hypertension. Supplement | Review of the work-up of mineralocorticoid (corticosteroid-induced) hypertension, including primary aldosteronism. It does not address malignant renovascular hypertension. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2643022 | Wockhardt | Tab 5 mg | Cancelled; no indication text available |
| 344710 | Biomed | Oral liq 1 mg per ml | Cancelled; no indication text available |
| 2663120 | Padagis | Tab 5 mg | Cancelled; no indication text available |

## Safety Considerations

- **Contraindications flagged in the knowledge graph (not label text):** kidney disease, anuria, diabetic nephropathy, diabetes mellitus, diabetic ketoacidosis, liver disease, hepatic coma and encephalopathy, pancreatitis, gallbladder disease, gout, hyperuricemia, systemic lupus erythematosus (including paediatric), potassium deficiency disease, hyperparathyroidism, osteitis fibrosa, neonatal jaundice, acidosis and acquired lactic acidosis.
- **Drug interactions:** The interaction query returned 14 records. These are pharmacological target interactions rather than drug-drug interactions. They include ENaC, ASIC1/2/3, sodium/hydrogen exchangers (NHE1, 2, 3, 5), several TRP channels (TRPA1, TRPC6, TRPC7, TRPP2, TRPV2) and the GABA-A receptor α6 subunit.
- **Hyperkalemia risk:** A case report in the retrieved literature (PMID 8615380) describes severe hyperkalemia with co-medication in a geriatric patient. This is a particular concern in renal impairment, which is common in malignant hypertensive settings.

Package insert warnings and contraindications could not be obtained, so please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for malignant renovascular hypertension is a model output only (evidence level L5). There are no clinical trials, and the sole paper is off-topic. Amiloride is not a primary therapy for renin-driven hypertension, and its hyperkalemia risk is a concern in renal compromise. In addition, all three New Zealand authorizations are cancelled.

**To proceed, the following is needed:**
- Mechanism of action data to support mechanistic analysis
- The Medsafe package insert, so warnings and contraindications can be reviewed (currently a blocking gap for safety screening)
- Clinical or well-designed preclinical evidence specific to renovascular hypertension
- If pursuing a better-supported direction, a focused evidence review for chronic pulmonary heart disease and hypoxic pulmonary hypertension
- Confirmation of whether any amiloride product could be supplied in New Zealand, given the current market status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

