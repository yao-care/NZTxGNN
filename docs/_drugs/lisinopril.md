---
layout: default
title: Lisinopril
parent: Model Prediction Only (L5)
nav_order: 273
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Lisinopril: predicted new indication Posteroinferior myocardial infarction (evidence level L5). Approved use: Hypertension, myocardial infarction"
monitor_inn: "LISINOPRIL"
predicted_indications: ["pulmonary hypertension owing to lung disease and/or hypoxia", "pulmonary hypertension with unclear multifactorial mechanism", "malignant hypertensive renal disease", "chronic pulmonary heart disease", "autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome"]
---

# Lisinopril
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

# Lisinopril: From Hypertension and Myocardial Infarction to Posteroinferior Myocardial Infarction

## One-Sentence Summary

Lisinopril is an ACE inhibitor originally used for hypertension and myocardial infarction.
The TxGNN model predicts it may be effective for **posteroinferior myocardial infarction**, but this is an anatomical subtype of an already-approved indication, so the high score mostly reflects ontology proximity.
There are **0 clinical trials** and **0 publications** specific to this prediction, so the evidence rests on model prediction alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, myocardial infarction (the data compiled for this report holds no approved-indication text from New Zealand licences; the indications listed come from the knowledge graph) |
| Predicted New Indication | Posteroinferior myocardial infarction |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, lisinopril is an angiotensin-converting enzyme (ACE) inhibitor that blocks the renin-angiotensin system (RAS). Its efficacy in hypertension and myocardial infarction is established.

Posteroinferior myocardial infarction is an anatomical subtype of myocardial infarction, an indication lisinopril already holds. The high TxGNN score (99.90%) therefore mostly reflects closeness in the disease ontology rather than new evidence. Mechanistically, ACE inhibition attenuates post-infarction remodelling and would be expected to apply across infarct locations, but no subtype-specific data were supplied.

**Other predictions in the top ranks**

| Rank | Predicted Indication | Score | Evidence Level | Comment |
|---|---|---|---|---|
| 2 | Posterolateral myocardial infarction | 99.90% | L5 | Subtype of an approved indication; no subtype-specific evidence |
| 3 | Pulmonary hypertension owing to lung disease and/or hypoxia | 99.89% | L4 | 15 papers retrieved, mostly off-target; one preclinical study suggests angiotensin II does not drive hypoxic pulmonary artery contraction, which argues against the mechanism |
| 4 | Pulmonary hypertension, unclear multifactorial mechanism | 99.89% | L5 | Prediction only |
| 5 | Malignant hypertensive renal disease | 99.89% | L5 | Extension of the approved hypertension indication |
| 6 | Malignant renovascular hypertension | 99.89% | L5 | Plausible, but ACE inhibitors carry a risk of acute renal failure in bilateral renal artery stenosis |
| 7 | Septal myocardial infarction | 99.88% | L4 | One 2024 case report on an ACE inhibitor plus coenzyme Q10; indirect support only |
| 8 | Braddock syndrome | 99.86% | L5 | Prediction only |
| 9 | Chronic pulmonary heart disease | 99.68% | L3 | The most notable lead (see below) |
| 10 | Autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome | 99.33% | L5 | Prediction only |

Among all predictions, **chronic pulmonary heart disease** (rank 9) has the most specific supporting literature: two small Russian-language clinical reports on lisinopril in chronic cor pulmonale and in pulmonary hypertension with endothelial dysfunction. It is classed as a research question, but the study designs and outcomes cannot be confirmed.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for posteroinferior myocardial infarction.

For reference, the most relevant literature among the other predictions (chronic pulmonary heart disease):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [17047621](https://pubmed.ncbi.nlm.nih.gov/17047621/) | 2006 | Clinical study (design unconfirmed) | Kardiologiia | Lisinopril 10 mg/day attenuated pulmonary hypertension and improved right ventricular systolic function and diastolic filling in heart failure patients with ischaemic heart disease and COPD |
| [14524095](https://pubmed.ncbi.nlm.nih.gov/14524095/) | 2003 | Clinical study (likely small, uncontrolled) | Problemy tuberkuleza i boleznei legkikh | Lisinopril in chronic cor pulmonale; no abstract available |
| [20852161](https://pubmed.ncbi.nlm.nih.gov/20852161/) | 2010 | Case report (safety) | Am J Health Syst Pharm | Severe bradycardia and hypotension with concomitant tizanidine and lisinopril |

---

## New Zealand Market Information

All three licences are cancelled, and no approved-indication text is available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2643669 | Teva Lisinopril | Tab 5 mg | Cancelled; no indication text available |
| 2643677 | Teva Lisinopril | Tab 10 mg | Cancelled; no indication text available |
| 2643685 | Teva Lisinopril | Tab 20 mg | Cancelled; no indication text available |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the compiled data. Please refer to the package insert for these.

- **Disease contraindications (from the knowledge graph, not label text)**: kidney disease, liver disease, pancreatitis, gallbladder disease, angioedema (including hereditary, acquired and ACE-inhibitor-induced forms), systemic lupus erythematosus, potassium deficiency, diabetes mellitus and diabetic ketoacidosis, neutropenia and agranulocytosis, aortic stenosis and aortic valve disease, hypotensive disorders, hyperparathyroidism, hepatic coma and encephalopathy, connective tissue disease, and bone disease.
- **Drug interactions**: 100 interactions were identified. Those shown include two **Major** ones, with potassium citrate and potassium bicarbonate (hyperkalaemia risk). The rest shown are **Moderate**, including corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide), antidiabetic drugs (metformin, alogliptin, saxagliptin, canagliflozin, dapagliflozin, empagliflozin, chlorpropamide), aspirin, morphine, bupropion, dronabinol, and bowel-preparation agents.
- **Literature signals**: a case report documents severe hypotension and bradycardia when lisinopril was combined with tizanidine, and a 2026 case report describes lisinopril-induced small bowel angioedema.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction is a subtype of an indication lisinopril already covers, so the score reflects ontology proximity rather than new evidence. There are no trials or literature specific to it, and lisinopril has no currently valid New Zealand licence. The most promising genuinely new direction is chronic pulmonary heart disease, but it is supported only by small, unconfirmed studies.

**To proceed, the following is needed:**
- The Medsafe package insert (warnings and contraindications), which is a blocking gap for safety screening
- Mechanism of action data (for example from DrugBank)
- Confirmation of subtype-specific data for infarct location, such as post-MI trial subgroup analyses
- For chronic pulmonary heart disease, full texts and design details of the Russian-language studies, and a search for controlled trials
- Clarification of why the licences were cancelled and whether any re-registration is planned
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

