---
layout: default
title: Lisinopril
parent: Model Prediction Only (L5)
nav_order: 273
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-30
monitor_inn: "LISINOPRIL"
predicted_indications: ["pulmonary hypertension owing to lung disease and/or hypoxia", "pulmonary hypertension with unclear multifactorial mechanism", "malignant hypertensive renal disease", "Braddock syndrome", "chronic pulmonary heart disease", "autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome"]
description: "Lisinopril: predicted new indication Pulmonary hypertension owing to lung disease and/or hypoxia (evidence level L5). Approved use: Not recorded in the local data"
---

# Lisinopril
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

## Lisinopril: Pulmonary hypertension owing to lung disease and/or hypoxia
{: .text-alpha }

## One-Sentence Summary

Lisinopril is an ACE inhibitor originally used for hypertension and myocardial infarction.
There are **0 clinical trials** and **0 publications** specific to this prediction, so the evidence rests on model prediction alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pulmonary hypertension owing to lung disease and/or hypoxia |
| TxGNN Prediction Score | 0.9989 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, lisinopril is an angiotensin-converting enzyme (ACE) inhibitor that blocks the renin-angiotensin system (RAS). Its efficacy in hypertension and myocardial infarction is established.

Posteroinferior myocardial infarction is an anatomical subtype of myocardial infarction, an indication lisinopril already holds. Mechanistically, ACE inhibition attenuates post-infarction remodelling and would be expected to apply across infarct locations, but no subtype-specific data were supplied.

**Other predictions in the top ranks**

| Rank | Predicted Indication | Score | Evidence Level | Comment |
|---|---|---|---|---|
| 3 | Pulmonary hypertension owing to lung disease and/or hypoxia | 0.9989 | L5 | 15 papers retrieved, mostly off-target; one preclinical study suggests angiotensin II does not drive hypoxic pulmonary artery contraction, which argues against the mechanism |
| 4 | Pulmonary hypertension, unclear multifactorial mechanism | 0.9989 | L5 | Prediction only |
| 5 | Malignant hypertensive renal disease | 0.9989 | L5 | Extension of the approved hypertension indication |
| 8 | Braddock syndrome | 0.9986 | L5 | Prediction only |
| 9 | Chronic pulmonary heart disease | 0.9968 | L3 | The most notable lead (see below) |
| 10 | Autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome | 0.9933 | L5 | Prediction only |

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


| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2643669 | Teva Lisinopril | Tab 5 mg | Valid; no indication text available |
| 2643677 | Teva Lisinopril | Tab 10 mg | Valid; no indication text available |
| 2643685 | Teva Lisinopril | Tab 20 mg | Valid; no indication text available |

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
The top prediction is a subtype of an indication lisinopril already covers, so the score reflects ontology proximity rather than new evidence. The most promising genuinely new direction is chronic pulmonary heart disease, but it is supported only by small, unconfirmed studies.

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

