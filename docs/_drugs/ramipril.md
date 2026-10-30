---
layout: default
title: Ramipril
parent: Model Prediction Only (L5)
nav_order: 398
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-30
monitor_inn: "RAMIPRIL"
predicted_indications: ["pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "malignant hypertensive renal disease", "Braddock syndrome", "brain stem infarction", "Prinzmetal angina", "intracerebral hemorrhage", "ABri amyloidosis", "cerebral artery occlusion"]
description: "Ramipril: predicted new indication Pulmonary hypertension with unclear multifactorial mechanism (evidence level L5). Approved use: Not recorded in the local data"
---

# Ramipril
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Ramipril: Pulmonary hypertension with unclear multifactorial mechanism
{: .text-alpha }

## One-Sentence Summary

Ramipril is an ACE inhibitor originally used for hypertension and for stroke and cerebrovascular disease.
The TxGNN model predicts it may be useful for **pulmonary hypertension owing to lung disease and/or hypoxia**, but there are **0 clinical trials** and only **indirect preclinical literature** (1 rat study directly on hypoxic pulmonary hypertension) behind this prediction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pulmonary hypertension with unclear multifactorial mechanism |
| TxGNN Prediction Score | 0.9993 |
| Evidence Level | L5 (preclinical evidence only) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 of 4 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 4 (valid: 4) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Ramipril is known to be an angiotensin-converting enzyme (ACE) inhibitor, so it reduces angiotensin II production and lowers vascular tone through the renin-angiotensin system (RAS). Its efficacy in hypertension and in stroke prevention is established.

The link to pulmonary hypertension is that RAS activity may also affect the tone and remodelling of pulmonary blood vessels. The most relevant study is a 1996 rat experiment (PMID 9200213), in which ramipril given during three weeks of chronic hypoxia lowered baseline perfusion pressure in the lungs and attenuated the effects of hypoxia on pulmonary vessels. This fits a RAS-mediated mechanism.

This support is weak. The result comes from a single animal study and has not been confirmed in humans. The other retrieved papers concern myocardial infarction, kidney capillaries and cardiac remodelling, not pulmonary hypertension. ACE inhibitors are not an established therapy for pulmonary hypertension.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Only the publications that bear on this indication are listed. The rest of the retrieved set (for example the etomidate case report, the toxoplasmosis case report and the endometriosis study) is unrelated to pulmonary hypertension.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9200213](https://pubmed.ncbi.nlm.nih.gov/9200213/) | 1996 | Preclinical (rat) | Physiological Research | Ramipril 10 mg/kg daily during 3 weeks of chronic hypoxia attenuated the effects of hypoxia on pulmonary blood vessels and lowered baseline perfusion pressure |
| [15671045](https://pubmed.ncbi.nlm.nih.gov/15671045/) | 2005 | Preclinical / human tissue | European Heart Journal | ACE2 expression rises after myocardial infarction in rats and human failing hearts; background on RAS biology, not pulmonary hypertension |
| [12069940](https://pubmed.ncbi.nlm.nih.gov/12069940/) | 2002 | Preclinical (rat) | Am J Physiol Regul Integr Comp Physiol | Transgenic rats with an active RAS showed somewhat better cardiac function after hypoxia-reoxygenation; indirect relevance only |
| [19380417](https://pubmed.ncbi.nlm.nih.gov/19380417/) | 2009 | Preclinical / cohort | Cardiovascular Research | ACE inhibition promoted endothelial progenitor cells and neoangiogenesis in cardiac pressure overload; indirect relevance only |
| [24907870](https://pubmed.ncbi.nlm.nih.gov/24907870/) | 2014 | Preclinical (rat) | Basic Research in Cardiology | Soluble guanylyl cyclase activation improved cardiac remodelling after myocardial infarction beyond ACE inhibition; not about pulmonary hypertension |

## New Zealand Market Information

All four licences are valid, and the records contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2635747 | Tryzan | Capsule 1.25 mg | Not recorded (valid) |
| 2635755 | Tryzan | Capsule 2.5 mg | Not recorded (valid) |
| 2635763 | Tryzan | Capsule 5 mg | Not recorded (valid) |
| 2635771 | Tryzan | Capsule 10 mg | Not recorded (valid) |

## Safety Considerations

- **Contraindication-related conditions (from knowledge-graph drug-disease edges, not label text)**: kidney disease, liver disease, gallbladder disease, renal artery obstruction, hypotensive disorder, hereditary angioedema (including C1 inhibitor deficiency), neutropenia and agranulocytosis, systemic lupus erythematosus (including paediatric), connective tissue disease, scleroderma and systemic sclerosis, bone disease, cartilage disease, and pyoureter.
- **Drug Interactions**: The interaction query returned 100 records. The ones listed in the data compiled for this report include:
  - **Major**: potassium citrate and potassium bicarbonate (risk of raised potassium).
  - **Moderate**: corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide), aspirin, morphine, bupropion, dronabinol, bowel-preparation products (picosulfuric acid, polyethylene glycol with electrolytes), and several antidiabetic drugs (metformin, glimepiride, chlorpropamide, alogliptin, and the SGLT2 inhibitors canagliflozin, dapagliflozin and empagliflozin).
- **Other considerations from the prediction data**: ACE inhibitors carry a risk of acute renal failure in bilateral renal artery stenosis.

The package insert warnings and contraindications could not be retrieved, so the label should be checked before any further work.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is very high (0.9993), but the support is limited to a single animal study from 1996, with no clinical trials and no human data. ACE inhibitors are not an established therapy for pulmonary hypertension. In addition, ramipril is marketed in New Zealand, with 4 valid licences out of 4.

Other predictions in the list were also reviewed. None has trial support:
- Intracerebral haemorrhage (L4) has only indirect animal data.
- Cerebral artery occlusion (L4) has one small human cerebral blood flow study and mixed animal results; it is the most promising candidate for further research, as it is consistent with ramipril's existing stroke-prevention role.
- Malignant renovascular hypertension and malignant hypertensive renal disease (both L5) are mechanistically plausible but have no evidence, and the first carries a renal safety concern.
- Prinzmetal angina, Braddock syndrome, ABri amyloidosis and brain stem infarction (all L5) have no ramipril-specific evidence.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe), as the safety data are currently incomplete
- Mechanism of action data for ramipril (from DrugBank)
- Confirmation of the 1996 rat findings in modern pulmonary hypertension models
- A systematic literature search for human studies of ACE inhibitors in pulmonary hypertension
- Clarification of the licensing situation, since all four New Zealand licences are valid
- A safety plan for patients with renal impairment, renal artery stenosis, hypotension or angioedema risk
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

