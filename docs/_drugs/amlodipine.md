---
layout: default
title: Amlodipine
parent: Model Prediction Only (L5)
nav_order: 35
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Amlodipine: predicted new indication Brain stem infarction (evidence level L5). Approved use: Hypertension, stroke-related disorders, cerebral infarction…"
monitor_inn: "AMLODIPINE"
predicted_indications: ["brain stem infarction", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "malignant hypertensive renal disease", "MRI defined brain infarct", "ABri amyloidosis", "intracerebral hemorrhage"]
---

# Amlodipine
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

# Amlodipine: From Hypertension to Brain Stem Infarction

## One-Sentence Summary

Amlodipine is a calcium channel blocker originally used for hypertension, stroke-related disorders, and Prinzmetal angina.
The TxGNN model predicts it may be effective for **brain stem infarction**, but this top-ranked prediction currently has **0 clinical trials** and **0 publications** supporting it.
It is essentially a subtype of an indication area already covered, so it is not a true repurposing signal. Of the other predictions, intracerebral haemorrhage has the strongest support (a completed Phase 3 trial of a triple-drug pill that includes amlodipine).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, stroke-related disorders, cerebral infarction, Prinzmetal angina (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Brain stem infarction |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 3 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Amlodipine blocks L-type calcium channels in vascular smooth muscle, causing vasodilation and lowering blood pressure. Detailed mechanism of action data is not available in the data compiled for this report, but this class effect is well established.

Brain stem infarction is a subtype of ischaemic stroke. Amlodipine is already linked to stroke and cerebral infarction in the knowledge graph, so this prediction extends an existing indication area rather than opening a new one. No trials or literature were found for this specific subtype.

Among the other predicted indications, the following are relevant:
- **Intracerebral haemorrhage (score 99.79%, L1):** Blood pressure lowering is the standard secondary prevention strategy. The completed Phase 3 TRIDENT trial tested a triple low-dose antihypertensive pill that includes amlodipine. Amlodipine's individual contribution cannot be isolated.
- **Cerebral artery occlusion (L4):** Several animal studies show neuroprotection, including synergy with atorvastatin. Clinical trials found are terminated or indirect.
- **Pulmonary hypertension due to lung disease/hypoxia (L4):** Only preclinical and case-level data. Vasodilators may worsen ventilation/perfusion matching in lung disease.
- **Malignant renovascular hypertension (L4):** Only paediatric case reports. This is close to the approved hypertension indication.
- **Pulmonary hypertension of unclear mechanism, malignant hypertensive renal disease, Braddock syndrome, MRI-defined brain infarct, ABri amyloidosis (L5):** Prediction only, with no trials or literature.

---

## Clinical Trial Evidence

No clinical trials are registered for brain stem infarction, the top-ranked prediction. The most relevant trials found for related predicted indications (intracerebral haemorrhage and cerebral artery occlusion) are listed below.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02699645](https://clinicaltrials.gov/study/NCT02699645) | Phase 3 | Completed | 1671 | TRIDENT: triple low-dose blood pressure pill (includes amlodipine) vs standard care for preventing recurrent stroke after intracerebral haemorrhage |
| [NCT07458880](https://clinicaltrials.gov/study/NCT07458880) | Not applicable | Recruiting | 140 | Triple antihypertensive medication for blood pressure control after intracerebral haemorrhage (TRICH score) |
| [NCT00805311](https://clinicaltrials.gov/study/NCT00805311) | Phase 4 | Terminated | 400 | Carotid endarterectomy vs optimal medical treatment in asymptomatic carotid stenosis; amlodipine not the focus |
| [NCT03783754](https://clinicaltrials.gov/study/NCT03783754) | Not applicable | Terminated | 4 | TRIDENT MRI sub-study; negligible data |
| [NCT03785067](https://clinicaltrials.gov/study/NCT03785067) | Phase 3 | Terminated | 1 | TRIDENT cognitive sub-study; no usable efficacy data |

---

## Literature Evidence

No publications were found for brain stem infarction. The most relevant publications for related predicted indications are listed below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [42019018](https://pubmed.ncbi.nlm.nih.gov/42019018/) | 2026 | RCT | N Engl J Med | Three low-dose antihypertensives in a single pill after intracerebral haemorrhage (TRIDENT results) |
| [34994269](https://pubmed.ncbi.nlm.nih.gov/34994269/) | 2022 | Trial design | Int J Stroke | Rationale and design of TRIDENT |
| [21538457](https://pubmed.ncbi.nlm.nih.gov/21538457/) | 2011 | Preclinical (animal) | J Neurosci Res | Amlodipine plus atorvastatin gave additive neuroprotection after transient middle cerebral artery occlusion in rats |
| [17070425](https://pubmed.ncbi.nlm.nih.gov/17070425/) | 2006 | Preclinical (animal) | Am J Hypertens | Amlodipine reduced stroke size in ApoE-deficient mice |
| [20971084](https://pubmed.ncbi.nlm.nih.gov/20971084/) | 2011 | Preclinical (animal) | Brain Res | Synergistic neuroprotection of amlodipine plus atorvastatin in metabolic syndrome rats |
| [11282121](https://pubmed.ncbi.nlm.nih.gov/11282121/) | 2001 | Preclinical (animal) | Eur J Pharmacol | Amlodipine attenuated pulmonary vascular remodelling in hypoxic rats |
| [10759437](https://pubmed.ncbi.nlm.nih.gov/10759437/) | 2000 | Crossover trial | Eur Respir J | Effect of amlodipine on pulmonary vascular responses to hypoxia in mountaineers |
| [24921631](https://pubmed.ncbi.nlm.nih.gov/24921631/) | 2014 | Preclinical (in vitro) | J Cardiovasc Pharmacol | Amlodipine protected cardiomyoblasts from hypoxia-induced apoptosis |
| [16467664](https://pubmed.ncbi.nlm.nih.gov/16467664/) | 2006 | Case report | J Hypertens | Management of severe renovascular hypertension in a child |

---

## New Zealand Market Information

All three licences are cancelled; there is currently no valid amlodipine licence in the data compiled for this report. Approved indication text is not available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2592509 | Vasorex | Tab 2.5 mg | Not available (cancelled) |
| 2592517 | Vasorex | Tab 5 mg | Not available (cancelled) |
| 2592525 | Vasorex | Tab 10 mg | Not available (cancelled) |

---

## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text):** liver failure and liver disease, kidney disease, pancreatitis, gallbladder disease, hypothyroidism and myxedema, angioedema (including hereditary and RAAS-blocker-induced), systemic lupus erythematosus, myopathy and muscle disease, familial periodic paralysis, potassium deficiency, diabetes mellitus and diabetic ketoacidosis, hyperuricaemia, neutropenia and agranulocytosis, hypotensive disorders, hyperparathyroidism and osteitis fibrosa. These are database associations and should be verified against the product label.
- **Drug interactions:** 100 interactions were identified, and the 20 reviewed are all of Moderate severity. They include corticosteroids (hydrocortisone, betamethasone, budesonide, dexamethasone), calcium salts (acetate, carbonate, chloride, citrate, gluconate, lactate and others), aspirin, aprepitant, bupropion, clarithromycin, clotrimazole, and SGLT2 inhibitors (canagliflozin, dapagliflozin).
- **Literature signals:** Case reports describe angioedema and overdose complications (hypotension, non-cardiogenic pulmonary edema). One case report links amlodipine to hypoxia during one-lung ventilation, likely through inhibition of hypoxic pulmonary vasoconstriction.

Package insert warnings and contraindications were not available, so please refer to the package insert for authoritative safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction, brain stem infarction, is supported by model score only (L5) and falls within an already-linked stroke indication area, so it adds little as repurposing. The strongest signal, intracerebral haemorrhage (L1, TRIDENT), tests amlodipine only as part of a triple-drug pill. In addition, no valid New Zealand licence currently exists.

**To proceed, the following is needed:**
- Review the full TRIDENT results (primary outcome and safety) to judge whether an amlodipine-specific benefit can be inferred
- Obtain Medsafe package insert warnings and contraindications to complete the safety screening
- Obtain detailed mechanism of action data
- Clarify the current New Zealand regulatory pathway, since all licences are cancelled
- Search for direct clinical evidence in brain stem infarction, or prioritise indications with stronger evidence (intracerebral haemorrhage, cerebral artery occlusion)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

