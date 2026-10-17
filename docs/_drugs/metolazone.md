---
layout: default
title: Metolazone
parent: Model Prediction Only (L5)
nav_order: 295
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Metolazone: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Hypertension / hypertensive disorder"
monitor_inn: "METOLAZONE"
predicted_indications: ["malignant hypertensive renal disease"]
---

# Metolazone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

# Metolazone: From Hypertension to Malignant Hypertensive Renal Disease

## One-Sentence Summary

Metolazone is a thiazide-like diuretic originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**, but **no clinical trials and no directly relevant publications** currently support this prediction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension / hypertensive disorder (from the knowledge graph; no approved indication text is available in the New Zealand licence record) |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 99.84% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorisation) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known information, metolazone is a thiazide-like diuretic. It is understood to lower blood pressure by inhibiting sodium-chloride reabsorption in the kidney, which reduces fluid volume. Its efficacy in hypertension is established, and mechanistically it could be applicable to hypertension-related kidney disease.

The predicted indication is closely related to the original one, which probably explains the high score. Malignant hypertensive renal disease is, however, a hypertensive emergency. It is usually managed with intravenous antihypertensives, and thiazide-type diuretics are not a standard therapy. The score is a graph-based prediction and most likely reflects proximity to the original hypertension indication rather than independent evidence.

Other high-scoring predictions follow the same pattern:
- **Malignant renovascular hypertension** (99.84%): the rationale is weak. This condition is driven mainly by renin-angiotensin activation, and diuretic-induced volume depletion may worsen it.
- **Pulmonary hypertension** (two subtypes, 99.83%): metolazone is sometimes added to loop diuretics for refractory fluid overload. This relieves symptoms and does not treat the pulmonary vascular disease.
- **Braddock syndrome** (99.78%): no mechanistic rationale can be established from the available data.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature directly supports the top predicted indication. The only publication retrieved across the top predictions is a preclinical study listed under pulmonary hypertension owing to lung disease and/or hypoxia. It is unrelated to pulmonary hypertension.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2916665](https://pubmed.ncbi.nlm.nih.gov/2916665/) | 1989 | Preclinical/Mechanistic | The American Journal of Physiology | In rats, thiazide receptors (measured by [3H]metolazone binding) in the renal cortex fell by about 90% after 10 minutes of acute renal ischemia, and the change was reversible. It offers no clinical support for this indication. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2438232 (cancelled) | Zaroxolyn | Tab 5 mg | Not recorded |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (these come from graph relationships, not from label text): multiple system atrophy, postural orthostatic tachycardia syndrome, orthostatic hypotension, liver failure, hepatic coma, hepatic encephalopathy, pancreatitis, systemic lupus erythematosus (including paediatric), potassium deficiency, diabetes mellitus, diabetic ketoacidosis, anuria, gout, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disorders, and HELLP syndrome.
- **Drug interactions**: 100 interactions were found. Most of those listed are Moderate; a few are Minor.
  - Moderate: acarbose, alogliptin, albiglutide (glucose-lowering agents); rabeprazole and omeprazole; hydrocortisone, dexamethasone, betamethasone, triamcinolone and beclomethasone (corticosteroids); amphotericin B (including the lipid complex); cholecalciferol; calcium phosphate; bupropion; morphine; bisacodyl.
  - Minor: doxycycline, hyoscyamine, atropine.

Please refer to the package insert for warnings and further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. There are no clinical trials and no supporting literature, so the evidence level is L5. Thiazide-type diuretics are not standard therapy for malignant hypertensive renal disease, which is a hypertensive emergency. The only New Zealand authorisation has been cancelled, and the knowledge graph flags several contraindications, including renal-related ones such as anuria.

**To proceed, the following is needed:**
- Clinical or mechanistic evidence linking metolazone to malignant hypertensive renal disease
- The mechanism of action data (DrugBank)
- The Medsafe package insert, to confirm warnings and contraindications
- A review of the knowledge-graph contraindications against the target patient population
- A clinical rationale for using a diuretic in a hypertensive emergency, and confirmation of whether any metolazone product can be supplied in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

