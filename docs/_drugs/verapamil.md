---
layout: default
title: Verapamil
parent: Model Prediction Only (L5)
nav_order: 478
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-28
monitor_inn: "Verapamil"
predicted_indications: ["obsolete bundle branch block", "malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome", "periodic paralysis with transient compartment-like syndrome"]
description: "Verapamil: predicted new indication Obsolete bundle branch block (evidence level L5). Approved use: Not recorded in the local data"
---

# Verapamil
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

## Verapamil: Obsolete bundle branch block
{: .text-alpha }

## One-Sentence Summary

Verapamil is an L-type calcium channel blocker, originally used for hypertension, atrial fibrillation, angina and ventricular tachycardia.
The TxGNN model predicts it may be effective for **obsolete bundle branch block** (a deprecated disease term), but this rests on the model score alone, with **0 clinical trials** and **0 publications** supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Obsolete bundle branch block |
| TxGNN Prediction Score | 0.9962 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 6 of 6) |
| Number of Authorizations | 6 (valid: 6) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, verapamil is a calcium channel blocker, and its efficacy in hypertension and supraventricular and ventricular arrhythmias is established. Data compiled for this report show it acts on L-type calcium channels (Cav1.1–1.4) and also interacts with other ion channels and CYP3A4.

The link to the predicted indication is weak. Verapamil slows atrioventricular nodal conduction. Bundle branch block is a conduction defect in the His-Purkinje system, so slowing conduction could worsen it rather than treat it. The very high score (0.9962) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The disease term is marked obsolete, so there is no clear clinical target.

Other top-ranked predictions are closer to the original indication but also lack clinical data:
- **Malignant hypertensive renal disease** (0.9927) and **malignant renovascular hypertension** (0.9927): plausible through vasodilation, but non-dihydropyridine calcium channel blockers are not first-line for malignant hypertension. The two publications retrieved for the latter are tangential (aldosterone/renin ratio confounders; a paediatric renal myofibromatosis case).
- **Pulmonary hypertension** (two subtypes, 0.9926): calcium channel blockers may help vasoreactive subsets, but verapamil's negative inotropic effect carries a risk of right heart failure. In lung disease or hypoxia, blunting hypoxic vasoconstriction may impair gas exchange.
- **Braddock syndrome** (0.9915) and **periodic paralysis with transient compartment-like syndrome** (0.9908): no clear mechanistic link or only a speculative one.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top-ranked prediction. Literature retrieved for lower-ranked predictions is mostly off-target (arrhythmia reviews, migraine, retinal neuroprotection, in vitro and animal hypoxia studies) and does not test verapamil in the predicted conditions.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2579391 | Isoptin Retard | Tab long-acting 120 mg | Not available |
| 2571897 | Isoptin SR | Tab long-acting 120 mg | Not available |
| 2535327 | Isoptin | Tab 40 mg | Not available |
| 2535351 | Isoptin | Inj 2.5 mg per ml, 2 ml ampoule | Not available |
| 2535343 | Isoptin SR | Tab long-acting 240 mg | Not available |

All six licences are valid.

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (graph associations, not label text): heart failure, atrioventricular block, hypotensive disorder, aortic stenosis, hypertrophic cardiomyopathy, ventricular tachycardia (certain forms), myasthenia gravis and other neuromuscular junction disease, kidney disease, liver disease, gallbladder disease, angioedema (several forms), neutropenia, agranulocytosis, renal artery obstruction, and connective tissue, bone and cartilage disease.
- **Drug Interactions**: The interaction query returned 13 entries, which are pharmacological target interactions rather than graded drug-drug interactions. They include L-type calcium channels (Cav1.1–1.4), two-pore channels (TPC1, TPC2), several potassium channels (Kir3.2, Kv1.7, Kv1.8, Kv3.2), NALCN, the plasma membrane monoamine transporter, and CYP3A4. CYP3A4 is the most clinically relevant for co-medication. The records also note that verapamil improved the in vitro activity of another agent against *Mycobacterium tuberculosis* and *M. abscessus*.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score. There are no trials or relevant literature, the disease term is obsolete, and verapamil's conduction-slowing action could be harmful in bundle branch block.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data
- A current, non-obsolete disease definition and clinical rationale for the predicted indication
- Consideration of better-supported candidates among the other predictions, such as hypertensive end-organ disease, with disease-specific clinical data
- Clarification of the regulatory pathway
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

