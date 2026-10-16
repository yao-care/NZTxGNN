---
layout: default
title: Digoxin
parent: Model Prediction Only (L5)
nav_order: 150
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Digoxin: predicted new indication Prinzmetal angina (evidence level L5). Approved use: Atrial fibrillation"
---

# Digoxin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

# Digoxin: From Atrial Fibrillation to Prinzmetal Angina

## One-Sentence Summary

Digoxin is a cardiac glycoside, originally used for atrial fibrillation and digitalis poisoning (as recorded in the knowledge graph).
The TxGNN model predicts it may be effective for **Prinzmetal angina**, but there are **0 clinical trials** and only **1 loosely related publication**, so the prediction is not supported by real-world evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atrial fibrillation; digitalis poisoning (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Prinzmetal angina |
| TxGNN Prediction Score | 99.81% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Digoxin is generally known as a Na+/K+-ATPase inhibitor with positive inotropic and vagomimetic effects, which is why it is used for rate control in atrial fibrillation.

The assessment found no credible mechanistic link between this action and Prinzmetal (vasospastic) angina. Digoxin has no known role in preventing coronary vasospasm, and cardiac glycosides may even raise vascular tone, which could theoretically aggravate vasospasm. The high score (0.998) is a graph-based prediction only.

The same applies to the other top-ranked predictions (duodenal obstruction, duodenal ulcer, duodenogastric reflux, an obsolete ischaemic stroke susceptibility term, and hypoalphalipoproteinaemia). None has clinical trials, and none has a plausible mechanistic rationale. Some are also unfavourable: digoxin commonly causes nausea, vomiting and anorexia, which would be undesirable in gastrointestinal conditions.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10736610](https://pubmed.ncbi.nlm.nih.gov/10736610/) | 1999 | Review | Acta Physiol Pharmacol Bulg | Chronopharmacology and its impact on antihypertensive treatment. It does not address digoxin in vasospastic angina, so it provides no support for this prediction. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2527197 | Lanoxin S29 | Oral liquid 50 mcg/ml | Not available (cancelled) |
| 2378078 | Lanoxin | Tablet 250 mcg | Not available (cancelled) |
| 2377012 | Lanoxin PG | Tablet 62.5 mcg | Not available (cancelled) |
| 426490 | Lanoxin | Oral liquid 50 mcg/ml | Not available (cancelled) |

---

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (graph-derived associations, not package insert text):
  - Cardiac: myocardial infarction, coronary thrombosis, cor pulmonale, Wolff-Parkinson-White syndrome, sick sinus syndrome (including familial), ventricular fibrillation, hypertrophic cardiomyopathy (including familial), atrioventricular block, myocarditis, constrictive pericarditis, restrictive cardiomyopathy
  - Metabolic and electrolyte: potassium deficiency, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease
  - Thyroid: myxedema, hypothyroidism, congenital hypothyroidism
  - Other: kidney disease, pyoureter, hemoglobinopathy
- **Drug Interactions**: The interaction screen returned 100 interactions. Of the 20 listed, most are Moderate and a few are Minor. Moderate examples include acarbose, rabeprazole, doxycycline, tetracycline, epinephrine, corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone), cholecalciferol, calcium phosphate, metformin, amphotericin B (including the lipid complex), bupropion and acetylsalicylic acid. Minor examples include albiglutide, hyoscyamine, atropine and glycopyrronium.

Digoxin has a narrow therapeutic index. Please refer to the package insert for full warnings and precautions.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a graph-based score. There are no clinical trials, the single linked publication is unrelated, and there is no credible mechanistic link between digoxin and vasospastic angina. All four New Zealand licences are cancelled, and several knowledge-graph contraindications involve cardiac conditions.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank) and a mechanistic analysis of digoxin in coronary vasospasm
- Package insert warnings and contraindications from Medsafe, which are required before safety screening can start
- Relevant preclinical or clinical studies of digoxin in Prinzmetal angina
- Confirmation of whether any digoxin product is currently marketed in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

