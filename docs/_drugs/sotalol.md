---
layout: default
title: Sotalol
parent: Model Prediction Only (L5)
nav_order: 428
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Sotalol: predicted new indication Sick sinus syndrome 2, autosomal dominant (evidence level L5). Approved use: Atrial fibrillation"
monitor_inn: "SOTALOL"
predicted_indications: ["sick sinus syndrome 2, autosomal dominant", "Wildervanck syndrome", "sarcoglycanopathy", "stroke disorder", "manic bipolar affective disorder", "macrocephaly, dysmorphic facies, and psychomotor retardation", "obsolete susceptibility to ischemic stroke"]
---

# Sotalol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

# Sotalol: From Atrial Fibrillation to Sick Sinus Syndrome 2 (Autosomal Dominant)

## One-Sentence Summary

Sotalol is a beta-blocker with class III antiarrhythmic activity, originally used to treat atrial fibrillation.
The TxGNN model predicts it may be effective for **sick sinus syndrome 2, autosomal dominant**, but **no clinical trials and no publications** support this prediction, and the mechanism points toward potential harm rather than benefit.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atrial fibrillation (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Sick sinus syndrome 2, autosomal dominant |
| TxGNN Prediction Score | 99.76% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no valid authorizations) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the compiled data. From general pharmacology, sotalol combines beta-adrenergic blockade with potassium channel blockade (class III activity). It is used for rhythm control in atrial fibrillation, where it slows sinus rate and atrioventricular conduction.

Sick sinus syndrome 2 is linked to loss of function of the HCN4 channel, which causes sinus bradycardia. Because sotalol slows the sinus rate and AV conduction, it would be expected to worsen sinus node dysfunction rather than treat it. The knowledge graph itself lists sick sinus syndrome and atrioventricular block as contraindications for sotalol.

The high TxGNN score (0.998) is therefore most likely a graph-based artifact of the shared arrhythmia context, not a sign of therapeutic potential.

Other predictions were also reviewed, and none has solid support:
- **Stroke disorder** (score 99.44%, L4): Sotalol's link is only indirect, via rhythm control in AF. Stroke prevention in AF relies on anticoagulation, not rhythm control.
- **Manic bipolar affective disorder** (99.43%, L4): The only literature concerns QT-prolongation interaction risks with antipsychotics, not efficacy.
- **Wildervanck syndrome, sarcoglycanopathy, macrocephaly/dysmorphic facies/psychomotor retardation, obsolete susceptibility to ischemic stroke** (L5): Prediction only, with no plausible mechanistic link or any supporting studies.

---

## Clinical Trial Evidence

Currently no related clinical trials are registered for the top predicted indication (sick sinus syndrome 2, autosomal dominant).

For reference, one trial was retrieved for the lower-ranked prediction of stroke disorder, and it does not test stroke outcomes:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00007605](https://clinicaltrials.gov/study/NCT00007605) | Phase 3 | Completed | 706 | CSP #399: amiodarone and sotalol for maintaining sinus rhythm in atrial fibrillation. It supports the original AF indication, and is only indirectly relevant to stroke. |

---

## Literature Evidence

Currently no related literature is available for the top predicted indication.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2127903 | Mylan | Tab 80 mg | Not available (cancelled) |
| 2127911 | Mylan | Tab 160 mg | Not available (cancelled) |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report. The following comes from the knowledge graph and interaction database.

- **Contraindications (knowledge graph, not label text)**: Long QT syndrome (including familial), torsades de pointes, asthma (intrinsic and allergic), bronchial disease, potassium deficiency, diabetes mellitus, diabetic ketoacidosis, hypotension, impaired renal function, atrioventricular block, sick sinus syndrome (including familial), and anaphylaxis.
- **Drug Interactions**: 100 interactions were identified. Major-level examples include **epinephrine**, **clarithromycin**, **picosulfuric acid**, and **polyethylene glycol (3350 with electrolytes)**. Many others are moderate, such as famotidine, bupropion, morphine, atropine, cimetidine, canagliflozin, and dapagliflozin.
- **Other concerns from the literature**: Sotalol carries proarrhythmic risk (QT prolongation, torsades de pointes) and requires renal dose adjustment. Beta-blockade can augment the torsadogenic action of QT-prolonging drugs such as antipsychotics.

Notably, the predicted indication (sick sinus syndrome) is itself on sotalol's contraindication list.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L5). No trials or publications support it, and sotalol's pharmacology would be expected to worsen sinus node dysfunction. Sick sinus syndrome is also a listed contraindication. None of the other predicted indications has direct supporting evidence. Sotalol currently has no valid New Zealand authorization.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from the Medsafe documents)
- Mechanism of action data from DrugBank
- Preclinical or clinical evidence that sotalol benefits, or at least does not harm, patients with HCN4-related sick sinus syndrome
- Clarification of regulatory pathway, since all New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

