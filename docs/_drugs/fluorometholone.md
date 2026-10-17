---
layout: default
title: Fluorometholone
parent: Model Prediction Only (L5)
nav_order: 206
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Fluorometholone: predicted new indication Postinfectious vasculitis (evidence level L5). Approved use: Uveitis, iritis, iridocyclitis…"
monitor_inn: "FLUOROMETHOLONE"
predicted_indications: ["postinfectious vasculitis", "post-bacterial disorder", "otitis externa", "infective urethral stricture", "post-infectious syndrome", "punctate epithelial keratoconjunctivitis", "infection-related hemolytic uremic syndrome", "Chagas cardiomyopathy"]
---

# Fluorometholone
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

# Fluorometholone: From Ocular Inflammation (Uveitis and Conjunctivitis) to Postinfectious Vasculitis

## One-Sentence Summary

Fluorometholone is a topical ocular corticosteroid, originally used for inflammatory eye conditions such as uveitis, iritis, iridocyclitis and allergic conjunctivitis.
The TxGNN model predicts it may be effective for **postinfectious vasculitis**, but there are **0 clinical trials** and **0 publications** supporting this prediction, so it rests on model output alone.
Among the other top-ranked predictions, only **punctate epithelial keratoconjunctivitis** has any related literature (2 indirect papers).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Uveitis, iritis, iridocyclitis, atopic/vernal/papillary conjunctivitis, corneal ulcer and related eye conditions (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Postinfectious vasculitis |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, fluorometholone is a synthetic glucocorticoid that acts on the glucocorticoid receptor (NR3C1). Its efficacy in ocular inflammation is established, so a general anti-inflammatory effect is mechanistically plausible in other inflammatory conditions.

For postinfectious vasculitis, however, the link is weak. It is a systemic condition, whereas fluorometholone is formulated only as 0.1% eye drops, which give minimal systemic exposure. No clinical or literature evidence supports its use here, and no suitable route of administration has been identified.

Other top-ranked predictions are mechanistically closer to the current uses:

- **Lens-induced iridocyclitis** and **glaucomatocyclitic crisis** are anterior uveitis-type conditions that overlap the original indications. No direct evidence exists, and corticosteroids can raise intraocular pressure, which matters especially in glaucoma-associated disease.
- **Punctate epithelial keratoconjunctivitis** is an ocular surface inflammatory condition and is biologically plausible. Steroid use in infectious keratoconjunctivitis needs caution because of infection risk.
- **Otitis externa** has a plausible mechanism, since topical corticosteroids are used in otic preparations (usually combined with anti-infectives). Fluorometholone itself has no evidence and is formulated as an ophthalmic product.
- Systemic or non-specific predictions (post-bacterial disorder, post-infectious syndrome, infection-related haemolytic uraemic syndrome, Chagas cardiomyopathy, infective urethral stricture) have no supporting evidence, and topical ocular dosing is poorly matched to them.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

For the top prediction (postinfectious vasculitis), currently no related literature is available.

Two papers were retrieved for the rank 6 prediction, punctate epithelial keratoconjunctivitis. They are related but not direct evidence for fluorometholone:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34011737](https://pubmed.ncbi.nlm.nih.gov/34011737/) | 2021 | Review | Indian Journal of Ophthalmology | Characterizes the sequelae of microsporidial keratoconjunctivitis and outlines its management |
| [35128186](https://pubmed.ncbi.nlm.nih.gov/35128186/) | 2021 | Cohort | Journal of Current Ophthalmology | Uses anterior segment OCT to describe the phases of epidemic keratoconjunctivitis and the effect of topical steroid on its course |

---

## New Zealand Market Information

Both authorizations are cancelled, and the records contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 264415 | FML | Eye drops 0.1% | Not recorded (cancelled) |
| 284866 | Flucon | Eye drops 0.1% | Not recorded (cancelled) |

---

## Safety Considerations

- **Contraindications (knowledge graph, not label text)**: the knowledge graph links fluorometholone to the following conditions as contraindications:
  - Ocular: herpes simplex virus keratitis, open-angle glaucoma, ocular hypertension, eye infectious disease, conjunctivitis, fungal infection of the eye, viral eye infection, ocular tuberculosis, corneal pigmentation
  - Metabolic and other: diabetes mellitus, diabetic ketoacidosis, porphyria (including inherited porphyria and porphyrin metabolism disease), G6PD deficiency, glycogen storage disease, cystinuria, galactosemia
- **Drug Interactions**: 11 potential interacting drugs were identified, all with unclassified severity. They are doxycycline, salbutamol, cyclosporine, acetaminophen, digoxin, brimonidine, cephalexin, alendronic acid, acyclovir, diazepam and fenofibrate.

Package insert warnings and contraindications could not be retrieved. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for postinfectious vasculitis is based on the model score alone (L5), with no trials or literature. A topical ocular steroid is poorly matched to a systemic disease, and both New Zealand licences are cancelled. The anterior uveitis-type predictions (lens-induced iridocyclitis, glaucomatocyclitic crisis) and punctate epithelial keratoconjunctivitis are better-grounded research questions, but they too lack direct evidence.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (currently a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- Direct clinical or literature evidence for any candidate indication
- An assessment of route and formulation compatibility, since only ophthalmic eye drops existed
- For ocular candidates, a plan for monitoring intraocular pressure and infection risk
- Clarification of why the licences were cancelled and whether any product could be re-supplied
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

