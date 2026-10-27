---
layout: default
title: Cyclopentolate
parent: Model Prediction Only (L5)
nav_order: 123
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
description: "Cyclopentolate: predicted new indication Cauda equina syndrome (evidence level L5). Approved use: Cycloplegia"
monitor_inn: "Cyclopentolate"
predicted_indications: ["cauda equina syndrome", "obsolete neurogenic bladder (disease)", "irritable bowel syndrome"]
---

# Cyclopentolate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Cyclopentolate: From Cycloplegia to Cauda Equina Syndrome
{: .text-alpha }

## One-Sentence Summary

Cyclopentolate is a topical antimuscarinic eye drop, originally used to paralyse the eye's focusing muscle (cycloplegia).
The TxGNN model predicts it may be effective for **cauda equina syndrome**, but this is a computational prediction only, with **0 clinical trials** and **0 publications** currently supporting it.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Cycloplegia (from the knowledge graph; the New Zealand licence record has no indication text) |
| Predicted New Indication | Cauda equina syndrome |
| TxGNN Prediction Score | 99.54% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 1 (0 currently valid) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on general pharmacology, cyclopentolate is a topical antimuscarinic (anticholinergic) agent. Its efficacy in cycloplegia is established, and mechanistically it may have a class-level link to bladder symptoms.

Cauda equina syndrome can cause bladder dysfunction (detrusor overactivity or retention), and antimuscarinics are used for some neurogenic bladder symptoms. However, the main treatment for cauda equina syndrome is urgent surgical decompression of the compressed nerve roots. An ophthalmic cycloplegic would not address that underlying problem.

The high score (0.995) is a model output only. No trials or literature were found, so the prediction should be treated as a hypothesis.

Other predictions from the same model, also at L5 and "Hold":
- **Obsolete neurogenic bladder (score 99.40%)**: Antimuscarinics block M3 receptors in bladder muscle, so a class-level rationale exists. Approved oral agents (oxybutynin, tolterodine, solifenacin) already cover this use, and the disease term is obsolete in the ontology.
- **Irritable bowel syndrome (score 99.27%)**: Antispasmodic antimuscarinics are used for IBS cramping. However, cyclopentolate's ophthalmic formulation and systemic anticholinergic effects (CNS effects, rapid heart rate, dry mouth) argue against a favourable risk-benefit profile.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 203351 (cancelled) | Cyclogyl | Eye drops 1% | Not recorded |

## Safety Considerations

- **Disease contraindications from the knowledge graph** (not label text): the following conditions are flagged as contraindicated or cautioned for this drug:
  - Eye: angle-closure (closed-angle) glaucoma
  - Cardiovascular: hypertension and hypertensive disorder, heart disease, cardiovascular disease, coronary artery disease, sudden cardiac arrest, heart conduction disease, arteriosclerosis disorder, atherosclerosis
  - Metabolic and endocrine: diabetes mellitus, diabetic ketoacidosis, hyperthyroidism, Graves disease, thyrotoxicosis
  - Urological: benign prostatic hyperplasia
  - Neurological and other: brain injury, brain disease, Down syndrome
- **Drug Interactions**: No interaction records were found.

These flags are a notable concern for any systemic repurposing, because the conditions listed overlap with populations that commonly have bladder or bowel symptoms. Please refer to the package insert for the official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All three predicted indications rest on model scores and class-level pharmacology alone, with no trials or publications. The only New Zealand licence is cancelled, the drug is formulated as an eye drop, and established alternatives already exist for the predicted uses. The main treatment for cauda equina syndrome is surgical, not pharmacological.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank)
- Package insert warnings and contraindications (from Medsafe) to allow safety screening
- Evidence of any clinical or preclinical study in the predicted indication
- Assessment of route and formulation feasibility (systemic or intravesical use versus the current ophthalmic form)
- Review of the obsolete disease term (neurogenic bladder) and mapping to current terms
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

