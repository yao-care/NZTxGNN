---
layout: default
title: Tetracycline
parent: Model Prediction Only (L5)
nav_order: 447
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-30
monitor_inn: "TETRACYCLINE"
predicted_indications: ["punctate epithelial keratoconjunctivitis", "chronic ethmoidal sinusitis", "chronic rhinosinusitis", "paranasal sinus neoplasm (disease)"]
description: "Tetracycline: predicted new indication Punctate epithelial keratoconjunctivitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Tetracycline
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Tetracycline: Punctate epithelial keratoconjunctivitis
{: .text-alpha }

## One-Sentence Summary

Tetracycline is a broad-spectrum antibiotic originally used for infections such as urinary tract infection, bronchitis, acne, conjunctivitis and pneumonia.
The TxGNN model predicts it may be effective for **Punctate Epithelial Keratoconjunctivitis**,
but **no clinical trials** and only **1 publication** (a case report) support this direction, so the evidence is weak.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Punctate epithelial keratoconjunctivitis |
| TxGNN Prediction Score | 0.9958 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, tetracycline is a member of the tetracycline antibiotic class, its efficacy in bacterial infections (including conjunctivitis) is established, and mechanistically it may be applicable to punctate epithelial keratoconjunctivitis.

Tetracycline is already linked to conjunctivitis, including chlamydial (trachoma/inclusion) conjunctivitis. Punctate epithelial keratitis can persist after such an infection resolves, so the link is plausible. Tetracyclines also have anti-inflammatory and matrix metalloproteinase (MMP)-inhibiting effects that may help ocular surface inflammation.

The one available publication describes persistent keratitis after chlamydial conjunctivitis. It does not show that tetracycline treats the keratitis. The high TxGNN score likely reflects closeness to existing conjunctivitis indications rather than new evidence. This is closer to an extension of an existing indication than true repurposing.

Other predicted indications from the same analysis (for context):

| Predicted Indication | TxGNN Score | Evidence Level | Literature | Recommendation |
|------|------|------|------|------|
| Chronic ethmoidal sinusitis | 0.9915 | L5 | 1 antibiotic susceptibility study (indirect) | Hold |
| Chronic rhinosinusitis | 0.9915 | L5 | 7 publications, including a Cochrane review (2016) and a doxycycline meta-analysis (2023); mostly doxycycline, not tetracycline | Research Question |
| Paranasal sinus neoplasm | 0.9910 | L5 | None | Hold |

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [1424659](https://pubmed.ncbi.nlm.nih.gov/1424659/) | 1992 | Case report/series | Cornea | Two cases of chlamydial follicular conjunctivitis treated with oral tetracycline or doxycycline; follicles resolved, but recurrent punctate epithelial keratitis followed. It does not show that tetracycline treats the keratitis. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2587106 (cancelled) | Accord | Tab 250 mg | Not available |

## Cytotoxicity

Tetracycline is an antibiotic, not an antineoplastic drug, so this section does not apply.

## Safety Considerations

- **Contraindication-related conditions (from the knowledge graph, not label text)**: liver failure and liver disease, kidney disease, kidney failure and uremia, glomerulonephritis, epilepsy, diabetes mellitus and diabetic ketoacidosis, atrial fibrillation, atrioventricular block, neutropenia and agranulocytosis, G6PD deficiency, blood coagulation disease, gout, peripheral neuropathy, *Clostridium difficile* colitis, respiratory failure, and others (30 conditions in total).
- **Drug Interactions**: 100 interactions were recorded, and 20 are listed here. Major interactions: isotretinoin, acitretin, aminolevulinic acid. Moderate interactions include ethinylestradiol, amoxicillin, ampicillin, aminophylline, atracurium, aluminum hydroxide, sodium citrate, bismuth subcitrate potassium, balsalazide, atovaquone, asparaginase and attapulgite. Minor interactions: acetazolamide, hydrochlorothiazide, amiloride.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only evidence for punctate epithelial keratoconjunctivitis is a single 1992 case report, with no registered trials. The high model score likely reflects proximity to existing conjunctivitis indications. Tetracycline is also cancelled-only in New Zealand, which limits practical use.

**To proceed, the following is needed:**
- Mechanism of action data
- The Medsafe package insert, to confirm warnings and contraindications (a blocking gap for safety screening)
- Controlled clinical evidence of tetracycline in punctate epithelial keratoconjunctivitis
- A comparison with currently marketed alternatives such as doxycycline, including the ocular route and formulation
- For chronic rhinosinusitis (the most-studied alternative prediction), a focused review of doxycycline-based evidence, since the current data are mostly class-level and of low certainty
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

