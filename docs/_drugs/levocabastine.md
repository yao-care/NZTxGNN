---
layout: default
title: Levocabastine
parent: Model Prediction Only (L5)
nav_order: 268
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Levocabastine: predicted new indication Allergic urticaria (evidence level L5). Approved use: Atopic conjunctivitis, papillary conjunctivitis, vernal conjunctivitis"
monitor_inn: "LEVOCABASTINE"
predicted_indications: ["allergic urticaria", "rosacea conjunctivitis"]
---

# Levocabastine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Levocabastine: From Allergic Conjunctivitis to Allergic Urticaria

## One-Sentence Summary

Levocabastine is a topical antihistamine, originally used to treat allergic conjunctivitis (atopic, papillary and vernal forms).
The TxGNN model predicts it may be effective for **allergic urticaria**,
but there are currently **0 registered clinical trials** and only **3 loosely related publications** (none showing efficacy in urticaria), so this remains a model-based hypothesis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atopic conjunctivitis, papillary conjunctivitis, vernal conjunctivitis |
| Predicted New Indication | Allergic urticaria |
| TxGNN Prediction Score | 99.18% |
| Evidence Level | L5 (the Evidence Pack's own scoring lists L4, but without clinical trials or mechanistic studies specific to urticaria, the criteria support only L5) |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, levocabastine is a selective histamine H1-receptor antagonist. Histamine H1 signalling on dermal blood vessels and sensory nerves drives the wheal and flare of allergic urticaria, so the mechanism is biologically plausible.

The original indications (allergic conjunctivitis) and urticaria are both histamine-mediated allergic conditions. However, levocabastine is marketed only as topical eye drops (and elsewhere as a nasal spray), so systemic exposure is low. Urticaria is a skin disease usually treated with oral H1 antihistamines, and efficacy of levocabastine in it has not been established.

The high TxGNN score reflects similarity to other H1 antagonists at the class level, not drug-specific evidence.

A second prediction, rosacea conjunctivitis (score 99.17%), has no trials or literature. Ocular rosacea is driven mainly by meibomian gland dysfunction, lid-margin inflammation, microbial and Demodex factors, and vascular dysregulation, so any benefit would likely be limited to symptomatic relief of itch or redness.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8938880](https://pubmed.ncbi.nlm.nih.gov/8938880/) | 1996 | Clinical study (randomised, double-blind, placebo-controlled crossover) | Rhinology | In 22 patients with allergic rhinitis, levocabastine nasal spray significantly reduced sneezing and rhinorrhoea after allergen challenge. Not a urticaria study. |
| [1685361](https://pubmed.ncbi.nlm.nih.gov/1685361/) | 1991 | Review | Clinical Pharmacokinetics | Reviews pharmacokinetic optimisation of second-generation H1 antihistamines in allergic rhinoconjunctivitis and chronic urticaria. Class-level information only. |
| [42585211](https://pubmed.ncbi.nlm.nih.gov/42585211/) | 2026 | Pharmacovigilance analysis | PLoS One | Descriptive analysis of adverse reactions to azelastine and levocabastine from the WHO VigiAccess database. Safety profile comparison, not efficacy. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2712571 | Livostin | Eye drops 0.5 mg per ml | Not available |

## Safety Considerations

- **Drug Interactions**: The only recorded interaction entry is a pharmacological target association with the NTS2 receptor (NTSR2). No affinity data were found to substantiate it, and no drug–drug interactions with a specified severity were recorded.

Please refer to the package insert for further safety information, including warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The high TxGNN score is driven by class-level similarity to other H1 antagonists. There are no clinical trials or urticaria-specific literature. The only marketed product is an eye drop with low systemic exposure, which makes efficacy in skin disease doubtful, and the rosacea conjunctivitis prediction has no supporting evidence at all.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Confirmed mechanism of action data from DrugBank
- Evidence of adequate systemic exposure or a suitable formulation for urticaria
- Any drug-specific clinical data in urticaria (or ocular rosacea) before further consideration
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

