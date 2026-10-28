---
layout: default
title: Econazole
parent: Model Prediction Only (L5)
nav_order: 171
evidence_level: L5
indication_count: 2
monitor_inn: "Econazole"
predicted_indications: ["ectothrix infectious disease", "endothrix infectious disease"]
last_modified_at: 2026-10-23
description: "Econazole: predicted new indication Ectothrix infectious disease (evidence level L5). Approved use: Tinea pedis, tinea corporis, tinea infection, cutaneous candidiasis…"
---

# Econazole
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

## Econazole: From Dermatophyte Skin Infections to Ectothrix Infectious Disease
{: .text-alpha }

## One-Sentence Summary

Econazole is an imidazole antifungal. It is used for skin fungal infections such as tinea pedis, tinea corporis and cutaneous candidiasis.
The TxGNN model predicts it may be useful for **ectothrix infectious disease** (a dermatophyte infection of the hair), but the data compiled for this report contain **0 clinical trials** and **0 publications** directly supporting this prediction, so it rests on model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Tinea pedis, tinea corporis, tinea infection, cutaneous candidiasis, pityriasis versicolor, dermatophytosis of groin and perianal area (relations recorded in the knowledge graph; the local licence records contain no indication text) |
| Predicted New Indication | Ectothrix infectious disease |
| TxGNN Prediction Score | 0.9997 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Number of approvals | 2 (both single-ingredient licences) |
| Recommended Decision | Hold |

A second prediction, **endothrix infectious disease** (score 0.9997), is also in the data. It has the same evidence status (L5, no trials or literature) and the same Hold recommendation.

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for econazole are not available in the data compiled for this report. From general pharmacology, econazole is an imidazole antifungal that inhibits fungal lanosterol 14-alpha-demethylase (CYP51). This blocks ergosterol synthesis and damages the fungal cell membrane. This explanation comes from general knowledge, not from the supplied data.

In ectothrix infection, fungal spores form a sheath on the outside of the hair shaft. The usual causes are *Microsporum* and *Trichophyton* species. These dermatophytes belong to the same group as the organisms in econazole's recorded skin indications (tinea pedis, tinea corporis). The prediction is therefore best seen as an extension of an existing dermatophyte use, not a novel repurposing.

There are practical limits. Topical econazole penetrates the hair follicle poorly, and hair and scalp infections generally need oral antifungal therapy. The same limits apply to endothrix infection, where the fungus grows inside the hair shaft. The high TxGNN score is a computational prediction and not clinical evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2165953 | Pevaryl | Cream 1% | Not stated in the licence records |
| 383872 | Pevaryl | Foaming solution 1%, 10 ml sachets | Not stated in the licence records |

Both licences are valid single-ingredient products. Neither carries an indication in the available records, so the uses listed above come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. They are not confirmed as approved indications.

## Safety Considerations

The pharmacology data list three recorded target interactions for econazole. They are molecular targets, not drug-drug interactions:
- **TRPM2** (human)
- **TRPV5** (human)
- **CYP8B1** (human): econazole binds to and inhibits this enzyme.

No contraindication relations are recorded in the knowledge graph. For warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has a very high model score and a plausible link to econazole's known dermatophyte activity. However, no clinical trial or publication directly studies this disease (hence L5). Topical econazole is also unlikely to work alone on hair-shaft infections, and the package insert safety review is still outstanding.

**To proceed, the following is needed:**
- The Medsafe package insert, to confirm approved indications, warnings and contraindications
- Mechanism-of-action data from DrugBank
- Evidence on hair-follicle penetration and efficacy of topical econazole in ectothrix and endothrix infection
- Comparison with standard oral antifungal therapy for scalp and hair dermatophytosis, to decide whether a topical-only use is worthwhile, or whether it would only serve as an adjunct
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

