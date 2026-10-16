---
layout: default
title: Clobetasol Propionate
parent: Model Prediction Only (L5)
nav_order: 107
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Clobetasol Propionate: predicted new indication Vulvar inverted follicular keratosis (evidence level L5). Approved use: Atopic eczema, atopic dermatitis, contact dermatitis…"
monitor_inn: "CLOBETASOL PROPIONATE"
predicted_indications: ["vulvar inverted follicular keratosis", "acrodermatitis chronica atrophicans", "exanthem (disease)", "acne keloid", "neonatal dermatomyositis", "secondary interstitial lung disease specific to childhood associated with a connective tissue disease", "amyopathic dermatomyositis"]
---

# Clobetasol Propionate
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

# Clobetasol Propionate: From Atopic Eczema and Dermatitis to Vulvar Inverted Follicular Keratosis

## One-Sentence Summary

Clobetasol propionate is a super-potent topical corticosteroid, originally used for inflammatory skin conditions such as atopic eczema, contact dermatitis, and seborrheic dermatitis.
The TxGNN model's top-ranked prediction is **vulvar inverted follicular keratosis**, but this prediction has **no clinical trials and no publications** behind it (evidence level L5, model prediction only).
The better-supported directions among the top predictions are **exanthem** (7 clinical trials, mostly in oral lichen planus) and **acne keloid** (1 open-label study).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atopic eczema, atopic dermatitis, contact dermatitis, occupational dermatitis, seborrheic dermatitis, seborrheic keratosis, scalp dermatosis (the approved indication text for the New Zealand licences is not available, so these come from the knowledge graph) |
| Predicted New Indication | Vulvar inverted follicular keratosis (rank 1) |
| TxGNN Prediction Score | 99.46% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

For comparison, the other top predictions are:

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Recommendation |
|------|------|------|------|------|
| 1 | Vulvar inverted follicular keratosis | 99.46% | L5 | Hold |
| 2 | Acrodermatitis chronica atrophicans | 99.30% | L5 | Hold |
| 3 | Exanthem (disease) | 99.26% | L2 | Research question |
| 4 | Acne keloid | 99.21% | L3 | Research question |
| 5 | Neonatal dermatomyositis | 99.16% | L5 | Hold |
| 6 | Secondary childhood interstitial lung disease associated with a connective tissue disease | 99.07% | L5 | Hold |
| 7 | Amyopathic dermatomyositis | 99.05% | L5 | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, clobetasol propionate is a synthetic glucocorticoid that acts on the glucocorticoid receptor (NR3C1). Its efficacy in inflammatory skin disease is established, and mechanistically it may be applicable to other inflammatory or immune-mediated skin conditions.

**Rank 1 (vulvar inverted follicular keratosis).** This is a benign epithelial lesion related to seborrheic keratosis, which is already among clobetasol's original indications in the knowledge graph. That graph proximity is probably what drives the high score. However, topical corticosteroids are not a recognised treatment for a keratotic neoplasm, so the mechanistic link is weak.

**Exanthem.** This is a broad ontology label. The linked trials mostly concern oral lichen planus and related mucocutaneous conditions, where the anti-inflammatory and immunosuppressive effects of a potent topical corticosteroid are plausible. The evidence is indirect for exanthem itself.

**Acne keloid.** Acne keloidalis nuchae is a chronic inflammatory follicular disorder with fibrotic scarring. Potent topical steroids may reduce perifollicular inflammation and limit fibroblast activity. A small open-label study supports a modest clinical signal.

**Other predictions.** Acrodermatitis chronica atrophicans is an infectious (Borrelia) condition treated with antibiotics. Steroids could be counterproductive. Topical clobetasol has no plausible route to affect interstitial lung disease. Dermatomyositis predictions are plausible for skin lesions in adults, but use in neonates raises serious systemic absorption concerns.

---

## Clinical Trial Evidence

No clinical trials are registered for the top-ranked prediction (vulvar inverted follicular keratosis). The table below shows trials linked to the best-supported prediction, **exanthem**. Note that most of these trials address oral lichen planus or lichen disease, not exanthem.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02573883](https://clinicaltrials.gov/study/NCT02573883) | Phase 3 | Completed | 52 | Clobetasol 0.05% ointment vs fractionated CO2 laser in vulvar lichen sclerosus; the most direct and highest-phase evidence in this set (results not provided in the data compiled) |
| [NCT04364555](https://clinicaltrials.gov/study/NCT04364555) | Phase 2/3 | Active, not recruiting | 90 | Multicenter, placebo-controlled trial of topical clobetasol rinse in symptomatic oral lichen planus |
| [NCT03592342](https://clinicaltrials.gov/study/NCT03592342) | Phase 2 | Completed | 140 | Double-blind, placebo-controlled trial of Rivelin®-CLO patches (0, 1, 5, 20 μg clobetasol) in oral lichen planus, applied twice daily for 4 weeks |
| [NCT03026478](https://clinicaltrials.gov/study/NCT03026478) | Phase 2 | Unknown | 30 | Clobetasol vs betamethasone in Orabase combined with clotrimazole in oral lichen planus |
| [NCT07278999](https://clinicaltrials.gov/study/NCT07278999) | N/A | Recruiting | 60 | Photobiomodulation vs photodynamic therapy vs topical clobetasol (standard of care) in erosive oral lichen planus |
| [NCT07536737](https://clinicaltrials.gov/study/NCT07536737) | N/A | Completed | 65 | Photodynamic therapy vs topical corticosteroids, looking at taste perception and salivary oxidative stress markers in oral lichen planus |
| [NCT03386643](https://clinicaltrials.gov/study/NCT03386643) | Phase 2 | Completed | 22 | Probiotic B. animalis HN019 in oral lichen planus; clobetasol is likely background or comparator therapy |

---

## Literature Evidence

No publications were found for the top-ranked prediction. The only publication in the data compiled is for **acne keloid** (rank 4):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16047868](https://pubmed.ncbi.nlm.nih.gov/16047868/) | 2005 | Open-label clinical study | Cutis | Clobetasol 0.05% and betamethasone valerate 0.12% foams were studied in 20 African American patients with mild to moderate acne keloidalis, using a pulsed-dose regimen for 8 to 12 weeks. The study was uncontrolled and non-randomized |

---

## New Zealand Market Information

All three licences are cancelled, and no currently valid licence exists. The approved indication text was not available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 229059 | Dermol | Cream 0.05% | Not available (cancelled) |
| 229040 | Dermol | Ointment 0.05% | Not available (cancelled) |
| 772755 | Dermol | Scalp application 0.05% | Not available (cancelled) |

---

## Safety Considerations

- **Contraindication signals from the knowledge graph** (these come from drug-disease relationships in the knowledge graph, not from package insert text): hyperglycaemia, diabetes mellitus, diabetic ketoacidosis, adrenocortical insufficiency, Addison disease, folliculitis, seborrheic dermatitis, glaucoma, skin atrophy, peripheral vascular disease, intermittent claudication, and peripheral arterial disease.
- **Drug interactions:** No conventional drug-drug interactions were identified. The only record is pharmacological: clobetasol binds the glucocorticoid receptor (NR3C1).
- **Population-specific concern:** As a super-potent topical steroid, clobetasol carries a high risk of systemic absorption and HPA-axis suppression in neonates and young children. This is relevant to the neonatal dermatomyositis prediction.

Please refer to the package insert for complete warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The top-ranked prediction (vulvar inverted follicular keratosis) rests on model output alone, with no trials or literature and a weak mechanistic link. Clobetasol is also not currently marketed in New Zealand (all licences cancelled).
- Exanthem and acne keloid are better supported and could be pursued as research questions. For exanthem the trials address lichen planus rather than exanthem, and for acne keloid the only evidence is a small uncontrolled study.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, so that safety screening can be completed
- Mechanism of action data from DrugBank
- A decision on which predicted indication to pursue, for example by clarifying the exanthem label against the lichen planus trial evidence, or by a targeted literature search for amyopathic dermatomyositis
- Confirmation of the design and results of trials whose relevance is unclear (for example NCT03592342)
- Route and formulation compatibility assessment, and a review of whether a re-registration pathway is needed given the cancelled New Zealand licences
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

