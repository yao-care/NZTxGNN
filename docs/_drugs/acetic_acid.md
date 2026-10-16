---
layout: default
title: Acetic Acid
parent: Model Prediction Only (L5)
nav_order: 16
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-15
description: "Acetic Acid: predicted new indication Post-bacterial disorder (evidence level L5). Approved use: Otitis externa"
---

# Acetic Acid
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

# Acetic Acid: From Otitis Externa to Post-Bacterial Disorder

## One-Sentence Summary

Acetic acid (marketed in New Zealand as Aci-Jel) was originally used for otitis externa and other external ear conditions.
The TxGNN model predicts it may be effective for **post-bacterial disorder**, but **no clinical trials and no publications** currently support this prediction.
It is a computational prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Otitis externa; external ear disease (from the knowledge graph; no approved indication text on the New Zealand licence) |
| Predicted New Indication | Post-bacterial disorder |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, acetic acid is a topical antibacterial and antiseptic agent. Its activity in ear infections is generally attributed to acidification and disruption of microbial cell function. Mechanistically, this could apply to some infection-related conditions.

However, the link to post-bacterial disorders is weak. Such conditions are generally immune-mediated or post-inflammatory sequelae rather than active local infections, so a topical acidifying antiseptic has no clear route to treat them. The very high score most likely reflects graph proximity to infection-related terms rather than a real therapeutic signal.

For context, the other top predictions (postinfectious vasculitis, post-infectious syndrome, Chagas cardiomyopathy, infective urethral stricture, infection-related haemolytic uraemic syndrome, drug-induced osteoporosis) also have no clinical evidence and no plausible mechanism for a topical agent. The only prediction with some supporting literature is **tinea corporis** (score 99.25%, evidence level L4, "Research Question" status). This is a secondary signal worth noting:

- Historical reports from 1946–1958 describe dilute or increasing concentrations of acetic acid used for tinea capitis and foot epidermophytosis.
- In vitro and chemical-peeling reports give indirect support for antifungal use against dermatophytes.
- These sources are old, uncontrolled or laboratory-based, and concern related dermatophytoses rather than tinea corporis itself.

## Clinical Trial Evidence

Currently no related clinical trials registered for post-bacterial disorder. None are registered for the other top-ranked predictions, including tinea corporis.

## Literature Evidence

No literature is available for post-bacterial disorder. The literature below relates to other predicted indications and is shown for context only.

**Post-infectious syndrome (rank 3):**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [20848144](https://pubmed.ncbi.nlm.nih.gov/20848144/) | 2011 | Review | Journal of Gastroenterology | Systematic review of animal models of post-infectious irritable bowel syndrome. It does not evaluate acetic acid as a treatment, so it is not supporting evidence |

**Tinea corporis (rank 9), most relevant items:**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31049344](https://pubmed.ncbi.nlm.nih.gov/31049344/) | 2019 | Clinical report/Review | Skin Appendage Disorders | Chemical peeling as an alternative to oral antifungals for onychomycosis in patients where oral drugs are unsuitable |
| [26857689](https://pubmed.ncbi.nlm.nih.gov/26857689/) | 2016 | In vitro study | Mycoses | Topical antifungal products compared in an in vitro onychomycosis model against *Trichophyton mentagrophytes* |
| [37256034](https://pubmed.ncbi.nlm.nih.gov/37256034/) | 2023 | Case series (safety) | Archives of Plastic Surgery | Burns caused by Korean folk remedies; a safety signal for uncontrolled topical acid-based remedies |
| [20256868](https://pubmed.ncbi.nlm.nih.gov/20256868/) | 1947 | Historical clinical series | Urologic and Cutaneous Review | Tinea capitis treated with special iodine and dilute acetic acid in a larger patient series |
| [20996178](https://pubmed.ncbi.nlm.nih.gov/20996178/) | 1946 | Historical clinical report | JAMA | Tinea capitis treated with special iodine and dilute acetic acid |
| [20983383](https://pubmed.ncbi.nlm.nih.gov/20983383/) | 1946 | Historical clinical report | Archives of Dermatology and Syphilology | Treatment of tinea capitis with special iodine and dilute acetic acid |
| [13616002](https://pubmed.ncbi.nlm.nih.gov/13616002/) | 1958 | Historical clinical report | Vestnik Dermatologii i Venerologii | Foot epidermophytosis treated with increasing concentrations of acetic acid |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 536962 | Aci-Jel | Jelly (glacial acetic acid 0.94%, hydroxyquinoline sulphate 0.025%, glycerol 5%, ricinoleic acid 0.75%) with applicator | Not stated in the data compiled for this report |

## Safety Considerations

- **Disease-level contraindication signals (knowledge graph, not label text):** inherited porphyria, porphyrin metabolism disease, porphyria, herpes simplex infection, geniculate herpes zoster.
- **Safety signal for topical acid use:** a 2023 case series describes burns from folk remedies, which indicates a risk of tissue injury with uncontrolled use of acid-based topical products.
- **Drug interactions:** no interactions were found in the queried data.

Please refer to the package insert for warnings and contraindications, as label-based safety text was not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for post-bacterial disorder rests on a model score alone (evidence level L5), with no trials, no literature and no plausible mechanism for a topical antiseptic. Acetic acid is marketed in New Zealand as a single-ingredient licence, but this does not support the new indication. Of the predicted indications, only tinea corporis has indirect, low-quality support, and it warrants a research question rather than a recommendation.

**To proceed, the following is needed:**
- The Medsafe package insert (warnings, contraindications and approved indications), which is a blocking gap for safety screening
- Mechanism of action data from DrugBank
- A clear mechanistic rationale for post-bacterial disorder, or a shift in focus to tinea corporis
- For tinea corporis, modern controlled studies of dermatophyte efficacy, with tolerability and skin-injury safety assessment
- Route and formulation compatibility assessment, since the current product is a gel with an applicator and not designed for skin or systemic use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

