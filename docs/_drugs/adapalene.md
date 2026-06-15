---
layout: default
title: Adapalene
parent: 僅模型預測 (L5)
nav_order: 17
evidence_level: L5
indication_count: 1
---

# Adapalene
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Adapalene: From Acne Treatment to Zinc, Elevated Plasma

## One-Sentence Summary

Adapalene is a synthetic retinoid (RAR-β/γ agonist) used topically for acne treatment.
The TxGNN model predicts it may be effective for **Zinc, Elevated Plasma**,
however with **0 clinical trials** and **0 publications** currently supporting this direction — making this a purely model-driven hypothesis with no clinical corroboration at this time.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Acne vulgaris (topical retinoid) |
| Predicted New Indication | Zinc, Elevated Plasma |
| TxGNN Prediction Score | 99.51% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on known pharmacology, Adapalene is a third-generation synthetic retinoid that selectively binds to retinoic acid receptors RAR-β and RAR-γ. Its established efficacy in acne treatment is attributed to modulation of keratinocyte differentiation and anti-inflammatory activity rather than any known role in metal ion metabolism.

The proposed mechanistic link in this repurposing hypothesis is highly speculative. The hypothesised pathway is: Adapalene (as a RAR-β/γ agonist) → upregulation of metallothionein gene expression → metallothionein (a zinc-chelating protein) → theoretical reduction of free plasma zinc concentrations. Retinoids and zinc metabolism do share an indirect biological relationship — zinc is a cofactor for retinol-binding protein (RBP) — but the directionality and dose-response of any such interaction are not well characterised.

The TxGNN score of 0.9951 likely reflects the model capturing a knowledge-graph path: retinoid → metallothionein → zinc. However, this remains a graph-inference signal only. This pathway has never been directly validated in clinical or cell-based studies, and "zinc, elevated plasma" is a laboratory abnormality rather than a discrete clinical disease entity, making the therapeutic relevance of this prediction particularly uncertain.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Adapalene has no registered authorizations in New Zealand as of the data cutoff (2026-06-06).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction is evidence level L5 — model inference only, with no supporting clinical trials or peer-reviewed literature. The proposed mechanism (retinoid → metallothionein → zinc chelation) is a highly speculative multi-step hypothesis that has not been tested in any experimental system, and the target condition ("zinc, elevated plasma") lacks a well-defined clinical disease framework for a repurposing study.

**To proceed, the following is needed:**
- Retrieve Adapalene's full mechanism of action data from DrugBank to confirm or refute the metallothionein upregulation hypothesis
- Conduct a targeted literature search for retinoid–zinc interactions in in vitro or animal models to assess whether any biological signal exists
- Define the clinical context more precisely: is "elevated plasma zinc" a standalone condition or secondary to another disease? This determines whether a therapeutic intervention is appropriate
- Obtain Taiwan/New Zealand package insert data to complete safety profiling before any forward-looking study design
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

