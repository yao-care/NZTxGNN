---
layout: default
title: Aflibercept
parent: 僅模型預測 (L5)
nav_order: 18
evidence_level: L5
indication_count: 1
---

# Aflibercept
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

# Aflibercept: From Neovascular Eye Disease to Esotropia

## One-Sentence Summary

Aflibercept is a VEGF-trap fusion protein used to treat neovascular (wet) age-related macular degeneration (AMD) and diabetic macular edema (DME) by suppressing pathological blood vessel growth in the eye.
The TxGNN model predicts it may be effective for **Esotropia** (convergent strabismus),
however **no clinical trials** and **no published literature** currently support this repurposing direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Neovascular AMD / Diabetic Macular Edema (based on known pharmacology; no regulatory data on file) |
| Predicted New Indication | Esotropia |
| TxGNN Prediction Score | 99.38% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Aflibercept is a recombinant fusion protein (VEGF-trap) that binds VEGF-A, VEGF-B, and placental growth factor (PlGF), blocking their interaction with VEGF receptors and thereby suppressing pathological neovascularization. Its established clinical role is in ophthalmic conditions driven by abnormal vessel growth — wet AMD, diabetic macular edema, and macular edema following retinal vein occlusion. A second formulation (ziv-aflibercept) is used in metastatic colorectal cancer, but that is a distinct drug entity (DB08886).

Esotropia is a form of convergent strabismus arising from neuromuscular imbalance of the extraocular muscles, accommodative refractive error, or neurodevelopmental anomalies. These pathological mechanisms have no established connection to the VEGF/angiogenesis axis. Aflibercept's VEGF-trap mechanism does not act on extraocular muscle innervation, motor neuron control, or the accommodative reflex pathway involved in esotropia.

The TxGNN model's high score (0.994) most likely reflects an **ophthalmological ontology overlap** in the knowledge graph — both conditions are classified as eye diseases — rather than a genuine mechanistic link. A highly speculative indirect hypothesis exists: VEGF carries neurotrophic signaling roles in neural development, but this remains entirely in early-stage basic research with no clinical translation evidence whatsoever. The biological plausibility for this repurposing candidate is low.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
There is no clinical trial or published literature evidence supporting the use of aflibercept in esotropia, and the mechanistic connection between VEGF inhibition and the neuromuscular or accommodative pathology underlying esotropia is biologically implausible based on current understanding. This remains an L5 model-only prediction.

**To proceed, the following is needed:**
- Preclinical studies (in vitro or animal models) demonstrating any effect of VEGF inhibition on extraocular muscle function or accommodative mechanisms
- Identification of a biologically plausible patient subpopulation where neovascular or inflammatory components contribute to strabismus (e.g., secondary strabismus from retinal disease)
- Full mechanism of action (MOA) data retrieved from DrugBank API to rule out any undocumented pathway connecting VEGF signaling to ocular motor control
- Safety data and package insert review from the originating regulatory authority before any further clinical evaluation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

