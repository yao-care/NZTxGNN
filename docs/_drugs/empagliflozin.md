---
layout: default
title: Empagliflozin
parent: 僅模型預測 (L5)
nav_order: 133
evidence_level: L5
indication_count: 3
---

# Empagliflozin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Empagliflozin: From Type 2 Diabetes to Classic Stiff Person Syndrome

## One-Sentence Summary

Empagliflozin is a selective SGLT2 (sodium-glucose cotransporter 2) inhibitor, globally established for the treatment of type 2 diabetes and heart failure with reduced ejection fraction.
The TxGNN model predicts it may be effective for **Classic Stiff Person Syndrome**,
however **no clinical trials** and **no publications** currently support this specific repurposing direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Type 2 diabetes / heart failure (global approval; no New Zealand authorisation on record) |
| Predicted New Indication | Classic Stiff Person Syndrome |
| TxGNN Prediction Score | 99.06% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorisations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Empagliflozin is a selective inhibitor of the sodium-glucose cotransporter 2 (SGLT2) expressed in the renal proximal tubule. By blocking glucose reabsorption it lowers blood glucose independently of insulin, and exerts pleiotropic downstream effects including reduced cardiac preload/afterload, mitochondrial protection, and suppression of NF-κB–driven systemic inflammation. Formal MOA data was not captured in the current Evidence Pack; the characterisation above is drawn from published pharmacology literature.

Classic Stiff Person Syndrome (SPS) is a rare autoimmune neurological disorder primarily mediated by antibodies against glutamic acid decarboxylase 65 (GAD65), resulting in progressive muscle rigidity and episodic spasms driven by impaired GABAergic inhibition. The theoretical bridge to empagliflozin relies on three indirect hypotheses: (1) SGLT2 inhibition may dampen NF-κB–mediated neuroinflammation, thereby reducing the autoimmune attack on GABAergic interneurons; (2) improved mitochondrial efficiency and reduced reactive oxygen species could confer neuroprotection; (3) scattered preclinical studies suggest SGLT2 inhibitors modulate autoimmune-related inflammatory markers. All three remain highly speculative.

There is no GAD65-antibody animal model data, no reported use of empagliflozin in SPS patients, and no case series or clinical trials. The mechanistic distance between a renal glucose transporter and a GABAergic autoimmune disease is substantial. The high TxGNN score (99.06%) most likely reflects shared graph topology in the knowledge graph — particularly nodes related to inflammation and metabolic signalling — rather than a direct pharmacological relationship.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Empagliflozin holds no registered product authorisations in New Zealand as of the data cutoff date (2026-06-07). The drug is not marketed domestically under any dosage form or brand name.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Although TxGNN assigns a very high prediction score (99.06%), the evidence base is L5 — model prediction only — with zero supporting clinical trials, literature, case reports, or preclinical SPS-model studies. No New Zealand regulatory footprint exists for empagliflozin, and the mechanistic link to Classic Stiff Person Syndrome remains entirely theoretical.

**To proceed, the following is needed:**
- Preclinical validation in a GAD65-antibody SPS animal model to test whether SGLT2 inhibition alters disease course
- Systematic review of pharmacovigilance databases for SPS onset or resolution in diabetic patients on empagliflozin
- Mechanistic studies confirming CNS penetration or indirect neuroimmune modulation by SGLT2 inhibitors
- Formal MOA documentation addressing neuroinflammatory pathways
- Safety and drug-interaction profiling for the neurological patient population (who may not have the metabolic comorbidities that define the established safety profile)
- New Zealand regulatory pathway assessment (Medsafe) before any clinical development is initiated locally
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

