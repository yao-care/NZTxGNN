---
layout: default
title: Clindamycin
parent: 僅模型預測 (L5)
nav_order: 77
evidence_level: L5
indication_count: 6
---

# Clindamycin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

# Clindamycin: From Bacterial Infections to Punctate Epithelial Keratoconjunctivitis

## One-Sentence Summary

Clindamycin is a lincosamide antibiotic with established use against Gram-positive bacteria, anaerobes, and selected intracellular pathogens.
The TxGNN model predicts it may be effective for **Punctate Epithelial Keratoconjunctivitis** (score: 99.97%),
however **no clinical trials and no supporting literature** have been identified for this specific indication — the prediction rests on model inference alone.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Bacterial infections (skin and soft tissue, respiratory tract, anaerobic and intracellular infections) |
| Predicted New Indication | Punctate Epithelial Keratoconjunctivitis |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why Is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on established pharmacological knowledge, Clindamycin belongs to the lincosamide antibiotic class. It inhibits bacterial protein synthesis by binding to the 50S ribosomal subunit, with activity against Gram-positive organisms (including *Staphylococcus* spp.), anaerobes, *Chlamydia trachomatis*, and *Toxoplasma gondii*. This broad spectrum provides a narrow but identifiable mechanistic bridge to ocular infections.

Punctate epithelial keratoconjunctivitis (PEK) is characterised by fine punctate epithelial defects scattered across the corneal surface. Its aetiology is multifactorial — including adenoviral infection, dry eye disease, lid exposure, and, in some cases, bacterial or chlamydial involvement. Clindamycin's activity against *Staphylococcus* spp. and *Chlamydia trachomatis* (a recognised cause of follicular keratoconjunctivitis) represents the only plausible mechanistic angle; however, the predominant drivers of PEK are non-bacterial, making antibiotic therapy of limited relevance to the core pathology.

The high TxGNN prediction score most likely reflects knowledge-graph diffusion from adjacent ocular-infection nodes (e.g., bacterial conjunctivitis, chlamydial eye disease) rather than a direct biological pathway linking clindamycin to PEK specifically. The complete absence of supporting clinical trials or published literature confirms that this prediction remains speculative.

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
Evidence level is L5 — the prediction is model-generated with no clinical trials and no published literature supporting clindamycin in punctate epithelial keratoconjunctivitis. The mechanistic link to the primary (non-bacterial) aetiology of PEK is weak, and safety data remain a blocking gap.

**To proceed, the following is needed:**

- Safety warnings and contraindications from the package insert (currently a **Blocking** data gap — cannot enter S1 safety screening without this)
- Mechanism of action data from DrugBank API (currently a **High-severity** data gap)
- A targeted literature search for clindamycin use specifically in *Chlamydia*-associated or *Staphylococcus*-triggered keratoconjunctivitis subtypes
- Reconsideration of the indication pipeline: among the six predicted indications in this pack, **postmenopausal atrophic vaginitis** (rank 6) carries the strongest biological rationale — clindamycin 2% vaginal gel is already a first-line therapy for bacterial vaginosis, which shares overlapping microbial ecology with this condition. That pathway may represent a more productive repurposing direction for further investigation.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

