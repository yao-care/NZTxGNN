---
layout: default
title: Rivastigmine
parent: 僅模型預測 (L5)
nav_order: 311
evidence_level: L5
indication_count: 1
---

# Rivastigmine
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

# Rivastigmine: From Dementia to Glaucoma

## One-Sentence Summary

> Rivastigmine is a cholinesterase inhibitor whose approved formulations (oral capsule, transdermal patch) were designed for the central nervous system to treat **dementia**.
> The TxGNN model predicts it may be effective for **Glaucoma**,
> with **0 clinical trials** and **3 publications** currently supporting this direction — all preclinical or mechanistic in nature.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Dementia (per drug's known central AChE-inhibitor mechanism; no formal indication text on file — 0 NZ licenses) |
| Predicted New Indication | Glaucoma |
| TxGNN Prediction Score | 99.27% |
| Evidence Level | L4 (preclinical/mechanism studies only) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for rivastigmine is not available in the current DrugBank extract (Data Gap). However, the evidence collected for this prediction describes rivastigmine as a dual acetylcholinesterase (AChE) / butyrylcholinesterase (BuChE) inhibitor that raises local acetylcholine concentration.

Cholinergic agents (e.g., pilocarpine, physostigmine) act on the ciliary muscle and trabecular meshwork to promote aqueous humor outflow and lower intraocular pressure (IOP) — a well-established pharmacological class for glaucoma. Rivastigmine shares this same AChE-inhibitory mechanism, giving it theoretical potential to reduce IOP; one rabbit study directly demonstrated this effect with topical rivastigmine.

That said, rivastigmine's approved formulations (oral capsule, transdermal patch) were designed for systemic/central delivery to treat dementia, not for topical ophthalmic administration. Human ocular safety, dosing, and formulation for this indication have not been established, which is why the current recommendation is Hold rather than proceeding to development.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39130374](https://pubmed.ncbi.nlm.nih.gov/39130374/) | 2024 | Review | Frontiers in Molecular Biosciences | Reviews cholinergic (muscarinic) agents for IOP reduction; notes systemic cholinergic adverse effects limit clinical use of this drug class |
| [27967267](https://pubmed.ncbi.nlm.nih.gov/27967267/) | 2017 | Review (patent literature) | Expert Opinion on Therapeutic Patents | Notes mild AChE inhibition has therapeutic relevance in Alzheimer's disease, myasthenia gravis, and glaucoma |
| [10673128](https://pubmed.ncbi.nlm.nih.gov/10673128/) | 2000 | Animal study (rabbit) | J Ocular Pharmacology and Therapeutics | Topical rivastigmine lowered intraocular pressure in normotensive rabbits, monitored hourly up to 8 hours post-dose |

---

## New Zealand Market Information

Not currently marketed in New Zealand; no product authorizations on file.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(TFDA package insert warnings/contraindications and DDI data are currently unavailable — flagged as a Blocking data gap, see Conclusion.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence is limited to mechanistic reasoning and a single animal (rabbit) pharmacology study — no clinical trials exist for rivastigmine in glaucoma, and the drug's existing formulations are not designed for ophthalmic delivery. This corresponds to Evidence Level L4, insufficient to advance beyond initial screening.

**To proceed, the following is needed:**
- TFDA/regulatory package insert with warnings and contraindications (currently a **Blocking** data gap — required before any safety assessment)
- Confirmed mechanism of action (MOA) data from DrugBank (currently **High**-severity data gap)
- Route compatibility assessment for a topical ophthalmic formulation (systemic/CNS formulation ≠ ocular route)
- Additional preclinical/human ocular safety and dosing studies before clinical development is considered
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

