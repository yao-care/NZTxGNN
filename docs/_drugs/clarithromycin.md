---
layout: default
title: Clarithromycin
parent: 僅模型預測 (L5)
nav_order: 76
evidence_level: L5
indication_count: 5
---

# Clarithromycin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Clarithromycin: From Bacterial Infections to Hyperamylasemia

## One-Sentence Summary

Clarithromycin is a macrolide antibiotic widely used to treat bacterial infections, including respiratory tract infections, skin and soft tissue infections, and *Mycobacterium avium* complex (MAC) infections in immunocompromised patients.
The TxGNN model predicts it may be effective for **Hyperamylasemia**, with **0 clinical trials** and **1 publication** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Bacterial infections (respiratory tract, skin, *H. pylori*, MAC) |
| Predicted New Indication | Hyperamylasemia |
| TxGNN Prediction Score | 99.35% |
| Evidence Level | L4 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why Is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack. Based on established pharmacology, clarithromycin is a macrolide antibiotic that inhibits bacterial protein synthesis by binding to the 50S ribosomal subunit and blocking peptide chain elongation. Beyond its direct antibacterial activity, clarithromycin exhibits clinically relevant immunomodulatory properties — including NF-κB suppression, downregulation of pro-inflammatory cytokines (IL-6, IL-8), and inhibition of matrix metalloproteinases — making it a first-line component of MAC infection treatment regimens.

Hyperamylasemia is a laboratory finding (elevated serum amylase) rather than a primary disease entity. It arises secondarily from conditions such as acute pancreatitis, parotitis, renal failure, and notably, pulmonary MAC/*Mycobacterium abscessus* infections. The mechanistic link proposed by TxGNN is therefore indirect: clarithromycin may incidentally reduce elevated amylase by treating the underlying mycobacterial infection, rather than targeting amylase production or clearance as a primary pharmacological effect.

The sole supporting publication (PMID 15228140) describes a single case of *M. abscessus* lung infection complicated by coincidental primary macroamylasemia, where clarithromycin was used as standard MAC therapy. Elevated amylase in that case was an infection complication, not a demonstrated drug target. This represents an incidental association surfaced through knowledge graph traversal, rather than evidence of a clinically meaningful repurposing opportunity.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [15228140](https://pubmed.ncbi.nlm.nih.gov/15228140/) | 2004 | Case Report | Japanese Respiratory Society Journal | *M. abscessus* pulmonary infection in a 76-year-old male complicated by primary macroamylasemia; clarithromycin was used as part of standard MAC therapy — elevated amylase was an infection complication, not a direct drug target |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN prediction appears to originate from an indirect knowledge graph association — clarithromycin treats MAC infections, and MAC infections can secondarily cause hyperamylasemia — rather than from any direct pharmacological effect on amylase metabolism. With no clinical trials, no prospective studies, and a single case report describing coincidental co-occurrence, there is insufficient biological and clinical rationale to advance this as a standalone repurposing candidate.

**To proceed, the following is needed:**

- Mechanism of action data (from DrugBank or primary literature) confirming whether clarithromycin has any direct effect on pancreatic or salivary amylase activity
- At least one prospective cohort study or controlled observation demonstrating amylase reduction attributable to clarithromycin independent of infection resolution
- Clarification of the intended therapeutic target: if the goal is to treat MAC-induced hyperamylasemia, this is already covered by the drug's approved antibacterial indication and does not constitute a repurposing opportunity
- New Zealand regulatory pathway assessment once a clinically meaningful indication is established
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

