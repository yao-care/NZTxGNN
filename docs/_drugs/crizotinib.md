---
layout: default
title: Crizotinib
parent: 僅模型預測 (L5)
nav_order: 87
evidence_level: L5
indication_count: 10
---

# Crizotinib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Crizotinib: From Non-Small Cell Lung Cancer to Fibromatosis, Gingival

## One-Sentence Summary

Crizotinib is a first-in-class ALK/ROS1/MET tyrosine kinase inhibitor with established efficacy in ALK-positive and ROS1-rearranged non-small cell lung cancer (NSCLC) in global markets, though it currently holds no marketing authorization in New Zealand.
The TxGNN model's top-ranked prediction suggests potential efficacy in **Fibromatosis, Gingival**;
however, **no clinical trials** and **no supporting publications** exist for this indication, and the prediction is considered a model-only signal with no mechanistic plausibility.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No New Zealand authorization on record (internationally approved for ALK+/ROS1+ NSCLC) |
| Predicted New Indication | Fibromatosis, Gingival |
| TxGNN Prediction Score | 99.81% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Formal mechanism of action data from the New Zealand package insert is unavailable. Based on the extensive published literature contained within this evidence pack, crizotinib is an ATP-competitive small-molecule inhibitor of three receptor tyrosine kinases: ALK (anaplastic lymphoma kinase), ROS1, and MET/c-Met. It was first granted accelerated FDA approval in 2011 for ALK-rearranged NSCLC — a molecular subtype representing 3–5% of all NSCLC cases — and subsequently expanded to ROS1-rearranged NSCLC. Its clinical activity arises from blocking constitutively activated oncogenic fusions (e.g., EML4-ALK, CD74-ROS1) that drive uncontrolled tumour cell proliferation.

Gingival fibromatosis is primarily driven by aberrant fibroblast proliferation through SOS1, CTGF, or VEGF pathways, with a hereditary subset caused by SOS1 gene mutations. The ALK/ROS1/MET axis that crizotinib targets has no established direct role in the known pathobiology of this condition. While MET/HGF signalling participates in general fibrotic processes, no clinical or preclinical evidence demonstrates MET-driven disease specifically in gingival fibromatosis.

This prediction most likely reflects a distal topological association between a "fibroproliferative" node and crizotinib within the TxGNN knowledge graph, rather than a biologically specific signal. A high prediction score in the absence of any supporting evidence should be interpreted as graph-level noise pending node-level auditing.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Crizotinib currently holds no marketing authorization in New Zealand. No product licenses are on record in the regulatory database.

---

## Cytotoxicity

Crizotinib is a targeted antineoplastic agent (ALK/ROS1/MET tyrosine kinase inhibitor) used in oncology; the following section applies.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted therapy — receptor tyrosine kinase inhibitor (ALK / ROS1 / MET) |
| Myelosuppression Risk | Low to moderate (not a primary cytotoxic mechanism; neutropenia reported at low incidence in clinical trials) |
| Emetogenicity Classification | Low to moderate |
| Monitoring Items | Liver function tests (ALT/AST) — fatal fulminant hepatic failure reported; ECG monitoring for QT prolongation and bradycardia; CBC; pulmonary function assessment for interstitial lung disease/pneumonitis |
| Handling Protection | Standard oral targeted therapy handling precautions apply; cytotoxic drug handling regulations recommended |

> Key toxicities identified from literature within this evidence pack: hepatotoxicity including fatal fulminant liver failure (PMID [26898609](https://pubmed.ncbi.nlm.nih.gov/26898609/)); simultaneous multiple cardiac toxicities including QT prolongation, bradycardia, and ventricular fibrillation (PMID [29717400](https://pubmed.ncbi.nlm.nih.gov/29717400/)); drug-induced organizing pneumonia/interstitial lung disease (PMID [37062732](https://pubmed.ncbi.nlm.nih.gov/37062732/)). Full toxicity profile and management strategies reviewed in PMID [41617059](https://pubmed.ncbi.nlm.nih.gov/41617059/).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN model assigns a high numeric score (99.81%) to crizotinib for gingival fibromatosis, but this prediction is unsupported by any clinical trial, published literature, or mechanistic rationale — the ALK/ROS1/MET targets of crizotinib have no known role in gingival fibromatosis pathogenesis, and the signal is attributed to distal knowledge graph topology rather than biological specificity.

**To proceed, the following would be needed:**
- Preclinical evidence (cell line or animal model) demonstrating involvement of ALK, ROS1, or MET signalling in gingival fibroblast pathology
- Knowledge graph node audit to verify whether the "fibromatosis, gingival" node is correctly mapped and whether this prediction reflects genuine signal or topological noise
- New Zealand regulatory pathway clarification for crizotinib (currently unregistered) before any clinical development consideration
- If MET/HGF pathway involvement in gingival fibromatosis is confirmed in preclinical studies, a prospective feasibility assessment would be warranted
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

