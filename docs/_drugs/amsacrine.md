---
layout: default
title: Amsacrine
parent: 僅模型預測 (L5)
nav_order: 29
evidence_level: L5
indication_count: 5
---

# Amsacrine
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

# Amsacrine: From Acute Leukemia to Hyperthyroidism

## One-Sentence Summary

Amsacrine (m-AMSA) is a cytotoxic antineoplastic agent established in the treatment of acute leukemia, acting via DNA intercalation and Topoisomerase II inhibition.
The TxGNN model predicts it may be effective for **Hyperthyroidism**, however this association is currently supported by **no clinical trials** and **no published literature**.
The high TxGNN score (99.37%) most likely reflects indirect knowledge-graph network connectivity rather than a genuine pharmacological relationship, and this prediction should be interpreted with significant caution.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Antineoplastic agent (acute leukemia); not registered in Taiwan |
| Predicted New Indication | Hyperthyroidism |
| TxGNN Prediction Score | 99.37% |
| Evidence Level | L5 |
| Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the current evidence package. Based on available scientific context, Amsacrine (m-AMSA) is a DNA-intercalating cytotoxic agent and Topoisomerase II inhibitor used in oncology — most notably for refractory acute non-lymphocytic leukaemia. Its mechanism involves insertion into the DNA double helix and stabilisation of the Topoisomerase II–DNA cleavage complex, generating DNA double-strand breaks that preferentially destroy rapidly proliferating cells. Cardiotoxicity is a documented class concern noted in the literature.

Hyperthyroidism is characterised by excessive synthesis and secretion of thyroid hormones (T3/T4), most commonly caused by Graves' disease, toxic nodular goitre, or thyroiditis. Standard treatment targets thyroid hormone synthesis (thionamides), radioiodine uptake, or peripheral adrenergic effects (beta-blockers). No established biological pathway connects Topoisomerase II inhibition or DNA intercalation to thyroid hormone regulation or thyroid gland physiology.

The repurposing rationale analysis explicitly identifies this prediction as a probable false positive: the TxGNN high score likely arises from indirect node linkages in the disease knowledge graph — such as shared comorbidity or immune-modulation nodes — rather than a real pharmacological interaction between Amsacrine and hyperthyroidism pathophysiology. This prediction does not meet the threshold for mechanistic plausibility required to advance beyond a model-prediction-only stage.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic (DNA intercalating agent / Topoisomerase II inhibitor) |
| Myelosuppression Risk | High — cytotoxic mechanism targeting rapidly dividing cells; haematological toxicity expected based on drug class |
| Emetogenicity Classification | Moderate |
| Monitoring Items | CBC with differential, ECG and cardiac function monitoring (known cardiotoxicity — confirmed in in vitro comparative studies with heart myocytes), liver and renal function |
| Handling Protection | Must follow cytotoxic drug handling regulations |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
There is no mechanistic basis connecting Amsacrine's Topoisomerase II inhibition and DNA intercalation to hyperthyroidism pathophysiology, no registered clinical trials, and no published literature supporting this indication. The TxGNN rank-1 prediction is assessed as a likely knowledge-graph false positive driven by indirect disease-node connectivity.

**To proceed, the following is needed:**
- Formal mechanism of action data (MOA) from DrugBank to enable structured mechanistic analysis
- Identification of a biologically plausible pathway linking Topoisomerase II inhibition to thyroid hormone synthesis or secretion (none currently known)
- At minimum one hypothesis-generating in vitro or in silico study before any clinical consideration can be entertained

**Alternative candidate warranting priority review:**
The rank-2 TxGNN prediction — **Plasma Cell Myeloma** (score 99.32%, Evidence Level L3) — is mechanistically more plausible (Topoisomerase II inhibition against a highly proliferative plasma cell tumour) and is supported by 7 published studies including two Phase II single-arm trials (PMID [6688199](https://pubmed.ncbi.nlm.nih.gov/6688199/) and [2913481](https://pubmed.ncbi.nlm.nih.gov/2913481/)). Although both trials showed insufficient single-agent efficacy, the existing evidence base makes Plasma Cell Myeloma a more productive target for repurposing evaluation — particularly in combination regimens or specific patient subgroups.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

