---
layout: default
title: Aprepitant
parent: 僅模型預測 (L5)
nav_order: 31
evidence_level: L5
indication_count: 10
---

# Aprepitant
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

# Aprepitant: From Chemotherapy-Induced Nausea and Vomiting to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Aprepitant (Emend) is a selective neurokinin-1 (NK1) receptor antagonist, approved internationally for prevention of chemotherapy-induced nausea and vomiting (CINV) and post-operative nausea and vomiting (PONV).
The TxGNN model predicts it may be effective for **Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD)**, however this prediction is currently supported by **no clinical trials and no published literature**.
This candidate is at the earliest possible evidence stage (L5) and requires substantial mechanistic investigation before any clinical consideration.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Prevention of chemotherapy-induced nausea and vomiting (CINV); prevention of post-operative nausea and vomiting (PONV) |
| Predicted New Indication | Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD) |
| TxGNN Prediction Score | 99.97% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack. Based on well-established pharmacological knowledge, Aprepitant is a selective, high-affinity NK1 receptor antagonist that competitively blocks Substance P from binding to neurokinin-1 receptors in both the central nervous system (particularly the brainstem vomiting center) and peripheral tissues. This mechanism underpins its proven efficacy in preventing acute and delayed CINV.

The predicted indication, Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD), is a rare X-linked disorder caused by gain-of-function mutations in the *AVPR2* gene encoding the vasopressin V2 receptor. Constitutive V2 receptor activation leads to unregulated water retention and severe dilutional hyponatremia, independent of circulating ADH levels. The theoretical mechanistic bridge to NK1 antagonism is indirect: Substance P and NK1 signaling have been reported to modulate vasopressin release and possibly renal collecting duct AVP-pathway responsiveness, suggesting that NK1 blockade could theoretically influence water homeostasis.

However, this connection must be characterized as tenuous. The TxGNN model's high confidence score most likely reflects topological proximity between water/electrolyte regulation nodes within the disease knowledge graph, rather than a direct or well-characterized biological mechanism. No published preclinical or clinical data support Aprepitant's use in NSIAD or related vasopressin axis disorders. This prediction should be treated strictly as a hypothesis-generating signal.

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
Despite the TxGNN model producing a near-maximum score of 99.97% for NSIAD, the mechanistic linkage between NK1/Substance P antagonism and constitutive AVPR2 gain-of-function is entirely speculative, and a comprehensive evidence search returned zero supporting publications or registered trials. Advancing without any preclinical evidence would not be scientifically justified.

**Noteworthy signals among other ranked predictions:**
Two additional indications in this candidate's top-10 list carry a "Research Question" recommendation rather than outright "Hold," and merit monitoring:
- **Pulmonary hypertension (rank 3):** NK1 receptors are expressed on pulmonary vascular smooth muscle and endothelium; Substance P promotes vasoconstriction and cell proliferation, and animal models suggest NK1 blockade may reduce pulmonary arterial pressure. Biologically plausible, but lacks human data.
- **Subarachnoid hemorrhage (rank 9):** Cerebrovascular vasospasm following SAH involves Substance P-mediated neurogenic inflammation and NK1 receptor activation on vascular smooth muscle; animal data suggest NK1 antagonism may attenuate delayed cerebral ischemia. Also biologically plausible, no human trials identified.

**To proceed on the primary NSIAD indication, the following is needed:**
- **Preclinical mechanistic study:** Determine whether NK1/Substance P signaling modulates AVPR2 constitutive activity, aquaporin-2 trafficking, or cAMP accumulation in renal collecting duct cells
- **Targeted literature search:** Systematic review for any intersection between Substance P signaling and AVPR2/NSIAD or related nephrogenic hyponatremia syndromes
- **MOA data acquisition:** Retrieve complete DrugBank pharmacology entry (secondary targets, off-target binding profile) to identify any unanticipated mechanistic pathways
- **Safety baseline:** Obtain full New Zealand/international prescribing information (warnings, contraindications, drug-drug interactions) before any research protocol can be designed
- **Regulatory pathway assessment:** Confirm whether Aprepitant would require new market authorization in New Zealand for any repurposed indication, given its current absence from the Medsafe register
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

