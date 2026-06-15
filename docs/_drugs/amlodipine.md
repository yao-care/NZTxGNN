---
layout: default
title: Amlodipine
parent: 僅模型預測 (L5)
nav_order: 26
evidence_level: L5
indication_count: 10
---

# Amlodipine
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

# Amlodipine: From Hypertension to Brain Stem Infarction

## One-Sentence Summary

Amlodipine is a dihydropyridine-class L-type calcium channel blocker (CCB), established globally as a first-line treatment for hypertension and chronic stable angina.
The TxGNN model predicts it may be effective for **Brain Stem Infarction**, with a prediction confidence of **99.94%**; however, **no clinical trials or published literature** currently support this specific indication, placing it at the lowest evidence tier.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hypertension / Chronic Stable Angina (not registered in New Zealand per available data) |
| Predicted New Indication | Brain Stem Infarction |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why Is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this dataset. Based on established pharmacological knowledge, amlodipine is a long-acting dihydropyridine CCB that blocks voltage-gated L-type calcium channels in vascular smooth muscle, producing arterial vasodilation and reducing systemic vascular resistance. This forms the basis of its antihypertensive and antianginal effects.

From a mechanistic standpoint, sustained blood pressure reduction by amlodipine could theoretically improve cerebral perfusion pressure and reduce ischemic insult to the brainstem. Calcium ion overload following neuronal ischemia is a well-characterised pathway of cell death, and CCBs have been investigated as neuroprotective candidates in broader cerebrovascular disease models—particularly in animal studies of middle cerebral artery occlusion (see rank 6, cerebral artery occlusion, for supporting preclinical data).

However, the brainstem presents unique anatomical and haemodynamic characteristics. Its vascular territory (vertebrobasilar system) differs substantially from the anterior circulation, and CCBs that lower blood pressure may paradoxically reduce perfusion in patients with fixed stenosis. No clinical trial or human study has directly investigated amlodipine for brain stem infarction specifically. The TxGNN prediction most likely reflects graph-level associations between antihypertensive drug classes and cerebrovascular disease categories broadly, rather than brainstem-specific pathobiology.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for amlodipine in brain stem infarction.

---

## Literature Evidence

Currently no related literature available for amlodipine in brain stem infarction.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Key warnings, contraindications, and drug interaction data were not available in the current dataset (identified as data gaps DG001 and DG002). TFDA package insert retrieval and DrugBank MOA query are required before any clinical feasibility assessment can proceed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Although the TxGNN model assigns a 99.94% confidence score to brain stem infarction as a new indication for amlodipine, there is zero supporting clinical or preclinical evidence specific to this diagnosis. A high model score alone is insufficient to justify advancement—particularly for a neurological indication with potentially complex haemodynamic contraindications.

**To proceed, the following is needed:**

- **MOA data** from DrugBank to formally document the biological plausibility of CCB activity in brainstem ischaemia
- **Safety data** (key warnings, contraindications, DDI profile) from the TFDA package insert—currently a blocking data gap
- **Preclinical literature search** focused specifically on vertebrobasilar ischaemia and CCB effects, to determine whether animal evidence exists comparable to that seen in middle cerebral artery occlusion models
- **Cross-indication prioritisation review**: Within this same evidence pack, **intracerebral haemorrhage (rank 10)** has substantially stronger support—a completed Phase 3 RCT (TRIDENT, NCT02699645, n = 1,671) in which amlodipine forms part of a triple antihypertensive regimen for post-ICH secondary prevention, with evidence graded at **L2 / Proceed with Guardrails**. Investigators should consider whether resources are better directed toward advancing the ICH indication first, before committing to brainstem infarction as the primary development target.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

