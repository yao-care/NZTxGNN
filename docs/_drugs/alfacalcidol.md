---
layout: default
title: Alfacalcidol
parent: 僅模型預測 (L5)
nav_order: 21
evidence_level: L5
indication_count: 5
---

# Alfacalcidol
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

# Alfacalcidol: From Renal Osteodystrophy to Familial Isolated Hypoparathyroidism Due to Impaired PTH Secretion

## One-Sentence Summary

Alfacalcidol (1α-hydroxyvitamin D₃) is a synthetic, pre-activated vitamin D analog traditionally used in the management of calcium and phosphate metabolism disorders — including renal osteodystrophy and secondary hyperparathyroidism — in patients whose renal 1α-hydroxylation capacity is compromised.
The TxGNN model predicts it may be effective for **Familial Isolated Hypoparathyroidism Due to Impaired PTH Secretion**, a rare genetic disorder causing deficient PTH output and chronic hypocalcemia.
The mechanistic rationale is pharmacologically direct and compelling; however, **no clinical trials and no published literature** specifically targeting this exact genetic subtype were identified, leaving the supporting evidence at the preclinical/mechanistic level.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not registered in New Zealand; established use in renal osteodystrophy and hypoparathyroidism (no local approved indication text available) |
| Predicted New Indication | Familial Isolated Hypoparathyroidism Due to Impaired PTH Secretion |
| TxGNN Prediction Score | 99.61% |
| Evidence Level | L4 (Mechanism-based rationale; no clinical trials or indication-specific literature identified) |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Alfacalcidol is a synthetic vitamin D analog that serves as a direct precursor to calcitriol (1,25-dihydroxyvitamin D₃), the biologically active form of vitamin D. Unlike native cholecalciferol, alfacalcidol requires only a single hepatic 25-hydroxylation step to become fully active — critically, it **bypasses the renal 1α-hydroxylation step** that is physiologically regulated by parathyroid hormone (PTH). Detailed pharmacodynamic data from the regulatory database is not yet available, but this mechanism is well-established in the pharmacological literature on active vitamin D analogs.

In familial isolated hypoparathyroidism due to impaired PTH secretion, deficient PTH output directly suppresses renal 1α-hydroxylase (CYP27B1) activity, leading to insufficient endogenous calcitriol synthesis, impaired intestinal calcium absorption, and chronic hypocalcemia. Alfacalcidol addresses this deficit at its root: by delivering a pre-activated vitamin D metabolite that completely circumvents the PTH-controlled enzymatic node. This constitutes a pharmacological **bypass strategy** — arguably one of the most mechanistically precise drug-disease alignments possible for PTH-deficiency spectrum disorders.

Alfacalcidol and calcitriol are already recognized standards of care for acquired forms of hypoparathyroidism (postsurgical, autoimmune), and their use is endorsed in international guidelines for chronic hypocalcemia management. The familial isolated genetic subtype shares the same downstream calcium deficiency pathophysiology, making the extrapolation mechanistically sound. The TxGNN knowledge graph likely captures this disease-class mechanistic overlap, explaining the very high prediction confidence of 99.61%.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for alfacalcidol in familial isolated hypoparathyroidism due to impaired PTH secretion.

---

## Literature Evidence

Currently no related literature specifically addressing alfacalcidol in familial isolated hypoparathyroidism due to impaired PTH secretion was identified.

---

## New Zealand Market Information

Alfacalcidol is not currently registered or marketed in New Zealand. No product authorizations are on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note for clinical teams:** Key risks to monitor for vitamin D analogs as a class include hypercalcemia, hypercalciuria, and nephrocalcinosis — particularly important in the absence of renal PTH-regulated feedback. Formal safety data from the regulatory database requires retrieval from the package insert (DG001 data gap).

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The pharmacological case for alfacalcidol in familial isolated hypoparathyroidism due to impaired PTH secretion is mechanistically robust — the drug directly replaces the calcitriol that PTH deficiency prevents the kidney from synthesizing, using a bypass route already validated in related hypoparathyroid conditions. The complete absence of indication-specific clinical evidence, combined with the drug's non-registered status in New Zealand, requires a structured evidence-building pathway before clinical advancement.

**To proceed, the following is needed:**
- Retrieve package insert from source (DG001) to document approved warnings, contraindications, and drug-drug interaction profile
- Retrieve formal mechanism of action data from DrugBank (DG002) to complete the pharmacodynamic documentation
- Conduct a systematic literature search for alfacalcidol and calcitriol use in **genetic** (familial/hereditary) hypoparathyroidism subtypes, extending beyond the TxGNN query to include GCM2 and CASR mutation carriers
- Assess named-patient access or regulatory pathway for use in New Zealand given zero local authorizations
- Identify patient registries for familial isolated hypoparathyroidism (e.g., PARADOX Registry) to retrieve real-world treatment outcome data
- Establish a pharmacovigilance protocol for hypercalcemia monitoring — especially critical in patients without the normal PTH-mediated calcium ceiling
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

