---
layout: default
title: Natalizumab
parent: 僅模型預測 (L5)
nav_order: 239
evidence_level: L5
indication_count: 5
---

# Natalizumab
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

# Natalizumab: From Multiple Sclerosis to Bronchitis

## One-Sentence Summary

> Natalizumab is a monoclonal antibody originally used to treat relapsing-remitting multiple sclerosis (inferred from the associated clinical literature, as official original-indication records are currently missing).
> The TxGNN model predicts it may be effective for **Bronchitis**,
> but this candidate currently has **0 clinical trials** and **0 publications** supporting it — the prediction rests on the model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Multiple Sclerosis (relapsing-remitting) — inferred from literature context; no official taiwan_regulatory/TFDA record available |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 99.46% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed, officially-sourced mechanism of action data (`original_moa`) is not currently available for this record. Based on information embedded in the associated repurposing rationale and literature evidence, Natalizumab is a recombinant humanized monoclonal antibody directed against the α4-subunit of α4β1 integrin (VLA-4). By binding VLA-4, it blocks leukocyte adhesion to vascular endothelium (VCAM-1) and thereby inhibits transmigration of activated lymphocytes and monocytes across the blood-brain barrier — the mechanism underlying its established use in relapsing forms of multiple sclerosis, where it reduces relapse frequency by limiting inflammatory cell infiltration into the CNS.

The link between multiple sclerosis and bronchitis is not disease-family related (CNS autoimmune disease vs. airway inflammatory/infectious disease), so the TxGNN score here is best read as a mechanism-based hypothesis rather than a clinical-similarity signal. VLA-4-mediated leukocyte trafficking is not CNS-specific — it has been studied preclinically in airway inflammation models (e.g., asthma), which is presumably the biological rationale the model is picking up on for bronchitis: reducing inflammatory cell recruitment to bronchial mucosa could theoretically reduce airway inflammation.

However, this mechanistic plausibility is entirely theoretical for bronchitis specifically. There are no clinical trials, no ICTRP records, and no PubMed literature in this evidence pack that test or even mention natalizumab in the context of bronchitis. Given that the same drug is well documented elsewhere in this evidence pack (see other predicted-indication branches) to carry serious immunosuppression-related risk (e.g., progressive multifocal leukoencephalopathy, PML), extending use to a comparatively low-severity, usually self-limited or infection-driven condition like bronchitis represents an unfavorable risk-benefit profile without further data.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Natalizumab is currently **not marketed** in New Zealand — 0 authorizations are on record, so no product/authorization table can be produced.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: `safety.key_warnings`, `safety.contraindications`, and `safety.ddi` are all unavailable in this evidence pack; TFDA package-insert warnings/contraindications are flagged in `meta.data_gaps` as a Blocking gap (DG001) that must be resolved before any safety-based go/no-go decision can be made.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The TxGNN score for bronchitis is high (99.46%), but the evidence level is L5 — a model prediction with zero corroborating clinical trials or literature. Combined with the missing official MOA, missing original-indication data, and a Blocking gap on TFDA safety labeling, there is currently no basis to move this candidate past initial screening.

**To proceed, the following is needed:**
- Resolve DG001 (TFDA package insert / warnings & contraindications) — currently blocking entry into the S1 safety screen
- Resolve DG002 (confirmed MOA documentation via DrugBank) to validate the mechanistic rationale above
- Preclinical or clinical evidence specifically evaluating VLA-4/α4-integrin inhibition in bronchitis or airway inflammation
- A risk-benefit analysis that accounts for natalizumab's known serious safety signals (e.g., PML) before considering it for a comparatively low-severity respiratory indication
- Confirmation of the drug's actual original approved indication(s), since `taiwan_regulatory.licenses` and `drug.original_indications` are both currently empty

---

### Additional Note: Other Predicted Indications in This Evidence Pack

This evidence pack contains 4 additional TxGNN-predicted indications for natalizumab beyond bronchitis, worth flagging for the reviewer:

- **Psoriasis** (rank 3, score 99.19%, L4) has by far the richest literature base (19 PubMed records), but the evidence points in the **opposite direction of the repurposing hypothesis**: the majority of these are case reports/reviews describing natalizumab **inducing or aggravating** psoriasis (paradoxical psoriasis), not treating it. Only one small cohort report (PMID 33589543, n=18) suggests improvement of comorbid psoriasis. This is a safety signal, not treatment-efficacy evidence, and should not be read as support for repurposing.
- **Parapsoriasis** (rank 2, 99.37%) and **Acute lichenoid pityriasis** (rank 5, 99.04%) are both supported only by the same single adverse-dermatologic-reaction case report (PMID 32470781), again describing drug-induced skin conditions rather than therapeutic benefit.
- **Severe nonproliferative diabetic retinopathy** (rank 4, 99.18%) has no supporting trials or literature at all (L5, same tier as bronchitis).

Taken together, this pattern suggests the TxGNN model may in part be capturing natalizumab's **adverse-event association network** (dermatologic reactions to VLA-4/integrin blockade) rather than genuine therapeutic repurposing signals for the dermatologic candidates. This should be considered when triaging which of the five candidates (if any) warrant further evidence-gathering resources.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

