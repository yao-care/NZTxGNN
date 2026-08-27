---
layout: default
title: Trastuzumab Deruxtecan
parent: 僅模型預測 (L5)
nav_order: 349
evidence_level: L5
indication_count: 1
---

# Trastuzumab Deruxtecan
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

# Trastuzumab Deruxtecan: From HER2-Targeted Cytotoxic Therapy to Drug-induced Osteoporosis

## One-Sentence Summary

Trastuzumab deruxtecan is a HER2-targeted antibody-drug conjugate (ADC) combining an anti-HER2 antibody with a cytotoxic topoisomerase I inhibitor payload; its original approved indication is not documented in the current evidence pack. The TxGNN model predicts a possible signal for **Drug-induced Osteoporosis**, but this is currently supported by **0 clinical trials** and **0 publications**, and the model's own rationale flags the prediction as likely knowledge-graph noise.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in evidence pack (drug class: HER2-targeted antibody-drug conjugate, cytotoxic chemotherapy) |
| Predicted New Indication | Drug-induced Osteoporosis |
| TxGNN Prediction Score | 99.31% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (DG002, High severity data gap). Based on the information present in this evidence pack, trastuzumab deruxtecan is known to be a HER2-targeted antibody-drug conjugate (ADC) — the antibody component binds HER2, and the conjugated cytotoxic payload is released intracellularly, placing it in the cytotoxic chemotherapy drug class.

Unlike typical repurposing signals where a shared pathway links the original and predicted indications, no such mechanistic link could be established here. Bone density regulation operates through the osteoclast–osteoblast (RANK/RANKL/OPG) axis, and there is no known direct interaction between HER2-ADC pharmacology and this pathway.

The model's own rationale explicitly flags this as a low-confidence signal: despite a high raw TxGNN score, the prediction has **zero corroborating clinical trials or literature**, and the evidence pack notes this is "suspected to be knowledge-graph relational noise (high score but no supporting evidence), requiring manual review of the underlying KG edge provenance." This prediction should be treated as hypothesis-generating only, not as an actionable repurposing candidate at this stage.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

This drug is currently **not marketed** in New Zealand (0 authorizations on file); no product/license records are available in the evidence pack.

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (HER2-directed antibody-drug conjugate) with cytotoxic payload |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Must follow cytotoxic drug handling regulations (ADC/cytotoxic payload class) |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication has no supporting clinical trials or literature (Evidence Level L5), and the mechanistic rationale itself flags the signal as likely graph noise rather than a biologically plausible connection. Combined with missing TFDA/insert safety data (DG001, Blocking) and missing MOA data (DG002, High), there is currently no basis to advance this candidate.

**To proceed, the following is needed:**
- Manual review of the underlying TxGNN knowledge-graph edge provenance for this prediction
- Confirmed original indication and mechanism of action (MOA) data
- Package insert / regulatory safety data (warnings, contraindications, DDI) — currently Blocking gap DG001
- Any preclinical or case-level evidence linking HER2-ADC therapy to bone density effects, if it exists
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

