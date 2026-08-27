---
layout: default
title: Lithium Carbonate
parent: 僅模型預測 (L5)
nav_order: 208
evidence_level: L5
indication_count: 10
---

# Lithium Carbonate
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

Using no specific skill here — this is a direct report-generation task fully specified by the prompt's own template; I'll follow it directly against the given evidence pack.

# Lithium Carbonate: From Bipolar Disorder to Pseudoachondroplasia

## One-Sentence Summary

Lithium carbonate (DrugBank ID: DB14509) is a long-established mood stabilizer, though this evidence pack does not itself contain original-indication or MOA data.
The TxGNN model's top prediction is **Pseudoachondroplasia**, a rare skeletal dysplasia, but this signal is currently supported by **0 clinical trials** and **0 publications** — it is a pure network-similarity prediction with no empirical backing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in evidence pack (`taiwan_regulatory.licenses` is empty; lithium carbonate is generally known as a mood stabilizer for bipolar disorder, but this is not sourced from the pack) |
| Predicted New Indication | Pseudoachondroplasia |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (flagged as a High-severity data gap in this pack). Lithium carbonate is widely known pharmacologically as an inhibitor of GSK-3β and a modulator of the Wnt signaling pathway, but no original-indication or efficacy data is included in this evidence pack to anchor a direct comparison.

Pseudoachondroplasia and bipolar disorder belong to entirely different physiological domains — one is a skeletal growth-plate disorder driven by a *COMP* gene mutation, the other a neuropsychiatric mood disorder. There is no established clinical or epidemiological relationship between the two conditions.

The mechanistic rationale supplied by the model is speculative: *COMP* mutations cause chondrocyte endoplasmic-reticulum stress and an unfolded-protein response, and lithium's known GSK-3β/Wnt inhibition could theoretically influence chondrocyte differentiation. However, this connection is not supported by any direct study of lithium in pseudoachondroplasia — it reflects TxGNN's knowledge-graph embedding similarity only, not experimental or clinical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

No marketing authorization is currently registered for lithium carbonate in New Zealand under this evidence pack (`total_licenses: 0`, market status: Not Marketed).

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: `safety.key_warnings`, `safety.contraindications`, and DDI data are all flagged as data gaps in this pack — TFDA/Medsafe label data has not yet been retrieved, which is also listed as a Blocking data gap (DG001) preventing S1 safety review.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The top-ranked prediction (Pseudoachondroplasia, L5/S0) has zero supporting clinical trials or literature — it is model output only, with a purely theoretical mechanistic link.
- Worth flagging separately: among the 10 candidate indications in this pack, **WHIM syndrome** (rank 9, L4/S1, "Research Question") stands out with a more concrete mechanistic rationale — lithium's established clinical use for raising neutrophil counts (via G-CSF stimulation) parallels the neutropenia seen in WHIM syndrome — making it a more plausible research lead than the top-ranked skeletal dysplasias, even though it is not the top TxGNN score.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) — currently Blocking (DG001)
- Confirmed mechanism of action data from DrugBank (DG002)
- Original indication and licensing data, currently absent from this pack
- If pursuing WHIM syndrome as an alternative research question: a literature search specifically on lithium-induced granulopoiesis and any case reports/series in CXCR4-related neutropenia
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

