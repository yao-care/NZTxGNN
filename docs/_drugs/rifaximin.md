---
layout: default
title: Rifaximin
parent: 僅模型預測 (L5)
nav_order: 305
evidence_level: L5
indication_count: 6
---

# Rifaximin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

Not needed here — I have everything required directly in the evidence pack. Proceeding to generate the report.

# Rifaximin: From (No Approved Indication on File) to Oral Candidiasis

## One-Sentence Summary

> Rifaximin is a rifamycin-class, gut-restricted oral antibiotic; it is not currently marketed in Taiwan and no approved-indication text is on file. The TxGNN model predicts it may be effective for **Oral Candidiasis**, but this direction is supported by only **1 publication** (no clinical trials), and that publication actually reports rifaximin use as a *risk factor* for resistant *Candida* infection — not a treatment effect.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no Taiwan license/approved-indication text on file (drug is not marketed in Taiwan) |
| Predicted New Indication | Oral Candidiasis |
| TxGNN Prediction Score | 99.75% |
| Evidence Level | L5 (model prediction only; the one literature hit contradicts rather than supports efficacy) |
| Taiwan Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for rifaximin is not available in this evidence pack. Based on general pharmacological knowledge, rifaximin is a non-absorbable, gut-restricted rifamycin derivative that inhibits bacterial DNA-dependent RNA polymerase; it is used clinically (outside Taiwan) for conditions such as hepatic encephalopathy, IBS-D, and traveler's diarrhea. It has **no known antifungal or anti-*Candida* mechanism**.

Oral candidiasis is a fungal infection, and there is no established pharmacological pathway by which a gut-selective antibacterial agent would be expected to treat it. The only literature retrieved for this prediction (PMID 34180023) points in the *opposite* direction: it describes rifaximin use as favouring the emergence of micafungin-resistant *Candida* infections in allogeneic HSCT recipients, most likely via disruption of gut microbial balance rather than any therapeutic effect. This is evidence of a **risk association, not a treatment signal**.

Taken together, the TxGNN score likely reflects proximity in the knowledge-graph embedding space (e.g., shared "gut/infection" nodes) rather than a genuine, evidence-backed mechanistic link. This is not a case where absence of trials simply reflects an early-stage but plausible hypothesis — the one piece of available evidence actively argues against the prediction.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34180023](https://pubmed.ncbi.nlm.nih.gov/34180023/) | 2021 | Case report/observational | Annals of Hematology | In allogeneic HSCT recipients, rifaximin use was associated with (favoured) emergence of micafungin-resistant *Candida* spp. infections — a risk signal, not a treatment finding |

---

## Taiwan Market Information

No authorizations on file. Rifaximin is not currently marketed in Taiwan (0 licenses).

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug–drug interaction data are not yet available in this evidence pack — TFDA package insert retrieval is flagged as a blocking data gap.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests solely on a TxGNN embedding score with no clinical trials and no mechanistic rationale for antifungal activity. The single available publication points against, not toward, therapeutic benefit — rifaximin use was linked to *increased* resistant-*Candida* risk rather than efficacy against oral candidiasis. This is insufficient, and directionally unfavorable, evidence to advance.

**To proceed, the following is needed:**
- TFDA package insert / warnings and contraindications data (currently a blocking data gap)
- Confirmed mechanism-of-action data for rifaximin (currently a data gap)
- In vitro or preclinical data testing rifaximin against *Candida* species, since no antifungal mechanism is currently established
- Independent re-review of the TxGNN prediction given that the only real-world evidence found contradicts the predicted direction
- If pursued further, safety monitoring should specifically address gut microbiome disruption and secondary fungal infection risk, not just standard antibacterial safety review
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

