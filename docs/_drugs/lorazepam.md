---
layout: default
title: Lorazepam
parent: 僅模型預測 (L5)
nav_order: 210
evidence_level: L5
indication_count: 10
---

# Lorazepam
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

# Lorazepam: From Anxiety Disorder to Trigeminal Nerve Neoplasm

## One-Sentence Summary

Lorazepam is a benzodiazepine GABA-A receptor modulator, established internationally as an anxiolytic and sedative-hypnotic (Taiwan-specific licensed indication text is not available in this evidence pack — the drug is currently **not marketed** in Taiwan). The TxGNN model's top-ranked prediction is **Trigeminal Nerve Neoplasm**, but this candidate is supported by **0 clinical trials** and **0 publications**, and the pipeline's own mechanistic review flags it as a likely false-positive association.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in Taiwan license data (lorazepam is internationally established for anxiety disorders / short-term anxiolytic and sedative use) |
| Predicted New Indication | Trigeminal Nerve Neoplasm |
| TxGNN Prediction Score | 99.87% |
| Evidence Level | L5 |
| Taiwan Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (blocking data gap). Based on known information, lorazepam is a benzodiazepine that acts as a positive allosteric modulator of the GABA-A receptor, increasing chloride channel opening frequency to produce sedative, anxiolytic, and anticonvulsant effects — a well-established, class-wide mechanism rather than a novel hypothesis.

However, this specific prediction does not hold up mechanistically. There is no known relationship between GABA-A modulation and the pathophysiology of a nerve sheath neoplasm. The evidence pack's own rationale assesses this as a likely **model false positive**, possibly arising from data confusion between "trigeminal nerve neoplasm" and "trigeminal neuralgia" — a symptomatic pain condition for which benzodiazepines are sometimes used adjunctively, but which is pathologically unrelated to tumour growth. No clinical trial or literature signal exists to counter this assessment.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Taiwan Market Information

Lorazepam is not currently marketed in Taiwan (0 licenses on file), so no product authorization records are available to list.

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are all currently unavailable — TFDA package insert extraction is a **blocking** data gap that prevents any S1 safety evaluation.)

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (Trigeminal Nerve Neoplasm) has zero clinical trial or literature support, and the pipeline's mechanistic review independently assesses it as a probable model artifact rather than a genuine pharmacological signal. Combined with a blocking gap in TFDA safety data, there is no basis to advance this candidate.

**To proceed, the following is needed:**
- TFDA package insert extraction (warnings/contraindications) — blocking gap DG001
- DrugBank mechanism-of-action data — gap DG002
- Any preclinical or case-level evidence specifically linking GABA-A modulation to trigeminal nerve neoplasm biology (none currently found)
- Consider redirecting evaluation to the model's rank-2 candidate (**insomnia**, L2 evidence, 23 trials/18 publications identified, "Proceed with Guardrails"), which is far better supported by existing evidence than this top-ranked candidate
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

