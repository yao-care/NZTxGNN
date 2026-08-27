---
layout: default
title: Valine
parent: 僅模型預測 (L5)
nav_order: 359
evidence_level: L5
indication_count: 10
---

# Valine
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

# Valine: From Essential Amino Acid Nutrient to Sclerosing Cholangitis

## One-Sentence Summary

Valine (DrugBank DB00161) is a branched-chain essential amino acid with no established therapeutic indication on file and is not currently marketed in New Zealand. The TxGNN model predicts a possible association with **Sclerosing Cholangitis**, but this direction is currently supported by only **2 publications** and **0 clinical trials**, and neither publication directly confirms a causal or therapeutic role for valine in this disease.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not applicable — no approved therapeutic indication on file (essential amino acid / nutritional component) |
| Predicted New Indication | Sclerosing Cholangitis |
| TxGNN Prediction Score | 99.42% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, valine is a branched-chain amino acid (BCAA) that functions primarily as a nutritional/metabolic substrate rather than a conventional pharmacological agent, and it has no on-file approved therapeutic indication to compare against sclerosing cholangitis.

The two literature hits returned for this pair are only loosely connected to the prediction. One (PMID 15790420) actually investigates plasma **tyrosine** — not valine — in relation to fatigue in primary sclerosing cholangitis and primary biliary cirrhosis, and is a keyword-adjacent rather than direct match. The other (PMID 39015781) is a Mendelian randomization study of blood metabolites and cholestatic liver disease risk, but it does not confirm valine specifically as a significant causal metabolite for sclerosing cholangitis.

Given the absence of MOA data, the absence of any clinical trial evidence, and the fact that neither retrieved publication directly substantiates a valine–sclerosing cholangitis mechanistic link, the biological plausibility of this prediction cannot currently be established beyond the model's statistical association.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39015781](https://pubmed.ncbi.nlm.nih.gov/39015781/) | 2024 | Mendelian Randomization | Frontiers in Medicine | Investigates causal relationships between blood metabolites/metabolic pathways and cholestatic liver diseases (PBC, PSC); does not confirm valine as a significant causal metabolite. |
| [15790420](https://pubmed.ncbi.nlm.nih.gov/15790420/) | 2005 | Observational | BMC Gastroenterology | Examines plasma amino acid patterns and fatigue in PBC/PSC; the amino acid studied is tyrosine, not valine — only tangentially relevant. |

---

## New Zealand Market Information

Valine is not currently marketed in New Zealand (0 authorizations on file); no product license data is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but there are no clinical trials and no literature directly confirming a mechanistic or causal link between valine and sclerosing cholangitis — the two retrieved papers are tangential (wrong amino acid) or inconclusive (metabolite not confirmed causal). Combined with the lack of MOA data and the drug's unmarketed status in New Zealand, evidence is insufficient to advance beyond model prediction.

**To proceed, the following is needed:**
- Confirmed mechanism of action data for valine (DrugBank API query, currently a blocking data gap)
- A follow-up Mendelian randomization or metabolomic study specifically isolating valine (not aggregate BCAA/amino acid panels) as a causal factor in sclerosing cholangitis
- TFDA/regulatory package insert or safety monograph data, since none is currently available
- Preclinical or mechanistic studies establishing biological plausibility before considering any clinical investigation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

