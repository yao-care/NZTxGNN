---
layout: default
title: Cabergoline
parent: 僅模型預測 (L5)
nav_order: 57
evidence_level: L5
indication_count: 5
---

# Cabergoline
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

# Cabergoline: From Prolactinoma to Pituitary Adenocarcinoma

## One-Sentence Summary

Cabergoline is a selective dopamine D2/D3 receptor agonist established as first-line treatment for prolactin-secreting pituitary adenomas (prolactinomas) and hyperprolactinemia.
The TxGNN model predicts it may be effective for **Pituitary Adenocarcinoma** — the rare malignant form of pituitary tumours.
Currently, **0 clinical trials** and **3 indirectly related publications** address this specific indication; the broader pituitary tumour evidence base provides mechanistic context, but direct support is absent.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Prolactinoma / Hyperprolactinemia |
| Predicted New Indication | Pituitary Adenocarcinoma |
| TxGNN Prediction Score | 99.06% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on established pharmacological knowledge, Cabergoline acts as a potent dopamine D2/D3 receptor agonist. Its primary therapeutic role is suppressing prolactin secretion from lactotroph cells in the pituitary gland, which in turn normalises prolactin levels and causes tumour shrinkage in prolactinomas. Recent research has expanded our understanding of its anti-tumour properties, showing that cabergoline can also induce autophagic cell death, cell cycle arrest, and apoptosis in pituitary tumour cells beyond simple hormone suppression — mechanisms reviewed in PMID 31597135.

Pituitary adenocarcinoma is the malignant counterpart of the far more common pituitary adenoma, defined by the presence of cerebrospinal fluid or distant metastases. Since both tumour types originate from pituitary gland cells and may express dopamine receptors, there is a plausible molecular rationale for TxGNN's prediction: if cabergoline exerts anti-proliferative effects in benign pituitary adenomas, it may carry analogous activity in malignant variants that retain dopaminergic signalling.

However, this extrapolation carries significant uncertainty. Pituitary adenocarcinoma represents fewer than 0.5% of all pituitary tumours, has a distinct and poorly characterised biology, and no clinical trial has specifically investigated cabergoline in this setting. The TxGNN model likely draws on the established cabergoline–pituitary tumour relationship (adenomas) and projects it onto the malignant end of the spectrum. This prediction is biologically reasoned but clinically unvalidated.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for pituitary adenocarcinoma specifically.

---

## Literature Evidence

The three retrieved publications are contextually related to cabergoline and pituitary pathology but do not directly study cabergoline as a treatment for pituitary adenocarcinoma.

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [41760078](https://pubmed.ncbi.nlm.nih.gov/41760078/) | 2026 | Case Report | Medicine | Multiple endocrine neoplasia type 1 (MEN1) with atypical course and uncertain MEN1 gene variant; illustrates coexistence of pituitary neoplasms within a multi-tumour syndrome relevant to the pituitary adenocarcinoma disease context |
| [20497940](https://pubmed.ncbi.nlm.nih.gov/20497940/) | 2010 | Case Report | Endocrine Practice | Long-term management of ectopic ACTH hypersecretion using cabergoline after adrenalectomy; demonstrates cabergoline's utility in controlling aggressive pituitary-related hormonal conditions |
| [33569966](https://pubmed.ncbi.nlm.nih.gov/33569966/) | 2021 | Case Report | Rev Esp Enf Dig | Patient with a known pituitary adenoma on cabergoline who was subsequently diagnosed with pancreatic adenocarcinoma; incidental co-occurrence, not evidence of anti-tumour activity |

---

## New Zealand Market Information

Cabergoline is not currently marketed in New Zealand. No authorisations are on record with Medsafe.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Pituitary adenocarcinoma is an ultra-rare malignancy for which no clinical trials of cabergoline exist, and the retrieved literature provides only incidental contextual evidence rather than direct therapeutic support. The TxGNN score is high, but this likely reflects the model's generalisation from cabergoline's well-established role in benign pituitary adenomas — a biological leap that requires prospective validation before any clinical inference can be drawn.

**To proceed, the following is needed:**
- Confirmed mechanism of action data (MOA) from DrugBank or pharmacological literature to support dopamine receptor expression in pituitary adenocarcinoma
- Systematic search for case reports or case series specifically documenting cabergoline use in pathologically confirmed pituitary adenocarcinoma
- Immunohistochemical or molecular profiling data on dopamine D2 receptor expression in pituitary adenocarcinoma tissue samples
- Full safety profile including key warnings and contraindications (TFDA/Medsafe package insert review)
- Drug interaction profile for cabergoline in oncology co-medication contexts
- Assessment of whether evidence from the closely related "pituitary cancer" predicted indication (Rank 3, 20 clinical trials, 20 publications) can be bridged to support this indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

