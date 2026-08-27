---
layout: default
title: Levothyroxine
parent: 僅模型預測 (L5)
nav_order: 206
evidence_level: L5
indication_count: 10
---

# Levothyroxine
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

# Levothyroxine: From Hypothyroidism to Endemic Goiter

## One-Sentence Summary

> Levothyroxine (LT4) is a synthetic thyroid hormone whose established use is thyroid hormone replacement in hypothyroidism (general pharmacological knowledge; this specific indication is not captured in the current evidence pack).
> The TxGNN model predicts it may also be effective for **Endemic Goiter**,
> with **1 clinical trial** and **20 publications** currently associated with this direction — though most of the literature addresses iodine-deficiency epidemiology/pathophysiology rather than LT4 efficacy directly.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not captured in evidence pack (general knowledge: hypothyroidism / thyroid hormone replacement) |
| Predicted New Indication | Endemic Goiter |
| TxGNN Prediction Score | 99.81% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Research Question |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (Data Gap). Based on general pharmacological knowledge, levothyroxine is synthetic T4, the standard replacement therapy for hypothyroidism, and its efficacy in that setting is well established.

Endemic goiter is caused primarily by iodine deficiency, which impairs thyroid hormone synthesis and drives compensatory thyroid enlargement via elevated TSH. Iodine supplementation is the traditional first-line intervention, but exogenous LT4/iodide combination therapy has long been used clinically to normalize thyroid hormone levels and suppress TSH-driven goiter growth — a mechanism directly analogous to LT4's role in hypothyroidism.

However, the current evidence base for this specific indication is thin: it consists mostly of epidemiological and pathophysiological literature on iodine deficiency and endemic goiter rather than direct randomized trials of LT4 itself. One multicenter study (PMID 3278876) directly compares LT4 monotherapy versus LT4+iodide in endemic goiter, which is the strongest direct signal currently available.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04482907](https://clinicaltrials.gov/study/NCT04482907) | N/A | Completed | 68 | Randomized placebo-controlled study of dill (*Anethum graveolens*) extract, not levothyroxine, in thyroiditis/nodular goiter patients; evaluated hormone levels, inflammatory markers, and nodule size by ultrasound. Relevance to LT4 is low (Grade C — different intervention, overlapping disease population only). |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3278876](https://pubmed.ncbi.nlm.nih.gov/3278876/) | 1988 | Multicenter comparative study | Deutsche medizinische Wochenschrift | 74 patients with diffuse endemic goiter treated with LT4 150µg/day alone vs. LT4 100µg + potassium iodide daily for 6 months, followed by iodide prophylaxis; compared goiter volume reduction between groups. |
| [25629792](https://pubmed.ncbi.nlm.nih.gov/25629792/) | 2015 | Cohort | Current Medical Research and Opinion | Examined thyroid function and birth outcomes in 460 pregnant women from non-goiter vs. endemic goiter areas (with/without iodine supplementation). |
| [4312017](https://pubmed.ncbi.nlm.nih.gov/4312017/) | 1969 | Trial (older) | American Journal of Clinical Nutrition | Prophylaxis and treatment of endemic goiter with iodized oil in rural Ecuador and Peru. |
| [36839362](https://pubmed.ncbi.nlm.nih.gov/36839362/) | 2023 | Review | Nutrients | Overview of iodine deficiency and iodine prophylaxis, covering thyroid hormone synthesis requirements and at-risk populations. |
| [2031356](https://pubmed.ncbi.nlm.nih.gov/2031356/) | 1991 | Review | World Journal of Surgery | Establishes iodine deficiency as primary cause of endemic goiter; discusses iodine prophylaxis programs and remaining causative factors. |
| [7704809](https://pubmed.ncbi.nlm.nih.gov/7704809/) | 1994 | Review | Current Therapy in Endocrinology and Metabolism | General review of endemic goiter etiology and management. |
| [6309889](https://pubmed.ncbi.nlm.nih.gov/6309889/) | 1983 | Clinical study | JCEM | Iodized oil injection in 58 goitrous patients from mildly iodine-deficient area; goiter size decreased, thyroid hormone/immunologic parameters tracked over 6 months. |
| [6304776](https://pubmed.ncbi.nlm.nih.gov/6304776/) | 1983 | Clinical study | Progress in Clinical and Biological Research | TSH secretion and regulation in endemic goiter/cretinism; shows inverse correlation between iodine intake and serum TSH. |
| [3098822](https://pubmed.ncbi.nlm.nih.gov/3098822/) | 1986 | Clinical study | Journal of Endocrinological Investigation | Serum thyroglobulin and TSH response to TRH in 56 nodular goiter patients from a severely iodine-deficient endemic area. |
| [263304](https://pubmed.ncbi.nlm.nih.gov/263304/) | 1978 | Clinical study | JCEM | Relationship between maternal thyroid status and fetal hypothyroidism in a severe endemic goiter region. |

---

## New Zealand Market Information

Currently not marketed in New Zealand; no product authorization records are available in the evidence pack (0 licenses on file).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Research Question**

**Rationale:**
The mechanistic link between LT4 and endemic goiter is well established (TSH suppression via exogenous thyroid hormone), and one multicenter comparative study directly supports LT4 use, but the surrounding evidence base is otherwise epidemiological/pathophysiological rather than interventional (Evidence Level L3), so this indication is not yet ready for a Go decision.

**To proceed, the following is needed:**
- TFDA/NZ package insert data — warnings and contraindications (currently a **Blocking** data gap that prevents entry into the S1 safety pre-assessment stage)
- DrugBank mechanism-of-action detail (High-severity data gap)
- Additional direct RCT evidence evaluating LT4 (not iodine alone) specifically in endemic goiter populations
- Confirmation of New Zealand licensing/market status, since no current authorizations exist
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

