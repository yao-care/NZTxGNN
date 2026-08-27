---
layout: default
title: Propylthiouracil
parent: 僅模型預測 (L5)
nav_order: 295
evidence_level: L5
indication_count: 3
---

# Propylthiouracil
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Propylthiouracil: From Hyperthyroidism to Thyroid Hormone Resistance (THRB Mutation)

## One-Sentence Summary

Propylthiouracil (PTU) is a thionamide-class antithyroid drug historically used to manage hyperthyroidism, including Graves' disease. TxGNN predicts a possible link to **resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta (RTH-β)**, but this is currently supported only by **0 clinical trials** and **6 case-report/mechanistic publications** — evidence is preliminary at best.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from New Zealand regulatory data (no licenses on record); based on established pharmacology, PTU is used for hyperthyroidism (e.g., Graves' disease) |
| Predicted New Indication | Resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta |
| TxGNN Prediction Score | 99.66% (global rank 3394) |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, propylthiouracil is a thionamide that inhibits thyroid peroxidase, blocking new thyroid hormone synthesis and also reducing peripheral conversion of T4 to T3 — its efficacy in hyperthyroidism (notably Graves' disease) is well established.

Resistance to thyroid hormone (RTH-β) is superficially related because it shares the same hormone axis and can present with biochemical findings that mimic hyperthyroidism (elevated T4/T3), sometimes leading to misdiagnosis. This overlap likely explains why TxGNN links the two conditions.

However, the mechanistic fit is questionable: RTH-β is caused by reduced *receptor* responsiveness to thyroid hormone, not excess hormone production. PTU works upstream by suppressing synthesis, so it does not correct the underlying receptor defect. Consistent with this, several of the literature items below describe RTH patients who were misdiagnosed as thyrotoxic and treated with PTU without clinical benefit (e.g., worsening goiter). This should be treated as a caution flag rather than confirmatory evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10724359](https://pubmed.ncbi.nlm.nih.gov/10724359/) | 1999 | Case report | Endocrine Journal | Thai woman with de novo L330S THRB mutation; previously treated with propylthiouracil for presumed thyrotoxicosis, with goiter enlarging rather than improving — illustrates misdiagnosis risk and lack of PTU benefit in RTH |
| [12201835](https://pubmed.ncbi.nlm.nih.gov/12201835/) | 2002 | Case report | Clinical Endocrinology | Two RTH cases (M313T mutation) in one family, including neonatal thyrotoxicosis; discusses diagnostic overlap with true thyrotoxicosis |
| [18561095](https://pubmed.ncbi.nlm.nih.gov/18561095/) | 2009 | Case report | Exp Clin Endocrinol Diabetes | Turkish family with P453A THRB mutation causing RTH; describes clinical/biochemical presentation |
| [14684607](https://pubmed.ncbi.nlm.nih.gov/14684607/) | 2004 | Preclinical (animal) | Endocrinology | Mouse model study of dominant-negative TR-β mutation (Δ337T) and its cardiac effects in RTH |
| [21909131](https://pubmed.ncbi.nlm.nih.gov/21909131/) | 2012 | Preclinical (animal) | Oncogene | Thrb(PV/PV) mouse model shows thyroid hormone activates tumor cell proliferation via mutant TR-β, linking RTH mutation to follicular thyroid carcinoma |
| [22919057](https://pubmed.ncbi.nlm.nih.gov/22919057/) | 2012 | Preclinical (animal) | Endocrinology | Role of TSH in spontaneous thyroid carcinoma development in mice with heterozygous THRB mutation |

---

## New Zealand Market Information

Propylthiouracil is not currently authorized for marketing in New Zealand (0 licenses on record in this evidence pack).

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA label warnings/contraindications and drug interaction data are marked as a Blocking data gap in this evidence pack — see Conclusion.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for this indication is limited to case reports and preclinical/animal mechanism studies (no clinical trials, no observational cohorts), and several of the case reports actually document PTU being *ineffective* when misapplied to RTH patients — mechanistically, PTU suppresses hormone synthesis but does not address the receptor-level defect that defines RTH. Combined with a Blocking data gap on safety labeling, the evidence does not support advancing this candidate at this time.

**To proceed, the following is needed:**
- TFDA/regulatory package insert data (warnings, contraindications) — currently a Blocking gap
- Confirmed mechanism of action (DrugBank MOA) to properly assess mechanistic plausibility for RTH-β
- Expert endocrinology review reconciling why a synthesis inhibitor would benefit a receptor-resistance disorder
- If pursued, a defined regulatory pathway for New Zealand market entry (currently unmarketed, 0 licenses)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

