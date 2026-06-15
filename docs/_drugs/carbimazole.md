---
layout: default
title: Carbimazole
parent: 僅模型預測 (L5)
nav_order: 64
evidence_level: L5
indication_count: 3
---

# Carbimazole
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

# Carbimazole: From Hyperthyroidism to Resistance to Thyroid Hormone due to TR-β Mutation

## One-Sentence Summary

Carbimazole is a thionamide-class antithyroid prodrug (of methimazole) established in the treatment of hyperthyroidism, including Graves' disease, by blocking thyroid hormone synthesis.
The TxGNN model predicts it may be effective for **resistance to thyroid hormone due to a mutation in thyroid hormone receptor beta (RTH-β)**, yet this prediction is mechanistically counterintuitive — suppressing T4/T3 in RTH-β patients is likely to worsen rather than improve the condition.
Currently **0 clinical trials** and **1 publication** support this direction, yielding an evidence level of **L5 (model prediction only)**.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hyperthyroidism (including Graves' disease, toxic nodular goitre) |
| Predicted New Indication | Resistance to thyroid hormone due to TR-β mutation (RTH-β) |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Carbimazole is a prodrug that is rapidly converted to methimazole after oral administration. As a thionamide antithyroid agent, it inhibits thyroid peroxidase (TPO), the enzyme responsible for organification of iodine and coupling of iodotyrosines — thereby blocking the synthesis of T3 and T4. It is an established first-line or second-line treatment for primary hyperthyroidism caused by excess thyroid hormone production. Detailed MOA data is not currently available in the DrugBank record, but the above mechanism is well characterised in the clinical literature.

RTH-β arises from loss-of-function mutations in the thyroid hormone receptor beta (TR-β) gene. Because the pituitary gland relies on TR-β to sense circulating T4/T3 and suppress TSH secretion, a mutated receptor creates a state of central insensitivity: TSH remains non-suppressed or even elevated despite high serum thyroid hormones, and the thyroid gland is continuously over-stimulated. Applying carbimazole in this context would lower circulating T4/T3 further, removing whatever residual negative feedback exists and driving TSH even higher — paradoxically worsening thyroid stimulation rather than correcting it.

The slim mechanistic argument in favour involves TR-α receptors, which remain intact in RTH-β patients. Peripheral tissues that predominantly use TR-α (heart, skeletal muscle) will experience full thyrotoxic effects from elevated T4/T3. In rare patients with severe cardiac or neuromuscular symptoms, a case could be made for partial suppression with antithyroid drugs as a symptomatic measure; however, the overall risk-to-benefit balance is unfavourable. This is a textbook example of a high-score TxGNN prediction that directly contradicts established endocrine physiology, and the one identified publication reinforces this concern rather than supporting therapeutic use.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [24165508](https://pubmed.ncbi.nlm.nih.gov/24165508/) | 2013 | Case Report / Diagnostic Review | BMJ Case Reports | A young man with persistently elevated fT4 (25–35.7 pmol/L) and paradoxically non-suppressed TSH (6.78–22.1 mIU/L) was misdiagnosed as hyperthyroid and treated intermittently with carbimazole for 10 years — a presentation strongly consistent with undiagnosed RTH-β. The case illustrates the clinical hazard of using carbimazole without confirming the underlying aetiology, and does not report therapeutic benefit. |

---

## New Zealand Market Information

Carbimazole currently holds no Medsafe authorisations in New Zealand. Please refer to international product labelling (e.g., UK, Australia, or the DrugBank monograph for DB00389) for dosage, formulation, and approved indication details.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Although TxGNN assigns a high prediction score of 99.71%, the mechanistic logic runs directly counter to established endocrine physiology: carbimazole suppresses thyroid hormone synthesis in a disease where the core defect is impaired T4/T3 feedback at the pituitary, meaning treatment would amplify TSH drive and likely worsen the condition. The only identified publication is a case report documenting inadvertent misuse, not a therapeutic benefit.

**To proceed, the following is needed:**
- Expert endocrinology review to identify any RTH-β sub-phenotype (e.g., severe peripheral thyrotoxic symptoms via TR-α) where time-limited antithyroid therapy might be justified
- Retrieval of complete MOA data from DrugBank (currently unavailable)
- Retrieval of formal safety, warning, and contraindication data (TFDA package insert; currently a blocking data gap)
- Clarification of whether TxGNN's graph embedding is capturing the structural similarity between RTH-β and Graves' disease nodes rather than a true mechanistic repurposing signal
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

