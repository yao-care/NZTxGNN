---
layout: default
title: Eplerenone
parent: 僅模型預測 (L5)
nav_order: 137
evidence_level: L5
indication_count: 5
---

# Eplerenone
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

# Eplerenone: From Hypertension / Heart Failure to Pulmonary Hypertension Owing to Lung Disease and/or Hypoxia

## One-Sentence Summary

Eplerenone is a selective mineralocorticoid receptor antagonist (MRA), approved internationally for hypertension and heart failure with reduced ejection fraction (HFrEF), though it is not currently registered in Taiwan.
The TxGNN model predicts it may be effective for **Pulmonary Hypertension Owing to Lung Disease and/or Hypoxia** (Group 3 PH),
with **0 clinical trials** and **20 publications** retrieved — all of which are general hypoxia biology reviews and do not directly study eplerenone in this indication.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hypertension; Heart failure with reduced ejection fraction (international approvals; no Taiwan registration) |
| Predicted New Indication | Pulmonary Hypertension Owing to Lung Disease and/or Hypoxia (Group 3 PH) |
| TxGNN Prediction Score | 99.50% |
| Evidence Level | L4 |
| Taiwan Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Eplerenone is a selective mineralocorticoid receptor antagonist (MRA) that competitively blocks the aldosterone receptor with greater selectivity than spironolactone, reducing off-target steroid side effects. The theoretical bridge to Group 3 pulmonary hypertension runs through the aldosterone–MR–RAAS axis: chronic hypoxia activates the renin–angiotensin–aldosterone system, and excess aldosterone is known to promote pulmonary vascular fibrosis, endothelial dysfunction, and right ventricular remodeling. In theory, blocking mineralocorticoid receptors could interrupt hypoxia-induced pulmonary vascular remodeling via the HIF-1α/RAAS interaction — a mechanistically coherent but as yet unvalidated pathway.

However, this reasoning is indirect. The 20 retrieved literature items are all general reviews of hypoxia biology — covering topics such as cerebral hypoxia, tumor hypoxia, and altitude physiology — and none directly evaluate eplerenone's efficacy in pulmonary hypertension. The gap between biological plausibility and actual clinical or preclinical data for this specific drug–disease pair is substantial.

It should also be noted that eplerenone's detailed mechanism of action data (MOA) was flagged as a data gap in this evidence pack, and Taiwan regulatory data is unavailable. Safety and efficacy conclusions must therefore be drawn from international sources pending further data retrieval.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

> ⚠️ **Important caveat**: The 20 retrieved publications are general hypoxia biology reviews and are not specific to eplerenone use in pulmonary hypertension. They provide background context on the disease mechanism but do not constitute direct clinical or preclinical evidence for this drug–indication pair. The relevance classification for all items was marked as "pending" in the evidence pack.

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [11172576](https://pubmed.ncbi.nlm.nih.gov/11172576/) | 2000 | Review | Respiratory Care Clinics of North America | Reviews four core mechanisms of hypoxemia (hypoventilation, V/Q mismatch, right-to-left shunt, low FiO₂); foundational disease-mechanism background |
| [21328446](https://pubmed.ncbi.nlm.nih.gov/21328446/) | 2011 | Review | Journal of Cellular Biochemistry | Hypoxia modulates growth, metabolism, angiogenesis, and pH homeostasis; contributes to vascular disease and cancer pathogenesis |
| [31706510](https://pubmed.ncbi.nlm.nih.gov/31706510/) | 2019 | Review | Trends in Cancer | Deubiquitinases (DUBs) regulate HIF abundance under hypoxia; emerging role as drug targets in cancer |
| [31961750](https://pubmed.ncbi.nlm.nih.gov/31961750/) | 2020 | Review | Annual Review of Immunology | Hypoxia shapes innate immunity and inflammatory responses through HIF-mediated oxygen sensing in healthy and inflamed tissue |
| [33862277](https://pubmed.ncbi.nlm.nih.gov/33862277/) | 2021 | Review | Ageing Research Reviews | Hypoxia in pulmonary disease or high altitude can cause neurodegeneration; paradoxically may also confer neuroprotection in aging |
| [34535359](https://pubmed.ncbi.nlm.nih.gov/34535359/) | 2021 | Review | Clinical Oncology | Tumor hypoxia drives radiotherapy and immunotherapy resistance; reviews hypoxia-targeted therapeutic modification strategies |
| [34618295](https://pubmed.ncbi.nlm.nih.gov/34618295/) | 2022 | Review | Metabolic Brain Disease | Acute and chronic hypoxia cause cognitive dysfunction through multiple molecular mechanisms; clinical and preclinical evidence reviewed |
| [37328448](https://pubmed.ncbi.nlm.nih.gov/37328448/) | 2023 | Original Research | Advanced Science | HIF-1α/NAT10/SEPT9 positive feedback loop drives glycolysis addiction and hypoxia tolerance in gastric cancer; anti-angiogenic resistance mechanism |
| [40347693](https://pubmed.ncbi.nlm.nih.gov/40347693/) | 2025 | Review | Redox Biology | Hypoxia is a prominent but poorly understood feature of multiple sclerosis; interacts with inflammation and vascular dysfunction |
| [40815459](https://pubmed.ncbi.nlm.nih.gov/40815459/) | 2025 | Opinion/Review | Revista Medica del IMSS | Hypobaric hypoxia at altitude vs sea level; differences in acclimatization between high-altitude residents and lowland visitors |

---

## Taiwan Market Information

Eplerenone is not currently registered or marketed in Taiwan. No authorization records are available in the TFDA database. Internationally, eplerenone is approved (e.g., by the US FDA and EMA) for hypertension and heart failure following myocardial infarction, marketed under brand names such as Inspra.

---

## Safety Considerations

Please refer to the package insert for safety information.

> ⚠️ **Critical risk flag (Rank 4 predicted indication — Malignant Renovascular Hypertension)**: Renovascular hypertension, especially bilateral renal artery stenosis, is a relative contraindication for mineralocorticoid receptor antagonists. Aldosterone in this context serves as a compensatory renal perfusion mechanism; MR blockade may precipitate acute kidney injury and life-threatening hyperkalemia. If any research design considers eplerenone in patients with concurrent renovascular disease, this safety signal must be explicitly addressed.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN model assigns a high prediction score (99.50%) for eplerenone in Group 3 pulmonary hypertension, and there is a biologically plausible mechanistic link via RAAS/aldosterone-driven pulmonary vascular remodeling. However, no clinical trials exist for this indication, and all 20 retrieved publications are general hypoxia biology reviews with no direct eplerenone-in-PH data. The evidence is classified as L4 (mechanistic inference only), which is insufficient to advance to clinical evaluation without dedicated preclinical validation.

**To proceed, the following is needed:**
- Dedicated preclinical studies (animal models of hypoxic PH) evaluating eplerenone's effect on pulmonary arterial remodeling, right ventricular hypertrophy, and hemodynamics
- Retrieval of eplerenone-specific MOA data from DrugBank to formally confirm the aldosterone–MR–HIF-1α mechanistic link (currently flagged as a data gap)
- Full safety review: download and parse the TFDA package insert (or international equivalents) to establish key warnings, contraindications, and drug interactions — currently all are marked as data gaps
- Targeted PubMed search refined to *eplerenone* AND (*pulmonary hypertension* OR *pulmonary vascular remodeling* OR *right ventricular*) to retrieve any missed preclinical or clinical publications not captured by the current hypoxia-broad query
- Assessment of eplerenone's known safety profile (hyperkalemia, renal impairment risk) in the target population — Group 3 PH patients often carry concurrent lung disease, cardiac comorbidities, and reduced renal reserve, all of which increase MRA-related adverse event risk
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

