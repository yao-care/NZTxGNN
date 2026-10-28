---
layout: default
title: Eplerenone
parent: Model Prediction Only (L5)
nav_order: 182
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-28
monitor_inn: "EPLERENONE"
predicted_indications: ["pulmonary hypertension owing to lung disease and/or hypoxia", "pulmonary hypertension with unclear multifactorial mechanism", "malignant hypertensive renal disease", "Braddock syndrome"]
description: "Eplerenone: predicted new indication Pulmonary hypertension owing to lung disease and/or hypoxia (evidence level L5). Approved use: Not recorded in the local data"
---

# Eplerenone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Eplerenone: Pulmonary hypertension owing to lung disease and/or hypoxia
{: .text-alpha }

## One-Sentence Summary

Eplerenone is a mineralocorticoid receptor antagonist originally used to treat hypertension.
The TxGNN model predicts it may be effective for **pulmonary hypertension owing to lung disease and/or hypoxia**,
but there are **no registered clinical trials** and the **13 publications** retrieved are mostly preclinical, indirect, or review-level, so this remains a research question.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pulmonary hypertension owing to lung disease and/or hypoxia |
| TxGNN Prediction Score | 0.9950 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, eplerenone is a selective mineralocorticoid receptor (MR) antagonist. Its efficacy in hypertension is established, and mechanistically it may be applicable to pulmonary hypertension driven by hypoxia.

The literature points to a plausible link. Hypoxia can activate MR signalling and aldosterone synthesis. In preclinical work, hypoxia-driven aldosterone production in pulmonary artery endothelial cells promoted pulmonary vascular fibrosis, and aldosterone signalling promoted abnormal pulmonary artery smooth muscle cell survival. In animal models, eplerenone also reduced hypoxia-related atrial remodelling, and MR antagonists improved established pulmonary arterial hypertension in a rat model.

These findings are indirect. The human data come from obstructive sleep apnoea with systemic hypertension, not from pulmonary arterial pressure or pulmonary vascular remodelling. The very high TxGNN score is a computational prediction and not clinical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27230560](https://pubmed.ncbi.nlm.nih.gov/27230560/) | 2016 | Clinical study (small) | Pol Arch Med Wewn | Eplerenone tested on obstructive sleep apnoea severity and arterial stiffness in resistant hypertension; indirect to pulmonary hypertension |
| [41483355](https://pubmed.ncbi.nlm.nih.gov/41483355/) | 2026 | Review | Curr Hypertens Rep | Reviews aldosterone blockade for blood pressure and oxygenation in sleep apnoea-related hypertension |
| [35043674](https://pubmed.ncbi.nlm.nih.gov/35043674/) | 2022 | Preclinical (rat) | Am J Physiol Lung Cell Mol Physiol | Eplerenone and spironolactone tested in established pulmonary arterial hypertension in the SU5416-hypoxia rat model, focusing on interventricular dependence |
| [29499691](https://pubmed.ncbi.nlm.nih.gov/29499691/) | 2018 | Preclinical | BMC Pulm Med | Eplerenone attenuated pulmonary vascular remodelling more than right ventricular remodelling in pulmonary arterial hypertension |
| [25001622](https://pubmed.ncbi.nlm.nih.gov/25001622/) | 2014 | Preclinical | Circulation | Hypoxia upregulates StAR and stimulates aldosterone synthesis in pulmonary artery endothelial cells, promoting vascular fibrosis |
| [27006450](https://pubmed.ncbi.nlm.nih.gov/27006450/) | 2016 | Preclinical | FASEB J | Aldosterone upregulates Raptor (mTORC1) and promotes abnormal pulmonary artery smooth muscle cell survival |
| [35568141](https://pubmed.ncbi.nlm.nih.gov/35568141/) | 2022 | Preclinical (rat) | Cardiovasc Pathol | Eplerenone improved atrial remodelling induced by chronic intermittent hypoxia |
| [35346763](https://pubmed.ncbi.nlm.nih.gov/35346763/) | 2022 | Preclinical (rat) | Exp Gerontol | Chronic intermittent hypoxia induced renal fibrosis via MR activation |
| [15199296](https://pubmed.ncbi.nlm.nih.gov/15199296/) | 2004 | Review | Curr Opin Nephrol Hypertens | Glucocorticoids can activate the mineralocorticoid receptor and contribute to hypertension |
| [39707386](https://pubmed.ncbi.nlm.nih.gov/39707386/) | 2024 | Preclinical | Cell Commun Signal | Eplerenone reduced cirrhosis-associated changes in hepatocyte glucose and lipid metabolism; hypoxia can activate hepatocyte MR |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2619512 | Inspra (valid) | Tab 25 mg | Not available |
| 2619520 | Inspra (valid) | Tab 50 mg | Not available |

---

## Safety Considerations

- **Disease-level contraindication signals (knowledge graph, not label text)**: liver disease, gallbladder disease, diabetic nephropathy, impaired renal function disease.
- **Drug Interactions**: 100 interactions were identified in total. Major-level examples include:
  - Aprepitant
  - Clarithromycin
  - Clotrimazole
  - Potassium chloride, potassium citrate and potassium bicarbonate (risk of hyperkalaemia)

  Moderate-level examples include corticosteroids (hydrocortisone, dexamethasone, prednisone, and others), SGLT2 inhibitors (canagliflozin, dapagliflozin, empagliflozin), acetylsalicylic acid, bupropion, miconazole, and the cannabinoids dronabinol and nabilone.

Package insert warnings and contraindications were not available for this report; please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a very high TxGNN score and on preclinical or indirect evidence. No clinical trials target pulmonary hypertension, and no human data address pulmonary pressures.

**To proceed, the following is needed:**
- Direct clinical evidence in pulmonary hypertension, such as a pilot or Phase 2 study with haemodynamic endpoints
- Detailed mechanism of action data
- Medsafe package insert warnings and contraindications, which are required for safety screening
- A regulatory pathway given the cancelled licences, including consideration of a new application or access route
- A safety plan for hyperkalaemia and renal impairment, given the interaction and contraindication signals
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

