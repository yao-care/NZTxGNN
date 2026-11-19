---
layout: default
title: Isotretinoin
parent: Model Prediction Only (L5)
nav_order: 247
evidence_level: L5
indication_count: 1
last_modified_at: 2026-11-19
monitor_inn: "ISOTRETINOIN"
predicted_indications: ["malignant hypertensive renal disease"]
description: "Isotretinoin: predicted new indication Malignant hypertensive renal disease (evidence level L5). In New Zealand: 3 Pharmac Schedule items"
---

# Isotretinoin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Isotretinoin: Malignant hypertensive renal disease
{: .text-alpha }

## One-Sentence Summary

According to the source record, isotretinoin is listed as originally used for hypertensive disorders. That listing conflicts with its well-known clinical use, which is severe recalcitrant acne, so it is probably a data-mapping error.
The prediction rests only on a model score.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 0.9901 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 3 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for isotretinoin: "The exact mechanism of action of Zenatane in the treatment of severe recalcitrant nodular acne in nonpregnant patients 12 years of age and older with multiple inflammatory nodules with a diameter of 5 mm or greater who are unresponsive to conventional therapy, including systemic antibiotics is unknown. Isotretinoin, a retinoid, inhibits sebaceous gland function and keratinization." ([DailyMed: Zenatane (isotretinoin) prescribing information, Dr. Reddy's Laboratories Limited](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=27b3cf26-f22e-5b70-1c24-009933b7c6ee), label version 2026-09-20; accessed 2026-11-19.) Isotretinoin (13-cis-retinoic acid) is a retinoid that acts through retinoic acid receptors to regulate cell differentiation, proliferation, and apoptosis. Preclinical work has explored retinoids in relation to the renin-angiotensin system and vascular remodelling. This is speculative and has not been shown for malignant renovascular hypertension.

The listed original indications (hypertensive disorder, hypertension) do not match the known clinical use of isotretinoin, which is severe acne. This suggests a data-mapping problem in the source record. Until it is resolved, the apparent link between the "original" and the predicted indication cannot be relied on.

The high score (0.9901) is the only support. The two predictions are therefore not independent signals. Retinoic acid signalling has been studied preclinically in renal injury and fibrosis, but no direct link to malignant hypertensive nephropathy has been established.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2400286 | Oratane | Isotretinoin | Cap 10 mg | Yes |
| 2400278 | Oratane | Isotretinoin | Cap 20 mg | Yes |
| 2339161 | Oratane | Isotretinoin | Cap 5 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Known safety concerns**: Teratogenicity, dyslipidaemia, and hepatotoxicity are well-recognised concerns for isotretinoin. They are not documented in the data compiled for this report, so confirm them against the package insert.
- **Knowledge-graph contraindication signals** (these are graph associations, not label text): anxiety and neurotic disorders, dysthymic disorder, psychotic and other mental disorders, kidney disease, liver disease, gallbladder disease, pancreatitis, inflammatory bowel disease, osteoporosis, systemic lupus erythematosus, diabetes mellitus and diabetic ketoacidosis, angioedema (including hereditary forms), neutropenia and agranulocytosis, potassium deficiency, and hypotensive disorder.
- **Drug Interactions**: 98 interactions are recorded. Major-level interactions include:
  - Doxycycline
  - Minocycline
  - Tetracycline
  - Vitamin A

  Moderate-level interactions include:
  - Systemic corticosteroids (betamethasone, budesonide, dexamethasone, hydrocortisone, prednisolone, prednisone, triamcinolone)
  - Warfarin and dicoumarol
  - Naltrexone
  - Ethanol
  - Ivacaftor and lumacaftor
  - Nitisinone
  - Fostamatinib
  - Topical abametapir

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support is a model score, with no clinical trials, no literature, and no mechanistic annotation. The original-indication record conflicts with the drug's known use, and the safety profile is unfavourable for a chronic cardiovascular indication.

**To proceed, the following is needed:**
- Resolve the original-indication mapping error in the source record.
- Retrieve and review the Medsafe package insert for warnings and contraindications.
- Obtain detailed mechanism of action data (for example, from DrugBank).
- Run a systematic literature and trial search for retinoids in malignant or renovascular hypertension.
- Assess the benefit-risk balance given teratogenicity, lipid effects, hepatotoxicity, and the high number of interactions.

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Zenatane (isotretinoin) prescribing information, Dr. Reddy's Laboratories Limited](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=27b3cf26-f22e-5b70-1c24-009933b7c6ee) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

