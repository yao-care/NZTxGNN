---
layout: default
title: Labetalol
parent: Model Prediction Only (L5)
nav_order: 253
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Labetalol: predicted new indication Malignant renovascular hypertension (evidence level L5). Approved use: Hypertensive disorder / hypertension"
monitor_inn: "LABETALOL"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension owing to lung disease and/or hypoxia", "pulmonary hypertension with unclear multifactorial mechanism"]
---

# Labetalol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Labetalol: From Hypertensive Disorder to Malignant Renovascular Hypertension

## One-Sentence Summary

Labetalol is an antihypertensive originally used to treat hypertensive disorders and hypertension.
The TxGNN model predicts it may be effective for **malignant renovascular hypertension**,
but there are **no registered clinical trials** and only **1 publication** (a single case report) behind this prediction, so the evidence rests essentially on the model score.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertensive disorder / hypertension (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Malignant renovascular hypertension |
| TxGNN Prediction Score | 99.08% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 5 (1 valid, 4 cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, labetalol combines alpha-1 and non-selective beta blockade. It is established in hypertension, and a link to severe or malignant hypertension is plausible. Renovascular hypertension is driven largely by renin-angiotensin activation, and beta-blockade reduces renin release.

The predicted indication is largely a severe subtype of the already established hypertension indication, so it is not a distinct repurposing opportunity. The only retrieved publication (PMID 7242419) describes a case of hallucinogen-induced vasculitis with malignant hypertension in which labetalol and minoxidil were used for initial blood pressure control. It does not evaluate renovascular hypertension directly.

The other top predictions are weaker:
- **Malignant hypertensive renal disease** (99.08%): strongly overlaps with the labelled indication, with no trials or literature.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (99.08%): no established pulmonary vasodilatory mechanism. Non-selective beta-blockade may worsen bronchospasm and hypoxia. The 10 retrieved papers concern systemic or pregnancy-related hypertension, and none evaluates pulmonary hypertension. The score likely reflects the shared term "hypertension".
- **Pulmonary hypertension with unclear multifactorial mechanism** (99.08%): no mechanistic rationale, trials or literature. Beta-blockers are generally used cautiously because of the risk of reduced cardiac output.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7242419](https://pubmed.ncbi.nlm.nih.gov/7242419/) | 1981 | Case report | Med J Aust | Malignant hypertension in a 20-year-old man after hallucinogen use, with arteritic renal vessel changes. Vasculitis responded to prednisone, and labetalol with minoxidil gave initial blood pressure control. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2604116 | Trandate (valid) | Inj 5 mg per ml, 20 ml ampoule | Not available |
| 2582163 | Trandate (cancelled) | Tab 200 mg | Not available |
| 2582155 | Trandate (cancelled) | Tab 100 mg | Not available |
| 2708809 | Biocon (cancelled) | Tab 100 mg | Not available |
| 2565579 | Presolol (cancelled) | Tab 200 mg | Not available |

## Safety Considerations

- **Contraindication signals from the knowledge graph** (these are not label text and need confirmation against the package insert): liver failure, liver disease, hepatic coma/encephalopathy, kidney disease, anuria, pancreatitis, gallbladder disease, diabetes mellitus, diabetic ketoacidosis, hypotensive disorder, atrioventricular block, pulmonary emphysema, bronchial disease, gout, systemic lupus erythematosus, anxiety and neurotic disorders, and several electrolyte or metabolic conditions (potassium deficiency, hypercalcaemia, phosphorus metabolism disease, hyperparathyroidism).
- **Drug interactions** (100 interactions recorded; selected examples):
  - Major: epinephrine, dolasetron.
  - Moderate: corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), anticholinergics (atropine, hyoscyamine, dicyclomine, methscopolamine), morphine, cimetidine, canagliflozin, dapagliflozin, chlorpropamide, and calcium salts.
  - Minor: acetylsalicylic acid, magnesium oxide.

The pulmonary-disease and bronchial-disease signals are particularly relevant to the pulmonary hypertension predictions. Please refer to the package insert for official warnings.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The 99.08% TxGNN score is a model prediction only. There are no clinical trials, and the single publication is a case report that does not directly address renovascular hypertension. The predicted indications are either subtypes of the already established hypertension indication or, for pulmonary hypertension, mechanistically doubtful and potentially unsafe.

**To proceed, the following is needed:**
- The Medsafe package insert, with warnings and contraindications (currently a blocking gap), to enable safety screening
- Mechanism of action data from DrugBank
- Clinical studies of labetalol in renovascular or malignant hypertension
- Confirmation that the predicted indications are clinically distinct from the existing hypertension indication
- Approved indication text for the valid injection licence (2604116)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

