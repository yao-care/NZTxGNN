---
layout: default
title: Perindopril
parent: Model Prediction Only (L5)
nav_order: 358
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Perindopril: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Hypertension / hypertensive disorder"
monitor_inn: "PERINDOPRIL"
predicted_indications: ["malignant hypertensive renal disease", "pulmonary hypertension owing to lung disease and/or hypoxia", "pulmonary hypertension with unclear multifactorial mechanism", "Braddock syndrome"]
---

# Perindopril
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Perindopril: From Hypertension to Malignant Hypertensive Renal Disease
{: .text-alpha }

## One-Sentence Summary

Perindopril is an ACE inhibitor originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**,
but there are currently **0 clinical trials** and only **1 publication** (not drug-specific), so the support is essentially a computational prediction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension / hypertensive disorder (from the knowledge graph; no approved indication text is available from the New Zealand licences) |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L4 (by the pre-assigned rating; no clinical trials, and the only publication is not about perindopril) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, perindopril belongs to the ACE inhibitor class. Its efficacy in hypertension is established, and mechanistically it may be applicable to malignant hypertensive renal disease.

Blocking the renin-angiotensin system (RAS) is biologically plausible in malignant hypertension with kidney involvement, where RAS activation drives vascular injury. ACE inhibitors are also used for renal protection in other settings.

However, the very high TxGNN score mostly reflects how close this condition is to the already-approved hypertension indication, not new evidence. The only publication retrieved concerns the function of a solitary kidney after nephrectomy for renal cancer, which is not a study of perindopril in this condition.

Other predicted indications are weaker still:
- **Malignant renovascular hypertension** (score 99.77%, no trials or literature): RAS blockade is plausible, but ACE inhibitors carry a known risk of acute renal function decline in bilateral renal artery stenosis or stenosis of a solitary kidney.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (score 99.75%): only preclinical rat studies (e.g., PMID 11282121 and 10602319) give indirect support. No clinical trials were found.
- **Pulmonary hypertension with unclear multifactorial mechanism** (99.75%) and **Braddock syndrome** (99.66%): no trials or literature, and the mechanistic link is weak or cannot be established.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36382821](https://pubmed.ncbi.nlm.nih.gov/36382821/) | 2022 | Other | Urologiia | Discusses loss of renal function after nephrectomy for renal cancer; not specific to perindopril or malignant hypertensive renal disease |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2643316 | Coversyl | Tab 8 mg | Not available (cancelled) |
| 2603950 | Coversyl | Tab 4 mg | Not available (cancelled) |
| 2603942 | Coversyl | Tab 2 mg | Not available (cancelled) |

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: The knowledge graph links perindopril with these conditions, which warrant caution: kidney disease, liver disease, gallbladder disease, pyoureter, renal artery obstruction, hypotensive disorder, angioedema (including acquired, hereditary, non-histaminic and RAAS-blocker-induced forms), neutropenia, agranulocytosis, connective tissue disease, bone disease and cartilage disease.
- **Drug Interactions**: 100 interactions were identified in total. Major-level examples include **potassium citrate** and **potassium bicarbonate** (risk of hyperkalaemia is typical for this class). Moderate-level examples include corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide), antidiabetic drugs (metformin, alogliptin, saxagliptin, canagliflozin, dapagliflozin, empagliflozin, chlorpropamide), acetylsalicylic acid, morphine, bupropion, dronabinol, and bowel-preparation products (picosulfuric acid, polyethylene glycol with electrolytes).
- **Case reports**: ACE-inhibitor-triggered angioedema, including a late-onset postoperative case in a patient taking perindopril (PMID 37678466), is a recognised airway risk.

Package insert warnings and contraindications were not available for this report; please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is mechanistically plausible but rests on a computational score alone. There are no clinical trials, the single retrieved publication is not about perindopril, and all New Zealand licences are cancelled. Safety signals (renal function decline in renal artery stenosis, angioedema) add further caution.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe)
- Mechanism of action data (from DrugBank)
- A targeted literature search for ACE inhibitors in malignant hypertension with renal involvement
- Clarification of the regulatory pathway, since no valid New Zealand licence currently exists
- A renal safety monitoring plan for patients with renovascular disease or a solitary kidney
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

