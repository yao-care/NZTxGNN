---
layout: default
title: Carvedilol
parent: Model Prediction Only (L5)
nav_order: 87
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Carvedilol: predicted new indication Malignant renovascular hypertension (evidence level L5). Approved use: Hypertension (hypertensive disorder)"
monitor_inn: "CARVEDILOL"
predicted_indications: ["malignant renovascular hypertension", "malignant hypertensive renal disease", "pulmonary hypertension with unclear multifactorial mechanism", "pulmonary hypertension owing to lung disease and/or hypoxia", "Braddock syndrome"]
---

# Carvedilol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

# Carvedilol: From Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Carvedilol is a beta-blocker with alpha-1 blocking activity, originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant renovascular hypertension**,
but **no clinical trials and no publications** currently support this specific prediction, so it rests on model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension (hypertensive disorder); no approved indication text is available from the New Zealand licences |
| Predicted New Indication | Malignant renovascular hypertension |
| TxGNN Prediction Score | 99.55% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, carvedilol is a non-selective beta-blocker with alpha-1 blocking activity. Beta-1 blockade can reduce renin release, and alpha-1 blockade causes vasodilation. Both are plausible contributors to blood pressure lowering in renovascular hypertension.

The predicted indication is closely related to carvedilol's existing use in hypertension, which is likely why the TxGNN score is so high. The score appears to reflect a class effect of antihypertensives rather than independent evidence. Malignant renovascular hypertension is also a severe, acute condition. Its definitive management (revascularization, intravenous agents) differs from chronic oral beta-blocker therapy, so the prediction should not be taken as support for routine use.

Other predicted indications (all with no clinical trials):
- **Malignant hypertensive renal disease** (99.55%, L5): essentially an extension of the existing hypertension use, with no supporting studies.
- **Pulmonary hypertension with unclear multifactorial mechanism** (99.54%, L5): beta-blockade is theoretically helpful for right ventricular remodelling, but beta-blockers are used cautiously in pulmonary arterial hypertension because of concern about reduced cardiac output.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (99.54%, L4): only indirect preclinical evidence (see below).
- **Braddock syndrome** (99.37%, L5): no clear mechanistic rationale; likely a knowledge-graph artefact.

## Clinical Trial Evidence

Currently no related clinical trials registered for the top predicted indication, nor for any of the other four predicted indications.

## Literature Evidence

Currently no related literature available for malignant renovascular hypertension.

For the fourth-ranked prediction, **pulmonary hypertension owing to lung disease and/or hypoxia**, indirect literature was retrieved. It is preclinical or tangential, not direct evidence in patients:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26291659](https://pubmed.ncbi.nlm.nih.gov/26291659/) | 2015 | Preclinical (animal) | Eur J Pharmacol | Carvedilol reversed hypertension induced by chronic intermittent hypoxia in rats |
| [16639551](https://pubmed.ncbi.nlm.nih.gov/16639551/) | 2006 | Preclinical (animal) | Pflugers Arch | Carvedilol inhibited right ventricular hypertrophy induced by chronic hypobaric hypoxia |
| [20508210](https://pubmed.ncbi.nlm.nih.gov/20508210/) | 2010 | Preclinical (animal) | Am J Respir Crit Care Med | Adrenergic receptor blockade reversed right heart remodelling and dysfunction in pulmonary hypertensive rats |
| [19789971](https://pubmed.ncbi.nlm.nih.gov/19789971/) | 2009 | Review | Cardiovasc Drugs Ther | Beta-blocker effects on lung function in heart failure; relevant to safety in lung disease |
| [24798657](https://pubmed.ncbi.nlm.nih.gov/24798657/) | 2014 | Crossover study (11 patients) | J Clin Hypertens | Compared nighttime carvedilol 20 mg with nifedipine 40 mg on sleep blood pressure in hypertensive patients with sleep apnoea |
| [25187802](https://pubmed.ncbi.nlm.nih.gov/25187802/) | 2014 | Preclinical (in vitro) | Exp Ther Med | Carvedilol inhibited cardiomyocyte apoptosis after simulated ischaemia/reperfusion via the TLR4/NF-κB pathway |
| [28862654](https://pubmed.ncbi.nlm.nih.gov/28862654/) | 2017 | Review | Nutrients | Antioxidant strategies for cardiovascular conditions associated with oxidative stress |
| [31242023](https://pubmed.ncbi.nlm.nih.gov/31242023/) | 2019 | Preclinical/mechanistic | Am J Physiol Lung Cell Mol Physiol | Interplay of hypoxia and β-adrenergic receptor signalling in pulmonary arterial hypertension |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2527324 | Carvedilol Sandoz | Tab 12.5 mg | Not recorded (cancelled) |
| 2527316 | Carvedilol Sandoz | Tab 25 mg | Not recorded (cancelled) |
| 2527332 | Carvedilol Sandoz | Tab 6.25 mg | Not recorded (cancelled) |

All three licences are cancelled, so there is currently no valid carvedilol product on the New Zealand market in the data compiled for this report.

## Safety Considerations

Package insert warnings and contraindications are not available in the data compiled for this report. Please refer to the package insert for authoritative safety information.

- **Contraindication-related conditions (from the knowledge graph, not label text)**: liver failure, asthma (including intrinsic and allergic asthma), bronchitis, pulmonary emphysema and other bronchial disease, diabetes mellitus and diabetic ketoacidosis, hypotension, peripheral vascular disease (including intermittent claudication and peripheral arterial disease), atrioventricular block, sick sinus syndrome (including the familial form), Prinzmetal angina, anaphylaxis, and intraoperative floppy iris syndrome.
- **Drug interactions**: 100 interactions were found in total; the first 20 returned are summarised here.
  - **Major**: epinephrine.
  - **Moderate**: corticosteroids (hydrocortisone, triamcinolone, dexamethasone, betamethasone, budesonide), hyoscyamine, atropine, bupropion, morphine, lorcaserin, canagliflozin, acetohexamide, and calcium salts (phosphate, acetate, carbonate, citrate).
  - **Minor**: acetylsalicylic acid, aluminium hydroxide, attapulgite.
- **Pulmonary hypertension context**: lung disease and bronchial conditions appear among the knowledge-graph contraindications, and beta-blockers are used cautiously in pulmonary arterial hypertension, so caution is warranted for the lung-related predictions.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for malignant renovascular hypertension has a very high model score, but no clinical trials or literature support it, and it largely reflects carvedilol's known antihypertensive class effect. Carvedilol also has no valid New Zealand licence at present, and key safety and mechanism data are missing.

**To proceed, the following is needed:**
- The Medsafe package insert (warnings, contraindications, approved indications), which is a blocking gap for safety screening
- Detailed mechanism of action data (for example from DrugBank)
- Clinical or observational evidence for renovascular or malignant hypertension specifically
- A review of the New Zealand licence situation (all three licences cancelled) and the availability of other carvedilol products
- A dedicated safety assessment for the lung-related predictions, given the knowledge-graph contraindications in asthma and bronchial disease
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

