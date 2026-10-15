---
layout: default
title: Metoprolol
parent: Model Prediction Only (L5)
nav_order: 296
evidence_level: L5
indication_count: 10
---

# Metoprolol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Metoprolol: From Hypertension and Myocardial Infarction to Malignant Hypertensive Renal Disease

## One-Sentence Summary

Metoprolol is a beta-1 selective blocker, originally used for hypertension, acute coronary syndrome and myocardial infarction.
The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**,
but **no clinical trials and no publications** directly support this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, acute coronary syndrome, myocardial infarction (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only (no currently valid licences) |
| Number of Authorizations | 9 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, metoprolol is a beta-1 adrenergic receptor blocker (the pharmacology data compiled for this report lists ADRB1 and ADRB2 as targets), and its efficacy in hypertension is established. Mechanistically it may be applicable to malignant hypertensive renal disease, because beta-1 blockade lowers heart rate, reduces renin release and lowers blood pressure.

However, the high score most likely reflects the closeness of this condition to the existing hypertension indication rather than independent evidence. This is largely an extension of an approved use, not a distinct repurposing signal.

Other top-ranked predictions show a similar pattern:
- **Malignant renovascular hypertension** (99.91%): mechanistically plausible through reduced renin secretion, but the only retrieved publication (PMID 1988765) is a pheochromocytoma diagnostic study and does not support it.
- **Myocardial infarction subtypes** (posteroinferior, posterolateral, septal; about 99.8%): these are sub-localisations of an existing indication, not new opportunities.
- **Chronic pulmonary heart disease** (99.40%): the only prediction with trial evidence, discussed below.
- **Pulmonary hypertension** (two forms), **Braddock syndrome** and **trichotillomania**: no supporting clinical data. For pulmonary hypertension, beta-blockers also raise a safety concern because of reduced right ventricular reserve.

## Clinical Trial Evidence

For the top prediction (malignant hypertensive renal disease), currently no related clinical trials are registered.

The best-supported alternative, chronic pulmonary heart disease, has these related trials (all in COPD or cardiopulmonary populations, none directly in pulmonary heart disease):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06825728](https://clinicaltrials.gov/study/NCT06825728) | Phase 4 | Not yet recruiting | 311 | Metoprolol doses in heart failure with reduced ejection fraction plus COPD; outcomes are all-cause death and rehospitalisation |
| [NCT02587351](https://clinicaltrials.gov/study/NCT02587351) | Phase 3 | Terminated | 532 | Metoprolol vs placebo to prevent COPD exacerbations; inconclusive because it was terminated |
| [NCT00384566](https://clinicaltrials.gov/study/NCT00384566) | Phase 4 | Withdrawn | 0 | Carvedilol vs metoprolol respiratory effects in heart failure; no data |
| [NCT05096884](https://clinicaltrials.gov/study/NCT05096884) | Early Phase 1 | Terminated | 14 | Post-COVID exertional dyspnoea with tachycardia; only tangentially related |

## Literature Evidence

For the top prediction, currently no related literature is available. For the closest related prediction (malignant renovascular hypertension), the single retrieved paper is not supportive.

Selected literature for chronic pulmonary heart disease (beta-blockers in COPD, often with heart failure):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38762800](https://pubmed.ncbi.nlm.nih.gov/38762800/) | 2024 | RCT | JAMA | Bisoprolol in COPD at high exacerbation risk (BICS); notes that metoprolol in an earlier trial did not reduce exacerbations and increased hospitalisations |
| [38587241](https://pubmed.ncbi.nlm.nih.gov/38587241/) | 2024 | RCT | N Engl J Med | Beta-blockers after myocardial infarction with preserved ejection fraction |
| [20413026](https://pubmed.ncbi.nlm.nih.gov/20413026/) | 2010 | Randomised crossover | J Am Coll Cardiol | Beta1-selective vs non-selective beta-blockers in heart failure with COPD |
| [32000982](https://pubmed.ncbi.nlm.nih.gov/32000982/) | 2020 | Cohort | Am J Cardiol | Metoprolol vs carvedilol outcomes in elderly patients with heart failure, COPD, diabetes and renal failure |
| [26844454](https://pubmed.ncbi.nlm.nih.gov/26844454/) | 2016 | Cohort | Medicine | Survival with beta-blockers in coexisting heart failure and COPD (Taiwan database) |
| [14520486](https://pubmed.ncbi.nlm.nih.gov/14520486/) | 2003 | Clinical study | Heart Vessels | Metoprolol appeared safe in coronary artery disease patients with COPD |
| [38771577](https://pubmed.ncbi.nlm.nih.gov/38771577/) | 2024 | Observational | JAMA Netw Open | Beta-blocker use and outcomes in COPD after myocardial infarction |
| [32250198](https://pubmed.ncbi.nlm.nih.gov/32250198/) | 2020 | Review | Expert Rev Respir Med | Practical recommendations for beta-blockers in COPD |
| [6851273](https://pubmed.ncbi.nlm.nih.gov/6851273/) | 1983 | Clinical physiological study | Clin Cardiol | Effects of oral metoprolol on cardiac and pulmonary function in COPD (6 patients) |

## New Zealand Market Information

All nine licences are cancelled, and no approved indication text is available for any of them.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2550369 | Metoprolol IV Mylan | Injection 1 mg/ml, 5 ml vial | Cancelled |
| 2639777 | Metoprolol IV Viatris | Injection 1 mg/ml, 5 ml vial | Cancelled |
| 2618680 | IPCA-Metoprolol | Tablet 100 mg | Cancelled |
| 2618672 | IPCA-Metoprolol | Tablet 50 mg | Cancelled |
| 2650703 | Myloc CR | Long-acting tablet 47.5 mg | Cancelled |

Other cancelled licences cover Myloc CR (23.75, 95 and 190 mg) and Slow-Lopresor (200 mg).

## Safety Considerations

- **Knowledge-graph contraindications (not label text):** The knowledge graph links metoprolol to contraindication with several conditions, including:
  - Cardiovascular and respiratory: hypotensive disorder, phaeochromocytoma.
  - Metabolic: diabetes mellitus, diabetic ketoacidosis, gout, hypercalcaemia, hyperparathyroidism.
  - Liver and pancreas: liver disease, hepatic encephalopathy, pancreatitis, gallbladder disease.
  - Neuromuscular: myasthenia gravis.
  - Autoimmune and skin: systemic lupus erythematosus, psoriasis.
  - Psychiatric: anxiety disorder, dysthymic disorder.
  - Other: anuria, neonatal jaundice.
  These come from graph edges and should be verified against the current product information.
- **Drug targets:** Pharmacology data lists beta-1 (ADRB1) and beta-2 (ADRB2) adrenoceptors as targets. Beta-2 activity is relevant to bronchospasm risk in lung disease.
- **Context-specific concerns:** In pulmonary hypertension and COPD, beta-blockers raise concern about reduced right ventricular reserve and bronchospasm. In BLOCK-COPD, metoprolol did not reduce exacerbations and was linked to more exacerbations requiring hospitalisation.

Please refer to the package insert for formal warnings and interaction information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction, malignant hypertensive renal disease, has no trials or literature. It is a close extension of the existing hypertension indication, and the score is likely driven by that proximity. All nine New Zealand licences are cancelled, so there is no current local product to repurpose. The most evidence-backed alternative, chronic pulmonary heart disease (L4), is better framed as a research question than as a go decision, because the evidence is indirect (COPD populations) and mixed, and the key Phase 3 trial was terminated.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Targeted literature search for metoprolol or beta-blockers in malignant hypertension with renal involvement
- Clarification of the current New Zealand supply and licensing situation
- For chronic pulmonary heart disease, a defined study question that addresses bronchospasm and right ventricular safety, and follow-up of NCT06825728
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

