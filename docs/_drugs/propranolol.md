---
layout: default
title: Propranolol
parent: Model Prediction Only (L5)
nav_order: 390
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Propranolol: predicted new indication Distal myopathy, Tateyama type (evidence level L5). Approved use: Hypertension, atrial fibrillation, hypertrophic cardiomyopathy…"
monitor_inn: "PROPRANOLOL"
predicted_indications: ["distal myopathy, Tateyama type", "congenital myopathy with excess of thin filaments", "chondroma", "cirrhotic cardiomyopathy"]
---

# Propranolol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Propranolol: From Hypertension and Cardiac Conditions to Distal Myopathy, Tateyama Type
{: .text-alpha }

## One-Sentence Summary

Propranolol is a non-selective beta-blocker, originally used for hypertension, atrial fibrillation, hypertrophic cardiomyopathy and infantile hemangioma.
The TxGNN model predicts it may be effective for **distal myopathy, Tateyama type**, but there are **0 clinical trials** and **0 publications** supporting this prediction, so it is a model-only signal.
The data compiled for this report contain more supportive literature for two other predicted indications, cirrhotic cardiomyopathy and cardiomyopathy, which are discussed below.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension, atrial fibrillation, hypertrophic cardiomyopathy, hemangioma (the approved indication text for the New Zealand licences is not available, so these come from the knowledge-graph indication list) |
| Predicted New Indication | Distal myopathy, Tateyama type (top-ranked prediction) |
| TxGNN Prediction Score | 99.40% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Hold |

**Other predictions in the top 6 (for context):**

| Rank | Predicted Indication | Score | Evidence Level | Recommendation |
|------|------|------|------|------|
| 1 | Distal myopathy, Tateyama type | 99.40% | L5 | Hold |
| 2 | Congenital myopathy with excess of thin filaments | 99.30% | L5 | Hold |
| 3 | Hypertrophic cardiomyopathy due to intensive athletic training | 99.17% | L5 | Hold |
| 4 | Chondroma | 99.14% | L5 | Hold |
| 5 | Cirrhotic cardiomyopathy | 99.12% | L4 | Research Question |
| 6 | Cardiomyopathy | 99.12% | L4 | Research Question |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, propranolol is a non-selective beta-adrenergic blocker. Its efficacy in hypertension and hypertrophic cardiomyopathy is established, but a mechanistic link to a rare distal myopathy is not supported.

For the top-ranked prediction, **distal myopathy, Tateyama type**, no plausible link between beta-adrenergic blockade and this rare condition can be drawn from the supplied data. The high score most likely reflects patterns in the knowledge graph rather than a biological rationale. Note also that the knowledge graph lists myopathy and muscular disease among propranolol's contraindication-type associations, which points in the opposite direction. The same applies to the second-ranked prediction, congenital myopathy with excess of thin filaments.

The cardiac predictions are more plausible. Hypertrophic cardiomyopathy is already an original indication, so the broader **cardiomyopathy** prediction is mechanistically consistent (lower heart rate and contractility, reduced arrhythmia burden, improved diastolic filling). It adds limited repurposing novelty beyond that subtype. **Cirrhotic cardiomyopathy** has a possible link through the effect of beta-blockade on QT prolongation, but other studies raise safety concerns for non-selective beta-blockers in advanced cirrhosis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for distal myopathy, Tateyama type, nor for any of the other top-6 predicted indications.

---

## Literature Evidence

No literature is available for the top-ranked prediction (distal myopathy, Tateyama type). The most relevant literature for the two predictions that have publications is shown below. Study designs for several entries could not be verified from titles and abstracts alone.

**Cirrhotic cardiomyopathy**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38738176](https://pubmed.ncbi.nlm.nih.gov/38738176/) | 2024 | Clinical study (design not verified) | Front Pharmacol | Examines whether propranolol corrects prolonged QT intervals in patients with cirrhosis |
| [35763518](https://pubmed.ncbi.nlm.nih.gov/35763518/) | 2022 | Clinical study (design not verified) | PLoS One | Real-time propranolol infusion and cardiac function across cirrhosis severity; blunted cardiovascular effects of beta-blockers |
| [32446716](https://pubmed.ncbi.nlm.nih.gov/32446716/) | 2020 | Clinical study (design not verified) | J Hepatol | Safety of non-selective beta-blockers questioned in refractory ascites; effects on cardiac function and renal perfusion |

**Cardiomyopathy (mainly hypertrophic cardiomyopathy)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [6686544](https://pubmed.ncbi.nlm.nih.gov/6686544/) | 1983 | Clinical physiology study | Eur Heart J | Effects of propranolol and verapamil on diastolic stiffness in hypertrophic cardiomyopathy |
| [7200796](https://pubmed.ncbi.nlm.nih.gov/7200796/) | 1982 | Clinical haemodynamic study | Br Heart J | Nifedipine plus propranolol appeared superior to nifedipine alone in 12 patients with hypertrophic obstructive cardiomyopathy |
| [1611637](https://pubmed.ncbi.nlm.nih.gov/1611637/) | 1992 | Crossover clinical study | Cardiology | Propranolol and disopyramide effects on left ventricular function at rest and exercise in 19 patients |
| [3673167](https://pubmed.ncbi.nlm.nih.gov/3673167/) | 1987 | Clinical study | Z Kardiol | Nifedipine plus propranolol in 15 patients over 6 to 24 months; treatment stopped in five due to deterioration or side effects |
| [7191199](https://pubmed.ncbi.nlm.nih.gov/7191199/) | 1980 | Clinical study | Am J Cardiol | Propranolol and arrhythmia in hypertrophic cardiomyopathy |
| [28431779](https://pubmed.ncbi.nlm.nih.gov/28431779/) | 2017 | Drug monograph | Profiles Drug Subst Excip Relat Methodol | General propranolol profile; lists hypertrophic cardiomyopathy among uses |

Other retrieved entries (for example SarcTrack and the RIα subunit paper) are indirect or preclinical and are not summarised here.

---

## New Zealand Market Information

All five licences are cancelled; there is no currently valid propranolol product in the data compiled for this report. Approved indication text is not available for these licences.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2681080 | Hikma-Propranolol | Oral liquid 4 mg per ml | Cancelled |
| 2618664 | IPCA-Propranolol | Tablet 40 mg | Cancelled |
| 2611414 | Drofate | Tablet 10 mg | Cancelled |
| 2401894 | Roxane-Propranolol | Oral liquid 4 mg per ml | Cancelled |
| 326070 | Cardinol LA | Long-acting capsule 160 mg | Cancelled |

---

## Safety Considerations

Package insert warnings and contraindications were not available. Please refer to the package insert for safety information.

- **Knowledge-graph contraindication associations** (not label text): bronchitis, anxiety and neurotic disorders, kidney disease, liver disease, pancreatitis, gallbladder disease, systemic lupus erythematosus, **myopathy, muscular disease and skeletal muscle disease**, familial periodic paralysis, diabetes mellitus and diabetic ketoacidosis, gout, hyperparathyroidism, hepatic coma and encephalopathy, anuria, myasthenia gravis, and others. The muscle-disease associations are directly relevant to the top-ranked myopathy predictions.
- **Drug interactions** (100 recorded; examples shown):
  - Major: epinephrine
  - Moderate: hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide, morphine, bupropion, atropine, hyoscyamine, methscopolamine, canagliflozin, chlorpropamide, cimetidine, lorcaserin, calcium acetate and calcium phosphate
  - Minor: aspirin, ascorbic acid, magnesium oxide
- **Cirrhosis-specific concern**: Published studies report impaired circulatory and renal function with non-selective beta-blockers in refractory ascites.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (distal myopathy, Tateyama type) rests on a model score alone (L5), with no trials, no literature and no plausible mechanism. The knowledge graph also associates propranolol with myopathy contraindications. Propranolol currently has no valid New Zealand licence. The only predictions with some supporting literature, cirrhotic cardiomyopathy and cardiomyopathy (both L4), are best treated as research questions. The latter largely overlaps with an existing indication.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data (for example from DrugBank)
- For cirrhotic cardiomyopathy: verification of the study designs for PMIDs 38738176, 35763518 and 32446716, and a safety assessment in advanced cirrhosis
- For the myopathy predictions: any biological rationale or case evidence, and review of the muscle-disease contraindication associations
- Clarification of the regulatory pathway, since all New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

