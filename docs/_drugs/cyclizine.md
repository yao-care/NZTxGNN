---
layout: default
title: Cyclizine
parent: Model Prediction Only (L5)
nav_order: 120
evidence_level: L5
indication_count: 7
monitor_inn: "Cyclizine"
predicted_indications: ["allergic urticaria", "nasal cavity disease", "cold urticaria", "rosacea conjunctivitis", "headache disorder", "trigeminal autonomic cephalalgia", "recalcitrant atopic dermatitis"]
last_modified_at: 2026-10-30
description: "Cyclizine: predicted new indication allergic urticaria (過敏性蕁麻疹) (evidence level L5). Approved use: Not recorded in the local data"
---

# Cyclizine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

## Cyclizine: first-generation H1 antihistamine → 過敏性蕁麻疹 (allergic urticaria)
{: .text-alpha }

## One-Sentence Summary

Cyclizine is a first-generation H1 antihistamine with antiemetic use. The TxGNN model predicts it may be useful for **allergic urticaria (過敏性蕁麻疹)**, with a score of **0.9998**. Currently there are **no clinical trials** and **no publications** supporting this specific direction, so the prediction rests on model output and pharmacological plausibility alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | allergic urticaria (過敏性蕁麻疹) |
| TxGNN Prediction Score | 0.9998 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Cyclizine is known pharmacologically as an H1 receptor ligand (target gene HRH1), and the pharmacology data describes its use for motion sickness and for nausea after surgery or radiotherapy.

Urticaria is driven largely by histamine release from mast cells, and H1 antihistamines are a standard drug class for it. Blocking the H1 receptor is therefore a biologically plausible route to symptom relief. This reasoning is a mechanistic inference only: no cyclizine-specific study in allergic urticaria was provided. Second-generation antihistamines are generally preferred in practice because they cause less sedation.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2231220 | Nausicalm | Tab 50 mg | Not recorded in the local data |
| 2595877 | Hameln | Inj 50 mg per ml, 1 ml ampoule | Not recorded in the local data |

## Safety Considerations

- **Contraindication-related conditions**: Conditions flagged as cautions for cyclizine in the compiled reference data (not taken from label text) include hypertension, epilepsy, kidney disease, liver disease, pancreatitis, gallbladder disease, adrenocortical insufficiency, hypothyroidism, peptic ulcer disease, inflammatory bowel disease, diabetes mellitus, benign prostatic hyperplasia, coronary artery disease, myocardial infarction, atherosclerosis, ocular hypertension, hemoglobinopathy and *Clostridium difficile* colitis.
- **Drug Interactions**: The interaction query returned one pharmacology entry, cyclizine acting at the H1 receptor (HRH1). No drug-drug interaction entries with severity levels were found.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is very high (0.9998), and H1 blockade is mechanistically plausible for urticaria. However, no trial or publication studies cyclizine in allergic urticaria, so the evidence level is L5 and the prediction remains purely model-based.

**To proceed, the following is needed:**
- Clinical or observational evidence of cyclizine in allergic urticaria
- Mechanism of action data to support the mechanistic link
- Package insert warnings and contraindications from Medsafe for a safety screen
- A comparison against second-generation antihistamines, given the sedation burden of cyclizine
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

