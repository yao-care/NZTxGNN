---
layout: default
title: Sodium Citrate
parent: Model Prediction Only (L5)
nav_order: 426
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-31
monitor_inn: "sodium citrate"
predicted_indications: ["papillary conjunctivitis", "nasal cavity disease", "stomach disease", "postgastrectomy syndrome", "gastroduodenitis", "familial visceral myopathy", "rosacea conjunctivitis", "allergic urticaria"]
description: "Sodium Citrate: predicted new indication Papillary conjunctivitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Sodium Citrate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Sodium Citrate: Papillary conjunctivitis
{: .text-alpha }

## One-Sentence Summary

Sodium citrate is a buffering agent, originally associated with acid-related stomach conditions (peptic ulcer, reflux, dyspepsia) and upper respiratory complaints (common cold, pharyngitis, rhinitis).
The TxGNN model predicts it may be effective for **papillary conjunctivitis**, but **no clinical trials and no publications** were found to support this prediction.
The result is a computational signal only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Papillary conjunctivitis |
| TxGNN Prediction Score | 0.9995 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known information, sodium citrate acts as a buffering antacid that neutralises gastric acid. This is plausible for its acid-related original indications, but there is no established mechanistic link to allergic or contact-lens-related inflammation of the eye.

The high score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The other top-ranked predictions show the same pattern: nasal cavity disease and gastroduodenitis sit close to the original indications, so they are not true repurposing signals. Others, such as familial visceral myopathy, rosacea conjunctivitis, allergic urticaria, and postgastrectomy syndrome, have no supporting mechanism or literature.

The only prediction with any literature is **stomach disease** (rank 4, score 0.9989, L4). That literature consists of in vitro and xenograft work on gastric cancer cells combining 3-bromopyruvate with sodium citrate (glycolysis inhibition), plus indirect or unrelated items. This is preclinical and a different mechanism from antacid use.

## Clinical Trial Evidence

Currently no related clinical trials registered for papillary conjunctivitis. No trials were found for any of the other predicted indications either.

## Literature Evidence

Currently no related literature available for papillary conjunctivitis.

For reference, the highest-ranked prediction with literature (stomach disease) has only preclinical or indirect publications:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [27163639](https://pubmed.ncbi.nlm.nih.gov/27163639/) | 2016 | In vitro/preclinical | Biochem Biophys Res Commun | 3-Bromopyruvate plus sodium citrate induced apoptosis in a gastric cancer cell line by inhibiting glycolysis |
| [26708213](https://pubmed.ncbi.nlm.nih.gov/26708213/) | 2016 | In vitro/preclinical | Oncol Rep | The same combination suppressed survivin and inhibited orthotopic gastric tumour growth in a model |
| [29115645](https://pubmed.ncbi.nlm.nih.gov/29115645/) | 2018 | Preclinical (xenograft/imaging) | Oncol Rep | FDG PET/CT used to evaluate glycolysis-targeted therapy in a gastric xenograft model |
| [25316860](https://pubmed.ncbi.nlm.nih.gov/25316860/) | 2014 | Community RCT (indirect) | J Infect Dis | Buffer solution given to counter stomach acidity and improve the oral poliovirus vaccine response |
| [1959368](https://pubmed.ncbi.nlm.nih.gov/1959368/) | 1991 | Clinical comparison (indirect) | Crit Care Med | Stress ulceration and pneumonia in ICU patients on sucralfate vs ranitidine; sodium citrate was not clearly the studied agent |
| [1777551](https://pubmed.ncbi.nlm.nih.gov/1777551/) | 1991 | Clinical/formulation (indirect) | Aliment Pharmacol Ther | Glucose-sodium acid citrate formulation reduced gastric damage from azapropazone |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2262673 | Micolette | Enema (sodium citrate 90 mg with sodium lauryl sulphoacetate 9 mg per ml, 5 ml) | Not recorded (licence valid) |

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (not taken from label text): hypertension, pancreatitis, adrenal insufficiency (including Addison disease and congenital adrenal hyperplasia), benign prostatic hyperplasia, anuria, impaired renal function, rhabdomyolysis, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disease, coronary artery disease and myocardial infarction, hemoglobinopathy, metabolic acidosis, and ocular hypertension and angle-closure glaucoma.
- **Drug Interactions**: 114 interactions were identified. The main ones in the data compiled for this report:
  - **Major**: aluminium hydroxide
  - **Moderate**: doxycycline, minocycline, tetracycline, levofloxacin, rosuvastatin, acetylsalicylic acid, iron, ephedrine, pseudoephedrine, acalabrutinib
  - **Minor**: sulfonylureas (acetohexamide, chlorpropamide, glipizide, glyburide, tolazamide, tolbutamide), lactulose, misoprostol

Package insert warnings were not available, so please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for papillary conjunctivitis rests on a model score alone, with no clinical trials, no literature, and no plausible mechanism.

**To proceed, the following is needed:**
- Mechanism of action data and a credible rationale linking sodium citrate to ocular inflammation
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Any preclinical or clinical evidence in papillary conjunctivitis
- Assessment of route and formulation feasibility (an ophthalmic preparation would be needed)
- Review of the interaction and contraindication profile for the intended population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

