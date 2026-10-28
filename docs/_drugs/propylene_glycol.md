---
layout: default
title: Propylene Glycol
parent: Model Prediction Only (L5)
nav_order: 391
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Propylene Glycol: predicted new indication Bronchitis (evidence level L4). Approved use: Dry eye syndrome, xerophthalmia"
monitor_inn: "PROPYLENE GLYCOL"
predicted_indications: ["bronchitis", "severe nonproliferative diabetic retinopathy", "diabetic retinopathy", "nuclear senile cataract", "cortical cataract", "senile cataract", "diabetic cataract", "tetanic cataract", "immature cataract", "craniostenosis cataract"]
---

# Propylene Glycol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Propylene Glycol: From Dry Eye Syndrome to Bronchitis
{: .text-alpha }

## One-Sentence Summary

Propylene glycol was originally used for dry eye syndrome (xerophthalmia), mainly as a lubricant component in eye products.
The TxGNN model predicts it may be effective for **bronchitis**, but there are **0 clinical trials** and only **3 publications** for this prediction, none showing a therapeutic benefit.
The literature actually describes propylene glycol as a possible airway irritant, so this prediction is likely a TxGNN model (deep learning) artifact.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Dry eye syndrome, xerophthalmia (the regulatory record contains no approved indication text) |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 0.9990 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, propylene glycol is a common excipient and lubricant vehicle, and it is used in ocular lubricant products for dry eye. It is not a disease-modifying drug.

There is no credible therapeutic link between dry eye and bronchitis. The retrieved literature describes propylene glycol as a constituent of e-cigarette liquids that may irritate the airways and contribute to lung disease risk, which is the opposite of a therapeutic effect. One mouse study (quercetin in an elastase/LPS COPD model) is unrelated to propylene glycol treatment. The very high TxGNN score is not supported by clinical or mechanistic evidence.

Other top-ranked predictions are similarly weak. Diabetic retinopathy has only indirect evidence, and the various cataract types and severe non-proliferative diabetic retinopathy have no supporting studies. Their scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for bronchitis.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26408554](https://pubmed.ncbi.nlm.nih.gov/26408554/) | 2015 | Review | Am J Physiol Lung Cell Mol Physiol | Reviews whether chronic e-cigarette use may cause lung disease, including chronic bronchitis and COPD. It does not show a therapeutic role for propylene glycol. |
| [28983782](https://pubmed.ncbi.nlm.nih.gov/28983782/) | 2017 | Review | Curr Allergy Asthma Rep | E-cigarette liquids and aerosols contain airway irritants and toxicants linked to worsening lung disease and asthma. |
| [20920189](https://pubmed.ncbi.nlm.nih.gov/20920189/) | 2010 | Preclinical (animal) | Respir Res | Quercetin reduced lung inflammation in a mouse COPD model. This study does not involve propylene glycol as a treatment. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2177447 | Midwest | Liquid | Not recorded (authorization valid) |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The bronchitis prediction rests only on a high model score. No trials support it, and the available literature points to potential airway irritation rather than benefit. The only New Zealand authorization has been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data and a credible biological rationale for respiratory use
- Package insert warnings and contraindications (currently missing and blocking safety screening)
- Direct clinical or preclinical evidence of benefit in bronchitis, including inhalation safety data
- Review of the other predicted indications, none of which have direct evidence. Diabetic retinopathy has only indirect evidence: a post-injection pain trial in which artificial tears were a comparator, and a study of a different compound, propylene glycol mannate sulfate.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

