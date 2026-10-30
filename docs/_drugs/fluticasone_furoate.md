---
layout: default
title: Fluticasone Furoate
parent: Model Prediction Only (L5)
nav_order: 212
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-30
monitor_inn: "fluticasone furoate"
predicted_indications: ["atopic eczema", "bronchitis", "2-hydroxyethyl methacrylate sensitization", "dermatitis, atopic", "contact dermatitis", "occupational dermatitis", "phototoxic dermatitis"]
description: "Fluticasone Furoate: predicted new indication Atopic eczema (evidence level L5). Approved use: Not recorded in the local data"
---

# Fluticasone Furoate
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

## Fluticasone Furoate: Atopic eczema
{: .text-alpha }

## One-Sentence Summary

Fluticasone furoate is an inhaled and intranasal corticosteroid, originally used for asthma and allergic rhinitis.
The TxGNN model predicts it may be effective for **atopic eczema**, but **no clinical trials** and **no directly relevant publications** currently support this prediction.
The evidence is a model prediction only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Atopic eczema |
| TxGNN Prediction Score | 0.9998 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Fluticasone furoate is known to be a potent glucocorticoid receptor agonist with anti-inflammatory and immunosuppressive effects. Its efficacy in asthma and allergic rhinitis is established.

Atopic eczema is an allergic, inflammatory skin disease that often coexists with asthma and allergic rhinitis. Topical corticosteroids are a standard therapy for it, so the mechanism is plausible.

However, the two retrieved papers do not test fluticasone furoate in eczema. One is a review of adrenal suppression with intranasal corticosteroids (a safety topic), and the other is a case study of allergen immunotherapy. The very high prediction score is therefore not backed by direct evidence. The same disease concept also appears as "dermatitis, atopic" in the model output, so these are not two independent signals.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [19571596](https://pubmed.ncbi.nlm.nih.gov/19571596/) | 2009 | Review | Neuroimmunomodulation | Intranasal corticosteroids and adrenal suppression; discusses tests of systemic effect on the HPA axis in patients with allergic rhinitis, asthma and atopic dermatitis. A safety topic, not an efficacy test in eczema |
| [40066386](https://pubmed.ncbi.nlm.nih.gov/40066386/) | 2025 | Case report | Indian J Otolaryngol Head Neck Surg | Allergen immunotherapy in patients with autoimmune disease; does not evaluate fluticasone furoate |

No clinical trials are currently registered for this drug and indication.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2494299 (cancelled) | Breo Ellipta | Powder for inhalation 100 mcg with vilanterol 25 mcg | Not recorded |
| 2674904 (cancelled) | Trelegy Ellipta | Powder for inhalation fluticasone furoate 100 mcg with umeclidinium 62.5 mcg and vilanterol 25 mcg | Not recorded |

Both authorizations are combination inhalers and both are cancelled.

## Safety Considerations

- **Contraindications (TxGNN model associations, not label text)**: The TxGNN model (deep learning) links the drug to the following conditions as contraindications. These associations may reflect similarities the model learned between related diseases, not biological evidence: measles, herpes simplex virus keratitis, adrenocortical insufficiency, Addison disease, varicella zoster infection, fungal infectious disease, dermatophytosis, pulmonary tuberculosis, ocular hypertension, glaucoma, parasitic infection, infectious disease, lower respiratory tract disease, paranasal sinus disease, nasal cavity disease, upper respiratory tract disease, adrenal gland hyperfunction and Cushing syndrome.
- **Drug Interactions**: No interaction records were found.
- **Adrenal suppression**: The retrieved review highlights this as a concern with intranasal corticosteroids.

Please refer to the package insert for authoritative warnings and contraindications. The label text could not be obtained.

## Other Predicted Indications (Brief)

| Predicted Indication | Score | Evidence Level | Comment |
|------|------|------|------|
| Bronchitis | 0.9989 | L5 | Indirect evidence only: a triple-therapy COPD RCT (PMID 38729884), a chronic cough study with fluticasone furoate (PMID 38733542) and an asthma RCT (PMID 21977941). None shows efficacy in bronchitis as a distinct indication. Flagged as a research question |

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for atopic eczema is mechanistically plausible but rests on a model score alone. There are no trials and no literature that test fluticasone furoate in this condition. Safety label data is also missing.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Direct clinical evidence of fluticasone furoate in atopic eczema, and a check of route compatibility (the available forms are inhaled and intranasal, whereas eczema treatment is topical)
- A review of the bronchitis and chronic cough findings as a possible separate research question
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

