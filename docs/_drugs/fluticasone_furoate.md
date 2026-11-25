---
layout: default
title: Fluticasone Furoate
parent: Model Prediction Only (L5)
nav_order: 212
evidence_level: L5
indication_count: 7
last_modified_at: 2026-11-25
monitor_inn: "fluticasone furoate"
predicted_indications: ["atopic eczema", "bronchitis", "2-hydroxyethyl methacrylate sensitization", "dermatitis, atopic", "contact dermatitis", "occupational dermatitis", "phototoxic dermatitis"]
description: "Fluticasone Furoate: predicted new indication Atopic eczema (evidence level L5). In New Zealand: 2 Pharmac Schedule items"
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
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for fluticasone furoate: "Fluticasone furoate is a synthetic trifluorinated corticosteroid with anti‑inflammatory activity. Fluticasone furoate has been shown in vitro to exhibit a binding affinity for the human glucocorticoid receptor that is approximately 29.9 times that of dexamethasone and 1.7 times that of fluticasone propionate." ([DailyMed: Arnuity Ellipta (fluticasone furoate) prescribing information, GlaxoSmithKline LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=50f193ee-1691-486d-9370-a664df63695c), label version 2023-10-18; accessed 2026-11-19.) Fluticasone furoate is known to be a potent glucocorticoid receptor agonist with anti-inflammatory and immunosuppressive effects. Its efficacy in asthma and allergic rhinitis is established.

Atopic eczema is an allergic, inflammatory skin disease that often coexists with asthma and allergic rhinitis. Topical corticosteroids are a standard therapy for it, so the mechanism is plausible.

However, the two retrieved papers do not test fluticasone furoate in eczema. One is a review of adrenal suppression with intranasal corticosteroids (a safety topic), and the other is a case study of allergen immunotherapy. The very high prediction score is therefore not backed by direct evidence. The same disease concept also appears as "dermatitis, atopic" in the model output, so these are not two independent signals.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [19571596](https://pubmed.ncbi.nlm.nih.gov/19571596/) | 2009 | Review | Neuroimmunomodulation | Intranasal corticosteroids and adrenal suppression; discusses tests of systemic effect on the HPA axis in patients with allergic rhinitis, asthma and atopic dermatitis. A safety topic, not an efficacy test in eczema |
| [40066386](https://pubmed.ncbi.nlm.nih.gov/40066386/) | 2025 | Case report | Indian J Otolaryngol Head Neck Surg | Allergen immunotherapy in patients with autoimmune disease; does not evaluate fluticasone furoate |

No clinical trials are currently registered for this drug and indication.

<!-- review:begin nz-lic-20261106-fluticasone_furoate-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Both authorizations are combination inhalers and both are cancelled." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-fluticasone_furoate-1 -->

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2674904 | Trelegy Ellipta | Fluticasone furoate with umeclidinium and vilanterol | Powder for inhalation fluticasone furoate 100 mcg… | Yes |
| 2494299 | Breo Ellipta | Fluticasone furoate with vilanterol | Powder for inhalation 100 mcg with vilanterol 25… | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

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
- Direct clinical evidence of fluticasone furoate in atopic eczema, and a check of route compatibility (the forms on the Pharmac Schedule are inhaled, whereas eczema treatment is topical)
- A review of the bronchitis and chronic cough findings as a possible separate research question

<!-- review:begin nz-lic-20261125-fluticasone_furoate-1 -->

> **Review correction (2026-11-25)**: Originally stated: "(the available forms are inhaled and intranasal, whereas eczema treatment is topical)" Corrected: the nasal spray and most inhalers originally collected for this page (Flixonase, Flixotide, Seretide) contain fluticasone propionate, a different ester, and have been removed; the fluticasone furoate items on the Pharmac Schedule are inhalation powders (Breo Ellipta, Trelegy Ellipta). Only the premise was changed; the recommendation is unchanged. Source: [Pharmac Pharmaceutical Schedule, Community Medicines (CPSReporting.xlsx)](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261125-fluticasone_furoate-1 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "Both authorizations are combination inhalers and both are cancelled" | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Arnuity Ellipta (fluticasone furoate) prescribing information, GlaxoSmithKline LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=50f193ee-1691-486d-9370-a664df63695c) |
| 2026-11-25 | Premise corrected: the fluticasone furoate items on the Pharmac Schedule are inhaled | Corrected | [Pharmac Pharmaceutical Schedule, Community Medicines (CPSReporting.xlsx)](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

