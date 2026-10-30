---
layout: default
title: Phenol
parent: Model Prediction Only (L5)
nav_order: 360
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-30
description: "Phenol: predicted new indication Acrodermatitis chronica atrophicans (evidence level L5). Approved use: Dermatitis"
monitor_inn: "phenol"
predicted_indications: ["acrodermatitis chronica atrophicans", "secondary interstitial lung disease specific to childhood associated with a connective tissue disease", "neonatal dermatomyositis", "amyopathic dermatomyositis", "acne keloid", "hydroa vacciniforme, familial", "severe nonproliferative diabetic retinopathy", "dry eye syndrome"]
---

# Phenol
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

## Phenol: From Dermatitis to Acrodermatitis Chronica Atrophicans
{: .text-alpha }

## One-Sentence Summary

Phenol is an active ingredient in a topical combination product (Egopsoryl TA), originally associated with the treatment of dermatitis.
The TxGNN model predicts it may be effective for **acrodermatitis chronica atrophicans**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction. The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Dermatitis |
| Predicted New Indication | Acrodermatitis chronica atrophicans |
| TxGNN Prediction Score | 0.9995 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, phenol is part of a topical combination product (with sulphur, menthol and allantoin), and the score for the predicted indication comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. Mechanistically, there is no established basis for use in the predicted indication.

Acrodermatitis chronica atrophicans is the late cutaneous stage of Lyme borreliosis and is treated with antibiotics. Phenol has no known antimicrobial role in this setting. The high score most likely reflects network proximity to other skin diseases, not a pharmacological rationale. The prediction should be treated as a hypothesis only.

For context, other predictions were also reviewed. Only **acne keloid** has some plausible, though indirect, support. Phenol is used in chemical peels that cause controlled skin injury and remodeling, and one uncontrolled study of a modified phenol peel (Exoderm) reported on wrinkles and acne scars. This is not keloid-specific evidence. Predictions for dry eye syndrome, dermatomyositis variants, diabetic retinopathy, and others lack plausible mechanisms or drug-specific evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the top predicted indication.

For reference, the one relevant publication found for a lower-ranked prediction (acne keloid):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [17204096](https://pubmed.ncbi.nlm.nih.gov/17204096/) | 2007 | Uncontrolled clinical study / case series | The Journal of Dermatology | Modified phenol peel (Exoderm) evaluated for facial wrinkles, acne scars and other skin problems in Asian patients, with the aim of reducing side-effects such as hypopigmentation |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 201863 | Egopsoryl TA | Soln 5% with sulphur 0.5%, menthol 0.75%, phenol 0.5% and allantoin crm 2.5% | Not specified |
| 201855 | Egopsoryl TA | Soln 5% with sulphur 0.5%, menthol 0.75%, phenol 0.5% and allantoin crm 2.5% | Not specified |

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text):** The graph links phenol to contraindications in conditions including liver disease, diabetes mellitus, diabetic ketoacidosis, neutropenia, agranulocytosis, epilepsy, heart conduction disease, atrioventricular block, sudden cardiac arrest, respiratory failure, peripheral vascular disease, active peptic ulcer disease, herpes simplex and herpes zoster infection, cytomegalovirus infection, and cancer (including lymphoma). These should be verified against the product label.
- **Phenol-peel concerns:** Cardiac toxicity (arrhythmia), scarring and pigmentary changes have been noted with phenol peels and would need to be addressed before any proposal in scarring-related indications.

No drug interaction records were found. Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction has no clinical trials or literature, no plausible mechanism, and a treatment standard (antibiotics) that phenol does not address. Evidence level is L5 (model prediction only).

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data for phenol (e.g., from DrugBank)
- Approved indication text for the two New Zealand licences
- If any direction is pursued, acne keloid (L4, research question) is the most plausible candidate, requiring keloid-specific preclinical or clinical data and a safety assessment of phenol peel toxicity
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

