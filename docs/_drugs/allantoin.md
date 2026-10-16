---
layout: default
title: Allantoin
parent: Model Prediction Only (L5)
nav_order: 28
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-15
description: "Allantoin: predicted new indication Severe nonproliferative diabetic retinopathy (evidence level L5). Approved use: Psoriasis, parapsoriasis, seborrheic dermatitis, pityriasis simplex…"
---

# Allantoin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

# Allantoin: From Psoriasis and Dermatitis to Severe Nonproliferative Diabetic Retinopathy

## One-Sentence Summary

Allantoin is a topical skin agent, originally used for inflammatory and scaling skin conditions such as psoriasis, seborrheic dermatitis and related dermatoses.
The TxGNN model predicts it may be effective for **severe nonproliferative diabetic retinopathy**,
but **no clinical trials and no publications** currently support this prediction, so it rests on a model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Psoriasis, parapsoriasis, seborrheic dermatitis, pityriasis simplex, dermatitis, seborrheic keratosis (from the knowledge graph; the New Zealand licences list no indication text) |
| Predicted New Indication | Severe nonproliferative diabetic retinopathy |
| TxGNN Prediction Score | 99.56% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, allantoin is a topical skin-conditioning agent with wound-healing, keratolytic and anti-irritant properties, and it is used in dermatological products for psoriasis and dermatitis.

A mechanistic link to diabetic retinopathy is not apparent. Retinopathy is a microvascular disease of the retina, and allantoin has no established effect on retinal vessels. The high score most likely reflects proximity in the knowledge graph rather than demonstrated biology. Delivery is also a major unresolved issue: a topical dermatological product would need to reach the eye, and any systemic exposure would be unclear.

Other predictions for this drug are mostly skin-related. Among them, **acne keloid** has the most plausible connection, since allantoin is used in scar-care products and is reported to influence fibroblast and extracellular matrix activity. That link is based on general knowledge, not on data compiled for this report, and the evidence for scar-modifying effects is weak. Other predicted conditions include acrodermatitis chronica atrophicans, neonatal and amyopathic dermatomyositis, familial hydroa vacciniforme, exanthem, and childhood connective-tissue-disease-associated interstitial lung disease. All are Hold with L5 evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 201863 | Egopsoryl TA | Solution 5% with sulphur 0.5%, menthol 0.75%, phenol 0.5% and allantoin cream 2.5% | Not stated |
| 201855 | Egopsoryl TA | Solution 5% with sulphur 0.5%, menthol 0.75%, phenol 0.5% and allantoin cream 2.5% | Not stated |

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease links, not from label text): porphyria (including inherited porphyria and porphyrin metabolism disease), kidney disease, pyoureter, haematologic disease, liver disease, gallbladder disease, G6PD deficiency, glycogen storage disease, cystinuria, galactosemia, folliculitis, seborrheic dermatitis and dermatitis. These may partly reflect other ingredients in the combination product (for example sulphur, menthol and phenol) and must be verified against the official label.
- **Drug Interactions**: No interaction records were found.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score, with no trials or publications, no known mechanistic link to retinal microvascular disease, and unresolved questions about ocular delivery. The only New Zealand products are multi-ingredient topical preparations, which are not suited to this indication.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data
- A targeted literature review, prioritising acne keloid and other skin-related predictions, which are more plausible than retinopathy
- Assessment of route and formulation feasibility, including ocular delivery and systemic exposure
- Clarification of approved indications for the two licences
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

