---
layout: default
title: Cinchocaine
parent: Model Prediction Only (L5)
nav_order: 98
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Cinchocaine: predicted new indication Bronchitis (evidence level L5). Approved use: Dermatitis"
monitor_inn: "cinchocaine"
predicted_indications: ["bronchitis", "acrodermatitis chronica atrophicans", "neonatal dermatomyositis", "secondary interstitial lung disease specific to childhood associated with a connective tissue disease", "acne keloid", "hydroa vacciniforme, familial", "amyopathic dermatomyositis"]
---

# Cinchocaine
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

## Cinchocaine: From Dermatitis to Bronchitis
{: .text-alpha }

## One-Sentence Summary

Cinchocaine (dibucaine) is an amide-type local anaesthetic, originally associated with dermatitis treatment and marketed in New Zealand in topical anorectal products.
The TxGNN model predicts it may be effective for **bronchitis**, but **no clinical trials and no publications** currently support this prediction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Dermatitis (from the knowledge graph; no approved indication text is available in New Zealand licence records) |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known pharmacology, cinchocaine is an amide-type local anaesthetic that blocks voltage-gated sodium channels. Its use in dermatitis is topical symptomatic relief rather than disease modification.

In theory, a local anaesthetic effect could dampen airway sensory nerve activity and the cough reflex, which is a loose rationale for bronchitis. However, no clinical or literature evidence supports this. The link rests only on the TxGNN prediction score, and the model may be reflecting network proximity rather than a true pharmacological connection.

Other top-ranked predictions (all scoring above 99.6%) are acrodermatitis chronica atrophicans, neonatal dermatomyositis, a childhood connective-tissue-disease-associated interstitial lung disease, acne keloid, familial hydroa vacciniforme and amyopathic dermatomyositis. All are model predictions only, with no trials or publications, and most have at best a symptomatic rationale (itch or pain relief) with no disease-modifying mechanism.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 208388 | Proctosedyl | Ointment, cinchocaine hydrochloride 5 mg per g (cancelled) | Not recorded |
| 208396 | Proctosedyl | Suppository, cinchocaine hydrochloride 5 mg per g (cancelled) | Not recorded |
| 257451 | Ultraproct | Ointment, fluocortolone pivalate 920 mcg + cinchocaine hydrochloride 5 mg per g (cancelled) | Not recorded |
| 257478 | Ultraproct | Suppository, fluocortolone pivalate 610 mcg + cinchocaine hydrochloride 1 mg (cancelled) | Not recorded |

---

## Safety Considerations

- **Contraindications (from knowledge graph, not label text)**: epilepsy, kidney disease, liver disease, gallbladder disease, pyoureter, sudden cardiac arrest, heart conduction disease, cardiovascular disease, heart disease, respiratory failure, myasthenia gravis (including adult-onset), neuromuscular junction disease, butyrylcholinesterase deficiency.
- **Drug Interactions**: No interaction records were found.
- Of note for the predicted indications: respiratory failure appears among the knowledge-graph contraindications, which is relevant to a respiratory indication such as bronchitis. Safety of amide anaesthetics in neonates and children is also a concern for several predicted paediatric conditions.

Please refer to the package insert for label warnings and contraindications, which were not available for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score, with no clinical trials, no literature and no established mechanistic link. All New Zealand licences are cancelled, and the available route of administration (topical anorectal) does not match what a bronchitis indication would require.

**To proceed, the following is needed:**
- Mechanism of action data for cinchocaine
- Medsafe package insert warnings and contraindications for a safety screen
- Preclinical or literature evidence for an airway or bronchitis effect
- Assessment of route and formulation compatibility (for example inhaled or lozenge) and safety given the respiratory-failure contraindication signal
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

