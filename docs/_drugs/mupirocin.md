---
layout: default
title: Mupirocin
parent: Model Prediction Only (L5)
nav_order: 313
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Mupirocin: predicted new indication Pleural empyema (evidence level L5). Approved use: Impetigo, bullous impetigo"
monitor_inn: "MUPIROCIN"
predicted_indications: ["pleural empyema (disease)", "punctate epithelial keratoconjunctivitis"]
---

# Mupirocin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Mupirocin: From Impetigo to Pleural Empyema

## One-Sentence Summary

Mupirocin is a topical antibiotic originally used to treat impetigo and bullous impetigo.
The TxGNN model predicts it may be effective for **pleural empyema**, but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the computational model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Impetigo, bullous impetigo (from the TxGNN knowledge graph; no approved indication text is available for the New Zealand licence) |
| Predicted New Indication | Pleural empyema |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L5 |
| New Zealand Market Status | ✓ Marketed |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. From general pharmacological knowledge, mupirocin inhibits bacterial isoleucyl-tRNA synthetase. It is active mainly against *Staphylococcus aureus* (including MRSA) and some streptococci. Its efficacy in skin infections such as impetigo is established.

These organisms are also among the pathogens found in pleural empyema, which is the plausible link behind the prediction. However, mupirocin is a topical agent with no meaningful systemic exposure. It is not suited to treating a deep-seated infection of the pleural space. The high TxGNN score (99.49%) is a computational output only. It should not be read as evidence of clinical benefit.

A second prediction, **punctate epithelial keratoconjunctivitis** (score 99.10%), also has no supporting trials or literature. This condition is often viral (e.g. adenovirus), so an antibacterial would have limited relevance. The marketed product is for skin or nasal use, and ocular safety and tolerability are not established.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 523712 | Bactroban | Oint 2% | Not available |

## Safety Considerations

- **Knowledge-graph contraindication signal**: The TxGNN knowledge graph records a contraindication link with **dermatitis**. This comes from graph data, not from package insert text.
- **Drug Interactions**: The interaction query returned 100 entries, all with an unclassified severity level. Examples include calcitriol, clotrimazole, morphine, metformin, pantoprazole, omeprazole, vancomycin, triamcinolone, prednisone and simvastatin. Clinical significance has not been graded.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score, with no clinical trials or literature. Mupirocin is topical with negligible systemic exposure, so it is a poor mechanistic fit for a deep pleural infection. The same applies to the ocular indication, for which no ophthalmic formulation exists.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank)
- Medsafe package insert warnings and contraindications. This gap blocks safety screening.
- Evidence that a suitable route of administration exists for the proposed indication
- Any clinical or preclinical studies in the predicted indications
- Clarification of the approved indication text for the New Zealand licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

