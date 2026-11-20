---
layout: default
title: Mupirocin
parent: Model Prediction Only (L5)
nav_order: 313
evidence_level: L5
indication_count: 2
last_modified_at: 2026-11-12
monitor_inn: "MUPIROCIN"
predicted_indications: ["pleural empyema (disease)", "punctate epithelial keratoconjunctivitis"]
description: "Mupirocin: predicted new indication Pleural empyema (disease) (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
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

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Mupirocin: Pleural empyema (disease)
{: .text-alpha }

## One-Sentence Summary

Mupirocin is a topical antibiotic originally used to treat impetigo and bullous impetigo.
The TxGNN model predicts it may be effective for **pleural empyema**, but there are currently **0 clinical trials** and **0 publications** supporting this direction, so the prediction rests on the computational model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pleural empyema (disease) |
| TxGNN Prediction Score | 0.9949 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action and Microbiology sections of the US FDA-approved prescribing information for mupirocin: "Mupirocin is an RNA synthetase inhibitor antibacterial. […] Mupirocin inhibits bacterial protein synthesis by reversibly and specifically binding to bacterial isoleucyl-transfer RNA (tRNA) synthetase. Mupirocin is bactericidal at concentrations achieved by topical administration." ([DailyMed: Pirnuo (mupirocin) prescribing information, Glenmark Pharmaceuticals Inc., USA](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=89c43f7b-1a88-45bf-a356-07518d4e7870), label version 2020-03-20; accessed 2026-11-20.) From general pharmacological knowledge, mupirocin inhibits bacterial isoleucyl-tRNA synthetase. It is active mainly against *Staphylococcus aureus* (including MRSA) and some streptococci. Its efficacy in skin infections such as impetigo is established.

These organisms are also among the pathogens found in pleural empyema, which is the plausible link behind the prediction. However, mupirocin is a topical agent with no meaningful systemic exposure. It is not suited to treating a deep-seated infection of the pleural space. The high TxGNN score (0.9949) is a computational output only. It should not be read as evidence of clinical benefit.

A second prediction, **punctate epithelial keratoconjunctivitis** (score 0.9910), also has no supporting trials or literature. This condition is often viral (e.g. adenovirus), so an antibacterial would have limited relevance. The marketed product is for skin or nasal use, and ocular safety and tolerability are not established.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 523712 | Bactroban | Mupirocin | Oint 2% | No |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

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

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-20 | "Detailed mechanism of action data is not currently available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Pirnuo (mupirocin) prescribing information, Glenmark Pharmaceuticals Inc., USA](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=89c43f7b-1a88-45bf-a356-07518d4e7870) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

