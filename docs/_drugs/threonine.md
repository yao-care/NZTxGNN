---
layout: default
title: Threonine
parent: Model Prediction Only (L5)
nav_order: 450
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Threonine: predicted new indication Gastroparesis (evidence level L5). Approved use: No approved indication text available"
monitor_inn: "threonine"
predicted_indications: ["gastroparesis (disease)"]
---

# Threonine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Threonine: From Metabolic Nutritional Support (MMA/PA Formulas) to Gastroparesis
{: .text-alpha }

## One-Sentence Summary

Threonine is an essential amino acid. In New Zealand it has been used in MMA/PA dietary-management powders (for methylmalonic and propionic acidaemia), although no approved indication text is recorded for these products.
The TxGNN model predicts it may be effective for **gastroparesis**, but there are currently **0 clinical trials** and **0 publications** supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | No approved indication text available |
| Predicted New Indication | Gastroparesis |
| TxGNN Prediction Score | 99.32% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, threonine is an essential amino acid used in specialised amino-acid formulas, and no mechanistic link to gastric motility, gastric emptying, or enteric neuromuscular function can be established from the data compiled for this report.

The high prediction score most likely reflects network proximity in the knowledge graph (for example, shared associations with gastrointestinal or metabolic nodes) rather than a validated biological pathway. One speculative possibility is that threonine is abundant in intestinal mucins, so it might play a role in mucosal integrity or general nutritional support. This has not been verified and would need independent confirmation.

The original indication is also not documented, because the authorization records contain no indication text. A direct comparison between the original and new indication is therefore not possible at this stage.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2668564 | MMA/PA Explore 5 | Powder, 12.5 g sachets | Not recorded (cancelled) |
| 2498812 | MMA/PA Anamix Junior | Powder (neutral), 18 g sachets | Not recorded (cancelled) |
| 2668572 | MMA/PA Express 15 | Powder, 25 g sachets | Not recorded (cancelled) |
| 2386313 | MMA/PA Anamix Infant | Powder, can | Not recorded (cancelled) |

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease contraindication links in the TxGNN knowledge graph, not from label text): phenylketonuria and disorders of phenylalanine metabolism.

No drug interaction records were found. For all other safety information, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials, no literature, and no documented mechanism linking threonine to gastroparesis. In addition, all New Zealand authorizations are cancelled, and package insert safety information has not been reviewed.

**To proceed, the following is needed:**
- Mechanism of action data (for example, from DrugBank) and a plausible biological link to gastric motility
- Package insert warnings and contraindications from Medsafe, needed before any safety screening
- A literature and trial search specific to threonine and gastroparesis to look for any supporting evidence
- Clarification of the original approved indications for the MMA/PA products
- Assessment of route and formulation compatibility, as well as the significance of the phenylalanine-metabolism contraindication signals
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

