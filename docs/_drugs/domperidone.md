---
layout: default
title: Domperidone
parent: Model Prediction Only (L5)
nav_order: 163
evidence_level: L5
indication_count: 1
---

# Domperidone
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

# Domperidone: From Gastrointestinal Symptom Management to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Domperidone is a peripheral dopamine D2/D3 receptor antagonist, used to manage dyspepsia, heartburn, epigastric pain, nausea and vomiting.
The TxGNN model predicts it may be effective for **nephrogenic syndrome of inappropriate antidiuresis (NSIAD)**,
but there are currently **0 clinical trials** and **0 publications** supporting this direction. The prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the New Zealand licence record (the clinical-use description lists dyspepsia, heartburn, epigastric pain, nausea and vomiting) |
| Predicted New Indication | Nephrogenic syndrome of inappropriate antidiuresis |
| TxGNN Prediction Score | 99.08% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (0 currently valid) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Pharmacology records show that domperidone acts on the dopamine D2 receptor (DRD2) and D3 receptor (DRD3). It is used for upper gastrointestinal symptoms such as dyspepsia, nausea and vomiting.

NSIAD is caused by activating mutations of the vasopressin V2 receptor (AVPR2), or less commonly by GNAS-related signalling changes. Dopamine D2 antagonism has no established direct link to V2 receptor activity or renal water handling. Any connection, such as dopamine-vasopressin interactions in the collecting duct or off-target effects, would be speculative and would need independent validation.

The high score (0.991) may reflect proximity within the knowledge graph, for example shared associations with water balance or hyponatraemia-related phenotypes, rather than a true mechanism. The original and new indications are therefore not clearly related, and this prediction should be treated as a hypothesis only.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2631903 | Domperidone Viatris (cancelled) | Tab 10 mg | Not recorded |

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (from drug-disease relationships in the TxGNN knowledge graph, not from label text): long QT syndrome, familial long QT syndrome, torsades de pointes, liver disease, gallbladder disease. These point mainly to cardiac rhythm (QT prolongation) and hepatic concerns.
- **Drug Interactions**: The data compiled lists pharmacological targets only (dopamine D2 receptor and D3 receptor), with no specific drug-drug interactions identified.

Please refer to the package insert for full safety information, as label warnings and contraindications were not available.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has only a model score behind it: no clinical trials, no literature and no documented mechanistic link between D2/D3 antagonism and NSIAD. The only New Zealand licence is cancelled, and the knowledge graph flags QT-related contraindications that would matter in a patient group with electrolyte disturbance.

**To proceed, the following is needed:**
- Mechanistic evidence linking dopamine D2/D3 antagonism to vasopressin V2 receptor signalling or renal water handling
- Preclinical or literature support for the NSIAD indication
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- Assessment of QT-prolongation risk and electrolyte monitoring in the target population
- Confirmation of product availability and route compatibility in New Zealand, since the only licence is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

