---
layout: default
title: Cycloserine
parent: Model Prediction Only (L5)
nav_order: 126
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Cycloserine: predicted new indication Irritable bowel syndrome (evidence level L5). Approved use: Pulmonary tuberculosis"
monitor_inn: "CYCLOSERINE"
predicted_indications: ["irritable bowel syndrome", "acne (disease)", "gastroparesis (disease)", "conjunctivitis", "insomnia (disease)", "pharyngitis", "nasal cavity disease"]
---

# Cycloserine
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

# Cycloserine: From Pulmonary Tuberculosis to Irritable Bowel Syndrome

## One-Sentence Summary

Cycloserine is a second-line antibiotic originally used to treat pulmonary tuberculosis.
The TxGNN model predicts it may be effective for **irritable bowel syndrome**, but **no clinical trials and no publications** currently support this direction, so the prediction rests on computation alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Pulmonary tuberculosis |
| Predicted New Indication | Irritable bowel syndrome |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, cycloserine is a D-alanine analogue that inhibits bacterial cell wall synthesis, and it also acts as a partial agonist at the NMDA receptor glycine site. Its efficacy in tuberculosis is established, but a mechanistic route to irritable bowel syndrome is not supported by the available data.

Possible links, such as the gut-brain axis, effects on gut microbiota, or visceral hypersensitivity, are speculative. The very high TxGNN score (rank 784 among predictions for this drug) reflects patterns in the knowledge graph and has not been verified by any clinical or literature evidence. This should be treated as a hypothesis only.

Other predicted indications fare no better. Acne, gastroparesis, conjunctivitis, insomnia, pharyngitis and nasal cavity disease also have no supporting trials. For insomnia, the literature points the opposite way: a case report describes cycloserine **causing** insomnia and psychosis in multidrug-resistant tuberculosis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for irritable bowel syndrome.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2566508 | Cyclorin | Cap 250 mg | Not stated (authorization cancelled) |

## Cytotoxicity

Cycloserine is an antibacterial agent rather than an antineoplastic drug, so this section does not apply.

## Safety Considerations

- **Disease-related cautions from the knowledge graph (not label text)**: The drug-disease contraindication data lists epilepsy, anxiety disorder, neurotic disorder, dysthymic disorder, psychotic disorder, schizophreniform disorder, schizoaffective disorder, mental disorder, substance abuse/dependence, kidney disease, pyoureter, vitamin B12 deficiency, megaloblastic anemia and sideroblastic anemia.
- **Neuropsychiatric effects**: A published case report describes cycloserine-induced insomnia and psychosis in a patient treated for multidrug-resistant tuberculosis.
- **Drug Interactions** (11 recorded):
  - **Major**: bupropion, ethanol, iohexol, iopamidol
  - **Moderate**: picosulfuric acid, polyethylene glycol (3350 with electrolytes), sodium sulfate, caffeine, dicoumarol, warfarin, lindane

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for irritable bowel syndrome has a high model score but no clinical trials, no literature, and no documented mechanistic link. The drug's known neuropsychiatric toxicity and its cancelled status in New Zealand add further barriers.

**To proceed, the following is needed:**
- Mechanism of action data (for example from DrugBank) and a plausible mechanistic rationale for irritable bowel syndrome
- Package insert warnings and contraindications from Medsafe for safety screening
- Preclinical or early clinical evidence of benefit in irritable bowel syndrome
- A review of whether any route and formulation could be made available in New Zealand, given the cancelled authorization
- A risk assessment of neuropsychiatric adverse effects and of the interactions listed above
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

