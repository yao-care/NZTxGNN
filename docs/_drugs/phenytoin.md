---
layout: default
title: Phenytoin
parent: Model Prediction Only (L5)
nav_order: 364
evidence_level: L5
indication_count: 3
last_modified_at: 2026-11-12
monitor_inn: "Phenytoin"
predicted_indications: ["trigeminal nerve neoplasm", "trigeminal neuralgia", "beta-ketothiolase deficiency"]
description: "Phenytoin: predicted new indication Trigeminal nerve neoplasm (evidence level L5). Approved use: Not recorded in the local data"
---

# Phenytoin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Phenytoin: Trigeminal nerve neoplasm
{: .text-alpha }

## One-Sentence Summary

Phenytoin is a long-established antiseizure medicine, originally used to treat epilepsy, including generalized tonic-clonic seizures, partial seizures and status epilepticus.
The TxGNN model predicts it may be effective for **trigeminal nerve neoplasm**, but there are **no clinical trials** and only **2 publications** retrieved, and these concern trigeminal neuralgia and Sturge-Weber syndrome rather than neoplasm. The prediction is therefore supported by model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Trigeminal nerve neoplasm |
| TxGNN Prediction Score | 0.9999 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (5 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 5 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Phenytoin is generally known as a voltage-gated sodium channel blocker. It limits repetitive neuronal firing, and the pharmacology data compiled here list the sodium channel Nav1.2 (SCN2A) as a target. That mechanism fits epilepsy well.

The link to a trigeminal nerve neoplasm is weak. The very high TxGNN score most likely reflects network proximity to trigeminal neuralgia and seizure-related nodes, not any antitumour activity. Phenytoin has no established antineoplastic mechanism in this setting. The retrieved literature covers trigeminal neuralgia treatment and Sturge-Weber syndrome, not tumours. This looks like a model artefact, not a real repurposing opportunity.

The related condition **trigeminal neuralgia** (rank 9, score 0.9997) is much better supported mechanistically and clinically. It is covered under Conclusion and Next Steps.

## Clinical Trial Evidence

Currently no related clinical trials registered.

<!-- review:begin nz-lic-20261106-phenytoin-1 -->

> **Review correction (2026-11-06)**: Originally stated: "Phenytoin is currently marketed in New Zealand, and all 5 of the 5 licences are valid." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 5 listings for this drug, 5 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-phenytoin-1 -->

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [17997704](https://pubmed.ncbi.nlm.nih.gov/17997704/) | 2007 | Review | Expert Rev Neurother | Overview of medical and surgical treatments for trigeminal neuralgia. It concerns a pain syndrome, not neoplasm. |
| [9157801](https://pubmed.ncbi.nlm.nih.gov/9157801/) | 1997 | Case series | An Esp Pediatr | Experience with 14 cases of Sturge-Weber syndrome (clinical features, course, treatment response). Not about neoplasm. |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2619431 | Dilantin | Phenytoin sodium | Cap 100 mg | Yes |
| 2619458 | Dilantin | Phenytoin sodium | Cap 30 mg | Yes |
| 391646 | Hospira | Phenytoin sodium | Inj 50 mg per ml, 5 ml ampoule | Yes |
| 2619466 | Dilantin Paediatric | Phenytoin sodium | Oral liq 30 mg per 5 ml | Yes |
| 2619474 | Dilantin Infatab | Phenytoin sodium | Tab 50 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

The knowledge graph flags the following conditions as contraindication-related for phenytoin (this comes from graph data, not from package insert text):

- **Cardiac:** long QT syndrome (including familial), torsades de pointes, hypotensive disorder
- **Liver:** liver failure, liver disease, hepatic coma, hepatic encephalopathy, gallbladder disease
- **Kidney:** kidney disease, pyoureter
- **Blood:** thrombocytopenia, neutropenia, agranulocytosis
- **Metabolic:** hyperglycemia, diabetes mellitus, diabetic ketoacidosis, inherited porphyria / porphyria
- **Autoimmune:** systemic lupus erythematosus (including paediatric)
- **Respiratory and sleep:** obstructive sleep apnea, sleep apnea syndrome, congenital central hypoventilation syndrome, Haddad syndrome
- **Psychiatric:** anxiety disorder, neurotic disorder, dysthymic disorder, substance abuse/dependence

Published case reports in the retrieved literature also describe phenytoin-associated thrombocytopenia, hyperglycemia and diabetic ketoacidosis, and erythema multiforme in patients receiving cranial radiotherapy (EMPACT syndrome). The EMPACT reports matter in oncology settings involving the brain.

For drug interactions, the only entry is pharmacological (target: Nav1.2 / SCN2A) and is not a drug-drug interaction. Please refer to the package insert for formal warnings, contraindications and interactions.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for trigeminal nerve neoplasm rests on model score alone (L5). There are no trials, the literature does not address tumours, and no antineoplastic mechanism is known.

<!-- review:begin nz-lic-20261106-phenytoin-2 -->

> **Review correction (2026-11-06)**: Originally stated: "There are no trials, the literature does not address tumours, and no antineoplastic mechanism is known. The product is also no longer licensed in New Zealand." Removed: it conflicted with the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 5 listings for this drug, 5 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-phenytoin-2 -->

**Other predicted indications in the data (all Hold unless noted):**
- **Trigeminal neuralgia (L3, "Research Question"):** The most plausible repurposing direction. Phenytoin shares the sodium channel mechanism of carbamazepine, the first-line therapy. Retrospective data support IV phenytoin for acute exacerbations (a 144-case cohort and another retrospective cohort), but guidelines treat it as non-first-line, and evidence for it is weak with no RCTs.
- **Reflex and stimulus-induced seizures** (micturition-induced, thinking, audiogenic, eating, orgasm-induced, startle, reading): These are subtypes within the existing epilepsy indication, so they are not true repurposing. Support is mostly preclinical or case-report level.
- **Beta-ketothiolase deficiency (L5):** No mechanistic link; the score of 0.9995 comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

**To proceed, the following is needed:**
- A package insert for phenytoin (Medsafe warnings and contraindications are missing, which blocks safety screening)
- Mechanism of action data from DrugBank
- For trigeminal neuralgia: a focused evidence review or a trial comparing phenytoin (especially IV for acute crises) with standard therapy, plus a decision on whether a licensed product could be re-supplied in New Zealand
- For trigeminal nerve neoplasm: preclinical or clinical evidence of antitumour activity, otherwise this indication should be deprioritised

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "Phenytoin is currently marketed in New Zealand, and all 5 of the 5 licences are valid" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "The product is also no longer licensed in New Zealand" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

