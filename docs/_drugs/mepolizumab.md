---
layout: default
title: Mepolizumab
parent: Model Prediction Only (L5)
nav_order: 285
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-28
monitor_inn: "MEPOLIZUMAB"
predicted_indications: ["thrombocytopenia due to immune destruction", "primary release disorder of platelets", "pseudo-von Willebrand disease", "autoimmune thrombocytopenic", "Glanzmann thrombasthenia"]
description: "Mepolizumab: predicted new indication Thrombocytopenia due to immune destruction (evidence level L5). Approved use: Not recorded in the local data"
---

# Mepolizumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Mepolizumab: Thrombocytopenia due to immune destruction
{: .text-alpha }

## One-Sentence Summary

Mepolizumab is an anti-IL-5 antibody, with eosinophilic pneumonia and pulmonary eosinophilia as its original indications.
The TxGNN model predicts it may be effective for **thrombocytopenia due to immune destruction**,
but support is thin: **0 clinical trials** and **1 publication** (a case report) have been compiled.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Thrombocytopenia due to immune destruction |
| TxGNN Prediction Score | 0.9966 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, mepolizumab is an anti-IL-5 monoclonal antibody that depletes eosinophils, and its use in eosinophil-driven conditions is established. Mechanistically, it could be relevant to immune thrombocytopenia only in a narrow setting.

The link between the original and new indications is indirect. The only supporting publication describes a steroid-resistant hypereosinophilic immune disorder, with a mixed thrombotic microangiopathy, treated with mepolizumab alongside other therapy. The cytopenia appeared to improve as the eosinophilia was controlled. Any benefit would therefore be secondary to correcting eosinophil-driven immune dysregulation, not a direct effect on antibody-mediated platelet destruction.

Classic immune thrombocytopenia is not eosinophil-mediated, so the mechanistic rationale is weak outside hypereosinophilic settings. The high TxGNN score reflects a computational prediction only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28648630](https://pubmed.ncbi.nlm.nih.gov/28648630/) | 2018 | Case report | Blood Cells, Molecules & Diseases | Steroid-resistant hypereosinophilic immune disorder resolved with mepolizumab, with concurrent improvement of a mixed thrombotic microangiopathy |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2606836 | Nucala (valid) | Inj 100 mg prefilled pen | Not available |

## Safety Considerations

Please refer to the package insert for safety information. No drug interaction records were found in the data compiled for this report.

## Other Predicted Indications (For Reference)

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Assessment |
|---|---|---|---|---|
| 2 | Primary release disorder of platelets | 0.9961 | L5 | Only a review of hypereosinophilic syndrome is linked; no plausible IL-5 pathway to platelet secretion defects |
| 3 | Pseudo-von Willebrand disease | 0.9944 | L5 | Caused by GP1BA gain-of-function variants; no mechanistic link, no literature |
| 4 | Autoimmune thrombocytopenic | 0.9933 | L5 | Overlaps with the top-ranked entry; should be consolidated with it |
| 5 | Glanzmann thrombasthenia | 0.9928 | L5 | Integrin αIIbβ3 defect; likely a false positive from network proximity |

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a single case report in a hypereosinophilic setting and a computational score. There are no registered trials, and the mechanism does not fit classic immune thrombocytopenia. The only New Zealand licence is cancelled, so there is no marketed product to build on.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which are currently missing and block safety screening
- Mechanism of action data from DrugBank
- Targeted literature review on anti-IL-5 therapy in eosinophil-associated immune cytopenias
- Clarification of whether any eosinophil-driven subset of immune thrombocytopenia exists to justify a trial
- Confirmation of current New Zealand availability, given the cancelled licence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

