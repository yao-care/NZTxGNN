---
layout: default
title: Filgrastim
parent: Model Prediction Only (L5)
nav_order: 200
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Filgrastim: predicted new indication Primary release disorder of platelets (evidence level L5). Approved use: Constitutional neutropenia, severe congenital neutropenia…"
monitor_inn: "FILGRASTIM"
predicted_indications: ["primary release disorder of platelets", "pseudo-von Willebrand disease", "Glanzmann thrombasthenia", "Scott syndrome", "hemorrhagic disorder due to a constitutional thrombocytopenia", "bleeding diathesis due to a collagen receptor defect", "C1 inhibitor deficiency", "serpinopathy with toxic serpin polymerization", "fetal and neonatal alloimmune thrombocytopenia", "platelet-type bleeding disorder"]
---

# Filgrastim
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Filgrastim: From Severe Congenital Neutropenia to Primary Release Disorder of Platelets

## One-Sentence Summary

Filgrastim (G-CSF) was originally used to treat chronic neutropenia conditions, including severe congenital neutropenia, constitutional neutropenia, and cyclic hematopoiesis.
The TxGNN model predicts it may be effective for **primary release disorder of platelets**, but **no clinical trials or publications** currently support this specific prediction.
This is a model-only prediction with no direct supporting evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Constitutional neutropenia, severe congenital neutropenia, cyclic hematopoiesis, chronic familial neutropenia (the approved indication text for the New Zealand licences is not available) |
| Predicted New Indication | Primary release disorder of platelets |
| TxGNN Prediction Score | 99.998% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, filgrastim is a granulocyte colony-stimulating factor (G-CSF) that acts on the granulocytic lineage through the G-CSF receptor. Its efficacy in neutropenia conditions is established.

The link to primary release disorder of platelets is weak. This is a platelet secretion defect, and there is no established mechanistic connection to G-CSF signalling. The high TxGNN score most likely reflects shared neighbours in the haematological disease network of the knowledge graph rather than a biological rationale.

The same pattern appears in the other top-ranked predictions, such as pseudo-von Willebrand disease, Glanzmann thrombasthenia and Scott syndrome. Each is a platelet-intrinsic or unrelated disorder with no plausible G-CSF mechanism. The prediction should be treated as a hypothesis only.

---

## Clinical Trial Evidence

Currently no related clinical trials are registered for the top predicted indication.

For reference, seven trials were matched to a related prediction, platelet-type bleeding disorder (rank 10). All are haematopoietic stem cell transplantation protocols in which filgrastim is likely a supportive agent for mobilization or engraftment. They do not test filgrastim for platelet disorders and are graded low relevance.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00296023](https://clinicaltrials.gov/study/NCT00296023) | NA | Completed | 25 | Low-dose allogeneic stem cell transplant in high-risk low-grade haematologic malignancies |
| [NCT00004232](https://clinicaltrials.gov/study/NCT00004232) | Phase 1 | Completed | Not reported | CD34-enriched stem cells to augment bone marrow in allogeneic transplant |
| [NCT00054236](https://clinicaltrials.gov/study/NCT00054236) | Phase 1 | Completed | 55 | Multiple cord blood unit transplant after non-myeloablative conditioning |
| [NCT00301912](https://clinicaltrials.gov/study/NCT00301912) | Phase 2 | Withdrawn | 0 | Busulfan and fludarabine preparative regimen; no data |
| [NCT00295997](https://clinicaltrials.gov/study/NCT00295997) | NA | Unknown | 35 | Non-myeloablative transplant with matched unrelated donors |
| [NCT00080925](https://clinicaltrials.gov/study/NCT00080925) | Phase 1 | Completed | 20 | T-cell depleted haploidentical transplant, reduced intensity |
| [NCT00004904](https://clinicaltrials.gov/study/NCT00004904) | Phase 1 | Completed | Not reported | T-cell depleted partially matched donor transplant with CD34 "veto" cells |

---

## New Zealand Market Information

Both New Zealand authorizations are cancelled, and no approved indication text is available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2561956 | Nivestim | Injection 300 mcg per 0.5 ml prefilled syringe | Not available |
| 2561964 | Nivestim | Injection 480 mcg per 0.5 ml prefilled syringe | Not available |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were recorded, and the 20 reviewed are all rated Moderate. They include:
  - Radiopharmaceuticals and radioimmunotherapy: iobenguane (I-131), fludeoxyglucose (18F), ibritumomab tiuxetan, tositumomab, tositumomab (I-131)
  - Cytotoxic and antineoplastic agents: paclitaxel, mercaptopurine, trimetrexate, azacitidine, bendamustine, altretamine, arsenic trioxide, abemaciclib
  - Others: levamisole, acalabrutinib, aldesleukin, alemtuzumab, asparaginase (E. coli), avapritinib, belantamab mafodotin

Please refer to the package insert for warnings and contraindications, as these were not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L5). There are no trials or publications for primary release disorder of platelets, and no plausible mechanism links G-CSF to a platelet secretion defect. Filgrastim also has no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Mechanism of action data for filgrastim, and a biological rationale connecting G-CSF signalling to platelet release function
- Preclinical or clinical evidence in platelet release disorders
- Medsafe package insert warnings and contraindications for a safety screen
- Clarification of the market situation, since both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

