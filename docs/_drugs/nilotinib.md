---
layout: default
title: Nilotinib
parent: Model Prediction Only (L5)
nav_order: 321
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-30
monitor_inn: "NILOTINIB"
predicted_indications: ["dermatofibrosarcoma protuberans"]
description: "Nilotinib: predicted new indication Dermatofibrosarcoma protuberans (evidence level L5). Approved use: Not recorded in the local data"
---

# Nilotinib
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

## Nilotinib: Dermatofibrosarcoma protuberans
{: .text-alpha }

## One-Sentence Summary

Nilotinib is a tyrosine kinase inhibitor originally used to treat BCR-ABL1-positive chronic myelogenous leukaemia.
The TxGNN model predicts it may be effective for **dermatofibrosarcoma protuberans (DFSP)**,
but at present there are **no registered clinical trials** and only **1 review article** supporting this direction, so the evidence is at the model-prediction and mechanism level.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Dermatofibrosarcoma protuberans |
| TxGNN Prediction Score | 0.9931 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data for nilotinib is not available in the data compiled for this report. The following reasoning is therefore based on the known pharmacology of the drug rather than on supplied data. Nilotinib inhibits BCR-ABL1, which drives chronic myelogenous leukaemia, and it also inhibits PDGFR-alpha/beta and KIT.

DFSP is characterised by a COL1A1-PDGFB translocation. This produces a constitutively active PDGFB ligand that drives PDGFR-beta signalling. Blocking PDGFR signalling is therefore a biologically plausible way to act against this tumour. Imatinib, a related tyrosine kinase inhibitor with the same target activity, is the established targeted therapy for unresectable or metastatic DFSP. This supports the plausibility of nilotinib as a related PDGFR-inhibiting agent, and it is consistent with the high TxGNN score.

This remains an inference. No trial or clinical data on nilotinib in DFSP were supplied.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29408302](https://pubmed.ncbi.nlm.nih.gov/29408302/) | 2018 | Review | Pharmacological Research | Review of small-molecule PDGFR inhibitors in neoplastic disorders, covering the role of PDGF/PDGFR signalling as a therapeutic target |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2439360 | Tasigna (valid) | Cap 200 mg | Not recorded |
| 2468115 | Tasigna (valid) | Cap 150 mg | Not recorded |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (tyrosine kinase inhibitor) |
| Myelosuppression Risk | Medium. The knowledge graph lists neutropenia, thrombocytopenia, anaemia and agranulocytosis among its contraindication-related conditions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC with differential, liver function, electrolytes (including potassium), blood glucose, ECG for QT interval |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Conditions flagged in the knowledge graph (not label text):**
  - Cardiovascular: long QT syndrome (including familial), torsades de pointes, hypertension
  - Hepatic and biliary: liver disease, gallbladder disease, hyperbilirubinaemia (including Gilbert syndrome), pancreatitis
  - Haematological: thrombocytopenia, neutropenia, agranulocytosis, anaemia
  - Metabolic and electrolyte: hyperglycaemia, diabetes mellitus, diabetic ketoacidosis, potassium deficiency, hypercalcaemia, nephrocalcinosis, phosphorus metabolism disorders, hypophosphataemia
- **Drug Interactions:** 100 interactions were identified. Examples of the **Major** level include:
  - Dexamethasone
  - Clarithromycin
  - Picosulfuric acid
  - Polyethylene glycol (3350 with electrolytes)

  **Moderate** examples include acid-suppressing drugs (famotidine, ranitidine, rabeprazole, omeprazole, cimetidine), corticosteroids (hydrocortisone, triamcinolone, betamethasone, budesonide), pioglitazone, loperamide, bupropion, aprepitant, bisacodyl and saxagliptin.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for authoritative safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support is a high model score, a plausible PDGFR-based mechanism and one general review, with no clinical trials. Both New Zealand licences are cancelled, and the package insert safety data needed for safety screening is missing.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which block safety screening
- Mechanism of action data from DrugBank
- Targeted literature on nilotinib in DFSP, including case series or trials, and comparison with imatinib
- Clarification of the reason for the licence cancellations and whether any nilotinib product is currently available in New Zealand
- A cardiac (QT) and metabolic safety monitoring plan, given the knowledge-graph contraindication signals and the Major drug interactions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

