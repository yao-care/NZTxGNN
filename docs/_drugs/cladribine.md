---
layout: default
title: Cladribine
parent: Model Prediction Only (L5)
nav_order: 102
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-15
description: "Cladribine: predicted new indication Parameningeal embryonal rhabdomyosarcoma (evidence level L5). Approved use: Hairy cell leukaemia, hairy cell leukaemia variant…"
monitor_inn: "CLADRIBINE"
predicted_indications: ["parameningeal embryonal rhabdomyosarcoma", "botryoid-type embryonal rhabdomyosarcoma of the vagina", "embryonal extrahepatic bile duct rhabdomyosarcoma", "prostate embryonal rhabdomyosarcoma", "extrahepatic bile duct rhabdomyosarcoma", "rhabdomyosarcoma (disease)", "liver sarcoma"]
---

# Cladribine
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

# Cladribine: From Hairy Cell Leukaemia to Parameningeal Embryonal Rhabdomyosarcoma

## One-Sentence Summary

Cladribine is a purine nucleoside analogue originally used for hairy cell leukaemia (including its variant) and relapsing-remitting multiple sclerosis.
The TxGNN model predicts it may be effective for **parameningeal embryonal rhabdomyosarcoma**,
but **no clinical trials and no directly relevant publications** currently support this prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hairy cell leukaemia, hairy cell leukaemia variant, relapsing-remitting multiple sclerosis (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Parameningeal embryonal rhabdomyosarcoma |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. From known pharmacology, cladribine is a purine nucleoside analogue that resists breakdown by adenosine deaminase and accumulates inside cells as 2-CdATP. This causes DNA strand breaks and apoptosis in lymphocytes and monocytes. Its activity depends on a high ratio of deoxycytidine kinase to 5'-nucleotidase, a pattern typical of lymphoid cells.

Its proven uses, hairy cell leukaemia and multiple sclerosis, both involve lymphoid cells. Rhabdomyosarcoma is a solid tumour of mesenchymal (muscle-lineage) origin. No evidence shows that cladribine's lymphocyte-selective mechanism applies to it.

The high score most likely reflects proximity within the knowledge graph to other cancers, and ontology clustering of rhabdomyosarcoma subtypes, rather than a validated biological link. The same applies to the other predictions in this family: botryoid-type embryonal rhabdomyosarcoma of the vagina, embryonal and unspecified extrahepatic bile duct rhabdomyosarcoma, prostate embryonal rhabdomyosarcoma, rhabdomyosarcoma in general, and liver sarcoma (scores 99.69% to 99.77%). These add no independent evidence. Standard rhabdomyosarcoma treatment is multi-agent chemotherapy (such as vincristine, actinomycin D and cyclophosphamide), and purine analogues have no established role.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the lead prediction.

For the lower-ranked prediction of liver sarcoma, one publication was retrieved, but it is off-target:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15241520](https://pubmed.ncbi.nlm.nih.gov/15241520/) | 2004 | Case report | Der Hautarzt | Successful cladribine therapy in smoldering systemic mastocytosis, a haematological neoplasm. It does not address liver sarcoma or rhabdomyosarcoma. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2595788 | Leustatin (cancelled) | Injection 1 mg/mL, 10 mL | Not recorded |
| 2207540 | Baxter (cancelled) | Injection 10 mg for ECP | Not recorded |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (purine nucleoside analogue / antimetabolite) |
| Myelosuppression Risk | High. The knowledge graph links cladribine to neutropenia, thrombocytopenia, anaemia and agranulocytosis as contraindicated conditions. |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Full blood count with differential, renal function, uric acid, and neurological assessment (peripheral neuropathy) |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Contraindications (from knowledge graph associations, not label text)**: kidney disease, pyoureter, thrombocytopenia, hyperuricaemia, neutropenia, agranulocytosis, peripheral nervous system disease, peripheral neuropathy, anaemia, macrocytic anaemia.
- **Drug Interactions**: 100 interactions were identified. The listed ones are mostly **Major**:
  - Corticosteroids: hydrocortisone, triamcinolone (including ophthalmic), dexamethasone, betamethasone, budesonide, prednisolone, prednisone.
  - Deferiprone.
  - Radiopharmaceuticals and radioimmunotherapies: ibritumomab tiuxetan, iobenguane (I-131), tositumomab (with and without I-131), samarium (153Sm) lexidronam, strontium chloride Sr-89.
  - **Moderate**: palifermin, dipyridamole, cilostazol, eltrombopag.
  - Acetylsalicylic acid is listed with an unknown severity level.

Package insert warnings were not available. Please refer to the package insert for the full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on model output alone (L5), with no clinical trials or relevant publications, and no plausible link between cladribine's lymphoid-selective mechanism and rhabdomyosarcoma. Both New Zealand licences are cancelled, and the drug carries significant myelosuppression and neurotoxicity concerns.

**To proceed, the following is needed:**
- Preclinical work first: cell-line sensitivity of rhabdomyosarcoma models to cladribine and deoxycytidine kinase / 5'-nucleotidase expression.
- Detailed mechanism-of-action data from DrugBank.
- The Medsafe package insert, to complete the warnings and contraindications for safety screening.
- Clarification of the regulatory and supply situation in New Zealand, since all licences are cancelled.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

