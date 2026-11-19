---
layout: default
title: Cladribine
parent: Model Prediction Only (L5)
nav_order: 102
evidence_level: L5
indication_count: 7
monitor_inn: "CLADRIBINE"
predicted_indications: ["parameningeal embryonal rhabdomyosarcoma", "botryoid-type embryonal rhabdomyosarcoma of the vagina", "embryonal extrahepatic bile duct rhabdomyosarcoma", "prostate embryonal rhabdomyosarcoma", "extrahepatic bile duct rhabdomyosarcoma", "rhabdomyosarcoma (disease)", "liver sarcoma"]
last_modified_at: 2026-11-19
description: "Cladribine: predicted new indication Parameningeal embryonal rhabdomyosarcoma (evidence level L5). In New Zealand: 2 Pharmac Schedule items"
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

## Cladribine: purine nucleoside analogue → Parameningeal embryonal rhabdomyosarcoma (parameningeal embryonal rhabdomyosarcoma)
{: .text-alpha }

## One-Sentence Summary

Cladribine is a purine nucleoside analogue that is cytotoxic to lymphocytes and monocytes.
The TxGNN model predicts it may be relevant to **parameningeal embryonal rhabdomyosarcoma** (score 0.9977),
but **no clinical trials and no publications** on this disease were found, so the prediction rests on the model alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Parameningeal embryonal rhabdomyosarcoma (parameningeal embryonal rhabdomyosarcoma) |
| TxGNN Prediction Score | 0.9977 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for cladribine: "The mechanism by which cladribine exerts its therapeutic effects in patients with multiple sclerosis has not been fully elucidated but is thought to involve cytotoxic effects on B and T lymphocytes through impairment of DNA synthesis, resulting in depletion of lymphocytes." ([DailyMed: Mavenclad (cladribine) prescribing information, EMD Serono, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=9c75e30a-a410-40f1-b653-04d532bd9144), label version 2026-05-27; accessed 2026-11-19.) Based on general pharmacology, cladribine resists breakdown by adenosine deaminase and accumulates inside cells as 2-CdATP. This causes DNA strand breaks and apoptosis in lymphocytes and monocytes. Its activity depends on a high ratio of deoxycytidine kinase to 5'-nucleotidase, a feature typical of lymphoid cells.

Rhabdomyosarcoma is a mesenchymal solid tumour, and nothing indicates that this mechanism applies to it. The high score most likely reflects the model's proximity between cancers rather than a validated biological link. Standard rhabdomyosarcoma therapy is multi-agent chemotherapy (for example vincristine, actinomycin D and cyclophosphamide), and purine analogues have no established role. Other predictions for this drug (other rhabdomyosarcoma subtypes and liver sarcoma) belong to the same disease family and add no independent support.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available for this disease. The only publication retrieved in the wider prediction set (PMID 15241520, 2004) describes cladribine in smoldering systemic mastocytosis, a different disease, and does not support this prediction.

---

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2595788 | Leustatin | Cladribine | Inj 1 mg per ml, 10 ml | Yes |
| 2207540 | Baxter | Cladribine | Inj 10 mg for ECP | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (purine nucleoside analogue) |
| Myelosuppression Risk | High (neutropenia, thrombocytopenia and anaemia are recognised concerns) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Complete blood count with differential, liver and renal function |
| Handling Protection | Follow cytotoxic drug handling regulations |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction screen. Those listed are mostly Major, including corticosteroids (hydrocortisone, prednisone, prednisolone, dexamethasone, betamethasone, budesonide, triamcinolone), deferiprone, and radiopharmaceuticals (ibritumomab tiuxetan, iobenguane I-131, tositumomab, samarium-153 lexidronam, strontium-89 chloride). Moderate interactions include palifermin, dipyridamole, cilostazol and eltrombopag.
- **Conditions of concern**: Kidney disease, neutropenia, thrombocytopenia, agranulocytosis, anaemia (including macrocytic anaemia), peripheral neuropathy and peripheral nervous system disease are flagged as conditions of concern. These are not taken from label text.

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction has a high model score but no clinical trials, no publications on this disease, and no plausible mechanistic link to lymphocyte-selective cytotoxicity. Cladribine also carries significant myelosuppression and neurotoxicity concerns.

**To proceed, the following is needed:**
- Preclinical work first: cell-line sensitivity of rhabdomyosarcoma and deoxycytidine kinase expression
- Detailed mechanism-of-action data
- Package insert warnings and contraindications from Medsafe
- A safety review for myelosuppression and interactions with corticosteroids

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism-of-action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Mavenclad (cladribine) prescribing information, EMD Serono, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=9c75e30a-a410-40f1-b653-04d532bd9144) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

