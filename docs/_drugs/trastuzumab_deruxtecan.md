---
layout: default
title: Trastuzumab Deruxtecan
parent: Model Prediction Only (L5)
nav_order: 463
evidence_level: L5
indication_count: 1
last_modified_at: 2026-11-19
monitor_inn: "TRASTUZUMAB DERUXTECAN"
predicted_indications: ["drug-induced osteoporosis"]
description: "Trastuzumab Deruxtecan: predicted new indication Drug-induced osteoporosis (evidence level L5). In New Zealand: 2 Pharmac Schedule items"
---

# Trastuzumab Deruxtecan
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

## Trastuzumab Deruxtecan: Drug-induced osteoporosis
{: .text-alpha }

## One-Sentence Summary

Trastuzumab deruxtecan is a HER2-directed antibody-drug conjugate carrying a topoisomerase I inhibitor payload, used in HER2-expressing cancers. The TxGNN model predicts it may be effective for **drug-induced osteoporosis**, but **no clinical trials and no publications** currently support this direction. The prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Drug-induced osteoporosis |
| TxGNN Prediction Score | 0.9931 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for trastuzumab deruxtecan: "Fam-trastuzumab deruxtecan-nxki is a HER2-directed antibody-drug conjugate. The antibody is a humanized anti-HER2 IgG1. The small molecule, DXd, is a topoisomerase I inhibitor attached to the antibody by a cleavable linker." ([DailyMed: Enhertu (fam-trastuzumab deruxtecan-nxki) prescribing information, Daiichi Sankyo Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=7e67e73e-ddf4-4e4d-8b50-09d7514910b6), label version 2026-05-15; accessed 2026-11-19.) Based on known pharmacology, trastuzumab deruxtecan is a HER2-directed antibody-drug conjugate. Its payload, deruxtecan (an exatecan derivative), inhibits topoisomerase I and is cytotoxic to HER2-expressing tumour cells.

The link between the original use and the predicted indication is weak. Bone remodelling (osteoblast/osteoclast balance, RANKL/OPG, Wnt signalling) is not a recognised target of HER2 blockade or topoisomerase I inhibition. The high score of 0.9931 (rank 5693) is a TxGNN model (deep learning) prediction. It likely reflects network proximity or shared associations rather than a biological rationale.

The predicted direction of effect is also questionable. Cytotoxic payloads and oncology therapies are more often associated with adverse bone effects than with bone protection. On current information, this prediction should be treated as a hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2697610 | Baxter | Trastuzumab deruxtecan | Inj 1 mg for ECP | Yes |
| 2678276 | Enhertu | Trastuzumab deruxtecan | Inj 100 mg per ml, 1 ml vial | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Antibody-drug conjugate with a cytotoxic topoisomerase I inhibitor payload (targeted therapy with cytotoxic component) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions (typically blood counts, liver and renal function) |
| Handling Protection | Handle according to cytotoxic drug handling regulations |

## Safety Considerations

- **Drug Interactions**: The interaction screening found 42 interacting drugs. Examples of the Major-level interactions shown are:
  - Deferiprone
  - Samarium (153Sm) lexidronam
  - Adalimumab
  - Baricitinib
  - Certolizumab pegol
  - Cladribine
  - Leflunomide
  - Natalizumab

  Moderate-level examples include palifermin, strontium chloride Sr-89, roflumilast, chloramphenicol, alemtuzumab, anakinra, azathioprine, canakinumab, dimethyl fumarate and niraparib. Many of these are immunosuppressive or myelosuppressive agents, so overlapping toxicity is a concern.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score (L5). There are no clinical trials or publications, no credible mechanistic link, and a plausible risk of adverse bone effects.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- A literature and trial search to confirm whether any bone-related evidence exists
- Preclinical or mechanistic evidence for an effect on bone remodelling
- Clarification of the original approved indications and licensing status in New Zealand

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Enhertu (fam-trastuzumab deruxtecan-nxki) prescribing information, Daiichi Sankyo Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=7e67e73e-ddf4-4e4d-8b50-09d7514910b6) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

