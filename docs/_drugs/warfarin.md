---
layout: default
title: Warfarin
parent: Model Prediction Only (L5)
nav_order: 484
evidence_level: L5
indication_count: 7
last_modified_at: 2026-10-30
monitor_inn: "Warfarin"
predicted_indications: ["heparin cofactor 2 deficiency", "factor 5 excess with spontaneous thrombosis", "antithrombin deficiency type 2", "thrombophilia", "rheumatoid arthritis", "breast fibrocystic disease", "benign mammary dysplasia"]
description: "Warfarin: predicted new indication Heparin cofactor 2 deficiency (evidence level L5). Approved use: Not recorded in the local data"
---

# Warfarin
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

## Warfarin: Heparin cofactor 2 deficiency
{: .text-alpha }

## One-Sentence Summary

Warfarin is a vitamin K antagonist anticoagulant, originally used for pulmonary embolism, thrombotic disease and intracranial embolism.
The TxGNN model predicts it may be effective for **heparin cofactor 2 deficiency**, but the support is thin: **0 clinical trials** and **3 publications**, all case or family reports.
This is essentially an extension of warfarin's existing use in thrombosis, not a new therapeutic concept.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Heparin cofactor 2 deficiency |
| TxGNN Prediction Score | 0.9987 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (6 of 6 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 6 (valid: 6) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Pharmacology data indicate that warfarin inhibits vitamin K epoxide reductase (VKORC1), which reduces the vitamin K-dependent clotting factors (II, VII, IX and X). This makes it a standard long-term anticoagulant.

Heparin cofactor II deficiency is an inherited thrombophilia that predisposes patients to thrombosis. Long-term anticoagulation is therefore a plausible way to prevent clots, which is why the model links it to warfarin's existing thrombotic-disease indications.

The score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The available publications are case reports of thrombosis in affected patients, some of whom received warfarin. They do not show that warfarin changes outcomes in this condition.

**Other predictions reviewed (none stronger):**

| Predicted Indication | Score | Evidence Level | Assessment |
|------|------|------|------|
| Factor 5 excess with spontaneous thrombosis | 0.9984 | L5 | No trials or literature; prediction only |
| Antithrombin deficiency type 2 | 0.9984 | L5 | One case report; overlaps with existing thrombotic use |
| Thrombophilia | 0.9975 | L3 | One Phase 3 trial (status unknown) plus mostly reviews; already standard use, not true repurposing |
| Rheumatoid arthritis | 0.9921 | L4 | Trials use warfarin only as an interaction probe; no efficacy rationale |
| Breast fibrocystic disease | 0.9920 | L5 | Score appears to be a danazol-interaction artifact |
| Benign mammary dysplasia | 0.9910 | L5 | Prediction only; no plausible mechanism |

## Clinical Trial Evidence

Currently no related clinical trials registered for heparin cofactor 2 deficiency.

For the closest related prediction, thrombophilia, one trial exists:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00895505](https://clinicaltrials.gov/study/NCT00895505) | Phase 3 | Unknown | 300 | D-dimer-guided duration of oral anticoagulation after unprovoked deep vein thrombosis; tests a duration strategy, no results available |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2214444](https://pubmed.ncbi.nlm.nih.gov/2214444/) | 1990 | Case report | Kyobu Geka | Right ventricular thrombosis in a 14-year-old with familial heparin cofactor II deficiency |
| [2033902](https://pubmed.ncbi.nlm.nih.gov/2033902/) | 1991 | Case report | Nihon Kyobu Shikkan Gakkai Zasshi | Woman with congenital antithrombin deficiency and pulmonary infarction; treated with warfarin for 7 years, then switched to heparin |
| [11570053](https://pubmed.ncbi.nlm.nih.gov/11570053/) | 2001 | Case report (family study) | Journal of UOEH | Family with multiple thromboses, including onset in infancy; warfarin started in one child who later had a recurrence |

## New Zealand Market Information

The drug is marketed, with 6 valid licences out of 6, and no approved indication text is available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2586967 | Coumadin | Tab 1 mg | Not available (valid) |
| 2586975 | Coumadin | Tab 2 mg | Not available (valid) |
| 2586983 | Coumadin | Tab 5 mg | Not available (valid) |
| 210587 | Marevan | Tab 1 mg | Not available (valid) |
| 210595 | Marevan | Tab 3 mg | Not available (valid) |

A sixth licence (210609, Marevan Tab 5 mg) is also cancelled.

## Safety Considerations

The package insert warnings and contraindications could not be retrieved, so please refer to the package insert for safety information.

The following comes from drug-disease contraindication links in the knowledge graph, not from label text, and should be verified against the label:
- **Cardiovascular and metabolic:** hypertension, congestive heart failure, hyperlipidemia, hypothyroidism
- **Liver and biliary:** liver failure, liver disease, gallbladder disease, cholestasis
- **Kidney:** impaired renal function, nephrotic syndrome
- **Blood:** blood coagulation disease, anemia
- **Other:** psychotic and mental disorders, substance abuse/dependence, connective tissue and bone disease, chronic diarrheal disease

**Drug interaction data:** The only entry is the pharmacological target, VKORC1, which is warfarin's mechanism rather than an interaction with another drug. Literature in the data also notes interactions with leflunomide and danazol, and a rare complication, warfarin-induced skin necrosis, which is relevant in protein C/S deficiency.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for heparin cofactor 2 deficiency is limited to three old case reports, with no trials. Warfarin is already used for thrombotic disease, so this is not a distinct repurposing opportunity.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications
- Mechanism of action data from DrugBank
- Controlled or cohort data on warfarin in inherited thrombophilias such as heparin cofactor II deficiency
- Confirmation of which warfarin products are currently supplied in New Zealand, where all 6 licences are valid
- Consideration of newer anticoagulants (DOACs) as comparators, as discussed in the 2024 ISTH communication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

