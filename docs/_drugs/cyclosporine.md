---
layout: default
title: Cyclosporine
parent: Model Prediction Only (L5)
nav_order: 127
evidence_level: L5
indication_count: 7
monitor_inn: "Cyclosporine"
predicted_indications: ["granulomatous disease, chronic, autosomal recessive", "brain aneurysm", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "congenital ichthyosiform erythroderma", "fetal erythroblastosis", "brachydactyly-syndactyly syndrome", "prolapse of lacrimal gland"]
last_modified_at: 2026-11-20
description: "Cyclosporine: predicted new indication Chronic granulomatous disease, autosomal recessive (evidence level L5). In New Zealand: 4 Pharmac Schedule items"
---

# Cyclosporine
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

## Cyclosporine: calcineurin inhibitor immunosuppressant → chronic granulomatous disease, autosomal recessive
{: .text-alpha }

## One-Sentence Summary

Cyclosporine is a calcineurin-inhibitor immunosuppressant that suppresses T-cell activation.
The TxGNN model predicts it may be effective for **autosomal recessive chronic granulomatous disease**, but the evidence is very thin: **0 clinical trials** and **1 publication**, which is about stem cell transplantation outcomes and does not test cyclosporine as a treatment.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Chronic granulomatous disease, autosomal recessive |
| TxGNN Prediction Score | 0.9968 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 4 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

According to the Clinical Pharmacology section of the US FDA-approved prescribing information for cyclosporine: "Cyclosporine is a potent immunosuppressive agent that in animals prolongs survival of allogeneic transplants involving skin, kidney, liver, heart, pancreas, bone marrow, small intestine, and lung. Cyclosporine has been demonstrated to suppress some humoral immunity and to a greater extent, cell-mediated immune reactions such as allograft rejection, delayed hypersensitivity, experimental allergic encephalomyelitis, Freund’s adjuvant arthritis, and graft versus host disease in many animal species for a variety of organs." ([DailyMed: Cyclosporine, Modfied (cyclosporine) prescribing information, Apotex Corp.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=5baaf78c-90f6-abcf-52c8-814490f3cf9f), label version 2026-09-10; accessed 2026-11-20.) Cyclosporine is known as a calcineurin inhibitor that suppresses T-lymphocyte activation, and it is used as an immunosuppressant in transplantation and in several inflammatory conditions.

This does not provide a clear mechanistic link to the predicted disease. Chronic granulomatous disease is caused by a defect in the phagocyte NADPH oxidase system, not by overactive T cells. Suppressing T-cell activation would not be expected to correct that defect. The only publication found concerns stem cell transplantation for this disease. In that setting, cyclosporine could at most appear as graft-versus-host disease prophylaxis, not as treatment of the disease itself. The high model score is therefore not supported by independent clinical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [22078471](https://pubmed.ncbi.nlm.nih.gov/22078471/) | 2012 | Cohort | The Journal of Allergy and Clinical Immunology | Survival after sibling or unrelated donor stem cell transplantation in chronic granulomatous disease; it does not evaluate cyclosporine as a therapy for the disease |

---

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 455598 | Neoral | Ciclosporin | Cap 100 mg | Yes |
| 455571 | Neoral | Ciclosporin | Cap 25 mg | Yes |
| 2011344 | Neoral | Ciclosporin | Cap 50 mg | Yes |
| 455563 | Neoral | Ciclosporin | Oral liq 100 mg per ml | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

---

## Safety Considerations

- **Drug Interactions**: Pharmacology data show that cyclosporine interacts with several transporters and proteins, including ABCG2, OATP1B1 (SLCO1B1), OATP1B3 (SLCO1B3), the sodium/bile acid cotransporter SLC10A1, FPR1, and the cyclophilins PPIA and PPID. These point to a potential for transporter-mediated interactions with co-administered drugs. They are not graded interaction levels.

Please refer to the package insert for full warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
There are no clinical trials, and the only publication is about stem cell transplantation, not about cyclosporine treating this disease. The pathology (a phagocyte oxidase defect) has no clear link to calcineurin inhibition, so the high model score is not supported by evidence.

**To proceed, the following is needed:**
- Any direct clinical or preclinical study of cyclosporine in chronic granulomatous disease, rather than as transplant GVHD prophylaxis
- A plausible mechanistic rationale linking calcineurin inhibition to the disease
- Mechanism of action data and package insert safety information (warnings and contraindications) from Medsafe

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-20 | "Detailed mechanism of action data is not currently available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Cyclosporine, Modfied (cyclosporine) prescribing information, Apotex Corp.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=5baaf78c-90f6-abcf-52c8-814490f3cf9f) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

