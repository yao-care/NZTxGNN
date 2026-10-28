---
layout: default
title: Cyclosporine
parent: Model Prediction Only (L5)
nav_order: 127
evidence_level: L5
indication_count: 7
monitor_inn: "Cyclosporine"
predicted_indications: ["granulomatous disease, chronic, autosomal recessive", "brain aneurysm", "colobomatous microphthalmia-rhizomelic dysplasia syndrome", "congenital ichthyosiform erythroderma", "fetal erythroblastosis", "brachydactyly-syndactyly syndrome", "prolapse of lacrimal gland"]
last_modified_at: 2026-10-28
description: "Cyclosporine: predicted new indication Chronic granulomatous disease, autosomal recessive (evidence level L5). Approved use: Not recorded in the local data"
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
| New Zealand Market Status | Marketed (valid licences: 4 of 4) |
| Number of Authorizations | 4 (valid: 4) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Cyclosporine is known as a calcineurin inhibitor that suppresses T-lymphocyte activation, and it is used as an immunosuppressant in transplantation and in several inflammatory conditions.

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

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 455598 | Neoral | Cap 100 mg | Not recorded |
| 455571 | Neoral | Cap 25 mg | Not recorded |
| 2011344 | Neoral | Cap 50 mg | Not recorded |
| 455563 | Neoral | Oral liq 100 mg per ml | Not recorded |

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
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

