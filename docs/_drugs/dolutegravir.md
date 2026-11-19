---
layout: default
title: Dolutegravir
parent: Model Prediction Only (L5)
nav_order: 162
evidence_level: L5
indication_count: 3
monitor_inn: "DOLUTEGRAVIR"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
last_modified_at: 2026-11-19
description: "Dolutegravir: predicted new indication feline acquired immunodeficiency syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Dolutegravir
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

## Dolutegravir: HIV integrase strand transfer inhibitor → feline acquired immunodeficiency syndrome
{: .text-alpha }

## One-Sentence Summary

Dolutegravir is an HIV integrase strand transfer inhibitor used against human immunodeficiency virus infection.
The TxGNN model predicts it may be relevant to **feline acquired immunodeficiency syndrome**, but there are **0 registered clinical trials** and only **1 publication** (a 2023 veterinary study of combination antiretroviral therapy in cats), so the evidence is very limited.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 0.9985 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action and Microbiology sections of the US FDA-approved prescribing information for dolutegravir: "Dolutegravir is an HIV-1 antiretroviral agent. […] Dolutegravir inhibits HIV integrase by binding to the integrase active site and blocking the strand transfer step of retroviral DNA integration which is essential for the HIV replication cycle. Strand transfer biochemical assays using purified HIV-1 integrase and pre-processed substrate DNA resulted in IC 50 values of 2.7 nM and 12.6 nM." ([DailyMed: Tivicay (dolutegravir sodium) prescribing information, ViiV Healthcare Company](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=63df5af3-b8ac-4e76-9830-2dbb340af922), label version 2026-08-25; accessed 2026-11-19.) Based on general pharmacology, dolutegravir blocks the integrase enzyme that HIV-1 uses to insert its genetic material into host cells. Feline immunodeficiency virus (FIV) is a lentivirus, in the same family as HIV, and carries an integrase enzyme related to that of HIV-1. This gives a plausible mechanistic rationale.

The high TxGNN score most likely reflects how closely this disease resembles human HIV/AIDS in the model's data. It should be read as a similarity signal, not as a measure of proven benefit.

The only publication retrieved (PMID 37112803) examined a combination of dolutegravir, tenofovir and emtricitabine in FIV-infected cats. However, it is a veterinary study of a combination regimen, and the available text does not show what the outcome was for dolutegravir specifically. This would also be veterinary rather than human repurposing. Because no cited study clearly evaluates dolutegravir alone for this disease, the evidence level is capped at L5.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37112803](https://pubmed.ncbi.nlm.nih.gov/37112803/) | 2023 | Preclinical/veterinary study | Viruses | Evaluated pharmacokinetics and clinical outcomes of a combination of dolutegravir, tenofovir and emtricitabine in FIV-infected domestic cats, along with immunophenotype. Results for dolutegravir alone are not available. |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2510979 | Tivicay | Dolutegravir | Tab 50 mg | Yes |
| 2674335 | Dovato | Dolutegravir with lamivudine | Tab 50 mg with lamivudine 300 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

The 62 drug interactions recorded include these main ones:

- **Major interactions (polyvalent cations and antacids)**: aluminium hydroxide, attapulgite, kaolin, magaldrate, calcium salts (acetate, carbonate, citrate, glubionate, gluconate, lactate, phosphate), iron, and magnesium salts (carbonate, chloride, citrate). These products can reduce absorption of dolutegravir.
- **Minor interactions**: aprepitant, cimetidine, clarithromycin, clotrimazole and dexamethasone.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high and the mechanism is plausible, since FIV is a lentivirus related to HIV. However, there are no registered trials, and the single publication tests a combination regimen in cats without clear results for dolutegravir. The prediction would also be a veterinary use, not a human one.

**To proceed, the following is needed:**
- The full text of PMID 37112803, to establish what role dolutegravir played and what the outcomes were
- Detailed mechanism of action data
- Package insert warnings and contraindications from Medsafe
- A clear decision on whether veterinary use is within the scope of this repurposing programme

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Tivicay (dolutegravir sodium) prescribing information, ViiV Healthcare Company](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=63df5af3-b8ac-4e76-9830-2dbb340af922) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

