---
layout: default
title: Emtricitabine
parent: Model Prediction Only (L5)
nav_order: 176
evidence_level: L5
indication_count: 3
last_modified_at: 2026-11-19
monitor_inn: "EMTRICITABINE"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
description: "Emtricitabine: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Emtricitabine
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

## Emtricitabine: Feline acquired immunodeficiency syndrome
{: .text-alpha }

## One-Sentence Summary

Emtricitabine is a nucleoside reverse transcriptase inhibitor, originally used to treat HIV infection and AIDS.
The TxGNN model predicts it may be effective for **feline acquired immunodeficiency syndrome**, but there are **0 clinical trials** and only **1 publication** (an animal study) supporting this direction.
This is a veterinary indication with very limited relevance to human drug repurposing.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 0.9992 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (7 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 7 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action and Microbiology sections of the US FDA-approved prescribing information for emtricitabine: "Emtricitabine is an antiretroviral drug. […] FTC, a synthetic nucleoside analog of cytidine, is phosphorylated by cellular enzymes to form emtricitabine 5'-triphosphate (FTC-TP), which inhibits the activity of the HIV-1 reverse transcriptase (RT) by competing with the natural substrate deoxycytidine 5'-triphosphate and by being incorporated into nascent viral DNA resulting in chain termination. FTC-TP is a weak inhibitor of mammalian DNA polymerases α, β, ε, and mitochondrial DNA polymerase γ." ([DailyMed: Emtriva (emtricitabine) prescribing information, Gilead Sciences, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=d6599395-3944-44f9-97f2-e0424c6b6a1f), label version 2026-11-06; accessed 2026-11-19.) Based on known information, emtricitabine is a nucleoside reverse transcriptase inhibitor (NRTI) with proven activity against HIV-1 reverse transcriptase, and it may be mechanistically applicable to related lentiviruses.

Feline immunodeficiency virus (FIV) is a lentivirus whose reverse transcriptase is structurally related to that of HIV, so NRTI activity is plausible. The very high TxGNN score most likely reflects this shared antiviral target class and the similarity between FIV and HIV.

The only supporting publication (PMID 37112803, 2023) evaluated a combination antiretroviral regimen (dolutegravir, tenofovir and emtricitabine) in FIV-infected cats. Only part of the abstract was available, so the specific contribution of emtricitabine and the clinical outcomes cannot be confirmed. Because this is a veterinary disease, it offers little value as a human repurposing opportunity.

The other predictions are weaker or not true repurposing:
- **Simian immunodeficiency virus infection** (score 0.9992, L4): The literature consists of macaque studies of emtricitabine with tenofovir for pre-exposure prophylaxis (PrEP) and of resistance mutations. These support the mechanism and the preclinical basis of human PrEP, but SIV is an animal model of HIV and is essentially the same therapeutic area as the approved indication.
- **Neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter** (score 0.9992, L5): No mechanistic link, trials or literature were identified. This should be treated as a computational artifact.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37112803](https://pubmed.ncbi.nlm.nih.gov/37112803/) | 2023 | Preclinical/Veterinary study | Viruses | Evaluated pharmacokinetics and clinical outcomes of combination antiretroviral therapy (dolutegravir, tenofovir, emtricitabine) in FIV-infected domestic cats; study type inferred from title and not verified |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2250063 | Emtriva | Emtricitabine | Cap 200 mg | Yes |
| 2705508 | TEEVIR | Efavirenz with emtricitabine and tenofovir disoproxil | Tab 600 mg with emtricitabine 200 mg and tenofovi… | Yes |
| 2727994 | TEEVIR (India) | Efavirenz with emtricitabine and tenofovir disoproxil | Tab 600 mg with emtricitabine 200 mg and tenofovi… | Yes |
| 2732041 | Tenofovir Efavirenz Emtricitabine(Mylan) | Efavirenz with emtricitabine and tenofovir disoproxil | Tab 600 mg with emtricitabine 200 mg and tenofovi… | Yes |
| 2644967 | Viatris | Efavirenz with emtricitabine and tenofovir disoproxil | Tab 600 mg with emtricitabine 200 mg and tenofovi… | Yes |
| 2719495 | Tenofovir Disoproxil Emtricitabine Mylan | Emtricitabine with tenofovir disoproxil | Tab 200 mg with tenofovir disoproxil 245 mg (300… | Yes |
| 2638088 | Tenofovir Disoproxil Emtricitabine Viatr | Emtricitabine with tenofovir disoproxil | Tab 200 mg with tenofovir disoproxil 245 mg (300… | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Contraindication-related associations (from a knowledge graph, not label text)**: The knowledge graph links emtricitabine to a number of conditions, including liver failure, liver disease, kidney disease, chronic hepatitis B and C virus infection, lactic acidosis, Fanconi renotubular syndrome, hypophosphatemia, osteomalacia, rickets, fatty liver disease and non-alcoholic steatohepatitis, long QT syndrome, torsades de pointes, epilepsy, and several psychiatric disorders. These are not confirmed label contraindications and should be verified against the official product information.
- **Drug Interactions**: 20 interactions were identified. The two **major** interactions are:
  - Leflunomide
  - Teriflunomide

  Most of the remaining interactions are moderate, including orlistat, naltrexone, cobicistat, methotrexate, cladribine, clofarabine, epirubicin, idelalisib, cabozantinib, brentuximab vedotin, tioguanine, trabectedin, asparaginase, pegaspargase and the interferon beta products. Eribulin is minor.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication is a veterinary disease with no clinical trials and a single animal study of a multi-drug regimen. Emtricitabine's contribution to the outcome cannot be isolated.

**To proceed, the following is needed:**
- Mechanism of action data for emtricitabine
- Package insert warnings and contraindications from Medsafe
- Review of the full text of PMID 37112803 to confirm the role of emtricitabine
- A clear decision on whether a veterinary indication falls within the scope of this repurposing programme

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Emtriva (emtricitabine) prescribing information, Gilead Sciences, Inc.](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=d6599395-3944-44f9-97f2-e0424c6b6a1f) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

