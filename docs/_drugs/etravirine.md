---
layout: default
title: Etravirine
parent: Model Prediction Only (L5)
nav_order: 189
evidence_level: L5
indication_count: 8
last_modified_at: 2026-11-24
monitor_inn: "ETRAVIRINE"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "fibroma of prostate", "Brenner tumor", "benign reproductive system neoplasm", "benign prostate phyllodes tumor", "obsolete familial combined hyperlipidemia"]
description: "Etravirine: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
---

# Etravirine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

## Etravirine: Feline acquired immunodeficiency syndrome
{: .text-alpha }

## One-Sentence Summary

Etravirine is a non-nucleoside reverse transcriptase inhibitor (NNRTI) originally used to treat HIV infection and AIDS.
The TxGNN model predicts it may be effective for **feline acquired immunodeficiency syndrome**, but **no clinical trials and no publications** currently support this prediction.
It is a model-only prediction and should be treated as a hypothesis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 0.9998 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action and Microbiology sections of the US FDA-approved prescribing information for etravirine: "Etravirine is an antiretroviral drug. […] Etravirine is an NNRTI of HIV-1. Etravirine binds directly to reverse transcriptase (RT) and blocks the RNA-dependent and DNA-dependent DNA polymerase activities by causing a disruption of the enzyme's catalytic site. Etravirine does not inhibit the human DNA polymerases α, β, and γ." ([DailyMed: Intelence (etravirine) prescribing information, Janssen Products LP](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=6a9cbc29-9f15-4b24-8d86-206b82887f3d), label version 2023-08-15; accessed 2026-11-19.) Based on known information, etravirine is an NNRTI that blocks HIV-1 reverse transcriptase. Its efficacy in HIV infection is established, and the TxGNN model (deep learning) scores for related immunodeficiency virus diseases may reflect similarities the model learned between related diseases, not biological evidence.

Feline immunodeficiency virus (FIV) is a lentivirus related to HIV, so the TxGNN model (deep learning) may have learned similarities between the two diseases. However, FIV reverse transcriptase is generally less sensitive to NNRTIs. The high score therefore comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, rather than validated antiviral activity in cats. No trials or literature were found to test this.

### Other Predictions Reviewed

The other top-ranked predictions are weaker or are not true repurposing:

- **Simian immunodeficiency virus infection (L4):** SIV, like HIV-2, is intrinsically resistant to most NNRTIs. The one related paper is an in vitro nanoparticle study of antiretroviral combinations against HIV (PMID 26529558), which is only indirect evidence.
- **Congenital HIV and AIDS related complex (L3):** These are HIV-1 disease states that fall under the existing indication, so they are not new indications. Supporting papers are observational or laboratory studies: etravirine use in pregnancy (case report of two patients, PMID 20587860), GI adverse events in HIV (PMID 19290032) and NNRTI resistance mutations (PMID 27067022).
- **Rare neurodevelopmental disorder, prostate and ovarian benign tumours, and obsolete familial combined hyperlipidaemia (L5):** There is no plausible mechanistic link and no supporting evidence. These scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the predicted indication of feline acquired immunodeficiency syndrome.

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2426900 | Intelence | Etravirine | Tab 200 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Knowledge-graph contraindication signals** (from graph relationships, not package insert text): chronic hepatitis C virus infection, hepatitis C virus infection, chronic hepatitis B virus infection, peripheral nervous system disease and peripheral neuropathy.
- **Drug Interactions:** 100 interactions were identified. The 20 reviewed here are mostly **Moderate**, with one Minor (ranitidine). Main ones include:
  - Acid suppressants: omeprazole, rabeprazole, cimetidine
  - Corticosteroids: dexamethasone, hydrocortisone, betamethasone, triamcinolone
  - Antidiabetic agents: pioglitazone, glimepiride, chlorpropamide, saxagliptin, linagliptin
  - Others: clarithromycin, metronidazole, aprepitant, dronabinol, eliglustat, naldemedine, naloxegol

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction rests on a TxGNN model (deep learning) score alone, which may reflect similarities the model learned between related diseases, not biological evidence, with no trials or literature. FIV and SIV are also expected to be poorly sensitive to NNRTIs.

<!-- review:begin nz-lic-20261106-etravirine-2 -->

> **Review correction (2026-11-06)**: Originally stated: "FIV and SIV are also expected to be poorly sensitive to NNRTIs. The only New Zealand licence is valid and the drug is marketed, but no package insert safety data is available." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-etravirine-2 -->

**To proceed, the following is needed:**
- In vitro activity data of etravirine against FIV reverse transcriptase, followed by veterinary pharmacology and safety studies if activity is shown
- Mechanism of action data for etravirine
- Package insert warnings and contraindications, obtained from Medsafe
- Review of the interaction profile and the hepatitis B/C and neuropathy signals before any use in a new population

<!-- review:begin nz-lic-20261031-etravirine-1 -->

> **Review correction (2026-10-31)**: Originally stated: "Package insert warnings and contraindications, obtained from Medsafe / Clarification of the regulatory position, since the only New Zealand licence is cancelled" Removed: it said the Schedule listings were cancelled, discontinued, or not current, which did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-10-31). The Quick Overview and the table under New Zealand Market Information show the current Pharmac Schedule figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261031-etravirine-1 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-10-31 | Removed: "Clarification of the regulatory position, since the only New Zealand licence is cancelled" | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "The only New Zealand licence is valid and the drug is marketed, but no package insert safety data is available" | Corrected (revised 2026-11-24; earlier version kept in the record) | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-19 | "Detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Intelence (etravirine) prescribing information, Janssen Products LP](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=6a9cbc29-9f15-4b24-8d86-206b82887f3d) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

