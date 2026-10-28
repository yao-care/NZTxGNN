---
layout: default
title: Etravirine
parent: Model Prediction Only (L5)
nav_order: 189
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-28
monitor_inn: "ETRAVIRINE"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "fibroma of prostate", "Brenner tumor", "benign reproductive system neoplasm", "benign prostate phyllodes tumor", "obsolete familial combined hyperlipidemia"]
description: "Etravirine: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). Approved use: Not recorded in the local data"
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
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, etravirine is an NNRTI that blocks HIV-1 reverse transcriptase. Its efficacy in HIV infection is established, and the TxGNN model (deep learning) scores for related immunodeficiency virus diseases may reflect similarities the model learned between related diseases, not biological evidence.

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

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2426900 | Intelence | Tab 200 mg | Not recorded (licence valid) |

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
The top prediction rests on a TxGNN model (deep learning) score alone, which may reflect similarities the model learned between related diseases, not biological evidence, with no trials or literature. FIV and SIV are also expected to be poorly sensitive to NNRTIs. The only New Zealand licence is valid and the drug is marketed, but no package insert safety data is available.

**To proceed, the following is needed:**
- In vitro activity data of etravirine against FIV reverse transcriptase, followed by veterinary pharmacology and safety studies if activity is shown
- Mechanism of action data for etravirine
- Package insert warnings and contraindications, obtained from Medsafe
- Clarification of the regulatory position, since the only New Zealand licence is cancelled
- Review of the interaction profile and the hepatitis B/C and neuropathy signals before any use in a new population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

