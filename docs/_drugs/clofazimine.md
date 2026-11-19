---
layout: default
title: Clofazimine
parent: Model Prediction Only (L5)
nav_order: 108
evidence_level: L5
indication_count: 3
monitor_inn: "CLOFAZIMINE"
predicted_indications: ["pneumocystosis", "malaria", "gastrin secretion abnormality"]
last_modified_at: 2026-11-19
description: "Clofazimine: predicted new indication pneumocystosis (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
---

# Clofazimine
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

## Clofazimine: riminophenazine antimycobacterial agent → Pneumocystosis (pneumocystosis)
{: .text-alpha }

## One-Sentence Summary

Clofazimine is a riminophenazine antimycobacterial drug with immunomodulatory properties.
The TxGNN model predicts it may be relevant to **pneumocystosis** (score 0.9990), but **no clinical trials** and only **3 publications** were found, and none of them shows clofazimine acting against Pneumocystis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | pneumocystosis |
| TxGNN Prediction Score | 0.9990 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action and Microbiology sections of the US FDA-approved prescribing information for clofazimine: "LAMPRENE is an anti-mycobacterial drug. […] Clofazimine exerts a slow bactericidal effect on Mycobacterium leprae (Hansen’s bacillus). Clofazimine inhibits mycobacterial growth and binds preferentially to mycobacterial DNA. Clofazimine also exerts anti-inflammatory properties in treating erythema nodosum leprosum reactions." ([DailyMed: Lamprene (clofazimine) prescribing information, Novartis Pharmaceuticals Corporation](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=2e07448f-f888-4747-80ab-570c1b621250), label version 2026-08-17; accessed 2026-11-19.) Clofazimine is a riminophenazine antimycobacterial agent, reported to have immunomodulatory and membrane-disrupting properties. Nothing in the data compiled for this report shows activity against Pneumocystis.

The retrieved literature mentions Pneumocystis only as a co-occurring opportunistic infection in people with AIDS. One randomised trial studied clofazimine as prophylaxis for *Mycobacterium avium* complex (MAC) in HIV patients, some of whom had a prior Pneumocystis pneumonia. A case report describes an AIDS patient with *M. kansasii* lung disease complicated by Pneumocystis pneumonia, who recovered after a regimen that added trimethoprim-sulfamethoxazole once the Pneumocystis pneumonia appeared. In neither paper was Pneumocystis the target of clofazimine.

The high score is a model output only. No clear mechanistic link has been established.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8501340](https://pubmed.ncbi.nlm.nih.gov/8501340/) | 1993 | Randomised open-label trial | The Journal of Infectious Diseases | Clofazimine as prophylaxis for disseminated MAC infection in 110 HIV patients; the target was MAC, not Pneumocystis |
| [2714863](https://pubmed.ncbi.nlm.nih.gov/2714863/) | 1989 | Case report | Infection | AIDS patient with *M. kansasii* lung disease complicated by Pneumocystis pneumonia; co-infection, not a treatment effect of clofazimine |
| [11363899](https://pubmed.ncbi.nlm.nih.gov/11363899/) | 1996 | Review | PI Perspective | General update on opportunistic infections; no abstract available |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 654450 | Lamprene | Clofazimine | Cap 50 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Cytotoxicity

This section is omitted because clofazimine is not an antineoplastic drug.

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction screening. Examples:
  - Major: cisapride, dolasetron
  - Moderate: famotidine, loperamide, bisacodyl, clarithromycin, levofloxacin, ondansetron, granisetron, palonosetron, lactulose, magnesium citrate, magnesium hydroxide, castor oil
  - Minor: metronidazole

Please refer to the package insert for other safety information (warnings and contraindications).

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. There are no registered trials, and none of the retrieved publications studies clofazimine against pneumocystosis. Pneumocystosis already has established therapies, and this report offers no basis to consider clofazimine in their place.

**To proceed, the following is needed:**
- Laboratory data on clofazimine activity against Pneumocystis
- Mechanism of action data for clofazimine
- Package insert warnings and contraindications (Medsafe data sheet)
- A review of the major interactions (cisapride, dolasetron) and their relevance to the likely patient population

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Lamprene (clofazimine) prescribing information, Novartis Pharmaceuticals Corporation](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=2e07448f-f888-4747-80ab-570c1b621250) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

