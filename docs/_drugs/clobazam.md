---
layout: default
title: Clobazam
parent: Model Prediction Only (L5)
nav_order: 106
evidence_level: L5
indication_count: 10
monitor_inn: "CLOBAZAM"
predicted_indications: ["febrile infection-related epilepsy syndrome", "perioral myoclonia with absences", "photosensitive occipital lobe epilepsy", "cryptogenic late-onset epileptic spasms", "atypical childhood epilepsy with centrotemporal spikes", "childhood onset epileptic encephalopathy", "benign occipital epilepsy", "early-onset epileptic encephalopathy and intellectual disability due to GRIN2A mutation", "restless legs syndrome", "polymicrogyria with optic nerve hypoplasia"]
last_modified_at: 2026-11-19
description: "Clobazam: predicted new indication febrile infection-related epilepsy syndrome (evidence level L5). In New Zealand: 1 Pharmac Schedule item"
---

# Clobazam
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Clobazam: GABA-A positive allosteric modulator → febrile infection-related epilepsy syndrome
{: .text-alpha }

## One-Sentence Summary

Clobazam is a benzodiazepine-class antiseizure medicine that enhances GABA-A receptor activity.
The TxGNN model predicts it may be useful for **febrile infection-related epilepsy syndrome**, but the data compiled for this report contain **no clinical trials** and only **2 publications**, both case-level reports on other drugs.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | febrile infection-related epilepsy syndrome |
| TxGNN Prediction Score | 0.9982 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

According to the Mechanism of Action section of the US FDA-approved prescribing information for clobazam: "The exact mechanism of action for clobazam, a 1,5-benzodiazepine, is not fully understood but is thought to involve potentiation of GABAergic neurotransmission resulting from binding at the benzodiazepine site of the GABA A receptor." ([DailyMed: Onfi (clobazam) prescribing information, Lundbeck Pharmaceuticals LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=de03bd69-2dca-459c-93b4-541fd3e9571c), label version 2026-10-16; accessed 2026-11-19.) Based on general pharmacology, clobazam is a benzodiazepine that positively modulates GABA-A receptors, and this class is widely used in refractory epilepsies.

Febrile infection-related epilepsy syndrome (FIRES) is a severe, refractory epileptic encephalopathy that typically follows a febrile illness in previously healthy children. It is often managed with GABAergic agents such as midazolam, lorazepam and barbiturates. This makes a GABA-A–based mechanism plausible for clobazam.

However, the mechanistic link is indirect and rests on class effect only. The two retrieved papers concern enteral lorazepam and perampanel, not clobazam. No study directly evaluates clobazam in this disease, so the evidence level is L5.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35770765](https://pubmed.ncbi.nlm.nih.gov/35770765/) | 2022 | Case series | Epileptic Disorders | Enteral lorazepam as a weaning strategy in midazolam-dependent FIRES patients (not clobazam) |
| [39958143](https://pubmed.ncbi.nlm.nih.gov/39958143/) | 2025 | Case report | Cureus | Perampanel may reduce barbiturate dependency in a 13-year-old with FIRES (not clobazam) |

## New Zealand Market Information

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 538620 | Frisium | Clobazam | Tab 10 mg | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

**Drug Interactions** (100 interactions recorded; examples below):

- **Major:** morphine, liposomal morphine, eliglustat
- **Moderate:** bupropion, aprepitant, omeprazole, esomeprazole, lansoprazole, cimetidine, clarithromycin, metoclopramide, fenfluramine, dexfenfluramine, dronabinol
- **Minor:** antacids such as magnesium oxide, aluminium hydroxide and calcium carbonate

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is very high (0.9982), but no clinical trials exist and the only literature involves other drugs in small case reports. Support for clobazam in this disease is a class-level extrapolation only.

**To proceed, the following is needed:**
- Clobazam-specific clinical data in febrile infection-related epilepsy syndrome (case series or trials)
- Detailed mechanism of action data
- Package insert warnings and contraindications from Medsafe
- A safety plan addressing CNS-depressant interactions (e.g. opioids) in critically ill patients

<!-- review:begin log -->

## Review Log

Below is the review log for this page. A statement that mechanism-of-action data were not available was replaced with an excerpt from the cited official prescribing information or database; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-19 | "Currently, detailed mechanism of action data is not available in the data compiled for this report" replaced with a sourced mechanism-of-action statement | Statement that mechanism-of-action data were not available replaced with a sourced excerpt | [DailyMed: Onfi (clobazam) prescribing information, Lundbeck Pharmaceuticals LLC](https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=de03bd69-2dca-459c-93b4-541fd3e9571c) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

