---
layout: default
title: Hydroxocobalamin
parent: Model Prediction Only (L5)
nav_order: 226
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-30
monitor_inn: "HYDROXOCOBALAMIN"
predicted_indications: ["esophageal varices without bleeding", "esophageal varices with bleeding"]
description: "Hydroxocobalamin: predicted new indication Esophageal varices without bleeding (evidence level L5). Approved use: Not recorded in the local data"
---

# Hydroxocobalamin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Hydroxocobalamin: Esophageal varices without bleeding
{: .text-alpha }

## One-Sentence Summary

Hydroxocobalamin is a form of vitamin B12, originally used to treat vitamin B12 deficiency, pernicious anemia and cyanide poisoning.
The TxGNN model predicts it may be effective for **esophageal varices without bleeding**, but this is a computational prediction only, with **0 clinical trials** and **0 publications** supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Esophageal varices without bleeding |
| TxGNN Prediction Score | 0.9923 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, hydroxocobalamin is a form of vitamin B12 and also acts as a cyanide scavenger. Its efficacy in B12 deficiency and cyanide poisoning is established, but neither use has a known connection to portal hypertension or variceal pressure.

No established mechanistic link to esophageal varices has been identified. One speculative pathway is nitric oxide (NO) scavenging. Hydroxocobalamin binds NO and can raise blood pressure, while excess NO contributes to splanchnic vasodilation and portal hypertension in cirrhosis. This hypothesis is not supported by any data compiled for this report.

The very high score (0.9923) comes from the TxGNN model (deep learning) and should be interpreted cautiously. It may reflect similarities the model learned between related diseases, not biological evidence. The same score was given to the related indication "esophageal varices with bleeding" (0.9923), which suggests the model is predicting at the disease-class level rather than from indication-specific evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2637391 (cancelled) | Hydroxocobalamin Panpharma | Inj 1 mg per ml, 1 ml ampoule | Not available |

## Safety Considerations

- **Contraindications (knowledge-graph based, not from label text)**: Hypertensive disorder and hypertension are listed as contraindicated conditions. This is relevant to the speculative NO-scavenging mechanism, which could raise blood pressure.
- **Drug Interactions**: 32 potential interactions were identified in total. Those rated **Moderate** are arsenic trioxide, chloramphenicol, iodide I-131 and iodide I-123. Others, with unrated severity, include omeprazole, lansoprazole, simvastatin, loperamide, prednisolone, ranitidine, acetylsalicylic acid, budesonide, folic acid, valsartan, ramipril, lorazepam, citalopram, acetaminophen, mirtazapine and levothyroxine.
- **Bleeding variant**: In acute variceal hemorrhage, any agent that raises blood pressure could be harmful because of hemodynamic instability and rebleeding risk. Standard care (terlipressin or octreotide, antibiotics, endoscopic therapy) has no role for hydroxocobalamin.

Please refer to the package insert for further warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone (L5), with no clinical trials, no literature and no established mechanistic link. The only plausible hypothesis (NO scavenging) is speculative and raises a safety concern in patients with hypertension. The New Zealand authorization has also been cancelled.

**To proceed, the following is needed:**
- Mechanism of action data (e.g., from DrugBank) to support a mechanistic link analysis
- Package insert warnings and contraindications from Medsafe, which are required for safety screening
- Preclinical or mechanistic studies testing whether NO scavenging affects portal pressure
- Systematic literature and trial searches specific to hydroxocobalamin and portal hypertension or varices
- Confirmation of a currently available formulation and route in New Zealand, since the only licence is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

