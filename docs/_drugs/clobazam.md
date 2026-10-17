---
layout: default
title: Clobazam
parent: Model Prediction Only (L5)
nav_order: 106
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Clobazam: predicted new indication Febrile infection-related epilepsy syndrome (evidence level L5). Approved use: Lennox-Gastaut syndrome"
monitor_inn: "CLOBAZAM"
predicted_indications: ["febrile infection-related epilepsy syndrome", "perioral myoclonia with absences", "photosensitive occipital lobe epilepsy", "cryptogenic late-onset epileptic spasms", "atypical childhood epilepsy with centrotemporal spikes", "benign occipital epilepsy", "early-onset epileptic encephalopathy and intellectual disability due to GRIN2A mutation", "restless legs syndrome", "polymicrogyria with optic nerve hypoplasia"]
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

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Clobazam: From Lennox-Gastaut Syndrome to Febrile Infection-Related Epilepsy Syndrome

## One-Sentence Summary

Clobazam is a benzodiazepine originally used for Lennox-Gastaut syndrome (a severe childhood epilepsy).
The TxGNN model predicts it may be effective for **febrile infection-related epilepsy syndrome (FIRES)**,
but so far there are **no registered clinical trials** and only **2 publications** (a case series and a case report), neither of which studied clobazam directly.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Lennox-Gastaut syndrome (the New Zealand licence record contains no approved indication text) |
| Predicted New Indication | Febrile infection-related epilepsy syndrome |
| TxGNN Prediction Score | 99.82% |
| Evidence Level | L4 (see note below) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

*Note: By the strict rules, a prediction with no trials and only case-level literature would rank as L5. The L4 grading here follows the underlying scoring, which gives credit for the plausible class-level mechanism.*

## Why is This Prediction Reasonable?

Detailed mechanism of action data are not available in the data compiled for this report. Based on known information, clobazam is a 1,5-benzodiazepine that enhances GABA-A receptor signalling. Its efficacy in Lennox-Gastaut syndrome is established, and mechanistically it may be applicable to FIRES.

FIRES is a refractory epileptic encephalopathy that occurs in previously healthy children after a febrile illness. It is usually managed with GABAergic agents such as midazolam, lorazepam and barbiturates. This gives a plausible class-level link to clobazam.

The link is indirect, however. The two retrieved papers describe enteral lorazepam as a weaning strategy after midazolam and perampanel for reducing barbiturate dependency. Neither studied clobazam. Evidence for clobazam in FIRES is therefore based only on class effect.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35770765](https://pubmed.ncbi.nlm.nih.gov/35770765/) | 2022 | Case series | Epileptic Disorders | Enteral lorazepam was an effective weaning substitute in midazolam-dependent FIRES patients |
| [39958143](https://pubmed.ncbi.nlm.nih.gov/39958143/) | 2025 | Case report | Cureus | Perampanel may help reduce barbiturate dependency in a 13-year-old boy with FIRES |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 538620 (cancelled) | Frisium | Tab 10 mg | Not recorded |

## Safety Considerations

- **Drug Interactions**: 100 interactions were found in the interaction screen. The Major-level ones in the first 20 results are:
  - Morphine (including liposomal morphine)
  - Eliglustat

  Moderate interactions include bupropion, aprepitant, omeprazole, esomeprazole, lansoprazole, cimetidine, clarithromycin, metoclopramide, fenfluramine, dexfenfluramine and dronabinol. Antacids such as magnesium oxide and aluminium hydroxide are Minor.

Please refer to the package insert for other safety information (warnings and contraindications).

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high and the GABAergic mechanism is plausible. However, there are no clinical trials, and the only literature concerns other drugs (lorazepam, perampanel). In New Zealand the only licence is cancelled, and the package insert safety data have not been obtained, so safety screening cannot proceed.

**To proceed, the following is needed:**
- Obtain the Medsafe package insert for warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data (for example from DrugBank)
- Clobazam-specific evidence in FIRES, such as case series, registry data or a prospective study
- Clarification of the regulatory pathway, since there is no valid New Zealand licence
- Review of the Major interactions (morphine, eliglustat), especially given the intensive-care setting in which FIRES is treated

**Other predictions (for reference):**
- Predictions ranked 2 to 10 (including perioral myoclonia with absences, photosensitive occipital lobe epilepsy, late-onset epileptic spasms, atypical childhood epilepsy with centrotemporal spikes, childhood-onset epileptic encephalopathy, benign occipital epilepsy, GRIN2A-related encephalopathy, restless legs syndrome, and polymicrogyria with optic nerve hypoplasia) have no trials. Their literature is indirect, or absent for four of them.
- "Childhood-onset epileptic encephalopathy" overlaps the existing Lennox-Gastaut indication and is not a genuinely novel repurposing signal.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

