---
layout: default
title: Pibrentasvir
parent: Model Prediction Only (L5)
nav_order: 365
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Pibrentasvir: predicted new indication Hepatitis B virus infection (evidence level L5). Approved use: Chronic hepatitis C virus infection"
monitor_inn: "pibrentasvir"
predicted_indications: ["hepatitis B virus infection", "HIV infectious disease", "hepatitis E virus infection", "hepatitis A virus infection", "hepatitis, viral, animal", "Omsk hemorrhagic fever", "Kyasanur forest disease", "simian immunodeficiency virus infection", "feline acquired immunodeficiency syndrome", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
---

# Pibrentasvir
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

## Pibrentasvir: From Chronic Hepatitis C to Hepatitis B Virus Infection
{: .text-alpha }

## One-Sentence Summary

Pibrentasvir is an HCV NS5A inhibitor, originally used to treat chronic hepatitis C virus infection.
The TxGNN model predicts it may be effective for **hepatitis B virus infection**, but **no clinical trials** and **no HBV-specific publications** support this direction.
The prediction is a model output only, and the available evidence points to a network-proximity artifact rather than a real mechanism.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic hepatitis C virus infection (the license record in the data compiled for this report has no approved indication text) |
| Predicted New Indication | Hepatitis B virus infection |
| TxGNN Prediction Score | 99.84% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, pibrentasvir is an HCV NS5A inhibitor, used as a partner drug in the glecaprevir/pibrentasvir combination (Maviret). Its efficacy in hepatitis C is well established.

The mechanistic case for hepatitis B is weak. HBV is a DNA virus that replicates through reverse transcription and has no NS5A homolog, so an NS5A inhibitor has no plausible direct target. The very high TxGNN score most likely reflects the closeness of "hepatitis B" to "hepatitis C" in the knowledge graph rather than a true pharmacological link. All the literature retrieved concerns HCV, not HBV. HBV reactivation is a known safety concern during HCV direct-acting antiviral therapy in coinfected patients, which is the opposite of an efficacy signal.

The other nine predictions (for example HIV, hepatitis E, hepatitis A, Omsk hemorrhagic fever, Kyasanur forest disease) share the same problem: no supporting mechanism and no disease-specific evidence. The HIV-related trials are HCV studies in HIV/HCV-coinfected people and do not test any anti-HIV effect.

## Clinical Trial Evidence

Currently no related clinical trials registered for hepatitis B virus infection.

## Literature Evidence

The publications linked to this prediction all address HCV or general viral hepatitis. None reports pibrentasvir activity against HBV.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31981264](https://pubmed.ncbi.nlm.nih.gov/31981264/) | 2020 | Cohort | Journal of Viral Hepatitis | Real-world study of glecaprevir/pibrentasvir in 108 Taiwanese HCV patients with CKD stage 4 or 5 (HCV outcomes) |
| [30982721](https://pubmed.ncbi.nlm.nih.gov/30982721/) | 2019 | Review | Lancet Gastroenterol Hepatol | HCV infection in children and adolescents; direct-acting antivirals have transformed HCV treatment |
| [35579223](https://pubmed.ncbi.nlm.nih.gov/35579223/) | 2022 | Review | Eur J Gen Pract | Practical overview of HCV diagnosis and treatment |
| [34092970](https://pubmed.ncbi.nlm.nih.gov/34092970/) | 2021 | Review | World J Gastroenterol | Pediatric HBV and HCV management; HBV treatment remains far from curative |
| [31041789](https://pubmed.ncbi.nlm.nih.gov/31041789/) | 2019 | Review | Semin Liver Dis | Retreatment of HCV patients after direct-acting antiviral failure |
| [31114957](https://pubmed.ncbi.nlm.nih.gov/31114957/) | 2019 | Review | Clin Pharmacokinet | Pharmacokinetic and pharmacodynamic considerations of HCV therapies |
| [29369303](https://pubmed.ncbi.nlm.nih.gov/29369303/) | 2018 | Review | AIDS Rev | Conference report on viral hepatitis treatment, including pangenotypic HCV regimens |
| [42127258](https://pubmed.ncbi.nlm.nih.gov/42127258/) | 2026 | Review | Top Antivir Med | CROI 2026 summary of viral hepatitis advances; no pibrentasvir activity against HBV reported |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2552019 | Maviret | Tab 100 mg with pibrentasvir 40 mg | Not recorded (authorization cancelled) |

## Safety Considerations

Please refer to the package insert for safety information. No drug interaction records were found in the data compiled for this report, and no knowledge-graph contraindications were listed.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a high model score. There is no plausible mechanism, no HBV trials, no HBV literature, and the only New Zealand authorization is cancelled. HBV reactivation during HCV antiviral therapy is a safety concern, not a sign of benefit.

**To proceed, the following is needed:**
- Mechanism of action data and any in vitro evidence of pibrentasvir activity against HBV
- Package insert warnings and contraindications, in particular the HBV reactivation warning
- Evidence of an active New Zealand regulatory pathway, since the only authorization is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

