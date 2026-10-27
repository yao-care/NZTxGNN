---
layout: default
title: Abacavir
parent: Model Prediction Only (L5)
nav_order: 11
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
description: "Abacavir: predicted new indication Simian immunodeficiency virus infection (evidence level L5). Approved use: HIV infectious disease"
monitor_inn: "abacavir"
predicted_indications: ["simian immunodeficiency virus infection", "feline acquired immunodeficiency syndrome", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
---

# Abacavir
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

## Abacavir: From HIV Infection / AIDS to Simian Immunodeficiency Virus Infection
{: .text-alpha }

## One-Sentence Summary

Abacavir is a nucleoside reverse transcriptase inhibitor originally used to treat HIV infection and AIDS.
The TxGNN model predicts it may be effective for **simian immunodeficiency virus (SIV) infection**, a non-human primate disease model.
Support is limited to **0 clinical trials** and **1 in vitro publication**, so this is largely a knowledge-graph extension of the original HIV use rather than a true human repurposing opportunity.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV infectious disease; AIDS (from the knowledge graph, because no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Simian immunodeficiency virus infection |
| TxGNN Prediction Score | 99.79% |
| Evidence Level | L4 (preclinical / in vitro evidence only) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available in the data compiled for this report. Based on known pharmacology, abacavir is a nucleoside reverse transcriptase inhibitor. It is phosphorylated inside cells to carbovir triphosphate, which terminates proviral DNA chain elongation.

SIV is a lentivirus closely related to HIV and has a homologous reverse transcriptase, so mechanistic plausibility is high. The high TxGNN score most likely reflects this viral similarity to HIV rather than a genuinely new therapeutic opportunity. SIV infection is a non-human primate model, not a human clinical indication.

Two other predictions were also generated:
- **Feline acquired immunodeficiency syndrome** (score 99.79%, L4): FIV is also a lentivirus whose reverse transcriptase can be inhibited by NRTIs. Evidence is a 2002 in vitro study of zidovudine, lamivudine and abacavir against FIV. This is a veterinary condition with limited relevance to human medicine.
- **Neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter** (score 99.78%, L5): No mechanistic link, clinical trials or literature were identified. This is a rare genetic disorder with no established relevance to abacavir's mechanism, so the prediction is speculative.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15040537](https://pubmed.ncbi.nlm.nih.gov/15040537/) | 2004 | In vitro susceptibility study | Antiviral Therapy | Tested 16 approved anti-HIV drugs and one experimental drug against HIV-2, SIV and SHIV strains to inform treatment and post-exposure prophylaxis |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 378038 | Ziagen (cancelled) | Tab 300 mg | Not available |
| 2632365 | Abacavir/Lamivudine Viatris (cancelled) | Tab 600 mg with lamivudine 300 mg | Not available |

## Safety Considerations

- **Knowledge-graph contraindications** (from drug-disease contraindication links, not package insert text): liver failure, liver disease, fatty liver disease, non-alcoholic steatohepatitis, kidney disease, pancreatitis, gallbladder disease, pyoureter, lactic acidosis, neutropenia, agranulocytosis, anaemia (including macrocytic anaemia), hypertriglyceridaemia, obesity disorders, myopathy and other muscle diseases (including myositis, dermatomyositis, familial periodic paralysis).
- **Drug Interactions**: 93 interactions are recorded in total. The two rated Moderate are **naltrexone** and **orlistat**. Many others (for example metformin, morphine, omeprazole, pantoprazole, prednisone, vancomycin, amphotericin B) have no severity rating.
- **Additional consideration**: abacavir carries a recognised risk of HLA-B*57:01-associated hypersensitivity.

Please refer to the package insert for full warnings and contraindications; the New Zealand package insert data could not be retrieved for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support is indirect in vitro evidence for a non-human primate model, with no clinical trials. The prediction mostly restates abacavir's existing anti-HIV activity, and the product is no longer actively licensed in New Zealand.

**To proceed, the following is needed:**
- Retrieval and review of the Medsafe package insert (warnings and contraindications), which is a blocking gap for safety screening
- Mechanism of action data from DrugBank
- A clear human clinical rationale, since SIV and FIV are animal conditions
- Mechanistic validation if the neurodevelopmental disorder prediction is to be pursued
- Confirmation of current regulatory and supply status in New Zealand, since both licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

