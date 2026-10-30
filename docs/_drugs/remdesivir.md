---
layout: default
title: Remdesivir
parent: Model Prediction Only (L5)
nav_order: 400
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-30
monitor_inn: "REMDESIVIR"
predicted_indications: ["multiple endocrine neoplasia", "HIV infectious disease", "simian immunodeficiency virus infection", "feline acquired immunodeficiency syndrome", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "homozygous familial hypercholesterolemia"]
description: "Remdesivir: predicted new indication Multiple endocrine neoplasia (evidence level L5). Approved use: Not recorded in the local data"
---

# Remdesivir
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

## Remdesivir: Multiple endocrine neoplasia
{: .text-alpha }

## One-Sentence Summary

Remdesivir is a nucleotide analog antiviral. The New Zealand licence record compiled for this report does not state an approved indication.
The TxGNN model's top prediction is **multiple endocrine neoplasia**, but there are **0 clinical trials** and **0 publications** supporting it, so this is a model-only prediction.
The second-ranked prediction, **HIV infectious disease**, has **1 clinical trial** and **20 publications** in the compiled data, but these relate almost entirely to COVID-19 rather than HIV.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Multiple endocrine neoplasia |
| TxGNN Prediction Score | 0.9950 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. From general pharmacology, remdesivir is a nucleotide analog prodrug that inhibits viral RNA-dependent RNA polymerase, giving it activity against several RNA viruses such as SARS-CoV-2 and Ebola.

Multiple endocrine neoplasia is a hereditary tumour syndrome (for example, driven by MEN1 or RET mutations) and is not caused by viral replication. No credible mechanistic link to an antiviral polymerase inhibitor could be identified. The high score (0.995, rank 4483) appears to be a TxGNN model (deep learning) artefact rather than a meaningful signal.

For context, the other predictions show the same pattern:
- **HIV infectious disease (score 0.9932)**: only an indirect link through the nucleotide-analog class. HIV relies on reverse transcriptase and integrase, not on RNA-dependent RNA polymerase.
- **Simian immunodeficiency virus infection and feline AIDS (0.9907 each)**: lentiviral, animal-model or veterinary indications. The identical scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.
- **Neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter (0.9903)** and **homozygous familial hypercholesterolemia (0.9903)**: genetic disorders with no plausible antiviral-sensitive pathway.

## Clinical Trial Evidence

No clinical trials are registered for the top-ranked prediction, multiple endocrine neoplasia.

For the second-ranked prediction (HIV infectious disease), one trial was retrieved:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04385719](https://clinicaltrials.gov/study/NCT04385719) | Phase 2 | Completed | 24 | Pharmacokinetic study in healthy Ugandan volunteers, testing how tenofovir/lamivudine or atazanavir/ritonavir affect remdesivir blood levels. It provides no HIV efficacy evidence (relevance grade C). |

## Literature Evidence

No literature is available for the top-ranked prediction, multiple endocrine neoplasia.

For the second-ranked prediction (HIV infectious disease), the retrieved publications are almost entirely about COVID-19 and are not direct evidence in HIV. The most relevant ones are:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33264556](https://pubmed.ncbi.nlm.nih.gov/33264556/) | 2021 | RCT | N Engl J Med | WHO Solidarity trial of repurposed antivirals (including remdesivir) in hospitalised COVID-19 patients |
| [38675889](https://pubmed.ncbi.nlm.nih.gov/38675889/) | 2024 | RCT | Viruses | No remdesivir resistance observed in the Phase 3 SIMPLE trials in severe and moderate COVID-19 |
| [35817072](https://pubmed.ncbi.nlm.nih.gov/35817072/) | 2022 | RCT | Lancet Respir Med | Tixagevimab-cilgavimab versus placebo in hospitalised COVID-19 patients also receiving remdesivir |
| [41136410](https://pubmed.ncbi.nlm.nih.gov/41136410/) | 2025 | Review | Nat Rev Dis Primers | Overview of acute SARS-CoV-2 infection |
| [34696838](https://pubmed.ncbi.nlm.nih.gov/34696838/) | 2021 | Review | The Enzymes | Mechanisms of viral RNA replication inhibition by nucleotide analogs, comparing SARS-CoV-2 RdRp with HIV reverse transcriptase |
| [34859557](https://pubmed.ncbi.nlm.nih.gov/34859557/) | 2022 | Observational | HIV Med | High frequency of potential drug interactions in HIV-positive patients receiving COVID-19 treatment |
| [36097825](https://pubmed.ncbi.nlm.nih.gov/36097825/) | 2023 | Cohort | Clin Infect Dis | Subgenomic RNA monitoring of remdesivir response in hospitalised COVID-19 patients |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2640139 | Veklury | Inj 100 mg vial | Not recorded (licence status: valid) |

## Cytotoxicity

Remdesivir is an antiviral rather than an antineoplastic drug, so no cytotoxicity assessment applies.

## Safety Considerations

Drug interactions: 100 interactions were identified in the interaction database. The 20 reviewed are all of Moderate severity, including:
- Antibiotics: doxycycline, minocycline, tetracycline, clarithromycin, levofloxacin, vancomycin, kanamycin, neomycin
- Antifungals: amphotericin B and its lipid complex
- Antidiabetics: acarbose, pioglitazone
- Others: bupropion, naltrexone, mesalazine, balsalazide, olsalazine, chenodeoxycholic acid, oxandrolone, oxymetholone

Please refer to the package insert for warnings and contraindications, as this information was not available in the compiled data.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction, multiple endocrine neoplasia, rests solely on a TxGNN model (deep learning) score with no clinical trials, no literature, and no plausible mechanism. The only prediction with any retrieved evidence (HIV) is supported by material that concerns COVID-19, not HIV. Remdesivir's only New Zealand licence is cancelled, and label safety information could not be obtained.

**To proceed, the following is needed:**
- Medsafe package insert (warnings, contraindications, approved indication), which is a blocking gap for safety screening
- Mechanism of action data from DrugBank
- Any preclinical or clinical evidence specific to the predicted indication
- Clarification of the cancelled licence status and the current availability of remdesivir in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

