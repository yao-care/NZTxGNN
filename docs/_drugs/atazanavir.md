---
layout: default
title: Atazanavir
parent: Model Prediction Only (L5)
nav_order: 47
evidence_level: L5
indication_count: 6
last_modified_at: 2026-10-15
description: "Atazanavir: predicted new indication Congenital HIV (evidence level L5). Approved use: HIV infectious disease"
monitor_inn: "atazanavir"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "obsolete familial combined hyperlipidemia", "congenital human immunodeficiency virus", "AIDS related complex"]
---

# Atazanavir
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **6** 
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

# Atazanavir: From HIV Infection to Congenital HIV

## One-Sentence Summary

Atazanavir is an HIV-1 protease inhibitor, originally used to treat HIV infection and AIDS.
The TxGNN model's top-ranked prediction is **feline acquired immunodeficiency syndrome**, a veterinary condition outside the scope of human repurposing. The most clinically meaningful prediction is **congenital HIV**, supported by **0 clinical trials** and **7 publications**, mostly observational safety and pharmacokinetic studies rather than efficacy trials.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV infectious disease; AIDS (from the knowledge graph, since no approved indication text is available in the licence records) |
| Predicted New Indication | Congenital HIV (the top-ranked prediction, feline acquired immunodeficiency syndrome, is a veterinary condition and was set aside as out of scope) |
| TxGNN Prediction Score | 99.71% (congenital HIV); 99.98% for the top-ranked feline prediction |
| Evidence Level | L3 (observational and pharmacovigilance studies; no clinical trials) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, atazanavir is an HIV-1 protease inhibitor, its efficacy in HIV infection is established, and it is mechanistically applicable to HIV transmitted from mother to child.

Congenital HIV is a vertical extension of the approved HIV indication. Blocking HIV-1 protease prevents viral maturation, which is relevant to both treating infected infants and reducing mother-to-child transmission. The supporting literature addresses use in pregnancy, including the following:
- Atazanavir exposure remains adequate in pregnancy regardless of tenofovir use.
- Placental efflux transporters (ABCB1, ABCG2, ABCC2) influence its transfer in animal studies.
- Large cohorts such as PHACS SMARTT track the safety of in utero antiretroviral exposure.

These studies support exposure and safety, not efficacy in congenital infection. The model score likely reflects graph proximity to the approved HIV indication. This is therefore an extension of an existing indication more than a new repurposing opportunity.

The other predictions are weaker:
- **Feline AIDS and simian immunodeficiency virus infection** (scores 99.98%): Animal conditions. The feline virus protease differs structurally from HIV-1 protease. The simian virus protease is homologous, so these are at most preclinical models.
- **Neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter** (99.98%): No mechanistic link was identified.
- **Obsolete familial combined hyperlipidemia** (99.82%): Protease inhibitors are associated with dyslipidemia, so the prediction likely reflects an adverse-effect association, not benefit.
- **AIDS related complex** (99.71%): A historical term for symptomatic HIV disease, already covered by the approved indication.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25383770](https://pubmed.ncbi.nlm.nih.gov/25383770/) | 2015 | Cohort | JAMA Pediatrics | Congenital anomalies and in utero antiretroviral exposure in HIV-exposed uninfected infants; some evidence of increased risk with specific agents |
| [24992294](https://pubmed.ncbi.nlm.nih.gov/24992294/) | 2015 | Pharmacokinetic cohort | Antiviral Therapy | Atazanavir exposure is effective during pregnancy regardless of tenofovir use |
| [27242802](https://pubmed.ncbi.nlm.nih.gov/27242802/) | 2016 | Cohort | Frontiers in Immunology | PHACS SMARTT study design assessing the safety of in utero antiretroviral exposure in over 3,500 exposed uninfected children |
| [40011239](https://pubmed.ncbi.nlm.nih.gov/40011239/) | 2025 | Case/non-case pharmacovigilance | Eur J Clin Pharmacol | European registry analysis of congenital anomaly risk after fetal antiretroviral exposure |
| [31595301](https://pubmed.ncbi.nlm.nih.gov/31595301/) | 2020 | Pharmacovigilance analysis | Clin Infect Dis | Pharmacovigilance data on dolutegravir safety in pregnancy and neural tube defects |
| [28459118](https://pubmed.ncbi.nlm.nih.gov/28459118/) | 2016 | Cohort | J AIDS Immune Res | Newborn hearing screening referrals in HIV-exposed uninfected infants in the SMARTT study |
| [29859254](https://pubmed.ncbi.nlm.nih.gov/29859254/) | 2018 | In vitro mechanistic | Reprod Toxicol | Interactions of atazanavir and ritonavir with placental ABC transporters in rats; use is associated with a higher risk of congenital anomalies |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2660024 | Atazanavir Viatris | Cap 200 mg | Not available (cancelled) |
| 2660016 | Atazanavir Viatris | Cap 150 mg | Not available (cancelled) |

---

## Safety Considerations

- **Contraindication-related conditions (knowledge graph, not label text)**: Liver failure, liver disease, gallbladder disease, hyperglycemia, diabetes mellitus, diabetic ketoacidosis, chronic hepatitis B and C virus infection, hemophilia, hyperbilirubinemia (including hereditary forms and Gilbert syndrome), nephrolithiasis, acute urate nephropathy, atrioventricular block, and lactic acidosis.
- **Drug Interactions**: 277 interactions were recorded. Examples of **Major** interactions include:
  - Famotidine
  - Ranitidine
  - Rabeprazole
  - Omeprazole
  - Loperamide
  - Triamcinolone
  - Budesonide

  Examples of **Moderate** interactions include acarbose, hydrocortisone, metformin, pioglitazone, aprepitant, aluminum hydroxide, and calcium carbonate.
- **Pregnancy-related signals**: The literature notes concerns about congenital anomalies after in utero antiretroviral exposure, including with atazanavir and ritonavir.

Please refer to the package insert for label warnings and contraindications, which were not available for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- There are no clinical trials, and the literature consists of observational, pharmacovigilance, and pharmacokinetic studies that support safety and exposure but not efficacy in congenital HIV. Both New Zealand licences are cancelled, which limits practical development, and the highest-scoring predictions are veterinary or lack a mechanistic link.

**To proceed, the following is needed:**
- The Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data (for example, from DrugBank)
- Efficacy data in congenital or perinatal HIV, and a check on whether the original HIV indication is already covered by other marketed protease inhibitors
- A review of pregnancy and neonatal safety, given the congenital anomaly signals
- Clarification of the regulatory and supply status, since no valid licence currently exists in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

