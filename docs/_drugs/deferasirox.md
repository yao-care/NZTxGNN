---
layout: default
title: Deferasirox
parent: Model Prediction Only (L5)
nav_order: 138
evidence_level: L5
indication_count: 5
---

# Deferasirox
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

# Deferasirox: From Beta-Thalassemia and Related Haemoglobin Disorders to HIV Infectious Disease

## One-Sentence Summary

Deferasirox is an iron chelator, originally associated with beta-thalassemia and related haemoglobin disorders.
The TxGNN model predicts it may be effective for **HIV infectious disease**,
but there are currently **no registered clinical trials** and only **2 publications** (one review and one preclinical study), so the prediction rests mainly on the model and a preclinical mechanistic hint.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Beta-thalassemia and related haemoglobin disorders (hereditary persistence of fetal haemoglobin, beta-thalassemia intermedia, hemoglobinopathy). Taken from the knowledge graph, as no approved-indication text is available for the New Zealand licences |
| Predicted New Indication | HIV infectious disease |
| TxGNN Prediction Score | 99.40% |
| Evidence Level | L4 (preclinical/mechanistic evidence only) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, deferasirox is an iron chelator, its use in iron-overload settings linked to beta-thalassemia is established, and mechanistically it may be relevant to HIV because iron status can influence viral biology.

A 2021 preclinical study (PMID 34550543) reported that iron in endolysosomes restricts HIV-1 Tat-mediated LTR transactivation by increasing Tat oligomerization and β-catenin expression. This suggests iron homeostasis can modulate HIV-1 transcription.

However, this is only a mechanistic link. The direction of effect for chelation is not established: depleting iron could either help or hinder viral transcription. No clinical data exist for deferasirox in HIV.

Other predictions in the list are weaker still. Chronic hepatitis C infection (score 99.39%) has a plausible biological rationale through hepatic iron accumulation but no direct evidence. The remaining three predictions (a rare neurodevelopmental disorder, an obsolete familial combined hyperlipidemia term, and dermatofibrosarcoma protuberans) have no supporting trials or literature, and the obsolete disease term likely reflects an ontology artifact.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34550543](https://pubmed.ncbi.nlm.nih.gov/34550543/) | 2021 | Preclinical/In vitro mechanistic study | Journal of neurovirology | Endolysosomal iron restricts Tat-mediated HIV-1 LTR transactivation by increasing Tat oligomerization and β-catenin expression |
| [16529348](https://pubmed.ncbi.nlm.nih.gov/16529348/) | 2006 | Review | Journal of the American Pharmacists Association | New-drug overview covering deferasirox among other agents; not specific to HIV |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2234769 | Exjade (cancelled) | Tab 125 mg dispersible | Not available |
| 2234777 | Exjade (cancelled) | Tab 250 mg dispersible | Not available |
| 2234750 | Exjade (cancelled) | Tab 500 mg dispersible | Not available |

---

## Safety Considerations

- **Contraindication signals from the knowledge graph (not label text)**: liver failure, liver disease, cirrhosis, kidney disease, impaired renal function, gallbladder disease, thrombocytopenia, neutropenia and agranulocytosis (including several congenital neutropenia syndromes), anaemia (including macrocytic anaemia), hyperbilirubinemia and Gilbert syndrome, active peptic ulcer disease, ocular hypertension, multiple organ dysfunction syndrome, and cancer.
- **Drug Interactions**: 100 interactions were found in the interaction database. Examples of those rated **Major** include corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone), amphotericin B (and its lipid, liposomal and cholesteryl sulfate formulations), acetylsalicylic acid, mesalazine and balsalazide. Examples rated **Moderate** include rabeprazole, pioglitazone, alosetron, aprepitant, clarithromycin, saxagliptin, aluminium hydroxide, cisapride and dexfenfluramine.

Package insert warnings and contraindications were not available for this report; please refer to the official package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The HIV prediction has a high model score but is supported only by one preclinical study with an unclear direction of effect, and there are no clinical trials. In addition, all New Zealand licences are cancelled, and the knowledge graph flags several serious contraindications (hepatic, renal and haematological) that are relevant in people living with HIV.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe), which are a blocking gap for safety screening
- Mechanism of action data (from DrugBank)
- Preclinical studies to determine whether iron chelation with deferasirox helps or hinders HIV-1 transcription and replication
- A clear plan on regulatory and supply status, given that no valid New Zealand licence currently exists
- A safety assessment for people with hepatic, renal or haematological comorbidities, and a review of the interaction profile with antiretroviral therapy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

