---
layout: default
title: Sulfadiazine
parent: Model Prediction Only (L5)
nav_order: 431
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-30
monitor_inn: "Sulfadiazine"
predicted_indications: ["pneumocystosis", "punctate epithelial keratoconjunctivitis"]
description: "Sulfadiazine: predicted new indication Pneumocystosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Sulfadiazine
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

## Sulfadiazine: Pneumocystosis
{: .text-alpha }

## One-Sentence Summary

Sulfadiazine is a sulfonamide antibiotic originally used for toxoplasmosis and related infections, as well as inflammatory bowel conditions.
The TxGNN model predicts it may be effective for **Pneumocystosis**,
but currently there are **0 registered clinical trials** and **19 publications** (mostly older reviews and case-based reports), so the evidence remains at the level of model prediction plus class-level mechanistic reasoning.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 0.9939 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known drug-class knowledge, sulfadiazine is a sulfonamide that inhibits dihydropteroate synthase (DHPS) in the folate synthesis pathway. This is inferred from class knowledge rather than from the dataset.

*Pneumocystis jirovecii* is also susceptible to folate pathway inhibition. Sulfonamides, notably sulfamethoxazole combined with trimethoprim, are established therapy for Pneumocystis pneumonia. Sulfadiazine's established use against *Toxoplasma gondii*, another folate-dependent opportunistic pathogen, further supports a plausible class-level link. Literature also describes pyrimethamine-sulfadiazine being used in AIDS patients for toxoplasmosis and Pneumocystis pneumonia.

However, no direct evidence was found for sulfadiazine as monotherapy or in a defined regimen for Pneumocystis infection. In addition, a paper in the literature set reports that mutations in the *P. jirovecii* DHPS gene confer cross-resistance to sulfa drugs, which may limit effectiveness.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

The 19 publications compiled are mainly reviews and observational or case reports; no randomized trials were identified. A selection of the most relevant:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2121456](https://pubmed.ncbi.nlm.nih.gov/2121456/) | 1990 | Review | Drugs | Summarises therapy and prophylaxis for systemic protozoan infections, including *P. carinii* and *T. gondii* |
| [7355683](https://pubmed.ncbi.nlm.nih.gov/7355683/) | 1980 | Review | American Family Physician | Overview of antiparasitic drugs; sulfadiazine listed in a regimen for resistant malaria, TMP-SMX for pneumocystis pneumonia |
| [2011633](https://pubmed.ncbi.nlm.nih.gov/2011633/) | 1991 | Review | Primary Care | Protozoan infections in AIDS, including Pneumocystis pneumonia and CNS toxoplasmosis |
| [2996829](https://pubmed.ncbi.nlm.nih.gov/2996829/) | 1985 | Review | Clinical Pharmacy | Therapies for infectious complications of AIDS, with Pneumocystis pneumonia the most common |
| [7909657](https://pubmed.ncbi.nlm.nih.gov/7909657/) | 1994 | Review | Annals of Internal Medicine | Advances in managing AIDS-related opportunistic infections |
| [29604970](https://pubmed.ncbi.nlm.nih.gov/29604970/) | 2018 | Review | Handbook of Clinical Neurology | CNS Toxoplasma infection in HIV; context for sulfonamide-based prophylaxis and treatment |
| [9562233](https://pubmed.ncbi.nlm.nih.gov/9562233/) | 1998 | Review | Int J Clin Pharmacol Ther | Nephrotoxicity of anti-infective drugs, including tubulo-obstructive effects from drug precipitation |
| [2645082](https://pubmed.ncbi.nlm.nih.gov/2645082/) | 1989 | Clinical report | Clinical Pharmacy | Pyrimethamine-sulfadiazine for treating Pneumocystis carinii pneumonia and toxoplasmosis in AIDS (no abstract available) |
| [2060528](https://pubmed.ncbi.nlm.nih.gov/2060528/) | 1991 | Retrospective chart review | Eur J Clin Microbiol Infect Dis | 35 AIDS patients on toxoplasmosis maintenance therapy; relapses of both toxoplasmosis and pneumocystosis observed |
| [15673759](https://pubmed.ncbi.nlm.nih.gov/15673759/) | 2005 | Laboratory study | Antimicrob Agents Chemother | *P. jirovecii* DHPS mutations confer cross-resistance to sulfa drugs |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2674912 | Sulfadiazin-Heyl (valid) | Tab 500 mg | Not available |

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report; please refer to the package insert for safety information.

The following information comes from the knowledge graph and drug interaction databases, not from label text:

- **Contraindicated or cautioned conditions (knowledge graph)**: porphyria (including inherited porphyria and porphyrin metabolism disease), kidney disease, hematologic disease (thrombocytopenia, neutropenia, leukopenia, agranulocytosis, aplastic anemia, macrocytic anemia, folic acid deficiency anemia), liver disease, gallbladder disease, asthma (intrinsic and allergic), G6PD deficiency, slow acetylation, glycogen storage disease, cystinuria, galactosemia, urinary tract obstruction, and superimposed infection.
- **Drug Interactions**: 82 interactions were identified; the 20 listed in the data are all of moderate severity. They fall mainly into these groups:
  - Antidiabetic agents (risk of hypoglycaemia): sulfonylureas (chlorpropamide, glimepiride, glipizide, glyburide, tolazamide), meglitinides (repaglinide, nateglinide), and multiple insulins (aspart, degludec, detemir, glargine, glulisine, human, lispro).
  - Others: balsalazide, picosulfuric acid, naltrexone.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high (0.9939), and the folate-pathway mechanism is plausible given the established role of sulfonamides in Pneumocystis therapy. The second predicted indication, punctate epithelial keratoconjunctivitis, has no supporting evidence or mechanistic rationale (L5, Hold).

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank to confirm the mechanistic link
- Direct comparative evidence for sulfadiazine versus established regimens (e.g., TMP-SMX) in Pneumocystis infection
- Assessment of the sulfa-resistance risk (DHPS mutations)
- Clarification of the New Zealand supply and licensing pathway, since the only licence is cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

