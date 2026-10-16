---
layout: default
title: Tolvaptan
parent: Model Prediction Only (L5)
nav_order: 456
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Tolvaptan: predicted new indication Polycystic kidney disease 3 with or without polycystic liver disease (evidence level L5). Approved use: Inappropriate ADH syndrome…"
---

# Tolvaptan
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

# Tolvaptan: From Inappropriate ADH Syndrome and Polycystic Kidney Disease to Polycystic Kidney Disease 3 (With or Without Polycystic Liver Disease)

## One-Sentence Summary

Tolvaptan is a selective vasopressin V2 receptor antagonist, originally used for inappropriate ADH syndrome (SIADH), nephrogenic syndrome of inappropriate antidiuresis and polycystic kidney disease.
The TxGNN model predicts it may be effective for **polycystic kidney disease 3 with or without polycystic liver disease (PKD3)**.
There are **no registered clinical trials** for this specific disease, but **20 publications** are available. Two of them are pivotal RCTs in the broader ADPKD population (PKD1/PKD2), not PKD3-specific trials.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Inappropriate ADH syndrome, nephrogenic syndrome of inappropriate antidiuresis, polycystic kidney disease (the data compiled for this report has no approved-indication text from the New Zealand licences) |
| Predicted New Indication | Polycystic kidney disease 3 with or without polycystic liver disease |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L1 (as assigned in the compiled data; derived from ADPKD-class RCTs, not PKD3-specific trials) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, tolvaptan is a selective vasopressin V2 receptor antagonist. In polycystic kidney disease, elevated cAMP signalling drives cyst fluid secretion and epithelial proliferation. Blocking the V2 receptor lowers renal cAMP, which slows cyst growth and the expansion of total kidney volume.

Tolvaptan is already an established therapy for ADPKD, which is part of its original indication. PKD3 (GANAB-related) shares the same cystic phenotype and cAMP-driven pathophysiology, so the mechanism may plausibly apply.

An important caveat is that the evidence comes from the broader ADPKD class (PKD1/PKD2), not from PKD3-specific studies. The polycystic liver component is also not addressed by tolvaptan.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23121377](https://pubmed.ncbi.nlm.nih.gov/23121377/) | 2012 | RCT | N Engl J Med | Tolvaptan in ADPKD. Preclinical work indicated V2 antagonists inhibit cyst growth and slow kidney function decline (TEMPO 3:4). |
| [29105594](https://pubmed.ncbi.nlm.nih.gov/29105594/) | 2017 | RCT | N Engl J Med | Tolvaptan in later-stage ADPKD. An earlier trial showed slower kidney volume growth and eGFR decline, but more aminotransferase and bilirubin elevations. |
| [39356039](https://pubmed.ncbi.nlm.nih.gov/39356039/) | 2024 | Systematic Review | Cochrane Database Syst Rev | Interventions for preventing ADPKD progression, including disease-modifying agents. |
| [40126492](https://pubmed.ncbi.nlm.nih.gov/40126492/) | 2025 | Review | JAMA | ADPKD review. It is the most common inherited kidney disorder, accounting for 5–10% of kidney failure in the US and Europe. |
| [41771281](https://pubmed.ncbi.nlm.nih.gov/41771281/) | 2026 | Review | Lancet | ADPKD seminar. There is a strong evidence base for tolvaptan as a disease-modifying treatment, supporting early use in high-risk patients. |
| [40844441](https://pubmed.ncbi.nlm.nih.gov/40844441/) | 2025 | Review | Am J Kidney Dis | ADPKD core curriculum. Kidney volume, function and genotype identify patients for disease-modifying therapy. |
| [40268755](https://pubmed.ncbi.nlm.nih.gov/40268755/) | 2025 | Review | Renal Failure | Overview of recent genetic and clinical advances in ADPKD. |
| [35487607](https://pubmed.ncbi.nlm.nih.gov/35487607/) | 2022 | Review | Clin Liver Dis | Polycystic kidney/liver disease. Tolvaptan in ADPKD can slow decline in kidney function and cyst growth. |
| [40726372](https://pubmed.ncbi.nlm.nih.gov/40726372/) | 2025 | Review | Curr Opin Nephrol Hypertens | Therapies beyond tolvaptan, which remains the only FDA-approved therapy targeting disease progression. |
| [32392574](https://pubmed.ncbi.nlm.nih.gov/32392574/) | 2021 | Not classified | Pediatr Res | Tolvaptan use in adolescents and young adults with rapidly progressing ADPKD. TEMPO 3:4 did not specifically assess this group. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2647508 | Jinarc (cancelled) | Tab 30 mg | Not available |
| 2647494 | Jinarc (cancelled) | Tab 15 mg | Not available |
| 2647451 | Jinarc (cancelled) | Tab 45 mg + 15 mg | Not available |
| 2647478 | Jinarc (cancelled) | Tab 60 mg + 30 mg | Not available |
| 2647486 | Jinarc (cancelled) | Tab 90 mg + 30 mg | Not available |

## Safety Considerations

- **Hepatotoxicity**: Liver injury monitoring is the major safety guardrail. The later-stage ADPKD trial reported more aminotransferase and bilirubin elevations with tolvaptan.
- **Knowledge-graph contraindication signals** (not label text): liver failure, cirrhosis of the liver, substance abuse/dependence, active peptic ulcer disease, demyelinating disease.
- **Drug Interactions**: The interaction screen returned 100 interacting drugs. Key examples:
  - **Major**: clarithromycin.
  - **Moderate**: aprepitant, dexamethasone, empagliflozin, loperamide, miconazole, naltrexone, rifaximin, rolapitant, and potassium salts (citrate, bicarbonate, chloride, gluconate).
  - **Minor**: fidaxomicin, naloxegol, prucalopride.

The package insert warnings and contraindications were not available for this report. Please refer to the New Zealand package insert for full safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Tolvaptan is an established disease-modifying therapy for ADPKD, supported by large RCTs and reviews, and PKD3 shares the same cAMP-driven cystic pathophysiology. However, there are no PKD3-specific trials. The drug has no currently valid New Zealand licence and carries a significant hepatotoxicity risk, so the decision requires caution.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- PKD3 (GANAB)-specific efficacy and safety data, or a justified extrapolation from the ADPKD evidence
- A liver function monitoring plan
- Clarification of regulatory status, since all New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

