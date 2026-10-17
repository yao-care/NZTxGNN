---
layout: default
title: Nitisinone
parent: Model Prediction Only (L5)
nav_order: 324
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Nitisinone: predicted new indication Renal tubular acidosis (evidence level L5). Approved use: Tyrosinemia"
monitor_inn: "NITISINONE"
predicted_indications: ["renal tubular acidosis", "galactosemia", "serpinopathy with toxic serpin polymerization", "C1 inhibitor deficiency", "glycogen storage disease", "adult polyglucosan body disease", "glycogen storage disease due to glucose-6-phosphatase deficiency", "Griscelli syndrome", "granulomatous disease, chronic, X-linked", "ermine phenotype"]
---

# Nitisinone
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

# Nitisinone: From Tyrosinemia to Renal Tubular Acidosis

## One-Sentence Summary

Nitisinone is an HPPD inhibitor originally used to treat hereditary tyrosinemia.
The TxGNN model predicts it may be effective for **renal tubular acidosis**, but **no clinical trials** and **no relevant publications** currently support this direction.
The prediction is a computational signal only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Tyrosinemia (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Renal tubular acidosis |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorisations) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. From general knowledge, nitisinone inhibits 4-hydroxyphenylpyruvate dioxygenase (HPPD), an enzyme in tyrosine breakdown. Blocking it prevents the build-up of toxic downstream metabolites such as succinylacetone in hereditary tyrosinemia type 1.

Renal tubular dysfunction (a Fanconi-type picture) is a feature of hereditary tyrosinemia type 1. This is probably why the model links the two conditions. However, that kidney involvement is caused by the toxic metabolites and is already covered by the original indication. No separate mechanistic rationale has been established for other forms of renal tubular acidosis, so the high score (0.9996) should be read as a graph-based association rather than evidence of therapeutic benefit.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for renal tubular acidosis.

For context, the second-ranked prediction (galactosemia) returned three narrative reviews on pediatric liver disease and inborn errors of metabolism. They discuss tyrosinemia and galactosemia as differential diagnoses and do not show benefit from nitisinone in galactosemia.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2677687 | Nitisinone LogixX Pharma | Cap 2 mg | Not recorded (cancelled) |
| 2677695 | Nitisinone LogixX Pharma | Cap 5 mg | Not recorded (cancelled) |
| 2677709 | Nitisinone LogixX Pharma | Cap 10 mg | Not recorded (cancelled) |

## Safety Considerations

- **Disease-level cautions (knowledge graph, not label text)**: the graph links nitisinone to kidney disease, pyoureter, thrombocytopenia, leukopenia and hypophosphatemia as contraindication or caution signals. Kidney disease is particularly relevant to a renal indication and needs clarification.
- **Drug Interactions**: 100 interactions were found, mostly of Moderate severity. Examples include abiraterone, amiodarone, amlodipine, nifedipine, ibuprofen, dexamethasone, pioglitazone, isotretinoin, atazanavir, aprepitant, apalutamide, baricitinib and bicalutamide. Armodafinil is rated Minor.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone. There are no clinical trials or supporting publications, and no independent mechanistic basis beyond the kidney involvement already seen in tyrosinemia. The drug also has no currently valid New Zealand authorisation.

**To proceed, the following is needed:**
- Mechanism-of-action data and a specific mechanistic hypothesis for renal tubular acidosis, distinguishing it from the tyrosinemia-related kidney effects
- Targeted literature and trial searches (including case reports and preclinical studies) for nitisinone in renal tubular acidosis
- The package insert warnings and contraindications, which are needed before any safety screening, with particular attention to kidney disease and blood-count effects
- Clarification of the regulatory position, since all three New Zealand authorisations are cancelled
- Route and formulation compatibility assessment (currently capsules only)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

