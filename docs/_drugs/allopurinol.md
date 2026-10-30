---
layout: default
title: Allopurinol
parent: Model Prediction Only (L5)
nav_order: 29
evidence_level: L5
indication_count: 10
monitor_inn: "ALLOPURINOL"
predicted_indications: ["hepatic porphyria", "early-onset familial noncirrhotic portal hypertension", "primitive portal vein thrombosis", "idiopathic copper-associated cirrhosis", "hepatoportal sclerosis", "hepatopulmonary syndrome", "disorder of phenylalanine metabolism", "immune-mediated necrotizing myopathy", "antisynthetase syndrome", "inflammatory myopathy with abundant macrophages"]
last_modified_at: 2026-10-30
description: "Allopurinol: predicted new indication hepatic porphyria (evidence level L5). Approved use: Not recorded in the local data"
---

# Allopurinol
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

## Allopurinol: Xanthine oxidase inhibitor → hepatic porphyria
{: .text-alpha }

## One-Sentence Summary

Allopurinol is a xanthine oxidase inhibitor. The TxGNN model predicts it may be relevant to **hepatic porphyria**, but the supporting evidence is thin: **no clinical trials** and **2 publications** (one hypothesis paper and one rat study), neither of which tests allopurinol in patients with this disease.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | hepatic porphyria |
| TxGNN Prediction Score | 0.9995 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Allopurinol is generally known as an inhibitor of xanthine oxidase, the enzyme that produces uric acid. How this would apply to hepatic porphyria is not established by the data compiled for this report.

The retrieved papers point to a possible, but indirect, link through heme metabolism. A 2019 hypothesis paper discusses targeting 5-aminolevulinate synthase, the rate-limiting enzyme of heme biosynthesis, as a way to treat acute hepatic porphyrias. A 1992 rat study examined how another drug (carbamazepine) worsens porphyria by affecting hepatic heme. Neither paper tests allopurinol as a treatment for this condition.

Older preclinical work suggests allopurinol can affect hepatic heme handling. This could be harmful in porphyria rather than helpful, so the direction of effect is unresolved. The prediction should be seen as a model-generated hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31443750](https://pubmed.ncbi.nlm.nih.gov/31443750/) | 2019 | Review (hypothesis) | Medical Hypotheses | Proposes metabolic targeting of 5-aminolevulinate synthase, via tryptophan or inhibition of heme use by tryptophan 2,3-dioxygenase, as therapy for acute hepatic porphyrias; does not study allopurinol directly |
| [1567472](https://pubmed.ncbi.nlm.nih.gov/1567472/) | 1992 | Preclinical (animal) | Biochemical Pharmacology | Acute carbamazepine dosing altered heme metabolism in rat liver, relevant to how drugs exacerbate hepatic porphyrias |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2666863 | Ipca-Allopurinol | Tab 300 mg | Not recorded |
| 2666855 | Ipca-Allopurinol | Tab 100 mg | Not recorded |

## Safety Considerations

- **Drug Interactions**: The interaction screen returned 100 recorded interactions. Those listed in the data include moderate-level interactions with aluminium hydroxide, attapulgite, kaolin, magaldrate and sucralfate (mostly antacids and adsorbent or protective agents), and minor-level interactions with amoxicillin and chlorpropamide. Other listed pairs (for example metformin, omeprazole, pantoprazole, doxycycline, morphine) have an unrated severity.

For warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction score is high, but no clinical trial or study directly tests allopurinol in hepatic porphyria. The only related literature is a hypothesis paper and an animal study on other drugs. Allopurinol's effect on heme metabolism could also be harmful in this disease.

**To proceed, the following is needed:**
- Mechanism of action data for allopurinol
- Package insert warnings and contraindications, in particular for patients with liver disease
- Preclinical or clinical data showing the direction of allopurinol's effect on heme biosynthesis in porphyria
- A safety assessment of the drug interactions listed above
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

