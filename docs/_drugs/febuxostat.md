---
layout: default
title: Febuxostat
parent: Moderate Evidence (L3-L4)
nav_order: 194
evidence_level: L4
indication_count: 3
---

# Febuxostat
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **3** 
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

# Febuxostat: From Hyperuricemia to Renal Hypouricemia

## One-Sentence Summary

Febuxostat is a xanthine oxidase inhibitor originally used to treat hyperuricemia.
The TxGNN model predicts it may be effective for **renal hypouricemia**, but the supporting evidence is thin: **1 clinical trial** of uncertain relevance and **2 publications** (narrative review and case-level reports).
The mechanism also points in the opposite direction, because febuxostat lowers uric acid, which is already low in this condition.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperuricemia (from the knowledge graph; no approved indication text is available in the New Zealand licence records) |
| Predicted New Indication | Hypouricemia, renal |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, febuxostat is a non-purine selective xanthine oxidase inhibitor that lowers serum uric acid, and its efficacy in hyperuricemia is established.

Renal hypouricemia is characterised by already low serum urate, typically from loss-of-function of urate transporters such as URAT1 or GLUT9. Further urate lowering is therefore mechanistically counterproductive, and the high TxGNN score probably reflects network proximity through urate-pathway genes rather than a true therapeutic fit. The direction of effect needs careful review.

There is one indirect and hypothetical link. A 2023 report describes a young athlete with familial renal hypouricemia (URAT1 mutations) and recurrent exercise-induced acute kidney injury. Hydration alone did not prevent episodes, and a xanthine oxidoreductase inhibitor was considered as prophylaxis, possibly by reducing urinary uric acid load. This is a single-patient observation, not evidence of efficacy.

Two other predicted indications were also reviewed: partial HPRT deficiency (rank 2, Hold) and Lesch-Nyhan syndrome (rank 3, Research Question). In both, urate-lowering with a xanthine oxidase inhibitor is mechanistically plausible, similar to allopurinol, but they are supported only by case reports and case series.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04398251](https://clinicaltrials.gov/study/NCT04398251) | Phase 4 | Unknown | 100 | Prospective controlled study of how uric acid control affects stone recurrence and renal function in patients with calculi and hyperuricemia. The record is titled only with an institution name (Shanghai Xu-hui Central Hospital), and its link to renal hypouricemia is unconfirmed (relevance grade C). |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clinical Rheumatology | Narrative review of hypouricemia (serum urate < 2 mg/dL) covering causes and clinical considerations for rheumatologists. |
| [36754409](https://pubmed.ncbi.nlm.nih.gov/36754409/) | 2023 | Case report | Internal Medicine (Tokyo) | A 16-year-old footballer with familial renal hypouricemia (URAT1 mutations) and recurrent exercise-induced acute kidney injury; hydration did not prevent episodes, and a xanthine oxidoreductase inhibitor was proposed as prophylaxis. |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2670356 | Febuxostat (Teva) | Tab 80 mg | Not available (cancelled) |
| 2670364 | Febuxostat (Teva) | Tab 120 mg | Not available (cancelled) |

---

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease contraindication links, not label text): stroke, cerebrovascular disorder, cerebral infarction, myocardial infarction, coronary thrombosis, and hemoglobinopathy. Cardiovascular and cerebrovascular conditions deserve particular attention.
- **Drug Interactions**: 22 interactions were recorded. **Major** interactions are with azathioprine, mercaptopurine, leflunomide, and teriflunomide. Moderate interactions include methotrexate, theophylline, aminophylline, dyphylline, oxtriphylline, tioguanine, clofarabine, epirubicin, trabectedin, asparaginase, pegaspargase, brentuximab vedotin, naltrexone, and interferon beta-1a, beta-1b and peginterferon beta-1a.
- **Disease-specific risk**: in Lesch-Nyhan syndrome and related disorders, xanthine oxidase inhibition may raise xanthine and hypoxanthine excretion and risk xanthine stone formation, so monitoring is needed.

Please refer to the package insert for further safety information, as label warnings and contraindications were not available for this report.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The evidence is limited to a model prediction, one trial of unconfirmed relevance, and a narrative review plus a single case report. The mechanism (further urate lowering in a condition of already low urate) runs against the proposed use. Febuxostat currently has no valid New Zealand licence.

**To proceed, the following is needed:**
- Review of the direction of effect to determine whether febuxostat would help or harm in renal hypouricemia
- Verification of NCT04398251 (condition, intervention, design)
- Full-text review of the 2023 case report for dosing and outcomes
- Mechanism of action data
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Clarification of the cancelled New Zealand regulatory status and route compatibility
- If a different candidate is preferred, a focused evaluation of partial HPRT deficiency or Lesch-Nyhan syndrome, where the mechanism fits better but evidence is limited to case reports
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

