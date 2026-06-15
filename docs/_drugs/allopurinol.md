---
layout: default
title: Allopurinol
parent: 僅模型預測 (L5)
nav_order: 23
evidence_level: L5
indication_count: 10
---

# Allopurinol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Allopurinol: From Gout / Hyperuricemia to Hepatic Porphyria

## One-Sentence Summary

Allopurinol is a well-established xanthine oxidase (XO) inhibitor, primarily used to treat gout and hyperuricemia by reducing uric acid synthesis.
The TxGNN model predicts it may be effective for **Hepatic Porphyria**, with **0 clinical trials** and **2 publications** currently supporting this direction.
The mechanistic rationale is biologically plausible but remains at the hypothesis stage, placing this candidate at evidence level L4.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Gout / Hyperuricemia (not retrievable from NZ regulatory data; 0 registered licenses) |
| Predicted New Indication | Hepatic Porphyria |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L4 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on established pharmacology, Allopurinol is a xanthine oxidase (XO) inhibitor that blocks the conversion of hypoxanthine → xanthine → uric acid, explaining its efficacy in gout and hyperuricemia. Beyond XO inhibition, allopurinol has also been reported to suppress the activity of δ-aminolevulinate synthase (ALAS) — the rate-limiting enzyme of hepatic heme biosynthesis.

Hepatic porphyria (including acute intermittent porphyria and related disorders) is mechanistically defined by excessive ALAS overactivation, leading to toxic accumulation of precursors such as δ-aminolevulinic acid (ALA) and porphobilinogen (PBG). If allopurinol's ALAS-suppressive activity operates in hepatic tissue, it could in principle reduce this toxic precursor burden — providing a direct molecular link between the drug's secondary pharmacology and the disease's core pathophysiology.

PMID 31443750 (Badawy, 2019, *Medical Hypotheses*) directly proposes metabolic targeting of hepatic ALAS as a strategy for acute hepatic porphyrias, discussing heme feedback regulation and relevant enzymatic pathways. This hypothesis paper gives the TxGNN prediction a biologically coherent anchor. However, there is no clinical trial data and no controlled human evidence to date; this remains a preclinical/mechanistic-level candidate (L4).

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [31443750](https://pubmed.ncbi.nlm.nih.gov/31443750/) | 2019 | Hypothesis / Mechanistic Review | Medical Hypotheses | Proposes metabolic targeting of hepatic ALAS via TDO inhibition or tryptophan as therapy for acute hepatic porphyrias; discusses heme biosynthesis feedback loops and enzymatic pathways directly relevant to allopurinol's potential mechanism of action in porphyria |
| [1567472](https://pubmed.ncbi.nlm.nih.gov/1567472/) | 1992 | Animal Study (Rat, in vivo) | Biochemical Pharmacology | Examines carbamazepine's acute effects on haem metabolism in rat liver via a porphyria exacerbation screening model; characterises drug interactions with heme/tryptophan pyrrolase pathway, providing mechanistic context for heme pathway drug effects |

---

## New Zealand Market Information

Allopurinol currently has **no registered authorizations** in the New Zealand regulatory database. No licensed products, dosage forms, or approved indications are on record. This is notable given allopurinol's widespread global use — independent verification against Medsafe's online database is recommended before drawing regulatory conclusions.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN prediction carries a plausible molecular basis — allopurinol's reported ALAS-inhibitory effect aligns directly with the core pathophysiology of hepatic porphyria — but is supported only by a single hypothesis paper and an indirect animal study, with no clinical trial evidence whatsoever. The L4 evidence level does not support clinical development without dedicated preclinical validation.

**To proceed, the following is needed:**
- Dedicated preclinical studies in established hepatic porphyria animal models (e.g., AIP mouse model) measuring the effect of allopurinol on urinary ALA/PBG excretion and ALAS activity
- Confirmation and quantification of allopurinol's ALAS-inhibitory potency in hepatic tissue (separate from its XO-inhibitory primary mechanism)
- Full mechanism of action documentation via DrugBank API query (currently a High-severity data gap)
- Safety profile review via Medsafe/TFDA package insert — currently a Blocking data gap that prevents formal safety screening
- Independent verification of NZ market status against the Medsafe database, given the discrepancy with global availability of this drug
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

