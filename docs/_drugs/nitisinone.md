---
layout: default
title: Nitisinone
parent: 僅模型預測 (L5)
nav_order: 245
evidence_level: L5
indication_count: 10
---

# Nitisinone
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

Using the provided Evidence Pack, here is the evaluation report for Nitisinone (DB00348).

# Nitisinone: From Hereditary Tyrosinemia Type 1 to Renal Tubular Acidosis

## One-Sentence Summary

> Nitisinone (NTBC) was originally developed to treat hereditary tyrosinemia type 1 (HT‑1) by blocking the tyrosine degradation pathway.
> The TxGNN model predicts it may also be effective for **Renal Tubular Acidosis**,
> with **0 registered clinical trials** but **2 supporting publications** (1 cohort study, 1 case series) — both drawn from the existing HT‑1 patient population rather than dedicated renal tubular acidosis trials.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hereditary Tyrosinemia Type 1 (HT‑1)* |
| Predicted New Indication | Renal Tubular Acidosis |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L3 (Observational studies) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

*\*Nitisinone's original approved indication (HT‑1) is not separately documented in the New Zealand regulatory data or the DrugBank field in this evidence pack (both are empty because the drug is not locally marketed); it is inferred from the consistent literature and mechanistic context supplied for every predicted indication.*

---

## Why is This Prediction Reasonable?

A formal DrugBank mechanism-of-action entry was not retrievable in this evidence pack. Based on the mechanistic context consistently referenced across the supporting literature, nitisinone is understood to act as an inhibitor of 4‑hydroxyphenylpyruvate dioxygenase (HPPD), the enzyme responsible for an early step in tyrosine catabolism. By blocking this step, it prevents the downstream formation of toxic intermediates (such as succinylacetone) that accumulate in HT‑1.

Renal tubular acidosis/dysfunction is a well-recognized complication of *untreated* HT‑1: the toxic tyrosine-pathway metabolites that accumulate in this disease are known to injure the proximal renal tubule. Because nitisinone already addresses the root biochemical cause of HT‑1 (by blocking metabolite accumulation), it is mechanistically plausible that it would also ameliorate or prevent this downstream renal complication.

Importantly, this is not a case of extrapolating to an unrelated disease mechanism — it is closer to treating a known complication of an already-approved indication. This distinguishes it from several of the model's other high-scoring predictions for this drug (see appendix below), where no comparable mechanistic link exists.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25172236](https://pubmed.ncbi.nlm.nih.gov/25172236/) | 2014 | Cohort | Molecular Genetics and Metabolism | Prospective description of the early effect of NTBC therapy on renal tubular function in HT‑1 patients; renal tubular dysfunction is highlighted as a core HT‑1 complication that NTBC therapy was assessed against. |
| [27109516](https://pubmed.ncbi.nlm.nih.gov/27109516/) | 2016 | Case Series | Indian Journal of Gastroenterology | Case series of 4 children with tyrosinemia treated with NTBC; long-term responders (~3 years of therapy) showed normal liver function and undetectable urine succinylacetone, with no reported renal tubular complications. |

---

## New Zealand Market Information

Nitisinone currently holds **no product authorizations in New Zealand** (total authorizations = 0); the drug is not marketed locally, so no dosage-form or approved-indication data is available from the regulatory register.

---

## Safety Considerations

Please refer to the package insert for safety information. (No structured warnings, contraindications, or drug-interaction data were retrievable in this evidence pack; TFDA/NZ package-insert data is flagged as a **Blocking** data gap — see Conclusion.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Renal tubular acidosis is a recognized downstream complication of HT‑1 pathophysiology, and nitisinone's established mechanism (reducing toxic tyrosine-pathway metabolite accumulation) provides a biologically plausible rationale for benefit. However, the supporting evidence consists only of 1 cohort study and 1 case series conducted in HT‑1 patients — not dedicated trials in a broader renal tubular acidosis population — and no clinical trials have been registered for this indication.

**To proceed, the following is needed:**
- TFDA/New Zealand package insert warnings, contraindications, and safety data (currently unavailable — flagged **Blocking** in this evidence pack, DG001)
- Confirmed formal mechanism-of-action documentation from DrugBank or equivalent source (flagged **High** priority gap, DG002)
- Prospective evidence specifically evaluating renal tubular function/acidosis as a primary endpoint, rather than as a secondary observation in HT‑1 cohorts
- A New Zealand market-access assessment, since the drug is not currently marketed or authorized locally

---

## Appendix: Other TxGNN-Predicted Indications (Lower Priority)

For completeness, TxGNN generated 9 additional high-scoring predictions for nitisinone. All were assessed as evidence level L4–L5 with a **Hold** recommendation due to absent or non-specific literature support, and none should be interpreted as repurposing opportunities at this time:

| Rank | Predicted Indication | Score | Evidence Level | Recommendation | Note |
|------|----------------------|-------|-----------------|-----------------|------|
| 2 | Galactosemia | 99.95% | L5 | Hold | 3 review-only articles; no GALT-pathway/HPPD mechanistic link — likely knowledge-graph topological proximity (both are pediatric metabolic liver diseases), not shared mechanism |
| 3 | Serpinopathy (toxic serpin polymerization) | 99.90% | L5 | Hold | Zero literature/trials; no known mechanistic link |
| 4 | C1 inhibitor deficiency | 99.90% | L5 | Hold | Zero literature/trials; no known mechanistic link |
| 5 | Glycogen storage disease | 99.89% | L5 | Hold | 1 general pediatric liver-disease review; indirect co-mention only |
| 6 | Adult polyglucosan body disease | 99.88% | L5 | Hold | Zero literature/trials |
| 7 | Glycogen storage disease (G6Pase deficiency) | 99.87% | L5 | Hold | Zero literature/trials |
| 8 | Griscelli syndrome | 99.87% | L5 | Hold | Zero literature/trials; note below |
| 9 | X-linked chronic granulomatous disease | 99.87% | L5 | Hold | Zero literature/trials; no known mechanistic link |
| 10 | Ermine phenotype | 99.87% | L4 | Hold | **⚠️ Directionality warning**: this is a known *adverse effect* of NTBC (drug-induced hypertyrosinemia causing skin depigmentation), not a treatable indication. The high score likely reflects the model learning an adverse-event association rather than a therapeutic one, and should not be read as a repurposing signal. |

These lower-ranked predictions illustrate a broader caution for this candidate: several high TxGNN scores appear to reflect knowledge-graph proximity between pediatric metabolic/liver diseases (or, in one case, an inverted adverse-reaction signal) rather than genuine mechanistic overlap. Renal tubular acidosis (rank 1) remains the only prediction in this set with both a coherent mechanistic rationale and independent literature support.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

