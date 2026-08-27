---
layout: default
title: Norfloxacin
parent: 僅模型預測 (L5)
nav_order: 248
evidence_level: L5
indication_count: 10
---

# Norfloxacin
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

# Norfloxacin: From Bacterial Infections to Septicemic Plague

## Note on Candidate Selection

This evidence pack contains 10 TxGNN-predicted indications for norfloxacin, all clustered at similar TxGNN scores (99.4–99.7%). Rank 1 by raw score ("hyperamylasemia") has **zero clinical, literature, or mechanistic support** — the pack itself annotates it as "無機轉關聯，無臨床或文獻證據支持" (no mechanistic link, no evidence). The same is true for ranks 2, 3, 4, 6, 7, 8, 9. Rank 5 ("punctate epithelial keratoconjunctivitis") has literature, but the pack flags it as a likely false positive — the cited cases are microsporidial (parasitic), not bacterial, and norfloxacin's antibacterial mechanism does not apply.

Of the 10 candidates, only **rank 10 (septicemic plague)** carries a biologically coherent rationale and a higher evidence tier (L3, decision stage S2 "Research Question"). This report focuses on that candidate rather than blindly following score rank, and lists the other 9 as screened-out in the appendix below.

---

## One-Sentence Summary

Norfloxacin is a fluoroquinolone-class antibacterial agent, historically used to treat bacterial infections (e.g., urinary tract infections) via DNA gyrase inhibition. Among the TxGNN predictions in this pack, the most credible new-indication candidate is **Septicemic Plague**, based on class-effect reasoning with CDC/WHO-recommended fluoroquinolones (ciprofloxacin, levofloxacin) — but it is supported only by **2 historical animal/in-vitro publications** and **no clinical trials or norfloxacin-specific plague data**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in evidence pack — no NZ/TW license records available (norfloxacin is a well-established fluoroquinolone antibacterial; specific approved wording pending TFDA package insert retrieval, DG001) |
| Predicted New Indication | Septicemic Plague |
| TxGNN Prediction Score | 99.37% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap DG002, High severity). Based on known pharmacology, norfloxacin is a **fluoroquinolone-class antibacterial**, and this class's efficacy against susceptible Gram-negative bacteria — including *Yersinia pestis*, the causative agent of plague — is well established for other members of the class (ciprofloxacin, levofloxacin, both CDC/WHO-recommended for plague treatment and post-exposure prophylaxis).

The mechanistic thread connecting norfloxacin's original antibacterial use to septicemic plague is a **class-effect argument**: all fluoroquinolones inhibit bacterial DNA gyrase/topoisomerase IV, and this mechanism is pathogen-agnostic within susceptible Gram-negative species. Since *Y. pestis* is susceptible to fluoroquinolones as a class, norfloxacin's inclusion is mechanistically plausible even without drug-specific trial data.

However, this rationale is indirect. The two supporting publications (PMID 10987101, 11057367) are animal/in-vitro studies from 2000, in Russian-language literature, and neither directly tests norfloxacin against *Y. pestis* in a clinical or robust preclinical model — one focuses on vaccine-antibiotic interaction in mice, the other on a different pathogen (*Vibrio cholerae*) entirely. This is class-level, not drug-level, evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10987101](https://pubmed.ncbi.nlm.nih.gov/10987101/) | 2000 | Animal Study | Antibiotiki i khimioterapiia | Compared combined emergency (fluoroquinolone) + specific (EV vaccine) prophylaxis vs. sequential use in mice; norfloxacin's effect on post-vaccination immunity was noted as lower than ciprofloxacin/ofloxacin/pefloxacin |
| [11057367](https://pubmed.ncbi.nlm.nih.gov/11057367/) | 2000 | In Vitro Study | Antibiotiki i khimioterapiia | Examined fluoroquinolone resistance mutants in *Vibrio cholerae* (not *Yersinia pestis*); indirect class-effect relevance only |

---

## New Zealand Market Information

No marketing authorizations are on record. `market_status` = 未上市 (Not Marketed), `total_licenses` = 0.

---

## Safety Considerations

Please refer to the package insert for safety information. (Note: TFDA/Medsafe package insert retrieval is a **Blocking** data gap (DG001) — this prevents entry into the S1 safety review stage.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for septicemic plague is limited to L3 class-effect reasoning drawn from two 25-year-old animal/in-vitro studies, with no norfloxacin-specific clinical or preclinical plague data. Safety review cannot proceed because the TFDA/Medsafe package insert data (DG001) is a blocking gap, and norfloxacin is not marketed in the target jurisdiction.

**To proceed, the following is needed:**
- Resolve DG001 (TFDA/Medsafe package insert — warnings/contraindications) to unblock S1 safety review
- Resolve DG002 (confirmed MOA from DrugBank) to strengthen the mechanistic rationale
- Norfloxacin-specific in vitro/in vivo efficacy data against *Yersinia pestis*
- Reassess whether the other 9 TxGNN-predicted indications in this pack merit any further screening, given current evidence gaps

---

## Appendix: Other Screened Candidates (Not Selected)

| Rank | Disease | TxGNN Score | Evidence Level | Reason Excluded |
|------|---------|------|------|------|
| 1 | Hyperamylasemia | 99.70% | L5 | No mechanistic link, no clinical/literature evidence |
| 2 | Polyclonal hyperviscosity syndrome | 99.70% | L5 | No mechanistic link, no evidence |
| 3 | Congenital analbuminemia | 99.67% | L5 | Genetic albumin defect, unrelated to antibacterial mechanism |
| 4 | Blood group incompatibility | 99.55% | L5 | Immunohematology issue, unrelated to DNA gyrase inhibition |
| 5 | Punctate epithelial keratoconjunctivitis | 99.54% | L4 | Cited literature is microsporidial (parasitic), not bacterial — likely TxGNN false positive |
| 6 | Premalignant hematological system disease | 99.48% | L5 | No mechanistic link, no evidence |
| 7 | Diffuse scleroderma | 99.44% | L5 | Autoimmune fibrotic disease, unrelated to antibacterial mechanism |
| 8 | Monoclonal gammopathy | 99.42% | L5 | Cited literature is about fluoroquinolone resistance in cancer patients, not treatment of the condition |
| 9 | Hematological disease with acquired peripheral neuropathy | 99.38% | L5 | No mechanistic link, no evidence |
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

