---
layout: default
title: Phenylalanine
parent: 僅模型預測 (L5)
nav_order: 276
evidence_level: L5
indication_count: 2
---

# Phenylalanine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Phenylalanine: From Essential Amino Acid (No Approved Indication) to Sclerosing Cholangitis

## One-Sentence Summary

Phenylalanine is an essential amino acid with no approved therapeutic indication on record and no market presence in New Zealand.
The TxGNN model predicts it may be effective for **Sclerosing Cholangitis**, but the supporting evidence pack (**0 clinical trials**, **4 publications**) does not actually support a treatment relationship — closer reading suggests a likely knowledge-graph false positive.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | No approved indication on record (phenylalanine is listed in DrugBank as an essential amino acid, not as a drug with a defined therapeutic indication) |
| Predicted New Indication | Sclerosing Cholangitis |
| TxGNN Prediction Score | 99.43% |
| Evidence Level | L5 (model prediction only, no supportive studies) |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (DG002, High severity gap). Phenylalanine is a nutritionally essential amino acid and metabolic precursor to tyrosine; it is not established as a pharmacological treatment for any hepatobiliary condition.

Critically, when the four supporting literature items are examined individually, none actually describe phenylalanine as a therapeutic intervention for sclerosing cholangitis:

- **PMID 15790420** examines plasma **tyrosine** (not phenylalanine) concentration and fatigue in primary biliary cirrhosis/PSC — an association study, not a treatment study.
- **PMID 32025163** is a serum metabolomics profiling study in cholangiocarcinoma — a biomarker/observational paper unrelated to phenylalanine supplementation or treatment.
- **PMID 8000512** and **PMID 2103382** both concern **N-formyl-methionyl-leucyl-phenylalanine (fMLP)**, a bacterial chemotactic peptide that shares only a substring with "phenylalanine." In these preclinical rat models, fMLP *induces* small-duct cholangitis — the opposite direction from a therapeutic effect.

Taken together, the mechanistic link between phenylalanine and sclerosing cholangitis is not supported by the retrieved evidence. The high TxGNN score (99.43%) most likely reflects a graph-embedding artifact — conflating phenylalanine with tyrosine (a shared metabolic pathway) and/or with the unrelated peptide fMLP (a lexical/substring overlap) — rather than a genuine pharmacological signal.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15790420](https://pubmed.ncbi.nlm.nih.gov/15790420/) | 2005 | Cohort | BMC Gastroenterology | Examines plasma **tyrosine** (not phenylalanine) levels and their relation to fatigue in PBC/PSC patients; an association study, no treatment data |
| [32025163](https://pubmed.ncbi.nlm.nih.gov/32025163/) | 2020 | Cohort | Journal of Clinical and Experimental Hepatology | Serum metabolomic profiling to distinguish cholangiocarcinoma from benign hepatobiliary disease; biomarker discovery, not a treatment study |
| [8000512](https://pubmed.ncbi.nlm.nih.gov/8000512/) | 1994 | Animal/Preclinical | Journal of Gastroenterology | In rats, the bacterial peptide **fMLP** (formyl-methionyl-leucyl-**phenylalanine**) *induced* small-duct cholangitis — an adverse/causal effect, not a therapeutic one |
| [2103382](https://pubmed.ncbi.nlm.nih.gov/2103382/) | 1990 | Preclinical | Journal of Gastroenterology and Hepatology | Describes enterohepatic circulation of bacterial chemotactic fMLP-type peptides; mechanistic background on gut-derived peptides, unrelated to phenylalanine therapy |

---

## New Zealand Market Information

Phenylalanine is not marketed in New Zealand and has no registered authorizations on record.

---

## Safety Considerations

Please refer to the package insert for safety information. (Note: TFDA package insert warnings/contraindications data are marked as a **Blocking** data gap — DG001 — and must be resolved before any S1 safety review can proceed.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on L5 evidence (model score only) and decision stage S0. Detailed review of all four cited publications shows none actually support a phenylalanine-to-sclerosing-cholangitis treatment relationship — two concern tyrosine, and two concern an unrelated bacterial peptide (fMLP) that *causes* cholangitis in animal models rather than treating it. This pattern is consistent with a knowledge-graph false positive rather than a genuine repurposing signal.

**To proceed, the following is needed:**
- Resolve DG001 (Blocking): obtain TFDA/manufacturer safety labeling before any further evaluation
- Resolve DG002 (High): obtain confirmed mechanism-of-action data for phenylalanine
- Independent verification of whether the TxGNN embedding is conflating phenylalanine with tyrosine or with fMLP-type peptides
- If pursued further, target literature/trial searches specifically on phenylalanine (not tyrosine or fMLP) in cholestatic/biliary disease models

*Note: A second candidate indication (congenital prothrombin deficiency, score 99.26%, L5/Hold) was also evaluated and shows a similarly unsupported evidence trail (single withdrawn, zero-enrollment trial on an unrelated drug/indication) — not detailed here but carries the same Hold recommendation.*
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

