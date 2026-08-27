---
layout: default
title: Fluticasone
parent: 僅模型預測 (L5)
nav_order: 158
evidence_level: L5
indication_count: 3
---

# Fluticasone
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Fluticasone: From Undocumented Original Indication to Predicted Migraine Disorder

## One-Sentence Summary

Fluticasone (DrugBank ID: DB13867) has no original indication or mechanism-of-action data available in this evidence pack, and it currently holds no marketing authorization in Taiwan.
The TxGNN model predicts it may be effective for **migraine disorder**, with a prediction score of **99.20%**,
but this is currently supported by **0 clinical trials** and only **1 publication** — and that single publication actually raises a safety concern rather than confirming efficacy.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented — Fluticasone holds no marketing authorization in Taiwan (0 licenses on file) |
| Predicted New Indication | Migraine disorder |
| TxGNN Prediction Score | 99.20% |
| Evidence Level | L4 |
| Taiwan Market Status | Not marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for fluticasone is not available in this evidence pack (flagged as Data Gap DG002, High severity). Based on the information embedded in the TxGNN repurposing rationale, fluticasone belongs to the corticosteroid class administered via nasal/inhaled routes. The hypothesis linking it to migraine rests on the idea that the anti-inflammatory action of intranasal/inhaled corticosteroids could theoretically reduce trigeminovascular neurogenic inflammation — one of several proposed mechanisms in migraine pathophysiology.

However, this link is explicitly described as speculative, with no direct pharmacological evidence behind it. More importantly, the only literature item retrieved for this indication points in the opposite direction: a 2008 WHO pharmacovigilance review (Pokladnikova et al.) found that intranasally administered corticosteroids — though generally considered to act locally with minimal systemic effect — were associated with unexpected reports of neuropsychiatric disturbances, including headache-related adverse effects. This is a safety signal, not supportive efficacy evidence.

Because fluticasone's original indication is undocumented in this pack and it has zero marketing authorizations in Taiwan, there is no established original-to-new-indication therapeutic bridge to reason from. The prediction should currently be treated as a computational hypothesis derived from TxGNN network similarity (score 99.20%, global rank 6361) rather than a mechanistically or clinically substantiated candidate.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [18681087](https://pubmed.ncbi.nlm.nih.gov/18681087/) | 2008 | Review | Annals of Allergy, Asthma & Immunology | WHO pharmacovigilance review of intranasal corticosteroids; despite being considered locally acting with minimal systemic effect, an unexpected cluster of neuropsychiatric case reports (including headache-related disturbances) was identified — a safety concern rather than evidence of migraine efficacy |

---

## Taiwan Market Information

Fluticasone currently holds no marketing authorization in Taiwan (0 licenses on file; market status: 未上市 / not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: key warnings, contraindications, and drug interaction data are all flagged as unavailable in this evidence pack — TFDA package insert data is a Blocking-severity data gap, DG001, that currently prevents this candidate from entering the S1 safety pre-evaluation stage.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The TxGNN score is high (99.20%), but the supporting evidence is thin (Evidence Level L4): zero clinical trials and a single publication whose findings raise a safety concern rather than confirm efficacy.
- The drug has no marketing history in Taiwan (0 authorizations) and no documented original indication or MOA to anchor a mechanistic rationale on.
- Safety data (TFDA warnings/contraindications) is a Blocking data gap (DG001), which by itself prevents the candidate from proceeding past initial safety screening (S1) regardless of predicted efficacy.
- The two lower-ranked predictions (Prinzmetal angina, migraine with brainstem aura) are weaker still — Evidence Level L5 with no clinical trials or literature at all — and should not be prioritized ahead of migraine disorder.

**To proceed, the following is needed:**
- TFDA package insert (warnings/contraindications) — download and parse per DG001 remediation plan
- Mechanism of action data via DrugBank API — per DG002 remediation plan
- Drug-drug interaction (DDI) data (currently `not_found`)
- Original indication and approval history for fluticasone, to establish a genuine original-to-new-indication rationale
- Route compatibility assessment — required administration route for migraine treatment vs. fluticasone's available routes (currently marked "pending")
- Preclinical or mechanistic studies directly evaluating corticosteroid effects on migraine pathophysiology, ideally resolving the conflicting safety signal from the WHO pharmacovigilance review
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

