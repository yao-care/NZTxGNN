---
layout: default
title: Quetiapine
parent: 僅模型預測 (L5)
nav_order: 297
evidence_level: L5
indication_count: 10
---

# Quetiapine
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

# Quetiapine: From Psychiatric Disorders to Trichotillomania

## One-Sentence Summary

Quetiapine is an atypical antipsychotic generally used for schizophrenia, bipolar disorder, and as adjunctive therapy in major depressive disorder (general clinical knowledge; no Taiwan/NZ regulatory indication text is on file for this drug). Among 10 TxGNN-predicted indications, **Trichotillomania** (hair-pulling disorder) is the only candidate with a genuine mechanistic rationale and drug-specific human data — supported by **7 publications** including case reports of quetiapine treating trichotillomania, though **no clinical trials** have been registered. The other 9 predicted indications (mostly ultra-rare genetic/ophthalmologic syndromes) lack any supporting evidence and are not clinically plausible for this drug class; they are not pursued further in this report.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available on file (see note below) |
| Predicted New Indication | Trichotillomania |
| TxGNN Prediction Score | 99.38% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

**Note on Original Indication:** No Taiwan/NZ license or indication text exists in the regulatory data provided (`market_status: 未上市`, 0 licenses). The original-indication context above (schizophrenia/bipolar disorder) reflects general public drug knowledge, not data extracted from this evidence pack, and should be confirmed against a formal source before use in any regulatory submission.

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in this evidence pack (`original_moa: [Data Gap]`). Based on generally known pharmacology, quetiapine is a D2/5-HT2A receptor antagonist. Low-dose atypical antipsychotics with this receptor profile are sometimes used adjunctively in obsessive-compulsive-spectrum and impulse-control disorders.

Trichotillomania is classified as an OCD-spectrum, body-focused repetitive behavior. The serotonin-dopamine modulation hypothesis provides a plausible mechanistic bridge between quetiapine's known pharmacology and this indication, which is consistent with why TxGNN surfaced the link.

Importantly, the literature is **mixed, not uniformly supportive**: alongside case reports of quetiapine improving trichotillomania, there is also a published case of quetiapine *inducing/exacerbating* obsessive-compulsive symptoms — including trichotillomania — in a patient with pre-existing OCD (PMID 11212595). This bidirectional signal is a real pharmacological consideration, not just a data gap, and should be treated as part of the evidence base rather than omitted.

For context, the top-ranked TxGNN prediction overall (retinal dystrophy with extraocular anomalies, score 99.57%) was reviewed against 15 retrieved publications, none of which mention quetiapine — this is assessed as embedding co-occurrence noise rather than real drug-disease evidence, and is not pursued. The remaining 8 of 10 predicted indications (ultra-rare genetic/dysmorphic syndromes) returned zero clinical trials and zero literature and are held for the same reason.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12405081](https://pubmed.ncbi.nlm.nih.gov/12405081/) | 2002 | Review/Case series | Psychiatry | Overview of trichotillomania pharmacotherapy; case report of a 33-year-old female with favorable clinical response to quetiapine |
| [19142421](https://pubmed.ncbi.nlm.nih.gov/19142421/) | 2008 | Case report | Revista Brasileira de Psiquiatria | "Quetiapine for the treatment of trichotillomania" (abstract not available) |
| [11212595](https://pubmed.ncbi.nlm.nih.gov/11212595/) | 2001 | Case report/Review | Journal of Psychiatry & Neuroscience | Reports quetiapine **exacerbating** obsessive-compulsive symptoms, including trichotillomania, in a patient with pre-existing OCD/bipolar disorder — a cautionary counter-signal |
| [38797877](https://pubmed.ncbi.nlm.nih.gov/38797877/) | 2025 | Review | International Journal of Dermatology | Notes lack of consensus treatment guidelines for trichotillomania; calls for better clinician education on pharmacological options |
| [17484394](https://pubmed.ncbi.nlm.nih.gov/17484394/) | 2006 | Review | The Journal of Practical Nursing | General treatment overview of trichotillomania (abstract not available) |
| [27840761](https://pubmed.ncbi.nlm.nih.gov/27840761/) | 2016 | Case report | Case Reports in Psychiatry | Trichotillomania as a manifestation of dementia; not quetiapine-specific |
| [20833945](https://pubmed.ncbi.nlm.nih.gov/20833945/) | 2010 | Case report | Psychosomatics | Recurrent Rapunzel syndrome and trichotillomania; not quetiapine-specific |

---

## New Zealand Market Information

Quetiapine is currently **not marketed** in New Zealand under this evidence pack's regulatory data source, with 0 authorizations on file. No product license records are available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug-interaction data are all marked as data gaps in this evidence pack; TFDA package insert warnings/contraindications are flagged as a **Blocking** data gap — DG001 — that must be resolved before a formal safety assessment can proceed.)

One literature-derived caution worth carrying forward: PMID 11212595 documents a case of quetiapine-induced/exacerbated obsessive-compulsive symptoms (including trichotillomania) — the drug's effect on this symptom cluster may not be uniformly positive.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for trichotillomania consists only of small case reports/series and reviews (L4, no RCTs), with one report describing a paradoxical worsening effect — the signal is mechanistically plausible but not yet actionable for a repurposing decision. The other 9 TxGNN-predicted indications lack any supporting evidence and are not viable candidates.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (Blocking gap, DG001)
- Confirmed original indication and MOA data from DrugBank or an authoritative label (DG002)
- A prospective or controlled study (even small open-label) specifically evaluating quetiapine in trichotillomania, given the mixed case-report signal
- Formal DDI dataset (current query returned `not_found`)
- Clarification of NZ/Taiwan market and licensing status, since no product is currently on file
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

