---
layout: default
title: Riluzole
parent: 僅模型預測 (L5)
nav_order: 306
evidence_level: L5
indication_count: 10
---

# Riluzole
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

# Riluzole: From Amyotrophic Lateral Sclerosis to ALS Susceptibility / Motor Neuron Disease Spectrum

> **Selection note:** TxGNN's raw rank #1–#2, #4–#5, #9 predictions (polymicrogyria, spondylometaphyseal dysplasia, trichomegaly-retina syndrome, arthrogryposis syndrome, mitochondrial myopathy) carry **no clinical trial or literature evidence**, and the evidence pack's own `repurposing_rationale` explicitly flags them as likely **TxGNN score-driven false positives** with no mechanistic link (all `Hold`, decision stage S0). This report instead focuses on **rank 8 — "amyotrophic lateral sclerosis, susceptibility to"** — the only candidate in the pack with substantive literature support and an actionable decision stage (S3, L1, *Proceed with Guardrails*).

## One-Sentence Summary

> Riluzole is the established therapy for Amyotrophic Lateral Sclerosis (ALS), approved based on pivotal trials showing modest survival benefit via inhibition of glutamate-mediated excitotoxicity.
> The TxGNN model's most clinically credible candidate among 10 predictions is **ALS Susceptibility** — essentially the known disease itself rather than a novel indication —
> supported by **20 publications** (mechanistic reviews and preclinical studies) but **no registered clinical trials** in this evidence pull.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Amyotrophic Lateral Sclerosis (ALS) — not present in `taiwan_regulatory.licenses` (empty); based on well-established external knowledge, confirmed by the pack's own literature (riluzole cited repeatedly as "the only approved ALS drug") |
| Predicted New Indication | Amyotrophic Lateral Sclerosis, susceptibility to |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L1 (per evidence pack scoring — see caveat below) |
| New Zealand Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

**Evidence-level caveat:** The pack labels this candidate L1, but the 20 literature items actually retrieved by this PubMed query are all reviews/preclinical studies — no RCT is present in the `evidence.literature` array. The L1 rating implicitly relies on riluzole's well-known pivotal Phase 3 trials (Bensimon 1994, *NEJM*; Lacomblez 1996, *Lancet*), which predate ClinicalTrials.gov and are therefore absent from the automated evidence pull. This is a **data-completeness gap**, not a data-quality error — it should be closed before this evidence level is relied upon for a formal decision.

## Why is This Prediction Reasonable?

`original_moa` is marked as a data gap in the structured drug record, but the repurposing rationale attached to this candidate provides the mechanism: riluzole inhibits presynaptic glutamate release and blocks voltage-dependent sodium channels, reducing excitotoxic injury to motor neurons.

"ALS, susceptibility to" is not a distinct disease area from the original indication — it sits within the same ALS/motor-neuron-disease nosology TxGNN's knowledge graph uses. This explains why the mechanistic link is direct rather than inferential: riluzole's approved use already addresses the pathophysiology this candidate represents. The prediction is best read as the model **correctly recovering a known drug-disease relationship**, which is useful as a validation signal for the TxGNN pipeline but offers limited incremental repurposing value on its own.

Several other candidates in this pack (rank 3 "lower motor neuron syndrome, late-adult onset," rank 6 "monomelic amyotrophy," rank 7 "Mills syndrome," rank 10 "ALS type 22") sit on the same motor-neuron-disease spectrum and share the same mechanistic rationale, but currently have **zero clinical trials or literature** and are only at decision stage S1–S2 — these are the candidates that would represent genuine novel repurposing opportunities and warrant future evidence collection.

## Clinical Trial Evidence

Currently no related clinical trials registered in this evidence pull. Note: riluzole's pivotal ALS trials (1994–1996) predate the ClinicalTrials.gov registry (est. 2000) and are not captured by automated registry queries.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [21128691](https://pubmed.ncbi.nlm.nih.gov/21128691/) | 2011 | Review | CNS Drugs | ALS pathophysiology/diagnosis/management review; riluzole remains the only medication shown to modestly prolong survival |
| [19593125](https://pubmed.ncbi.nlm.nih.gov/19593125/) | 2009 | Review | Current Opinion in Neurology | Riluzole remains the only drug with proven efficacy in ALS despite intensive research into disease mechanisms |
| [22646982](https://pubmed.ncbi.nlm.nih.gov/22646982/) | 2011 | Preclinical Review | Expert Opinion on Drug Discovery | Reviews riluzole as the sole approved ALS therapeutic, improving survival by 2–3 months; discusses need for new agents |
| [20942785](https://pubmed.ncbi.nlm.nih.gov/20942785/) | 2010 | Review | CNS & Neurological Disorders Drug Targets | Genetic determinants of ALS as therapeutic targets; riluzole cited as the only available treatment |
| [9178165](https://pubmed.ncbi.nlm.nih.gov/9178165/) | 1997 | Review | Journal of Neurology | Glutamate hypothesis of motor neuron injury — the core excitotoxicity mechanism riluzole targets |
| [16723044](https://pubmed.ncbi.nlm.nih.gov/16723044/) | 2006 | Review | Expert Reviews in Molecular Medicine | Proposed ALS mechanisms and pathways to treatment, including excitotoxicity |
| [20942786](https://pubmed.ncbi.nlm.nih.gov/20942786/) | 2010 | Review | CNS & Neurological Disorders Drug Targets | ALS diagnosis, pathogenesis, and therapeutic targets overview |
| [8061281](https://pubmed.ncbi.nlm.nih.gov/8061281/) | 1994 | Preclinical | Neuroreport | Direct study of riluzole's neuroprotective effect against ALS CSF-mediated excitotoxicity in neuronal cultures |
| [31108504](https://pubmed.ncbi.nlm.nih.gov/31108504/) | 2019 | Preclinical | Human Molecular Genetics | iPSC-derived ALS motor neurons show altered glutamate receptor/calcium dynamics; notes riluzole's mechanism of glutamatergic inhibition |
| [22763933](https://pubmed.ncbi.nlm.nih.gov/22763933/) | 2012 | Review | Praxis | ALS diagnosis and treatment overview |

## New Zealand Market Information

Riluzole currently holds **no market authorization in New Zealand** (`market_status`: 未上市 / Not Marketed; `total_licenses`: 0). No product listings are available to summarize.

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are all flagged as data gaps in this evidence pack — notably `DG001`, TFDA/local package-insert warnings and contraindications, is marked **Blocking** for safety pre-assessment.)

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Riluzole's mechanism and prior ALS approval make this candidate low-risk from a pharmacological-plausibility standpoint, but the evidence base retrieved here is mechanistic/review-level only — no pivotal RCTs or safety data are present in the pack, and the "new indication" substantially overlaps the drug's known approved use rather than representing a distinct novel opportunity.

**To proceed, the following is needed:**
- Resolve `DG001` (Blocking): obtain TFDA/local package insert warnings and contraindications before any safety pre-assessment
- Resolve `DG002`: formally source riluzole's MOA from DrugBank into the structured `original_moa` field
- Incorporate riluzole's pivotal Phase 3 RCTs (Bensimon 1994; Lacomblez 1996) into the evidence base, since the current automated pull misses pre-registry trials
- Clarify the clinical/regulatory distinction between "ALS" and "ALS, susceptibility to" as a billing/indication code before treating this as an actionable repurposing signal
- If pursuing NZ market entry, initiate a Medsafe regulatory pathway assessment given current "not marketed" status
- For genuine novel repurposing value, prioritize evidence collection on ranks 3, 6, 7, and 10 (lower motor neuron syndrome, monomelic amyotrophy, Mills syndrome, ALS type 22), which share riluzole's mechanistic rationale but currently lack any clinical trial or literature support
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

