---
layout: default
title: Ipilimumab
parent: 僅模型預測 (L5)
nav_order: 178
evidence_level: L5
indication_count: 2
---

# Ipilimumab
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

# Ipilimumab: From Cutaneous Melanoma to Non-Cutaneous Melanoma

## One-Sentence Summary

Ipilimumab is an anti-CTLA-4 monoclonal antibody whose established use is in cutaneous melanoma, where checkpoint blockade is now standard of care. The TxGNN model predicts it may also be effective in **non-cutaneous melanoma** (uveal/mucosal subtypes), a direction already supported by **50 clinical trials** and **5 publications** identified in this evidence pack.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Melanoma (cutaneous) — established anti-CTLA-4 checkpoint indication; formal New Zealand license text is not available (drug not currently marketed in NZ) |
| Predicted New Indication | Non-cutaneous melanoma (uveal/mucosal) |
| TxGNN Prediction Score | 99.02% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed DrugBank mechanism-of-action text was not available in this evidence pack. Based on known pharmacology, ipilimumab is an anti-CTLA-4 monoclonal antibody that blocks an inhibitory co-stimulatory checkpoint on T cells, releasing the brake on anti-tumour T-cell activation. This mechanism already underlies its established role in cutaneous melanoma, either as monotherapy or combined with nivolumab (anti-PD-1).

Non-cutaneous melanoma subtypes (uveal and mucosal) arise from the same melanocyte lineage and are known to exploit CTLA-4-dependent immune evasion, giving biological plausibility to extending checkpoint blockade beyond the cutaneous subtype. However, uveal and mucosal melanomas have a lower tumour mutational burden and a distinct immune microenvironment compared with cutaneous disease, and historically show lower response rates to checkpoint inhibitors. This means the mechanistic rationale is sound, but efficacy cannot simply be assumed to transfer 1:1 from cutaneous melanoma — subgroup-specific evidence (see below) is what actually supports this candidate.

*Note: A second candidate flagged by TxGNN, choroideremia (score 99.06%, rank 7176), was excluded from this report. It is a CHM-gene retinal degeneration with no known biological connection to CTLA-4 signalling, has zero supporting trials or literature, and was scored L5/Hold — most likely a knowledge-graph embedding artifact rather than a genuine repurposing signal.*

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02224781](https://clinicaltrials.gov/study/NCT02224781) | Phase 3 | Active, not recruiting | 267 | DREAMseq: compares sequencing of ipilimumab+nivolumab vs. dabrafenib+trametinib in unresectable/metastatic BRAF-mutant melanoma — highest-grade direct evidence for treatment sequencing |
| [NCT01654692](https://clinicaltrials.gov/study/NCT01654692) | Phase 2 | Completed | 86 | Ipilimumab + fotemustine in unresectable locally advanced or metastatic melanoma; safety and efficacy assessed |
| [NCT01927419](https://clinicaltrials.gov/study/NCT01927419) | Phase 2 | Completed | 142 | Randomized, double-blind: nivolumab + ipilimumab vs. ipilimumab monotherapy in previously untreated unresectable/metastatic melanoma |
| [NCT02939300](https://clinicaltrials.gov/study/NCT02939300) | Phase 2 | Completed | 18 | Ipilimumab + nivolumab in melanoma leptomeningeal metastases — direct evidence in a difficult-to-treat subgroup |
| [NCT04133948](https://clinicaltrials.gov/study/NCT04133948) | Phase 1/2 | Completed | 44 | DONIMI trial: neoadjuvant domatinostat + nivolumab ± ipilimumab in IFN-γ-signature-low stage III melanoma |
| [NCT01496807](https://clinicaltrials.gov/study/NCT01496807) | Phase 1 | Completed | 31 | Ipilimumab (Yervoy) + peginterferon (Sylatron) tolerability and autoimmune antibody effects in advanced melanoma |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28183255](https://pubmed.ncbi.nlm.nih.gov/28183255/) | 2018 | Review | Current Cancer Drug Targets | Systematic review of melanoma adjuvant trials (2000–2015); positions checkpoint inhibitors within evolving adjuvant landscape |
| [29466692](https://pubmed.ncbi.nlm.nih.gov/29466692/) | 2018 | Review | Discovery Medicine | Clinical update on anti-PD-1 antibodies alone or combined with ipilimumab as frontline therapy for advanced melanoma |
| [24999899](https://pubmed.ncbi.nlm.nih.gov/24999899/) | 2014 | Cohort/Expanded Access | Medical Journal of Australia | Real-world efficacy/tolerability of ipilimumab in pretreated **cutaneous, uveal, and mucosal** melanoma — directly evaluates the non-cutaneous subgroup |
| [37887546](https://pubmed.ncbi.nlm.nih.gov/37887546/) | 2023 | Cohort | Current Oncology | Retrospective cohort comparing survival outcomes with anti-PD-1 ± ipilimumab by age group in advanced melanoma |
| [40236344](https://pubmed.ncbi.nlm.nih.gov/40236344/) | 2025 | Case Report | Cureus | Case of metastatic melanoma to the transverse colon treated with immunotherapy; highlights GI perforation as an immune-related adverse event |

## New Zealand Market Information

Ipilimumab is not currently marketed in New Zealand — no authorizations are on record.

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Immunotherapy (anti-CTLA-4 immune checkpoint inhibitor) |
| Myelosuppression Risk | Low — direct myelosuppression is uncommon; risk profile is dominated by immune-related adverse events rather than bone marrow suppression |
| Emetogenicity Classification | Low |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic rationale is sound and is backed by L1-level evidence, including a completed randomized Phase 2 trial and real-world cohort data specifically covering uveal and mucosal melanoma, plus a large body of supportive Phase 3 trials in the broader melanoma population. However, non-cutaneous subtypes are known to respond less robustly to checkpoint blockade than cutaneous melanoma, so guardrails around subtype-specific efficacy expectations are warranted.

**To proceed, the following is needed:**
- Detailed mechanism-of-action documentation (DrugBank MOA field is currently a data gap)
- TFDA/NZ package insert warnings and contraindications (currently a blocking data gap — DG001)
- Subgroup-specific efficacy data isolating uveal vs. mucosal melanoma response rates
- New Zealand regulatory pathway assessment, given the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

