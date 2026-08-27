---
layout: default
title: Sertraline
parent: 僅模型預測 (L5)
nav_order: 319
evidence_level: L5
indication_count: 8
---

# Sertraline
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Sertraline: From Major Depressive Disorder to Agoraphobia

## One-Sentence Summary

Sertraline (DrugBank DB01104) is a widely used SSRI antidepressant. Among the eight new indications TxGNN predicted for it, six are personality-disorder diagnoses that tie for the top score (99.93%) but are flagged in this same evidence pack as likely artifacts of dense "psychiatric disorder" clustering in the knowledge graph, with sparse or unrelated literature support. **Agoraphobia**, ranked lower numerically (score 99.54%), is by far the best-evidenced candidate — supported by **4 clinical trials** (including a completed Phase 4 RCT, N=321) and **19 publications**, including two network meta-analyses — and is therefore the focus of this report.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not specified in the regulatory data pack (no licenses on file). Sertraline is a well-established SSRI, generally indicated for major depressive disorder and anxiety-spectrum conditions including panic disorder. |
| Predicted New Indication | Agoraphobia |
| TxGNN Prediction Score | 99.54% (rank 4295 of model output) |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

**Note on indication selection:** The nominal rank-1 prediction (histrionic personality disorder, score 99.93%) and four other personality-disorder predictions tied at the same score are scored **L4–L5 / Hold** in this evidence pack — their supporting literature (e.g., MMPI-2 subscales, binge-eating disorder, lupus erythematosus) does not actually address those diagnoses. Agoraphobia is the only candidate in this pack that reaches L1 evidence and an active "Proceed" recommendation, so it is used as the primary subject of this report.

---

## Why is This Prediction Reasonable?

Detailed drug-specific mechanism-of-action data was not available in this evidence pack (marked as a High-severity data gap, DG002, pending DrugBank API lookup). Based on general pharmacological knowledge, sertraline is a selective serotonin reuptake inhibitor (SSRI) that increases synaptic serotonin availability, which is believed to dampen autonomic hyperarousal and anticipatory anxiety.

Panic disorder and agoraphobia are clinically intertwined — agoraphobia frequently develops as a secondary complication of recurrent, unpredictable panic attacks, and the two are diagnosed and treated together in most clinical trial designs (e.g., "panic disorder with or without agoraphobia" is the standard trial inclusion criterion seen throughout the evidence below). Sertraline is already an established, guideline-recommended treatment for panic disorder in multiple jurisdictions, so its mechanistic and clinical rationale extends naturally to agoraphobia as a shared-pathophysiology indication rather than an unrelated new disease area.

By contrast, the personality-disorder predictions that nominally scored higher lack this kind of mechanistic bridge: their evidence, per the pack's own rationale annotations, mostly concerns depression comorbidity, treatment-resistance augmentation, or incidental case reports — not treatment of the personality disorder itself.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00677352](https://clinicaltrials.gov/study/NCT00677352) | Phase 4 | Completed | 321 | Randomized, double-blind, multicenter comparison of sertraline vs. paroxetine in panic disorder; direct head-to-head efficacy/safety RCT |
| [NCT00182533](https://clinicaltrials.gov/study/NCT00182533) | Phase 4 | Terminated | 170 | Sertraline in generalized social phobia with comorbidity; evaluated safety/efficacy in patients with co-occurring anxiety conditions, but stopped early |
| [NCT05210153](https://clinicaltrials.gov/study/NCT05210153) | N/A | Unknown | 148 | Utility of plasma drug-level monitoring and CYP2C19 genotyping for sertraline dose personalization (pharmacokinetic, not efficacy, evidence) |
| [NCT05930912](https://clinicaltrials.gov/study/NCT05930912) | N/A | Unknown | 1 | Psychoanalytic treatment context in ASD with comorbid anxiety; n=1, weak relevance to sertraline pharmacotherapy |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35045991](https://pubmed.ncbi.nlm.nih.gov/35045991/) | 2022 | Network Meta-analysis | BMJ | Compares SSRIs and other drug classes for remission rates and adverse-event risk in panic disorder with/without agoraphobia |
| [38014714](https://pubmed.ncbi.nlm.nih.gov/38014714/) | 2023 | Network Meta-analysis (Cochrane) | Cochrane Database Syst Rev | Systematic comparison of pharmacological treatments for panic disorder in adults |
| [16053461](https://pubmed.ncbi.nlm.nih.gov/16053461/) | 2005 | RCT | Bosnian J Basic Med Sci | Placebo-controlled comparison of sertraline vs. alprazolam for panic disorder with/without agoraphobia |
| [16505130](https://pubmed.ncbi.nlm.nih.gov/16505130/) | 2006 | RCT | Am J Geriatric Psychiatry | CBT vs. sertraline for anxiety disorders (including agoraphobia) in older adults |
| [12191627](https://pubmed.ncbi.nlm.nih.gov/12191627/) | 2002 | RCT | J Psychiatric Research | Pooled data from 4 sertraline/placebo trials; early improvement predicts remission in panic disorder |
| [11206597](https://pubmed.ncbi.nlm.nih.gov/11206597/) | 2000 | RCT | J Clin Psychiatry | Sertraline response in panic disorder patients at risk for poor outcome, including those with agoraphobia |
| [9734541](https://pubmed.ncbi.nlm.nih.gov/9734541/) | 1998 | RCT | Am J Psychiatry | Double-blind multicenter trial establishing efficacy and safety of sertraline in panic disorder |
| [36573969](https://pubmed.ncbi.nlm.nih.gov/36573969/) | 2022 | Review | JAMA | General review of anxiety disorders, including panic disorder/agoraphobia treatment landscape |
| [11110016](https://pubmed.ncbi.nlm.nih.gov/11110016/) | 2000 | Review | Int Clin Psychopharmacol | Summarizes comparator-controlled SSRI trials (including sertraline) for panic disorder and agoraphobia |
| [37676054](https://pubmed.ncbi.nlm.nih.gov/37676054/) | 2023 | Systematic Review | Expert Rev Neurother | Pharmacological management of panic disorder in older patients |

---

## New Zealand Market Information

Currently no marketing authorizations on file for sertraline in New Zealand (0 licenses; `market_status: 未上市` — not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are all marked as data gaps in this evidence pack; DG001 — missing Medsafe/TFDA package insert warnings — is flagged as **Blocking** for safety review.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Sertraline's use in panic disorder with agoraphobia is backed by L1 evidence — a completed Phase 4 RCT plus two independent network meta-analyses — and a coherent shared-pathophysiology mechanism. However, the drug has zero regulatory authorizations and no package-insert safety data in this market, so it cannot move past S1 safety screening yet.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a Blocking data gap (DG001)
- Confirmed mechanism-of-action data from DrugBank (DG002)
- Local regulatory pathway assessment given current "not marketed" status (0 licenses)
- Route-of-administration compatibility check (currently unassessed/pending in this pack)
- Discard or deprioritize the six tied top-score personality-disorder predictions — their literature support does not substantiate a treatment effect and should not consume further review resources
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

