---
layout: default
title: Ketamine
parent: 僅模型預測 (L5)
nav_order: 186
evidence_level: L5
indication_count: 1
---

# Ketamine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Ketamine: From General Anesthesia to Headache Disorder

## One-Sentence Summary

> Ketamine is a dissociative anesthetic and NMDA receptor antagonist, historically used for induction and maintenance of general anesthesia and, off-label, for pain management.
> The TxGNN model predicts it may be effective for **Headache Disorder**,
> with **40 clinical trials** identified in the search and **20 publications** screened, of which a focused subset directly addresses headache/migraine/cluster headache indications.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | General anesthesia (induction/maintenance), dissociative anesthetic — not confirmed in the New Zealand regulatory record (drug is currently unmarketed) |
| Predicted New Indication | Headache Disorder |
| TxGNN Prediction Score | 99.33% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the evidence pack (`original_moa` is a Data Gap). Based on known pharmacology, ketamine is an NMDA receptor antagonist and dissociative anesthetic; its efficacy in general anesthesia is well established, and low ("sub-dissociative") doses are already used off-label as an analgesic in emergency and pain-management settings.

The mechanistic rationale for headache disorder is that NMDA receptor blockade can suppress **central sensitization** and **cortical spreading depression**, two processes implicated in the pathophysiology of migraine, cluster headache, and refractory chronic daily headache. This is why a number of tertiary headache/pain centers already use ketamine infusions off-label for status migrainosus and treatment-refractory headache.

This is not ketamine's original approved indication, and the mechanistic link — while biologically plausible and reflected in a growing body of emergency-department and headache-clinic trials — has not yet been confirmed by a large, dedicated Phase 3 program specifically powered for headache disorder.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03081416](https://clinicaltrials.gov/study/NCT03081416) | Phase 3 | Completed | 80 | THINK Trial: randomized, single-blind, placebo-controlled study of intranasal sub-dissociative ketamine vs. standard therapy for primary headache syndromes in the ED |
| [NCT05306899](https://clinicaltrials.gov/study/NCT05306899) | Phase 3 | Recruiting | 56 | KetHead Study: multicenter, placebo-controlled RCT of high-dose IV ketamine infusion for chronic daily headache via reversal of receptor-mediated sensitization |
| [NCT04179266](https://clinicaltrials.gov/study/NCT04179266) | Phase 1/2 | Completed | 23 | Proof-of-concept study of intranasal ketamine spray for chronic cluster headache |
| [NCT06608277](https://clinicaltrials.gov/study/NCT06608277) | Phase 2 | Recruiting | 175 | Multicenter RCT comparing ketamine, stellate ganglion block, and combination therapy vs. sham for TBI-associated headache and PTSD |
| [NCT04814381](https://clinicaltrials.gov/study/NCT04814381) | Phase 4 | Recruiting | 90 | Single infusion of ketamine combined with magnesium sulfate for refractory chronic cluster headache |
| [NCT02657031](https://clinicaltrials.gov/study/NCT02657031) | Phase 4 | Completed | 54 | Check Trial: multicenter RCT comparing low-dose ketamine vs. compazine for ED headache control |
| [NCT03221569](https://clinicaltrials.gov/study/NCT03221569) | Phase 4 | Unknown | 60 | Sub-dissociative ketamine vs. ketorolac for acute tension-type headache/migraine in the ED |
| [NCT02697071](https://clinicaltrials.gov/study/NCT02697071) | N/A | Completed | 34 | Randomized, double-blind, placebo-controlled trial of sub-dissociative ketamine for acute migraine-type headache in the ED |
| [NCT04860713](https://clinicaltrials.gov/study/NCT04860713) | Phase 4 | Completed | 5 | Open-label RCT of oral ketamine + aspirin vs. rimegepant for acute ED headache |
| [NCT02388321](https://clinicaltrials.gov/study/NCT02388321) | Phase 4 | Terminated | 22 | RCT comparing intranasal sub-dissociative ketamine to intranasal fentanyl for moderate-to-severe pediatric ED pain (indirect support; small sample, terminated early) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35356451](https://pubmed.ncbi.nlm.nih.gov/35356451/) | 2022 | Cohort | Frontiers in Neurology | Retrospective cohort assessing efficacy, duration, and safety of combined IV lidocaine and ketamine infusions for headache disorders |
| [41321235](https://pubmed.ncbi.nlm.nih.gov/41321235/) | 2026 | Guideline | Headache | 2025 American Headache Society update on parenteral pharmacotherapies (including ketamine) for acute migraine in the emergency department |
| [34919214](https://pubmed.ncbi.nlm.nih.gov/34919214/) | 2022 | Review | Drugs | Review of acute and prophylactic drug therapy for cluster headache |
| [38870050](https://pubmed.ncbi.nlm.nih.gov/38870050/) | 2024 | Review | Expert Review of Neurotherapeutics | Update on trigeminal neuralgia pharmacotherapy noting ketamine as a potential adjuvant alongside newer CGRP-targeted migraine agents |
| [37421541](https://pubmed.ncbi.nlm.nih.gov/37421541/) | 2023 | Review | Current Pain and Headache Reports | Evidence-based review of complex regional pain syndrome, relevant to ketamine's central-sensitization mechanism shared with refractory headache |

---

## New Zealand Market Information

Ketamine currently has **no marketing authorization** recorded in the New Zealand regulatory data (0 licenses, market status "Not Marketed").

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug interaction data are not currently available; the TFDA/Medsafe package insert lookup is flagged as a **Blocking** data gap that prevents formal S1 safety pre-assessment.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic rationale (NMDA antagonism → reduced central sensitization/cortical spreading depression) is biologically plausible and supported by one completed Phase 3 RCT (THINK Trial) plus several ongoing Phase 2–4 trials directly targeting migraine, cluster headache, and chronic daily headache — but overall evidence is rated L3, ketamine is not currently marketed in New Zealand, and safety data (warnings, contraindications, DDI) are entirely unavailable, which is a **Blocking** gap for any S1 safety pre-assessment.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently blocking (DG001)
- Confirmed mechanism of action data via DrugBank API (DG002)
- DDI data (current query status: not found)
- Results from ongoing Phase 3 trials (NCT05306899 KetHead Study, expected completion 2026-06; NCT06608277)
- Regulatory assessment of ketamine's controlled-substance/misuse-potential status for the headache indication pathway
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

