---
layout: default
title: Doxazosin
parent: 僅模型預測 (L5)
nav_order: 126
evidence_level: L5
indication_count: 2
---

# Doxazosin
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

# Doxazosin: From Hypertension / BPH to Migraine Disorder

## One-Sentence Summary

Doxazosin is a selective α1-adrenergic receptor blocker clinically established for the treatment of hypertension and benign prostatic hyperplasia (BPH). The TxGNN model predicts it may be effective for **Migraine Disorder**, with **0 clinical trials** and **1 publication** (a 1997 expert opinion) currently supporting this direction. Evidence is very limited, and the biological plausibility is low-to-moderate.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hypertension / Benign Prostatic Hyperplasia (pharmacological class reference; no NZ regulatory record available) |
| Predicted New Indication | Migraine Disorder |
| TxGNN Prediction Score | 99.20% |
| Evidence Level | L4 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Doxazosin is a selective α1-adrenergic receptor blocker. By occupying α1 receptors on vascular smooth muscle, it reduces peripheral vascular resistance — the mechanism underlying its use in hypertension and BPH. Detailed MOA data from the regulatory dataset is not currently available; the following analysis draws on established pharmacological knowledge.

The mechanistic hypothesis linking doxazosin to migraine prophylaxis rests on three pathways: (1) α1 receptors are widely distributed in cerebral vasculature, and their blockade may modulate cerebrovascular tone, which is dysregulated during migraine attacks; (2) sympathetic overactivation is thought to trigger cortical spreading depression (CSD), a key pathophysiological event in migraine — α1 blockade may attenuate this process; (3) the established first-line migraine prophylactic agents are β-blockers (e.g., propranolol), confirming that adrenergic signalling broadly plays a role in migraine pathophysiology and lending indirect biological plausibility to adrenergic blockade as a therapeutic strategy.

However, biological plausibility is assessed as low-to-moderate overall. No clinical trial has ever been registered for doxazosin in migraine. The only supporting evidence is a single 1997 expert opinion with a sample of 10 patients and a 50% discontinuation rate due to side effects. The absence of any follow-up research in nearly 30 years substantially limits confidence in this prediction. The high TxGNN score (99.20%) likely reflects the model's graph proximity between doxazosin and migraine disorder rather than independent clinical signal.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [9074296](https://pubmed.ncbi.nlm.nih.gov/9074296/) | 1997 | Expert Opinion | Headache | Small case series (n=10) of migraine patients treated with terazosin or doxazosin in a general neurology practice; 9/10 showed decreased migraine frequency, severity, or both; 5/10 discontinued due to side effects; no serious adverse events reported |

---

## New Zealand Market Information

Doxazosin is not currently marketed in New Zealand. No Medsafe authorizations are on record.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The sole supporting evidence is a 1997 expert opinion with only 10 patients and a 50% discontinuation rate due to adverse effects; no clinical trials have been initiated in the nearly 30 years since publication. The drug is not marketed in New Zealand, creating a significant additional regulatory hurdle before any repurposing pathway could be pursued.

**To proceed, the following is needed:**
- Mechanism of action (MOA) data from DrugBank or primary pharmacology literature
- Full safety profile — key warnings, contraindications, and drug-drug interactions — sourced from the approved package insert
- At minimum, a structured retrospective case series or pilot feasibility study with pre-defined efficacy and safety endpoints
- Regulatory pathway scoping for a non-marketed drug seeking a new indication in New Zealand
- Assessment of the second TxGNN-predicted indication (Migraine with Brainstem Aura, score 99.19%, L5) — currently no evidence exists and the decision is Hold pending any primary data
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

