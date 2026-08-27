---
layout: default
title: Moclobemide
parent: 僅模型預測 (L5)
nav_order: 231
evidence_level: L5
indication_count: 2
---

# Moclobemide
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

# Moclobemide: From Depression to Agoraphobia

## One-Sentence Summary

> Moclobemide is a reversible, selective MAO-A inhibitor (RIMA) with established efficacy in depression, social anxiety disorder, and panic disorder.
> The TxGNN model predicts it may be effective for **Agoraphobia**,
> with **0 registered clinical trials** but **12 supporting publications**, including 2 head-to-head RCTs conducted specifically in panic disorder with agoraphobia.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Depression / Panic Disorder (established RIMA-class indications per cited literature; no official label data available) |
| Predicted New Indication | Agoraphobia |
| TxGNN Prediction Score | 99.43% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed formal mechanism-of-action documentation (DrugBank MOA field) is not available for moclobemide in this evidence pack. Based on the mechanistic literature cited in the prediction rationale, moclobemide is a reversible, selective inhibitor of monoamine oxidase A (RIMA), which raises synaptic concentrations of noradrenaline, serotonin, and dopamine. This pharmacological class has established efficacy in depression, social anxiety disorder, and panic disorder.

Agoraphobia is clinically highly comorbid with panic disorder and is frequently diagnosed jointly as "panic disorder with agoraphobia." Two head-to-head RCTs directly enrolled this combined patient population: Loerch et al. (1999) compared moclobemide, CBT, and their combination, while Krüger & Dahl (1999) compared moclobemide 450 mg/day against clomipramine 150 mg/day in a multicenter double-blind design.

Because agoraphobia and panic disorder share overlapping serotonergic/noradrenergic pathophysiology and are routinely co-diagnosed, the existing RCT and review evidence for moclobemide in panic disorder/agoraphobia provides plausible, though indirect, support for the TxGNN prediction. No identified trial used agoraphobia as a standalone primary endpoint, so this should be regarded as an indication extension rather than a direct on-label finding.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10448444](https://pubmed.ncbi.nlm.nih.gov/10448444/) | 1999 | RCT | Br J Psychiatry | Randomised placebo-controlled trial comparing moclobemide, CBT, and their combination in panic disorder with agoraphobia |
| [10361962](https://pubmed.ncbi.nlm.nih.gov/10361962/) | 1999 | RCT | Eur Arch Psychiatry Clin Neurosci | Multicenter double-blind RCT: moclobemide 450 mg/day vs clomipramine 150 mg/day in DSM-III-R panic disorder with/without agoraphobia (n=135) |
| [28867934](https://pubmed.ncbi.nlm.nih.gov/28867934/) | 2017 | Review | Dialogues Clin Neurosci | Guideline-based review of pharmacotherapy for anxiety disorders including panic disorder/agoraphobia |
| [32002937](https://pubmed.ncbi.nlm.nih.gov/32002937/) | 2020 | Review | Adv Exp Med Biol | Review of psychopharmacological treatment options for panic disorder/agoraphobia and related anxiety disorders |
| [8313401](https://pubmed.ncbi.nlm.nih.gov/8313401/) | 1993 | Review | Clin Neuropharmacol | RCT/review of reversible MAO-A inhibitors (incl. moclobemide) in panic disorder |
| [7717094](https://pubmed.ncbi.nlm.nih.gov/7717094/) | 1995 | Review | Acta Psychiatr Scand Suppl | Review of RIMA (moclobemide) efficacy across depressive and anxiety disorders |
| [2248064](https://pubmed.ncbi.nlm.nih.gov/2248064/) | 1990 | Review | Acta Psychiatr Scand Suppl | Review: MAOIs effective in panic disorder with agoraphobia, social phobia, and atypical depression |
| [16850261](https://pubmed.ncbi.nlm.nih.gov/16850261/) | 2006 | Cohort/Imaging RCT | Metab Brain Dis | SPECT comparison of citalopram vs moclobemide effects on resting brain perfusion in social anxiety disorder |
| [7892341](https://pubmed.ncbi.nlm.nih.gov/7892341/) | 1995 | Case report | Psychiatr Prax | Case report: treatment-refractory panic disorder with agoraphobia responded to imipramine + moclobemide + behavior therapy |
| [12006898](https://pubmed.ncbi.nlm.nih.gov/12006898/) | 2002 | Case study/Commentary | J Clin Psychopharmacol | Reanalysis of moclobemide response data in panic disorder using pre-post regression slopes |

---

## New Zealand Market Information

Moclobemide currently holds no marketing authorization in New Zealand (0 licenses on record).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two RCTs directly studied moclobemide in panic disorder with agoraphobia and multiple reviews corroborate the mechanistic plausibility of MAO-A inhibition for this indication class, but no trial used agoraphobia as a standalone primary endpoint and no trials or authorizations exist in New Zealand.

**To proceed, the following is needed:**
- Official package insert / label data (warnings, contraindications) — currently a Blocking data gap preventing full safety review
- Confirmed DrugBank mechanism-of-action record
- New Zealand regulatory pathway assessment given current "Not Marketed" status
- Agoraphobia-specific trial or registry data, since existing evidence is drawn from panic disorder with comorbid agoraphobia rather than agoraphobia as a primary endpoint
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

