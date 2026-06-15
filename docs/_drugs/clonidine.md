---
layout: default
title: Clonidine
parent: 僅模型預測 (L5)
nav_order: 82
evidence_level: L5
indication_count: 10
---

# Clonidine
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

# Clonidine: From Hypertension to Tourette Syndrome

## One-Sentence Summary

Clonidine is a central alpha-2 adrenergic agonist, widely recognized as an antihypertensive agent and used off-label for several neuropsychiatric conditions.
The TxGNN model predicts it may be effective for **Tourette Syndrome** — the highest-evidence prediction in this dataset (ranked 5th by model score, but 1st by clinical evidence strength) — with **3 clinical trials** and **19 publications** currently supporting this direction, including two randomized controlled trials published in 2024.

> **Note on TxGNN top-ranked prediction**: The model's highest-scored prediction is faciodigitogenital syndrome (Aarskog–Scott syndrome, score 99.9999%). However, no biological plausibility or supporting literature exists for this association (Evidence Level L5, Decision: Hold). This report therefore features **Tourette Syndrome** as the most clinically actionable and evidence-backed prediction from the full candidate list.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hypertension (central sympatholytic antihypertensive) |
| Predicted New Indication | Tourette Syndrome |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on established pharmacological knowledge, Clonidine acts as a central alpha-2 adrenergic receptor agonist. By stimulating presynaptic alpha-2A receptors in the locus coeruleus and prefrontal cortex, it suppresses norepinephrine (NE) release and reduces overall sympathetic outflow — the same mechanism by which it lowers blood pressure in hypertension.

Tourette Syndrome (TS) is a neurodevelopmental disorder characterized by involuntary motor and vocal tics, driven in part by dysregulation of the basal ganglia–corticostriatal circuit. Excess noradrenergic signaling is thought to heighten tic urge and impair impulse control within this circuit. Clonidine's inhibition of NE release from locus coeruleus neurons directly targets this noradrenergic overdrive, dampening the cortical hyperexcitability associated with tic generation. This is the same mechanism underlying its FDA-approved extended-release formulation (Kapvay) for ADHD — the most common comorbidity in TS patients — lending further biological credibility to this prediction.

A 2025 animal study (PMID 40392363) adds a secondary mechanistic dimension: Clonidine reduced neuroinflammatory markers, including elevated interleukin-2 in the basal ganglia of a TS rat model. This anti-inflammatory effect parallels postmortem findings in TS patients, suggesting that Clonidine may act on both the noradrenergic and neuroinflammatory axes of TS pathophysiology.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00370838](https://clinicaltrials.gov/study/NCT00370838) | Phase 4 | Completed | 12 | Head-to-head double-blind crossover RCT comparing Clonidine directly against levetiracetam for tic suppression in children with TS. The most directly relevant trial in this dataset for Clonidine efficacy in TS. |
| [NCT00152750](https://clinicaltrials.gov/study/NCT00152750) | Phase 4 | Unknown | 32 | Investigates whether nighttime Clonidine treatment reduces daytime aggression in children with TS and comorbid ADHD, positioning Clonidine as a safer, better-tolerated alternative to neuroleptics. |
| [NCT01172288](https://clinicaltrials.gov/study/NCT01172288) | Phase 2 | Completed | 31 | Double-blind RCT testing N-acetylcysteine (NAC) vs. placebo for tic reduction in TS children; Clonidine and guanfacine are cited as standard-of-care alpha-2 agonist comparators, providing contextual benchmarks. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [39258554](https://pubmed.ncbi.nlm.nih.gov/39258554/) | 2024 | RCT | Clinical Neuropharmacology | Multicenter, randomized, double-blind, placebo-controlled trial of the clonidine adhesive patch in TS. Provides the strongest and most recent direct evidence for Clonidine efficacy in reducing tic severity. |
| [38695046](https://pubmed.ncbi.nlm.nih.gov/38695046/) | 2024 | RCT | Psychiatry Investigation | RCT assessing clonidine adhesive patch efficacy and safety specifically in TS patients with comorbid ADHD — the dominant TS comorbidity — demonstrating dual-target benefit from a single agent. |
| [36528030](https://pubmed.ncbi.nlm.nih.gov/36528030/) | 2023 | Network Meta-Analysis | Lancet Child & Adolescent Health | Comprehensive network meta-analysis comparing all pharmacological interventions for TS in youth. Provides comparative efficacy and tolerability positioning for Clonidine relative to other tic-suppressing agents. |
| [34757514](https://pubmed.ncbi.nlm.nih.gov/34757514/) | 2022 | Clinical Guideline | European Child & Adolescent Psychiatry | Updated European Society for the Study of Tourette Syndrome (ESSTS) guidelines on pharmacological treatment of TS, incorporating Clonidine into the evidence-based treatment algorithm. |
| [31061209](https://pubmed.ncbi.nlm.nih.gov/31061209/) | 2019 | Systematic Review | Neurology | Comprehensive AAN-affiliated systematic review evaluating the efficacy and risk profile of all tic treatments; foundational reference supporting alpha-2 agonists as first-line options. |
| [34286606](https://pubmed.ncbi.nlm.nih.gov/34286606/) | 2021 | Systematic Review | Journal of Psychopharmacology | Quality-of-evidence assessment for TS pharmacotherapy across all ages, specifically evaluating methodological rigor of Clonidine trials. |
| [40392363](https://pubmed.ncbi.nlm.nih.gov/40392363/) | 2025 | Animal Study | Journal of Neuroimmune Pharmacology | Clonidine reduces IL-2 and other neuroinflammatory markers in basal ganglia of a TS rat model, supporting an anti-inflammatory mechanism complementary to its noradrenergic action. |
| [24210663](https://pubmed.ncbi.nlm.nih.gov/24210663/) | 2014 | Clinical Study | Psychiatry Research | Examines Clonidine's effects on sensorimotor gating (prepulse inhibition) in TS patients, probing a potential neurophysiological mechanism beyond simple tic suppression. |
| [1414629](https://pubmed.ncbi.nlm.nih.gov/1414629/) | 1992 | Clinical Study | Advances in Neurology | Comparative clinical study of Clonidine and Clonazepam in TS, one of the early controlled evaluations establishing Clonidine's role in the TS treatment landscape. |
| [89558](https://pubmed.ncbi.nlm.nih.gov/89558/) | 1979 | Clinical Study | Lancet | Seminal first report of Clonidine improving TS symptoms in children unresponsive to haloperidol. Proposed the noradrenergic hypothesis of TS and established the foundational rationale for alpha-2 agonist therapy in this disorder. |

---

## New Zealand Market Information

Clonidine (DB00575) currently holds **no registered authorizations** in New Zealand (Medsafe). The drug is not available through the standard prescription market as of the data cutoff. Any use would require a special clinical authorisation or alternative import pathway.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(All safety data fields — key warnings, contraindications, and drug interactions — returned no results in this evidence pack. Full safety review is required before clinical advancement.)*

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two multicenter randomized double-blind placebo-controlled trials published in 2024, a Lancet network meta-analysis, and updated European clinical practice guidelines collectively provide Level 1 evidence for Clonidine's efficacy in Tourette Syndrome. The mechanistic basis is well-characterized, FDA approval of the alpha-2 agonist class for ADHD (a closely related indication) provides strong regulatory precedent, and decades of clinical use lend a familiar safety profile in pediatric populations.

**To proceed, the following is needed:**
- Obtain and review the full package insert (or equivalent SmPC) to document key warnings, contraindications, cardiovascular precautions (hypotension, bradycardia, rebound hypertension on abrupt discontinuation), and drug interactions
- Confirm mechanism of action documentation via DrugBank API (DG002 remediation)
- Assess New Zealand regulatory pathway: determine Medsafe registration requirements, as no current authorizations exist (0 licenses)
- Define target formulation strategy: evaluate whether the adhesive patch formulation (as studied in the 2024 RCTs) or oral extended-release form is more suitable for the New Zealand market
- Develop a safety monitoring plan specifying cardiovascular and CNS parameters (blood pressure, heart rate, sedation, ECG if needed) for the TS patient population
- Clarify the proposed age group: pediatric dosing and monitoring requirements differ substantially from adults in this indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

