---
layout: default
title: Clonazepam
parent: 僅模型預測 (L5)
nav_order: 81
evidence_level: L5
indication_count: 3
---

# Clonazepam
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

# Clonazepam: From Seizures to Restless Legs Syndrome

## One-Sentence Summary

Clonazepam is a long-acting benzodiazepine anticonvulsant, globally approved for the treatment of epilepsy and panic disorder, though not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Restless Legs Syndrome (RLS)**,
with **no registered clinical trials** but **20 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Epilepsy / seizure disorders, panic disorder (global approvals; not registered in New Zealand) |
| Predicted New Indication | Restless Legs Syndrome |
| TxGNN Prediction Score | 99.65% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Clonazepam is a benzodiazepine that acts as a positive allosteric modulator of GABA-A receptors, potentiating inhibitory GABAergic neurotransmission throughout the central and peripheral nervous system. Detailed mechanism of action data from the regulatory dossier is currently unavailable; however, based on its established pharmacological class, clonazepam's anticonvulsant and sedative-hypnotic properties arise from enhanced chloride channel conductance, reducing neuronal excitability. Its long half-life (18–50 hours) supports sustained overnight activity.

In Restless Legs Syndrome, clonazepam's therapeutic rationale operates via two distinct pathways. First, by reducing spinal cord reflex excitability through GABA-A modulation, it suppresses Periodic Limb Movements in Sleep (PLMS) — a hallmark co-morbidity of RLS that severely fragments sleep architecture. Second, its sedative-hypnotic properties shorten sleep latency and preserve deeper sleep stages (N2/N3), directly addressing the insomnia burden that dominates the clinical presentation of moderate-to-severe RLS.

Critically, clonazepam does **not** target the dopaminergic pathway, which is the primary pathophysiological driver of RLS. Its role is therefore **symptomatic rather than disease-modifying** — most appropriate as an adjunctive agent when dopamine agonists are inadequate or not tolerated, or in patients where PLMS and sleep maintenance are the dominant concerns. A 1984 randomized double-blind crossover trial (PMID 6380197), a 2017 Cochrane systematic review (PMID 28319266), and a 2025 AASM clinical practice guideline (PMID 39324694) all recognize clonazepam as a clinically used agent for RLS, lending biological and clinical plausibility to the TxGNN prediction.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for Clonazepam in Restless Legs Syndrome.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [28319266](https://pubmed.ncbi.nlm.nih.gov/28319266/) | 2017 | Systematic Review (Cochrane) | Cochrane Database Syst Rev | Benzodiazepines (particularly clonazepam) widely used for RLS (~25% of patients in large surveys); review acknowledges clinical use but notes formal RCT evidence remains limited |
| [39324694](https://pubmed.ncbi.nlm.nih.gov/39324694/) | 2025 | Clinical Practice Guideline | J Clin Sleep Med | AASM clinical practice guideline for treatment of RLS and PLMD in adults and pediatric patients; evidence-based drug selection framework |
| [38708125](https://pubmed.ncbi.nlm.nih.gov/38708125/) | 2024 | Narrative Review | Tremor Other Hyperkinetic Mov | Historical overview identifying 17 articles on clonazepam use in RLS/PLMS; among 16,694 RLS patients surveyed, ~25% received benzodiazepines singly or in combination |
| [36692194](https://pubmed.ncbi.nlm.nih.gov/36692194/) | 2023 | Systematic Review & Meta-analysis | J Clin Sleep Med | Systematic review of pharmacological suppression of PLMS; assessed efficacy of multiple drug classes including benzodiazepines with meta-analytic summary |
| [31942156](https://pubmed.ncbi.nlm.nih.gov/31942156/) | 2019 | Prospective Open-Label RCT | J Mid-Life Health | Head-to-head comparison of clonazepam vs nortriptyline in women aged 40+ with RLS; evaluated rate, frequency, and severity of RLS symptoms |
| [18925578](https://pubmed.ncbi.nlm.nih.gov/18925578/) | 2008 | Evidence-Based Review | Movement Disorders | MDS task force evidence-based review of RLS treatments; classified clonazepam as "likely efficacious" based on available literature |
| [24363103](https://pubmed.ncbi.nlm.nih.gov/24363103/) | 2014 | Narrative Review | Neurotherapeutics | Overview of evolving RLS treatment landscape; benzodiazepines discussed as secondary agents when first-line dopaminergic therapies are insufficient |
| [6380197](https://pubmed.ncbi.nlm.nih.gov/6380197/) | 1984 | RCT (Crossover) | Acta Neurol Scand | Earliest randomized double-blind crossover trial of clonazepam vs placebo in 6 RLS patients; significant improvement in subjective sleep quality and leg dysaesthesia |
| [9444111](https://pubmed.ncbi.nlm.nih.gov/9444111/) | 1997 | Clinical Review | ANNA Journal | Clonazepam for RLS in end-stage renal disease; reviews pharmacokinetic profile supporting use in patients with impaired kidney function |
| [3510520](https://pubmed.ncbi.nlm.nih.gov/3510520/) | 1986 | Early Clinical Report | Am Fam Physician | Early description of RLS clinical features and management; clonazepam cited as an effective agent for controlling leg dysaesthesia and associated insomnia |

---

## New Zealand Market Information

Clonazepam is not currently registered or marketed in New Zealand. No product authorizations were identified in the regulatory database search conducted on 2026-03-29.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Clonazepam has a well-recognized historical role in RLS management, acknowledged by both a 2017 Cochrane systematic review and a 2025 AASM clinical practice guideline, with mechanistic plausibility rooted in GABA-A–mediated suppression of PLMS and sleep continuity improvement. The absence of registered clinical trials does not reflect lack of evidence — it reflects the drug's age predating modern trial registration requirements; published controlled data and broad expert consensus support its conditional use.

**To proceed, the following is needed:**
- Obtain complete safety profile: package insert warnings, contraindications, and drug-drug interaction data (currently unavailable — blocking for clinical implementation)
- Assess dependency and withdrawal risk: long-acting benzodiazepines carry well-known tolerance, dependence, and rebound insomnia risks requiring structured prescribing protocols
- Special population evaluation: elderly patients face heightened risk of falls, cognitive impairment, and respiratory depression — age-specific dosing guidance is essential
- New Zealand regulatory registration pathway to be assessed if market access is intended
- Prospective RCT with modern outcome measures (IRLS scale, actigraphy, polysomnography) would elevate the evidence base from L3 to L2/L1 and support a formal indication extension
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

