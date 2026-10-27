---
layout: default
title: Acetylcysteine
parent: Model Prediction Only (L5)
nav_order: 17
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Acetylcysteine: predicted new indication Thrombotic disease (evidence level L5). Approved use: Bronchitis, paracetamol poisoning, cystic fibrosis, bronchiectasis…"
monitor_inn: "ACETYLCYSTEINE"
predicted_indications: ["thrombotic disease", "closed-angle glaucoma", "nasal cavity disease", "dry eye syndrome", "pharyngitis", "acute laryngopharyngitis", "tracheal disease", "transient ischemic attack (disease)", "laryngotracheitis", "angle-closure glaucoma"]
---

# Acetylcysteine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Acetylcysteine: From Bronchitis and Paracetamol Poisoning to Thrombotic Disease
{: .text-alpha }

## One-Sentence Summary

Acetylcysteine (N-acetylcysteine, NAC) was originally used for bronchitis, paracetamol poisoning, cystic fibrosis and related conditions.
The TxGNN model predicts it may be effective for **thrombotic disease**, with **7 clinical trials** and **20 publications** currently supporting this direction.
The evidence is concentrated in thrombotic microangiopathies (TA-TMA and TTP) rather than thrombosis in general, and most results have not yet been reported.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Bronchitis, paracetamol poisoning, cystic fibrosis, bronchiectasis, amyloidosis (per the knowledge-graph indication list; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Thrombotic disease |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L1 (the compiled data label this L1, but the Phase 3 trial's outcomes are not available, so it should be treated cautiously) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the compiled data. Based on known information, NAC is a thiol compound with antioxidant properties, established in respiratory disease and paracetamol overdose, and mechanistically may be applicable to thrombotic microangiopathies.

The proposed link is that NAC can reduce disulfide bonds in von Willebrand factor (VWF) multimers. This lowers VWF-platelet binding and microthrombus formation, which matters in TTP. Preclinical mouse and baboon TTP models support this. NAC's antioxidant action may also reduce the prothrombotic phenotype seen in renal insufficiency, which a Phase 2 trial (RENACTIF) is testing.

There are important caveats. "Thrombotic disease" is a broad label, and the evidence sits mainly in TTP and transplant-associated thrombotic microangiopathy (TA-TMA). Several listed trials address immune thrombocytopenia (ITP), a platelet disorder that is not strictly thrombotic. The relationship to the original respiratory and antidote indications is therefore indirect.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03252925](https://clinicaltrials.gov/study/NCT03252925) | Phase 3 | Completed | 170 | NAC for transplant-associated thrombotic microangiopathy; most direct evidence, but outcomes are not available in the compiled data |
| [NCT07279610](https://clinicaltrials.gov/study/NCT07279610) | Phase 2/3 | Active, not recruiting | 44 | Single-arm study of NAC for TA-TMA; results not yet available |
| [NCT03636932](https://clinicaltrials.gov/study/NCT03636932) | Phase 2 | Completed | 40 | Randomized, double-blind, placebo-controlled crossover trial of NAC to reduce the thrombotic phenotype in renal insufficiency (RENACTIF); biomarker-based endpoints |
| [NCT01808521](https://clinicaltrials.gov/study/NCT01808521) | Early Phase 1 | Completed | 3 | Pilot of intravenous NAC in suspected TTP alongside plasma exchange; hypothesis-generating only |
| [NCT04368598](https://clinicaltrials.gov/study/NCT04368598) | Phase 2 | Unknown | 44 | High-dose dexamethasone plus NAC in newly diagnosed ITP; only indirectly relevant |
| [NCT03460808](https://clinicaltrials.gov/study/NCT03460808) | Phase 1/2 | Unknown | 200 | Atorvastatin, NAC and danazol in steroid-resistant or relapsed ITP; combination design confounds attribution to NAC |
| [NCT05551624](https://clinicaltrials.gov/study/NCT05551624) | Early Phase 1 | Completed | 15 | Atorvastatin plus NAC on platelet count in ITP; indirect relevance |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [41977015](https://pubmed.ncbi.nlm.nih.gov/41977015/) | 2026 | Systematic review | J Clin Med | Systematic review and critical appraisal of NAC therapy in TTP |
| [42338865](https://pubmed.ncbi.nlm.nih.gov/42338865/) | 2026 | Systematic review | Cureus | Systematic review of NAC as adjunctive therapy in TTP |
| [37311880](https://pubmed.ncbi.nlm.nih.gov/37311880/) | 2023 | Cohort study | Ann Hematol | Retrospective cohort examining NAC and in-hospital mortality in acquired TTP; NAC use remains controversial |
| [28011677](https://pubmed.ncbi.nlm.nih.gov/28011677/) | 2017 | Preclinical | Blood | NAC in mouse and baboon TTP models, supporting the VWF-based mechanism |
| [32243196](https://pubmed.ncbi.nlm.nih.gov/32243196/) | 2020 | Review | Expert Rev Hematol | Repurposed drugs and novel agents in TTP, including NAC |
| [28416507](https://pubmed.ncbi.nlm.nih.gov/28416507/) | 2017 | Review | Blood | Overview of TTP pathophysiology and management |
| [33540569](https://pubmed.ncbi.nlm.nih.gov/33540569/) | 2021 | Review | J Clin Med | TTP pathophysiology, diagnosis and management |
| [36410267](https://pubmed.ncbi.nlm.nih.gov/36410267/) | 2022 | Review | J Infect Public Health | NAC in COVID-19, including its prothrombotic state |

## New Zealand Market Information

Both licences are cancelled, and the records contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2260794 | DBL Acetylcysteine | Inj 200 mg per ml, 10 ml ampoule | Not recorded (cancelled) |
| 2701928 | Hikma Acetylcysteine | Inj 200 mg per ml, 10 ml vial | Not recorded (cancelled) |

## Safety Considerations

- **Conditions flagged in the knowledge graph (not label text)**: peptic ulcer disease, hypotensive disorder, respiratory failure, asthma, bronchial disease and esophageal varices.
- **Drug Interactions**: The interaction screen returned 100 entries. Those with a rated level include activated charcoal (Minor) and inhaled rapid-acting human insulin (Moderate). Many others, such as mannitol, bupropion, pantoprazole, metformin, doxycycline, morphine and vancomycin, have no severity rating.

Please refer to the package insert for warnings and contraindications, as label-based information was not available.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A completed Phase 3 trial and a second Phase 2/3 trial in TA-TMA, plus preclinical TTP data and two recent systematic reviews, give a plausible direction. However, outcome data are not available, the evidence is confined to thrombotic microangiopathies, and no valid New Zealand licence currently exists.

**To proceed, the following is needed:**
- Published results of NCT03252925 and NCT07279610, and appraisal of the TTP systematic reviews
- Narrowing the target from "thrombotic disease" to TA-TMA or TTP
- Package insert warnings and contraindications (Medsafe), plus mechanism of action data from DrugBank
- A regulatory pathway given that both New Zealand licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

