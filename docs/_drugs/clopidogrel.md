---
layout: default
title: Clopidogrel
parent: 僅模型預測 (L5)
nav_order: 83
evidence_level: L5
indication_count: 8
---

# Clopidogrel
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

# Clopidogrel: From Atherothrombosis Prevention to Migraine with Brainstem Aura

## One-Sentence Summary

Clopidogrel is a thienopyridine-class antiplatelet agent (P2Y12 receptor antagonist) widely used to prevent atherothrombotic cardiovascular events including acute coronary syndrome and ischemic stroke.
The TxGNN model predicts it may be effective for **Migraine with Brainstem Aura** — a posterior-circulation migraine subtype often associated with patent foramen ovale (PFO) — with **0 dedicated clinical trials** but **16 relevant publications** supporting this specific subtype.
Notably, the closely related broader indication **Migraine Disorder** (rank 2, TxGNN 99.44%) is backed by **8 clinical trials** and **20 publications**, including the completed Phase 4 CANOA RCT (n=220, published in *JAMA*), providing a strong mechanistic bridge.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Prevention of atherothrombotic events (ACS, recent MI/stroke, PAD) |
| Predicted New Indication | Migraine with Brainstem Aura |
| TxGNN Prediction Score | 99.44% |
| Evidence Level | L3 (observational studies + systematic review for brainstem aura subtype) |
| Taiwan Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold (Research Question — subtype-specific trial needed) |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data for clopidogrel was not available in this evidence pack. Based on established pharmacology, clopidogrel is a prodrug that requires hepatic CYP2C19 activation to generate its active thiol metabolite, which then irreversibly blocks the P2Y12 ADP receptor on platelets. This prevents ADP-mediated conformational change of the GPIIb/IIIa receptor, reducing platelet aggregation and arterial thrombus formation.

Migraine with brainstem aura is mechanistically linked to PFO-mediated right-to-left cardiac shunting. In affected patients, activated platelets in the venous circulation release ADP, serotonin, and vasoactive microemboli that bypass pulmonary filtration and reach the posterior cerebral vasculature directly. This is hypothesized to trigger cortical spreading depression (CSD) within brainstem-projecting circuits, producing the characteristic aura symptoms (diplopia, dysarthria, ataxia, tinnitus). By blocking P2Y12-mediated platelet activation, clopidogrel may interrupt this cascade at its source.

Beyond the vascular hypothesis, preclinical evidence (PMID 31722730) demonstrates that P2Y12 receptor activation on microglia within the trigeminal nucleus caudalis drives neuroinflammation via the RhoA/ROCK pathway in chronic migraine models, and a mouse study (PMID 34363208) confirmed P2Y12 involvement in nitroglycerin-induced migraine-like behaviour. These findings suggest clopidogrel may act both peripherally (anti-platelet) and centrally (anti-neuroinflammatory) — a dual mechanism that is mechanistically plausible but has not yet been tested in dedicated brainstem aura trials.

---

## Clinical Trial Evidence

No clinical trials have been registered specifically for clopidogrel in **migraine with brainstem aura**. The following trials address the closely related broader migraine disorder indication (rank 2), with identical proposed mechanism (PFO + platelet activation) and direct relevance to brainstem aura as a subtype:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00799045](https://clinicaltrials.gov/study/NCT00799045) | Phase 4 | Completed | 220 | CANOA trial: Clopidogrel + aspirin vs. aspirin alone following transcatheter ASD closure; directly evaluates whether antiplatelet combination prevents new-onset migraine post-procedure |
| [NCT02938182](https://clinicaltrials.gov/study/NCT02938182) | Phase 4 | Unknown | 50 | Prospective evaluation of clopidogrel as prophylaxis for migraineurs with confirmed right-to-left shunt; directly tests the proposed mechanism |
| [NCT05546320](https://clinicaltrials.gov/study/NCT05546320) | Phase 4 | Unknown | 1,000 | COMPETE trial: Three-arm comparison of anticoagulation vs. antiplatelet therapy (clopidogrel arm) vs. standard migraine medications in PFO-associated migraine; largest ongoing study |
| [NCT04946734](https://clinicaltrials.gov/study/NCT04946734) | Phase 3 | Active, Not Recruiting | 440 | SPRING trial: PFO transcatheter closure vs. medical therapy (includes antiplatelet arm) for migraine relief; multicenter randomized design |
| [NCT02777359](https://clinicaltrials.gov/study/NCT02777359) | Phase 2 | Unknown | 100 | High-risk PFO closure for migraine; clopidogrel used as standard post-procedure antiplatelet therapy |
| [NCT00562289](https://clinicaltrials.gov/study/NCT00562289) | Phase 3 | Completed | 664 | CLOSE trial: PFO closure vs. anticoagulation vs. antiplatelet therapy for stroke recurrence; migraine as secondary endpoint, antiplatelet arm includes clopidogrel |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39989443](https://pubmed.ncbi.nlm.nih.gov/39989443/) | 2025 | Systematic Review | *Headache* | Comprehensive review of antithrombotic agents (including clopidogrel) as migraine preventives; synthesises available evidence across antiplatelet and anticoagulant drug classes |
| [26551304](https://pubmed.ncbi.nlm.nih.gov/26551304/) | 2015 | RCT | *JAMA* | CANOA main results: Clopidogrel + aspirin significantly reduced new-onset migraine attacks after transcatheter ASD closure vs. aspirin alone (primary endpoint met) |
| [32965476](https://pubmed.ncbi.nlm.nih.gov/32965476/) | 2021 | RCT / Secondary Analysis | *JAMA Cardiology* | CANOA one-year follow-up: Migraine reduction persisted at 6–12 months after clopidogrel cessation, suggesting durable benefit beyond treatment period |
| [24836213](https://pubmed.ncbi.nlm.nih.gov/24836213/) | 2014 | Pilot RCT | *Cephalalgia* | First pilot randomised controlled trial of clopidogrel as migraine prophylaxis; demonstrated feasibility and preliminary efficacy signal supporting larger trials |
| [40144614](https://pubmed.ncbi.nlm.nih.gov/40144614/) | 2025 | Systematic Review | *Indian J Thorac Cardiovasc Surg* | Systematic review of new-onset headache after transcatheter ASD closure; characterises incidence and management, including role of antiplatelet therapy |
| [32848048](https://pubmed.ncbi.nlm.nih.gov/32848048/) | 2020 | Observational | *J Investig Med* | Clopidogrel 75 mg/day added to existing prophylaxis reduced migraine frequency in drug-refractory patients with confirmed PFO over 3–6 months (n=15 with PFO among 26 evaluated) |
| [30478066](https://pubmed.ncbi.nlm.nih.gov/30478066/) | 2018 | Retrospective Cohort | *Neurology* | Clinical experience with off-label thienopyridines (clopidogrel, prasugrel) in migraineurs with PFO; documents symptomatic improvement in a real-world series |
| [24770421](https://pubmed.ncbi.nlm.nih.gov/24770421/) | 2014 | Retrospective Review | *Cephalalgia* | Clopidogrel as primary (non-procedural) therapy in migraineurs with right-to-left shunt; supports platelet activation → paradoxical embolization pathway |
| [16103551](https://pubmed.ncbi.nlm.nih.gov/16103551/) | 2005 | Observational | *Heart* | Seminal observation: Clopidogrel reduced migraine with aura after transcatheter PFO and ASD closure; provided the initial clinical rationale for this repurposing hypothesis |
| [15966922](https://pubmed.ncbi.nlm.nih.gov/15966922/) | 2005 | Case Series | *J Interv Cardiol* | Intense migraine developed in 5/13 patients after ASD closure; dramatic near-immediate relief achieved with 300 mg clopidogrel loading dose, suggesting acute platelet-mediated mechanism |

---

## Taiwan Market Information

Clopidogrel is **not currently registered in Taiwan** (0 TFDA authorizations on record). For reference, clopidogrel (Plavix®) holds regulatory approval in the United States (FDA), European Union (EMA), Japan (PMDA), and numerous other markets for atherothrombotic cardiovascular indications. Any use in Taiwan for migraine prevention would constitute off-label use pending formal regulatory application.

---

## Safety Considerations

Please refer to the package insert for safety information. (Formal warning and contraindication data was not retrieved in this evidence pack.)

The following safety signals are noted from literature retrieved during evidence collection:

- **Clopidogrel-associated inflammatory arthritis**: A case report (PMID 38107217) documented acute inflammatory arthritis developing within 5 days of initiating clopidogrel. This is a rare adverse event but constitutes a relevant safety signal for any musculoskeletal repurposing pathway.
- **Bleeding risk**: As an irreversible antiplatelet agent, clopidogrel substantially increases bleeding risk; concomitant NSAIDs, aspirin, or anticoagulants amplify this risk. A case of intracerebral haemorrhage with celecoxib co-administration (PMID 11793622) and spontaneous haemarthrosis with aspirin co-administration (PMID 12624808) have been reported.
- **CYP2C19 pharmacogenomics**: Clopidogrel efficacy is highly dependent on CYP2C19 metaboliser status; poor metabolisers (*2/*3 alleles, prevalent in East Asian populations) may have insufficient antiplatelet effect — a critical consideration for Taiwan patients.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic hypothesis linking clopidogrel to migraine with brainstem aura via PFO-mediated platelet activation is biologically coherent and supported by convergent clinical observations; however, the *specific brainstem aura subtype* lacks any dedicated clinical trial, and the broader migraine evidence base — while promising (CANOA RCT, JAMA 2015) — involves post-procedural contexts rather than standalone pharmacotherapy, limiting direct generalisability.

**To proceed, the following is needed:**
- A pre-specified brainstem aura subgroup analysis within the ongoing COMPETE (NCT05546320, n=1,000) or SPRING (NCT04946734, n=440) trials
- Mandatory PFO screening (contrast-enhanced transcranial Doppler) as an inclusion criterion, given that the effect appears concentrated in patients with confirmed right-to-left shunting
- Retrieval of the TFDA package insert to complete the safety gap (DG001)
- DrugBank MOA and pharmacokinetic profile to address the data gap (DG002), including CYP2C19 genotype distribution in the intended patient population
- Clarification of whether migraine with brainstem aura can be operationally distinguished from migraine with aura for trial stratification purposes (ICHD-3 criteria review)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

