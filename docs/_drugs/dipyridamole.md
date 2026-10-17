---
layout: default
title: Dipyridamole
parent: Model Prediction Only (L5)
nav_order: 157
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Dipyridamole: predicted new indication Prinzmetal angina (evidence level L5). Approved use: Atrial fibrillation"
monitor_inn: "DIPYRIDAMOLE"
predicted_indications: ["stroke disorder", "thrombotic disease", "sick sinus syndrome 2, autosomal dominant", "transient ischemic attack (disease)", "sarcoglycanopathy", "Wildervanck syndrome", "macrocephaly, dysmorphic facies, and psychomotor retardation", "cavernous sinus thrombosis", "lateral sinus thrombosis"]
---

# Dipyridamole
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

# Dipyridamole: From Atrial Fibrillation to Prinzmetal Angina

## One-Sentence Summary

Dipyridamole is an antiplatelet and vasodilator drug, listed in the knowledge graph under atrial fibrillation as its original indication.
The TxGNN model's top-ranked prediction is **Prinzmetal angina** (score 99.99%), but the supporting evidence for this prediction is weak: **0 clinical trials** and **10 publications**, mostly describing dipyridamole as a stress-testing agent rather than a therapy.
The strongest evidence in the data compiled for this report is for **stroke disorder** and **transient ischemic attack (TIA)**, which are established secondary prevention uses (typically with aspirin) rather than novel repurposing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Atrial fibrillation (from the knowledge graph; no approved indication text is available from local licences) |
| Predicted New Indication | Prinzmetal angina (rank 1) |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L4 for Prinzmetal angina (L1 for stroke disorder and TIA) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Hold for Prinzmetal angina; Proceed with Guardrails for stroke disorder and TIA |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, dipyridamole inhibits adenosine reuptake and phosphodiesterase activity. This raises extracellular adenosine and intracellular cAMP/cGMP, producing coronary vasodilation and inhibition of platelet aggregation.

**Prinzmetal angina (rank 1):** The mechanistic case is weak. The literature mainly uses dipyridamole as a stress-testing agent that can provoke or unmask coronary vasospasm in variant angina. One report describes aminophylline termination of dipyridamole stress triggering vasospasm. In this setting dipyridamole is a diagnostic or provocative tool, not a treatment. Vasodilation and coronary steal could even be harmful in vasospastic disease. The high TxGNN score therefore should not be read as support for therapeutic use.

**Stroke disorder and TIA (ranks 2 and 5):** Here the mechanism fits well. The antiplatelet effect, plus possible antioxidant and neuroprotective effects, underlies the use of aspirin plus extended-release dipyridamole in secondary stroke prevention. This is a guideline-recognized, established use rather than a new repurposing finding. Evidence is for the combination with aspirin, not dipyridamole alone.

**Other predictions (ranks 3 and 4, 6 to 10):** Thrombotic disease has a coherent but non-specific mechanism with only older reviews. Sick sinus syndrome 2, sarcoglycanopathy, Wildervanck syndrome, the macrocephaly syndrome, cavernous sinus thrombosis and lateral sinus thrombosis rest on model prediction alone, with no trials or literature. For sick sinus syndrome, adenosine potentiation could even worsen bradyarrhythmia.

---

## Clinical Trial Evidence

No clinical trials are registered for Prinzmetal angina, the top-ranked prediction. The trials below relate to the best-supported predictions (stroke disorder and TIA).

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00161070](https://clinicaltrials.gov/study/NCT00161070) | Phase 4 | Completed | 4500 | ESPRIT: aspirin plus dipyridamole or mild anticoagulation vs aspirin alone after cerebral ischaemia of arterial origin (TIA or minor stroke) |
| [NCT00562588](https://clinicaltrials.gov/study/NCT00562588) | Phase 4 | Completed | 551 | EARLY: Aggrenox started within 24 hours of stroke vs after 7 days of aspirin in ischaemic stroke |
| [NCT00238667](https://clinicaltrials.gov/study/NCT00238667) | Phase 3 | Completed | 250 | Feasibility of comparing anticoagulants vs antiplatelets in cervical artery dissection (dipyridamole not the focus) |
| [NCT00562289](https://clinicaltrials.gov/study/NCT00562289) | Phase 3 | Completed | 664 | PFO closure or anticoagulants vs antiplatelet therapy to prevent stroke recurrence (dipyridamole not the primary variable) |
| [NCT00738894](https://clinicaltrials.gov/study/NCT00738894) | N/A | Completed | 664 | Gore REDUCE: PFO closure plus antiplatelets vs antiplatelets alone (dipyridamole at most part of comparator) |
| [NCT02630862](https://clinicaltrials.gov/study/NCT02630862) | N/A | Completed | 240 | Oxidative stress and oxysterol profiling with antithrombotic treatment after carotid revascularization (biomarker study) |
| [NCT06053021](https://clinicaltrials.gov/study/NCT06053021) | N/A | Unknown | 1200 | Antiplatelet therapy in acute ischaemic stroke with thrombocytopenia (dipyridamole not clearly specified) |
| [NCT06983795](https://clinicaltrials.gov/study/NCT06983795) | N/A | Not yet recruiting | 1000 | Asia-Pacific registry of PFO and ischaemic stroke (indirectly relevant) |

---

## Literature Evidence

**Prinzmetal angina (rank 1)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [633593](https://pubmed.ncbi.nlm.nih.gov/633593/) | 1978 | Pharmacologic response study | Jpn Circ J | Response of rest angina, including variant angina, to several drugs including dipyridamole; propranolol was ineffective or aggravated attacks |
| [3421166](https://pubmed.ncbi.nlm.nih.gov/3421166/) | 1988 | Case series / diagnostic | Am J Cardiol | Aminophylline termination of dipyridamole stress can trigger coronary vasospasm in variant angina |
| [3190956](https://pubmed.ncbi.nlm.nih.gov/3190956/) | 1988 | Cohort | Br Heart J | Reproducibility of exercise testing in patients with ST elevation and different dipyridamole test responses |
| [2022043](https://pubmed.ncbi.nlm.nih.gov/2022043/) | 1991 | Review | Circulation | Pathophysiology of noninvasive functional evaluation of coronary stenosis, including dipyridamole testing |

**Stroke disorder and TIA (ranks 2 and 5)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15569877](https://pubmed.ncbi.nlm.nih.gov/15569877/) | 2005 | Meta-analysis | Stroke | Individual patient data meta-analysis of randomized trials of dipyridamole, with or without aspirin, for secondary prevention after stroke or TIA |
| [17636684](https://pubmed.ncbi.nlm.nih.gov/17636684/) | 2007 | Cochrane systematic review | Cochrane Database Syst Rev | Adding dipyridamole to aspirin lowered risk 22% in one trial, but a wider review found little difference vs other antiplatelet combinations |
| [23871093](https://pubmed.ncbi.nlm.nih.gov/23871093/) | 2013 | Meta-analysis | J Neurol Sci | Efficacy and safety of aspirin plus dipyridamole vs aspirin in secondary prevention after TIA or stroke |
| [31177983](https://pubmed.ncbi.nlm.nih.gov/31177983/) | 2019 | Network meta-analysis | Stroke | Efficacy and safety of antiplatelet agents after noncardioembolic stroke or TIA |
| [34399713](https://pubmed.ncbi.nlm.nih.gov/34399713/) | 2021 | Network meta-analysis | BMC Neurol | Comparative effectiveness and harms of antiplatelet drugs for secondary stroke prevention |
| [34211246](https://pubmed.ncbi.nlm.nih.gov/34211246/) | 2021 | Review | Aust Prescr | Aspirin, clopidogrel, or aspirin plus dipyridamole are first-line options for secondary stroke prevention without atrial fibrillation |
| [11786451](https://pubmed.ncbi.nlm.nih.gov/11786451/) | 2002 | Meta-analysis | BMJ | Collaborative meta-analysis of antiplatelet therapy in high-risk patients |
| [20955428](https://pubmed.ncbi.nlm.nih.gov/20955428/) | 2010 | Review | Ann N Y Acad Sci | Possible antithrombotic and neuroprotective benefits of dipyridamole in acute stroke |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2710919 | Dipyridamole - Strides | Modified-release capsule 200 mg | Not stated; licence status: cancelled |

---

## Safety Considerations

- **Contraindications (knowledge-graph associations, not label text):** The knowledge graph links dipyridamole with contraindications in a broad set of conditions, including kidney disease, liver disease, gallbladder disease, thrombocytopenia, peptic ulcer disease (including active), hypotensive disorder, gout, acute coronary syndrome, myocardial infarction, coronary thrombosis, hemophilia, anaemia, hemoglobinopathy, thrombocytopenic purpura, congenital prothrombin deficiency, vitamin K deficiency haemorrhagic disease, gastritis, nasal cavity polyp, Reye syndrome, Alzheimer disease, Pick disease, substance abuse/dependence, and several electrolyte or metabolic disorders (potassium deficiency, hypercalcemia, nephrocalcinosis, phosphorus metabolism disease). These should be verified against the current package insert.
- **Drug Interactions:** The interaction screen identified 100 potential interactions. Those flagged Moderate are dexfenfluramine, sibutramine and fenfluramine. Others in the sample, with severity not classified, include acetylsalicylic acid, cimetidine, omeprazole, pantoprazole, lansoprazole, metformin, glimepiride, rosiglitazone, morphine, prednisone, simvastatin, vancomycin and amphotericin B.
- **Tolerability:** Headache can limit adherence (noted in the trial-based rationale for stroke and TIA).

Package insert warnings and contraindications were not available for this report, so please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold for Prinzmetal angina (top-ranked prediction); Proceed with Guardrails for stroke disorder and TIA**

**Rationale:**
- Prinzmetal angina has a high model score but no registered trials, and the literature shows dipyridamole as a vasospasm provoker in diagnostic testing, so a therapeutic role is unsupported and potentially harmful.
- Stroke disorder and TIA are backed by large Phase 3/4 trials (e.g., ESPRIT, n=4500) and several meta-analyses, but this is an established use of the aspirin combination rather than new repurposing, and the only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Detailed mechanism of action data from DrugBank
- Confirmation of current availability in New Zealand, including whether any aspirin plus dipyridamole product is supplied
- A review of the high interaction burden, particularly with other antiplatelet and anticoagulant agents
- For Prinzmetal angina, any therapeutic (not diagnostic) evidence before reconsidering the Hold decision
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

