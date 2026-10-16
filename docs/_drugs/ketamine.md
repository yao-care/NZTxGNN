---
layout: default
title: Ketamine
parent: Model Prediction Only (L5)
nav_order: 250
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
description: "Ketamine: predicted new indication Headache disorder (evidence level L5). Approved use: Not recorded in the licence data"
monitor_inn: "KETAMINE"
predicted_indications: ["headache disorder"]
---

# Ketamine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Ketamine: From No Recorded Approved Indication to Headache Disorder

## One-Sentence Summary

Ketamine is an injectable anaesthetic/analgesic agent, but the New Zealand licence record compiled for this report contains no approved indication text.
The TxGNN model predicts it may be effective for **headache disorder**, with **9 clinical trials** and **19 publications** currently supporting this direction.
Several of the trials are completed randomised studies, but most are small, and the one Phase 3 trial with a completed status (THINK) is the strongest direct evidence.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the licence data |
| Predicted New Indication | Headache disorder |
| TxGNN Prediction Score | 99.33% |
| Evidence Level | L1 (see note below) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Proceed with Guardrails |

*Note on evidence level:* The L1 criterion is two or more completed Phase 3 RCTs. Only one Phase 3 trial (NCT03081416) is completed; the other (NCT05306899) is still recruiting. Strictly applied, the evidence is closer to L2. The L1 rating is carried over from the compiled scoring, and I recommend it be re-checked.

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general pharmacology, ketamine is an NMDA receptor antagonist. Central sensitisation and glutamatergic signalling contribute to migraine, cluster headache and chronic headache pain, including cortical spreading depression and trigeminovascular activation. Blocking NMDA receptors may reduce this hyperexcitability and pain amplification.

No approved indication is on record, so a direct comparison between original and new indication is not possible. Ketamine's established use as an analgesic and anaesthetic agent, together with its use in other chronic pain conditions, makes a pain-related application plausible. The very high TxGNN score (0.993) is consistent with this reasoning.

Evidence is strongest for acute headache or migraine in the emergency department and weaker for chronic headache. Mechanistic support rests on general pharmacology rather than drug-specific labelling.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03081416](https://clinicaltrials.gov/study/NCT03081416) | Phase 3 | Completed | 80 | THINK trial: intranasal sub-dissociative ketamine vs standard care for acute headache in the ED (single-blind, placebo-controlled) |
| [NCT05306899](https://clinicaltrials.gov/study/NCT05306899) | Phase 3 | Recruiting | 56 | KetHead: multicentre, placebo-controlled RCT of high-dose IV ketamine infusion for chronic daily headache; no results yet |
| [NCT02657031](https://clinicaltrials.gov/study/NCT02657031) | Phase 4 | Completed | 54 | Low-dose ketamine vs prochlorperazine (Compazine) for headache in the ED |
| [NCT02697071](https://clinicaltrials.gov/study/NCT02697071) | Not applicable | Completed | 34 | Randomised placebo-controlled trial of sub-dissociative ketamine for acute migraine-type headache in the ED |
| [NCT04814381](https://clinicaltrials.gov/study/NCT04814381) | Phase 4 | Completed | 93 | Single ketamine infusion combined with magnesium sulfate for refractory chronic cluster headache (combination limits attribution) |
| [NCT03221569](https://clinicaltrials.gov/study/NCT03221569) | Phase 4 | Unknown | 60 | Ketamine vs ketorolac for acute tension-type headache in the ED |
| [NCT04179266](https://clinicaltrials.gov/study/NCT04179266) | Phase 1/2 | Completed | 23 | Proof-of-concept study of intranasal ketamine spray in chronic cluster headache |
| [NCT04860713](https://clinicaltrials.gov/study/NCT04860713) | Phase 4 | Completed | 5 | Oral ketamine + aspirin vs rimegepant for acute headache in the ED; very small, combination formulation |

One further registered study (NCT02403687, PACE) is a broad observational analgesic study of topical NSAIDs and is not headache-specific, so it is not counted as supporting evidence.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [41321235](https://pubmed.ncbi.nlm.nih.gov/41321235/) | 2026 | Guideline | Headache | American Headache Society 2025 update on parenteral pharmacotherapies for acute migraine in the ED |
| [34703891](https://pubmed.ncbi.nlm.nih.gov/34703891/) | 2021 | Systematic review | J Dent Anesth Pain Med | Efficacy of ketamine in migraine and other primary headaches vs placebo and active comparators |
| [35356451](https://pubmed.ncbi.nlm.nih.gov/35356451/) | 2022 | Retrospective cohort | Front Neurol | IV lidocaine and ketamine infusions for inpatient headache disorders: efficacy, duration and safety |
| [36310189](https://pubmed.ncbi.nlm.nih.gov/36310189/) | 2023 | Systematic review / meta-analysis | J Neurol | Preventive treatments for refractory chronic cluster headache |
| [34806767](https://pubmed.ncbi.nlm.nih.gov/34806767/) | 2021 | Review | Headache | Second-line injectable options for migraine in the ED after first-line failure |
| [34919214](https://pubmed.ncbi.nlm.nih.gov/34919214/) | 2022 | Review | Drugs | Drug treatment of cluster headache, acute and prophylactic |
| [32410204](https://pubmed.ncbi.nlm.nih.gov/32410204/) | 2020 | Review | Curr Neurol Neurosci Rep | ED and inpatient management of headache in children and adolescents |
| [38870050](https://pubmed.ncbi.nlm.nih.gov/38870050/) | 2024 | Review | Expert Rev Neurother | Pharmacotherapy update for trigeminal neuralgia; ketamine mentioned as a possible adjuvant |

Other retrieved publications concern ketamine/esketamine in depression and other conditions and are only indirectly relevant to headache.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2719371 | Ketalar (cancelled) | Inj 100 mg per ml, 2 ml vial | Not recorded |

## Safety Considerations

- **Disease-level cautions (from the knowledge graph, not label text):** Ketamine has contraindication links with hypertension and hypertensive disorders, stroke and cerebrovascular disorders, cerebral infarction, myocardial infarction, coronary artery disease and myocardial ischaemia, intracranial hypertension and pseudotumor cerebri, ocular hypertension, respiratory failure, hyperthyroidism/thyrotoxicosis/Graves disease, delirium, substance abuse/dependence, and hemoglobinopathy.
- **Drug Interactions:** 100 interactions were found. Moderate-level interactions include opioids (morphine, liposomal morphine, opium, diphenoxylate), cannabinoids (dronabinol, nabilone), metoclopramide, and sympathomimetic or stimulant agents (sibutramine, phentermine, ephedrine, fenfluramine, isometheptene, topical epinephrine). Several others (e.g. atropine, amphotericin B, famotidine, omeprazole, ondansetron) are of unknown severity.

Package insert warnings and contraindications were not available; please refer to the product information for full safety details.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple randomised trials, including one completed Phase 3 trial, and several reviews support ketamine in acute headache and migraine, backed by a plausible NMDA-related mechanism. However, the trials are small, there is no approved indication on record, no currently valid New Zealand licence, and package insert safety information is missing. The cardiovascular and cerebrovascular cautions are especially relevant for headache populations.

**To proceed, the following is needed:**
- Obtain and review the Medsafe package insert for warnings and contraindications (a blocking gap for safety screening)
- Obtain DrugBank mechanism of action data
- Results from the ongoing Phase 3 KetHead trial (NCT05306899) and a re-check of the evidence level
- Clarify the regulatory pathway given the cancelled licence, and the route of administration (intranasal vs intravenous), since the only recorded product is an injection
- A patient-selection and monitoring plan that excludes or closely monitors patients with cardiovascular, cerebrovascular, intracranial pressure and substance-use risk
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

