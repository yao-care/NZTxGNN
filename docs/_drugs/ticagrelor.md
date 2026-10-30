---
layout: default
title: Ticagrelor
parent: Model Prediction Only (L5)
nav_order: 451
evidence_level: L5
indication_count: 9
last_modified_at: 2026-10-30
monitor_inn: "TICAGRELOR"
predicted_indications: ["intracranial arteriosclerosis", "Monckeberg arteriosclerosis", "priapism", "ischemic disease", "May-Thurner syndrome", "livedo reticularis", "vascular ectasia", "angiodysplasia", "fibrocartilaginous embolism"]
description: "Ticagrelor: predicted new indication Intracranial arteriosclerosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Ticagrelor
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **9** 
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

## Ticagrelor: Intracranial arteriosclerosis
{: .text-alpha }

## One-Sentence Summary

Ticagrelor is an antiplatelet drug originally used for acute coronary syndrome and peripheral arterial disease.
The TxGNN model predicts it may be effective for **intracranial arteriosclerosis**,
with **2 clinical trials** (neither with results yet) and **2 publications** (a cohort study and a trial design paper) currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Intracranial arteriosclerosis |
| TxGNN Prediction Score | 0.9997 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold (research question) |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, ticagrelor is a reversible P2Y12 platelet inhibitor. Its efficacy in acute coronary syndrome is established, and mechanistically it may be applicable to intracranial arteriosclerosis.

Symptomatic intracranial atherosclerotic stenosis causes stroke through platelet-driven thrombosis and artery-to-artery embolism, so P2Y12 inhibition is mechanistically plausible. Both coronary and intracranial disease arise from atherosclerosis with platelet-mediated thrombosis, which links the original and new indications.

Ticagrelor is also not affected by CYP2C19 loss-of-function status, which may be an advantage over clopidogrel in carriers. Evidence from stroke and TIA populations (for example, the CYP2C19 loss-of-function carrier trial in the New England Journal of Medicine, 2021) supports this general direction. However, those studies concern stroke/TIA broadly, not intracranial atherosclerosis specifically.

**Evidence level note:** The L2 label reflects the on-disease Phase 3 trial (CAPTIVA), which is still active and has no results. Strictly, no completed RCT in intracranial atherosclerosis is yet available, so the evidence is weaker than the label suggests.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT05047172](https://clinicaltrials.gov/study/NCT05047172) | Phase 3 | Active, not recruiting | 1683 | CAPTIVA: tests whether rivaroxaban, ticagrelor or both are superior to clopidogrel for lowering 1-year ischemic stroke, intracerebral haemorrhage or vascular death. No results yet; ticagrelor's exact role in the arms is not confirmed. |
| [NCT06714526](https://clinicaltrials.gov/study/NCT06714526) | N/A | Recruiting | 100 | Pilot trial of genotype-guided P2Y12 inhibitor selection (including ticagrelor) vs conventional clopidogrel in symptomatic intracranial atherosclerotic disease. Exploratory, no results. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39658130](https://pubmed.ncbi.nlm.nih.gov/39658130/) | 2025 | Cohort | Journal of Neurointerventional Surgery | Experience with ticagrelor 60 mg twice daily plus aspirin 81 mg vs standard aspirin and clopidogrel for intracranial stenting. |
| [39862061](https://pubmed.ncbi.nlm.nih.gov/39862061/) | 2025 | Trial design paper | International Journal of Stroke | Design and early progress of CAPTIVA, which compares dual antithrombotic regimens against clopidogrel plus aspirin in symptomatic intracranial stenosis. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2724820 | Ticagrelor Mylan | Tab 90 mg | Valid; no indication text recorded |
| 2638770 | Ticagrelor Sandoz | Tab 90 mg | Valid; no indication text recorded |
| 2724286 | Ticagrelor Sandoz S29 | Tab 90 mg | Valid; no indication text recorded |

## Safety Considerations

- **Knowledge-graph contraindication associations** (not from label text): liver failure, liver disease, gallbladder disease, intracranial haemorrhage. The last is particularly relevant for intracranial disease, where the CAPTIVA primary outcome includes intracerebral haemorrhage.
- **Drug Interactions** (100 interactions were found; examples shown):
  - Major: clarithromycin
  - Moderate: acetylsalicylic acid (aspirin), morphine, simvastatin, aprepitant, dexamethasone, budesonide, miconazole, saxagliptin
  - Minor: naloxegol, prucalopride

Please also refer to the package insert for full warnings and contraindications, which were not available in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Hold (research question)**

**Rationale:**
The prediction score is very high and the mechanism is plausible, but no completed trial in intracranial atherosclerosis exists yet. The key Phase 3 trial (CAPTIVA) is still running and ticagrelor is only one of several tested options.

**To proceed, the following is needed:**
- CAPTIVA results (expected completion around January 2027), with ticagrelor-specific arm outcomes
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data from DrugBank
- A bleeding-risk plan, particularly for intracranial haemorrhage
- Clarification of New Zealand supply and licensing, since 3 of 3 licences are valid and the drug is marketed

## Other Predicted Indications (Brief Note)

- **Ischemic disease** (score 0.9964, L1): Large RCTs in coronary, stroke/TIA and diabetic atherosclerosis populations support ticagrelor, but this largely overlaps existing indications, so it is a label consolidation rather than novel repurposing. It could proceed with guardrails (bleeding risk, patient selection, duration and aspirin combination).
- **Other predictions** (Monckeberg arteriosclerosis, priapism, May-Thurner syndrome, livedo reticularis, mesenteric vascular occlusion, vascular ectasia, angiodysplasia, fibrocartilaginous embolism): Prediction-only or indirect evidence, with weak or implausible mechanisms. Angiodysplasia is a bleeding disorder, so this prediction is potentially harmful. Recommended: Hold.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

