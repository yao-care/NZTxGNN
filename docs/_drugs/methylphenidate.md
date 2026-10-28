---
layout: default
title: Methylphenidate
parent: Model Prediction Only (L5)
nav_order: 292
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-28
monitor_inn: "Methylphenidate"
predicted_indications: ["faciodigitogenital syndrome", "chondromyxoid fibroma", "specific developmental disorder", "dysthymic disorder"]
description: "Methylphenidate: predicted new indication Faciodigitogenital syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Methylphenidate
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

## Methylphenidate: Faciodigitogenital syndrome
{: .text-alpha }

## One-Sentence Summary

Methylphenidate is a central nervous system stimulant originally used to treat attention deficit hyperactivity disorder (ADHD) and narcolepsy.
The TxGNN model predicts it may be effective for **faciodigitogenital syndrome** (Aarskog-Scott syndrome), but there are currently **0 clinical trials** and **0 publications** supporting this specific prediction.
The score is a model output only, and the evidence is at the prediction level.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Faciodigitogenital syndrome |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 13 of 13) |
| Number of Authorizations | 13 (valid: 13) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Pharmacology records show that methylphenidate acts on the norepinephrine transporter (NET, SLC6A2) and the dopamine transporter (DAT, SLC6A3). This is consistent with its known role as a dopamine and norepinephrine reuptake inhibitor in the central nervous system.

Faciodigitogenital syndrome is a rare X-linked disorder with skeletal and genital anomalies. Some patients also show ADHD-like behavioural features. Any benefit of methylphenidate would therefore be limited to symptomatic behavioural management, not to the underlying disease.

No direct mechanistic link is supported by the available data. The very high TxGNN score reflects a model prediction only, and it should be treated with caution.

Other predictions in the model output are weaker or less plausible for this drug:
- **Specific developmental disorder** (score 0.9999, L4): the literature is almost entirely ADHD-focused, which is already an approved indication. It is indirect evidence for other developmental disorders.
- **Dysthymic disorder** (score 0.9911, L4): there is only indirect evidence from ADHD with comorbid subsyndromal depression. There is no direct evidence in dysthymia as a primary diagnosis.
- **Chondromyxoid fibroma** (score 0.9999, L5): there is no plausible mechanistic link, and this is likely a TxGNN model (deep learning) artefact.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for faciodigitogenital syndrome.

## New Zealand Market Information

All 13 licences are currently valid. Five representative licences are listed below. The data contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 437204 | Rubifen | Immediate-release tablet 10 mg | Valid |
| 2349329 | Ritalin | Immediate-release tablet 10 mg | Valid |
| 2116782 | Rubifen SR | Sustained-release tablet 20 mg | Valid |
| 2562782 | Methylphenidate ER - Teva | Extended-release tablet 36 mg | Valid |
| 2711176 | Methylphenidate Sandoz XR | Modified-release tablet 18 mg | Valid |

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (these come from drug-disease relationships in the knowledge graph, not from package insert text):
  - Cardiovascular and cerebrovascular: hypertension, coronary artery disease, myocardial infarction, stroke and cerebral infarction, sudden cardiac arrest, heart conduction disease
  - Psychiatric: bipolar disorder, psychotic disorders (including schizoaffective disorder), anorexia nervosa, substance abuse/dependence
  - Neurological: epilepsy
  - Haematological: thrombocytopenia, leukopenia, anaemia, haemoglobinopathy
  - Other: glaucoma, hyperthyroidism (Graves disease, thyrotoxicosis)
- **Pharmacological interactions**: the drug binds to the norepinephrine transporter (NET) and the dopamine transporter (DAT). These are target interactions, not drug-drug interaction severity ratings.

Please refer to the package insert for full warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score. There are no clinical trials or publications for faciodigitogenital syndrome, and no mechanistic link is supported. In addition, all 13 New Zealand licences are valid (13 of 13), and package insert safety data is unavailable.

**To proceed, the following is needed:**
- Mechanism of action data (MOA) from DrugBank
- Medsafe package insert warnings and contraindications
- Case-level or trial evidence in faciodigitogenital syndrome, or a defined behavioural target (for example ADHD-like symptoms in these patients)
- Review of the cardiovascular, psychiatric and seizure-related safety flags for the target population

**Other predictions to consider:** specific developmental disorder and dysthymic disorder (both L4, "Research Question") may be worth exploring as research questions. This would be mainly in patients with comorbid ADHD.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

