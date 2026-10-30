---
layout: default
title: Minoxidil
parent: Moderate Evidence (L3-L4)
nav_order: 303
evidence_level: L3
indication_count: 10
last_modified_at: 2026-10-30
description: "Minoxidil: predicted new indication Hypotrichosis simplex of the scalp (evidence level L3). Approved use: Hypertension and alopecia"
monitor_inn: "MINOXIDIL"
predicted_indications: ["hypotrichosis simplex of the scalp", "congenital hypotrichosis milia", "diffuse alopecia areata", "pseudopelade of Brocq", "pulmonary arterial hypertension", "pulmonary arteriovenous malformation (disease)", "pulmonary arterial hypertension associated with congenital heart disease", "primary hereditary glaucoma", "pulmonary arterial hypertension associated with chronic hemolytic anemia", "pulmonary arterial hypertension associated with schistosomiasis"]
---

# Minoxidil
{: .fs-9 }

Evidence Level: **L3** | Predicted Indications: **10** 
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

## Minoxidil: From Hypertension and Alopecia to Hypotrichosis Simplex of the Scalp
{: .text-alpha }

## One-Sentence Summary

Minoxidil is a vasodilator originally used for severe hypertension and later established as a hair-growth agent for alopecia.
The TxGNN model predicts it may be effective for **hypotrichosis simplex of the scalp**,
but there are **no registered clinical trials** and only **3 publications** (case reports and small case series, all using combination regimens) supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension and alopecia (the New Zealand licence records contain no indication text) |
| Predicted New Indication | Hypotrichosis simplex of the scalp |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L3 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on the published literature, minoxidil is a prodrug whose active metabolite, minoxidil sulfate, opens potassium channels. This relaxes vascular smooth muscle and, in the scalp, is thought to prolong the anagen (growth) phase of the hair cycle and improve follicular microcirculation.

Hypotrichosis simplex of the scalp is a rare hereditary hair-thinning disorder (linked to variants in the CDSN gene) that looks clinically similar to androgenetic alopecia, a condition in which minoxidil is well established. The shared hair-loss phenotype makes the prediction mechanistically plausible.

An important caveat: all three publications on this condition combine minoxidil with other treatments (growth factors, botanic extracts, or platelet-rich plasma). The effect of minoxidil alone therefore cannot be isolated.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35761391](https://pubmed.ncbi.nlm.nih.gov/35761391/) | 2022 | Case report/series | Dermatologic Therapy | Hereditary hypotrichosis simplex of the scalp treated with oral minoxidil plus growth factors |
| [36651821](https://pubmed.ncbi.nlm.nih.gov/36651821/) | 2023 | Case report | Journal of Dermatological Treatment | A 14-year-old patient treated with platelet-rich plasma injections plus topical 2% minoxidil |
| [39902296](https://pubmed.ncbi.nlm.nih.gov/39902296/) | 2024 | Case report | Frontiers in Genetics | An 8-year-old boy with a CDSN mutation treated with botanic extracts plus minoxidil |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2662981 | Minoxidil Roma | Tablet 10 mg | Not recorded (licence valid) |
| 300438 | Loniten | Tablet 10 mg | Not recorded (licence valid) |

## Safety Considerations

- **Contraindication signals (from a knowledge graph, not label text)**: Disease associations linked to minoxidil include cardiovascular conditions (myocardial infarction, heart disease, pericarditis, pericardial effusion, cardiac tamponade), cerebrovascular conditions (stroke, cerebral infarction), orthostatic hypotension, kidney disease, phaeochromocytoma and related tumours, and scalp dermatosis.
- **Drug Interactions**: 100 interactions were identified. The main ones rated Moderate include corticosteroids (hydrocortisone, prednisone, prednisolone, dexamethasone, betamethasone, budesonide, triamcinolone), SGLT2 inhibitors (canagliflozin, dapagliflozin, empagliflozin, ertugliflozin), opioids (morphine, opium), cannabinoids (dronabinol, nabilone), bupropion and sapropterin.

For full warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The model score is very high and the mechanism is plausible, with three recent case-level reports in this disease. However, all of them use combination therapy, there are no registered trials, and although minoxidil is marketed in New Zealand with 2 valid licences of 2, this does not establish use for this disease. The evidence supports further investigation, not routine use.

**To proceed, the following is needed:**
- Mechanism of action data for minoxidil from DrugBank
- The Medsafe package insert, to confirm warnings and contraindications (a blocking gap for safety screening)
- Evidence isolating the effect of minoxidil alone, such as a controlled or monotherapy study in hereditary hypotrichosis simplex
- A cardiovascular safety plan, particularly for oral use (fluid retention, tachycardia, pericardial effusion), and a review of the product's licence status and available routes of administration in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

