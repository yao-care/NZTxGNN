---
layout: default
title: Saccharin
parent: Model Prediction Only (L5)
nav_order: 416
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Saccharin: predicted new indication Obsolete bundle branch block (evidence level L5). Approved use: Not recorded"
monitor_inn: "saccharin"
predicted_indications: ["obsolete bundle branch block", "ventricular tachycardia"]
---

# Saccharin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Saccharin: From No Documented Original Indication to Obsolete Bundle Branch Block

## One-Sentence Summary

Saccharin is a non-nutritive artificial sweetener, used in the New Zealand products Ora-Sweet SF and Ora-Blend SF (oral suspension vehicles), and no approved therapeutic indication is recorded for it.
The TxGNN model predicts it may be effective for **obsolete bundle branch block**, but there are **0 clinical trials** and **0 publications** supporting this prediction.
This is a model-only prediction (a second prediction, ventricular tachycardia, is also unsupported by any evidence).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded (the licence records contain no approved indication text) |
| Predicted New Indication | Obsolete bundle branch block |
| TxGNN Prediction Score | 99.39% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Saccharin is a non-nutritive artificial sweetener with no known antiarrhythmic or cardiac conduction activity, and no pharmacological indication is documented for it.

No mechanistic link to bundle branch block can be established. The high TxGNN score is a computational output only and is not backed by any clinical trial or published study. In addition, the predicted disease term is flagged as obsolete in the disease ontology, which further reduces its clinical relevance as a repurposing target.

For completeness, the model's second-ranked prediction is **ventricular tachycardia** (score 99.26%). It likewise has no mechanistic link, no trials and no literature. Ventricular tachycardia is a life-threatening arrhythmia with established therapies, so a repurposing claim without clinical evidence would be inappropriate.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2377268 | Ora-Sweet SF | Suspension | Not recorded (cancelled) |
| 2377225 | Ora-Blend SF | Suspension | Not recorded (cancelled) |

## Safety Considerations

Please refer to the package insert for safety information. No warnings, contraindications or drug interaction data were available in the data compiled for this report, and the package insert has not yet been reviewed.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests only on a model score, with no clinical trials, no literature, no known mechanism and no recorded safety data. Saccharin's products are also cancelled in New Zealand, and the predicted disease term is obsolete.

**To proceed, the following is needed:**
- Retrieve and review the Medsafe package insert for warnings and contraindications (a blocking gap for safety screening)
- Obtain mechanism of action data (for example from DrugBank)
- Any preclinical or clinical evidence linking saccharin to cardiac conduction or arrhythmia
- Confirmation of a current, clinically valid disease target, since the predicted term is obsolete
- Assessment of route and dosage-form compatibility with the proposed use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

