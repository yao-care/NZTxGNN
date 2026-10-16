---
layout: default
title: Siltuximab
parent: Model Prediction Only (L5)
nav_order: 423
evidence_level: L5
indication_count: 8
last_modified_at: 2026-10-15
description: "Siltuximab: predicted new indication Extracutaneous mastocytoma (evidence level L5). Approved use: Castleman disease"
monitor_inn: "SILTUXIMAB"
predicted_indications: ["extracutaneous mastocytoma", "hepatic infarction", "hepatic veno-occlusive disease", "peliosis hepatis", "Kaposi's sarcoma (disease)", "autosomal recessive familial Mediterranean fever", "aggressive systemic mastocytosis", "autosomal dominant familial periodic fever"]
---

# Siltuximab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **8** 
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

# Siltuximab: From Castleman Disease to Extracutaneous Mastocytoma

## One-Sentence Summary

Siltuximab is an anti-IL-6 monoclonal antibody, originally used for Castleman disease (and listed with Kimura disease in the knowledge graph).
The TxGNN model predicts it may be effective for **extracutaneous mastocytoma**,
but there are currently **0 clinical trials** and **0 publications** supporting this prediction, so it rests on model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Castleman disease (the New Zealand licence records contain no approved indication text; this is taken from the knowledge graph) |
| Predicted New Indication | Extracutaneous mastocytoma |
| TxGNN Prediction Score | 99.64% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Siltuximab is a chimeric monoclonal antibody that blocks interleukin-6 (IL-6). Detailed mechanism of action data is not available in the data compiled for this report. However, its class is well known, and its efficacy in Castleman disease, a lymphoproliferative disorder driven by IL-6, is the basis of its original use.

The link between Castleman disease and extracutaneous mastocytoma is weak. IL-6 may play a role in mast cell biology, and IL-6 is elevated in mastocytosis, but mast cell neoplasms are driven mainly by other pathways (such as KIT mutations). No direct mechanistic link is supported by the available data, so this connection is speculative.

The high TxGNN score (99.64%) is a computational prediction only. It is not backed by clinical or literature evidence and should be treated as a hypothesis for investigation.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2500655 | Sylvant | Inj 400 mg vial | Not recorded (cancelled) |
| 2500663 | Sylvant | Inj 100 mg vial | Not recorded (cancelled) |

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified in the interaction screening; the first 20 returned include one **Major** interaction and many Moderate ones.
  - **Major**: Deferiprone
  - **Moderate**: Simvastatin, apixaban, warfarin, rivaroxaban, dicoumarol, lidocaine, hydrocodone, theophylline, roflumilast, aminophylline, oxtriphylline, tacrolimus, fentanyl, nifedipine, tetanus toxoid vaccine
  - **Minor**: Zinc salts (sulfate, acetate, gluconate, chloride)
  - A plausible explanation is that IL-6 blockade can normalise suppressed CYP450 enzyme activity, altering exposure to drugs with a narrow therapeutic index (for example warfarin, theophylline, tacrolimus). This is not stated in the data and should be confirmed against the product information.

Please refer to the package insert for warnings and contraindications, as this information was not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by the model score, with no clinical trials or publications and a weak mechanistic link. Siltuximab also has no currently valid New Zealand licence.

**To proceed, the following is needed:**
- Mechanistic evidence for IL-6 involvement in mast cell neoplasms, and any preclinical or case-level data
- Package insert warnings and contraindications from Medsafe
- Detailed mechanism of action data
- Clarification of availability, since both New Zealand licences are cancelled
- Review of the other predicted indications. Kaposi's sarcoma has indirect support (shared HHV-8 biology with HHV-8-associated Castleman disease, L4, "Research Question"), and may be a better candidate to prioritise than the top-ranked prediction.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

