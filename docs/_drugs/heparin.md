---
layout: default
title: Heparin
parent: Model Prediction Only (L5)
nav_order: 223
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-15
description: "Heparin: predicted new indication Thrombophilia due to protein C deficiency, autosomal recessive (evidence level L5). Approved use: Pulmonary embolism, disseminated intravascular coagulation…"
---

# Heparin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

# Heparin: From Pulmonary Embolism to Thrombophilia due to Protein C Deficiency (Autosomal Recessive)

## One-Sentence Summary

Heparin is an injectable anticoagulant, originally used for pulmonary embolism, disseminated intravascular coagulation and purpura fulminans.
The TxGNN model predicts it may be useful for **thrombophilia due to protein C deficiency, autosomal recessive**,
but there are currently **0 clinical trials** and **0 publications** supporting this specific indication, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Pulmonary embolism, disseminated intravascular coagulation, purpura fulminans (from the TxGNN knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Thrombophilia due to protein C deficiency, autosomal recessive |
| TxGNN Prediction Score | 99.29% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 6 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general knowledge, heparin enhances the activity of antithrombin (SERPINC1), which in turn inhibits thrombin and factor Xa. This suppresses clot formation.

Severe congenital protein C deficiency causes a hypercoagulable state. Its typical complication is purpura fulminans, which is already among heparin's original indications. This biological link makes the prediction plausible: an anticoagulant that works through antithrombin could help control clotting when the protein C pathway is missing.

However, no trials or literature specific to this indication were found. The high TxGNN score is a computational prediction only. Standard care relies on protein C concentrate plus anticoagulation, so heparin's likely role is as adjunct or bridging therapy, which would need to be confirmed against clinical guidelines.

*Note: a second predicted indication, primary release disorder of platelets (score 99.06%), was reviewed. It is not supported. Heparin is an anticoagulant that may worsen bleeding tendency and can cause heparin-induced thrombocytopenia. The retrieved literature was general and did not show heparin treating this disorder.*

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the leading predicted indication.

## New Zealand Market Information

All six licences are cancelled, and no approved indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2339897 | Pfizer | Inj 1,000 iu per ml, 5 ml ampoule | Cancelled |
| 333085 | Hospira | Inj 25,000 iu per ml, 0.2 ml | Cancelled |
| 2034255 | Hospira | Inj 5,000 iu per ml, 1 ml | Cancelled |
| 2717840 | Wockhardt | Inj 10 iu per ml, 5 ml ampoule (flushing solution) | Cancelled |
| 2651815 | Heparin Sodium Panpharma | Inj 5,000 iu per ml, 5 ml vial | Cancelled |

## Safety Considerations

Package insert warnings and contraindications were not available for this report; please refer to the package insert for safety information.

- **Contraindications suggested by the knowledge graph** (not label text): liver failure, thrombocytopenia, heparin-induced thrombocytopenia, haemophilia and other blood coagulation diseases, antithrombin III deficiency, Glanzmann thrombasthenia and qualitative platelet defects, intracranial or intracerebral haemorrhage, placental abruption, active peptic ulcer disease, ulcerative colitis and inflammatory bowel disease, infective endocarditis, diabetic or hypertensive retinopathy and other retinal disease, osteoporosis, biliary tract disease and obstructive jaundice.
- **Drug interactions**: the interaction query returned two pharmacological targets rather than drug-drug interactions: antithrombin (SERPINC1), the intended anticoagulant target, and the SARS-CoV-2 Spike glycoprotein, to which heparin binds.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for this indication is a model prediction (L5) with no trials or literature. Heparin has no currently valid New Zealand licence. Safety documentation is also missing.

**To proceed, the following is needed:**
- Clinical evidence for heparin in protein C deficiency (case series, guidelines, or trials), and its role relative to protein C concentrate
- Mechanism of action data (e.g., from DrugBank)
- Medsafe package insert warnings and contraindications
- Clarification of the regulatory pathway, since all New Zealand licences are cancelled
- A bleeding-risk and monitoring plan for use in this population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

