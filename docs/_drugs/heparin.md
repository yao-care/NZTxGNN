---
layout: default
title: Heparin
parent: Model Prediction Only (L5)
nav_order: 223
evidence_level: L5
indication_count: 2
last_modified_at: 2026-10-28
monitor_inn: "Heparin"
predicted_indications: ["thrombophilia due to protein C deficiency, autosomal recessive", "primary release disorder of platelets"]
description: "Heparin: predicted new indication Thrombophilia due to protein C deficiency, autosomal recessive (evidence level L5). Approved use: Not recorded in the local data"
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

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Heparin: Thrombophilia due to protein C deficiency, autosomal recessive
{: .text-alpha }

## One-Sentence Summary

Heparin is an injectable anticoagulant, originally used for pulmonary embolism, disseminated intravascular coagulation and purpura fulminans.
The TxGNN model predicts it may be useful for **thrombophilia due to protein C deficiency, autosomal recessive**,
but there are currently **0 clinical trials** and **0 publications** supporting this specific indication, so the prediction rests on the model alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Thrombophilia due to protein C deficiency, autosomal recessive |
| TxGNN Prediction Score | 0.9929 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 6 of 6) |
| Number of Authorizations | 6 (valid: 6) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on general knowledge, heparin enhances the activity of antithrombin (SERPINC1), which in turn inhibits thrombin and factor Xa. This suppresses clot formation.

Severe congenital protein C deficiency causes a hypercoagulable state. Its typical complication is purpura fulminans, which is already among heparin's original indications. This biological link makes the prediction plausible: an anticoagulant that works through antithrombin could help control clotting when the protein C pathway is missing.

However, no trials or literature specific to this indication were found. The high TxGNN score is a computational prediction only. Standard care relies on protein C concentrate plus anticoagulation, so heparin's likely role is as adjunct or bridging therapy, which would need to be confirmed against clinical guidelines.

*Note: a second predicted indication, primary release disorder of platelets (score 0.9906), was reviewed. It is not supported. Heparin is an anticoagulant that may worsen bleeding tendency and can cause heparin-induced thrombocytopenia. The retrieved literature was general and did not show heparin treating this disorder.*

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the leading predicted indication.

## New Zealand Market Information

All 6 licences are valid, and no approved indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2339897 | Pfizer | Inj 1,000 iu per ml, 5 ml ampoule | Valid |
| 333085 | Hospira | Inj 25,000 iu per ml, 0.2 ml | Valid |
| 2034255 | Hospira | Inj 5,000 iu per ml, 1 ml | Valid |
| 2717840 | Wockhardt | Inj 10 iu per ml, 5 ml ampoule (flushing solution) | Valid |
| 2651815 | Heparin Sodium Panpharma | Inj 5,000 iu per ml, 5 ml vial | Valid |

## Safety Considerations

Package insert warnings and contraindications were not available for this report; please refer to the package insert for safety information.

- **Contraindications suggested by the knowledge graph** (not label text): liver failure, thrombocytopenia, heparin-induced thrombocytopenia, haemophilia and other blood coagulation diseases, antithrombin III deficiency, Glanzmann thrombasthenia and qualitative platelet defects, intracranial or intracerebral haemorrhage, placental abruption, active peptic ulcer disease, ulcerative colitis and inflammatory bowel disease, infective endocarditis, diabetic or hypertensive retinopathy and other retinal disease, osteoporosis, biliary tract disease and obstructive jaundice.
- **Drug interactions**: the interaction query returned two pharmacological targets rather than drug-drug interactions: antithrombin (SERPINC1), the intended anticoagulant target, and the SARS-CoV-2 Spike glycoprotein, to which heparin binds.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for this indication is a model prediction (L5) with no trials or literature. Safety documentation is also missing.

**To proceed, the following is needed:**
- Clinical evidence for heparin in protein C deficiency (case series, guidelines, or trials), and its role relative to protein C concentrate
- Mechanism of action data (e.g., from DrugBank)
- Medsafe package insert warnings and contraindications
- Clarification of the regulatory pathway, since all 6 New Zealand licences are valid
- A bleeding-risk and monitoring plan for use in this population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

