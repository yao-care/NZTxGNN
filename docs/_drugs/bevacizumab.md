---
layout: default
title: Bevacizumab
parent: Model Prediction Only (L5)
nav_order: 64
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Bevacizumab: predicted new indication Epiglottis neoplasm (evidence level L5). Approved use: Peritoneal cancer, ovarian malignant epithelial tumour, breast cancer…"
monitor_inn: "BEVACIZUMAB"
predicted_indications: ["epiglottis neoplasm", "benign neoplasm of tongue", "tumor of testis and paratestis", "benign neoplasm of hypopharynx", "benign neoplasm of floor of mouth", "cervical neuroblastoma", "cystic neoplasm", "nasal cavity inverting papilloma", "mesenchymoma", "schwannoma of jugular foramen"]
---

# Bevacizumab
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

## Bevacizumab: From Ovarian, Breast and Other Cancers to Epiglottis Neoplasm
{: .text-alpha }

## One-Sentence Summary

Bevacizumab is a VEGF-targeting antibody, originally used for cancers such as peritoneal and ovarian cancer, breast cancer, metastatic colon cancer and glioblastoma.
The TxGNN model predicts it may be effective for **epiglottis neoplasm**, but **no clinical trials and no publications** were found for this specific indication, so the prediction rests on the model score alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Peritoneal cancer, ovarian malignant epithelial tumour, breast cancer, metastatic colon cancer, glioblastoma, among others (source: knowledge graph indication records; no approved-indication text is available from the NZ licences) |
| Predicted New Indication | Epiglottis neoplasm |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the data compiled for this report. Based on general knowledge, bevacizumab is a monoclonal antibody that binds VEGF-A and inhibits tumour angiogenesis. Its use is established in several solid tumours, including ovarian, peritoneal, breast, colon and brain cancers.

Epiglottis neoplasms are head and neck tumours. VEGF-driven angiogenesis plays a role in many such tumours, so an anti-VEGF agent is mechanistically plausible. However, no study specific to epiglottis neoplasm was found, and the similarity between the original and predicted indications has not been assessed. This is a model-based hypothesis only.

Other predictions in the list (for example benign tongue and floor-of-mouth neoplasms, cervical neuroblastoma, nasal inverting papilloma) also have mostly indirect, head-and-neck-related evidence. The cystic neoplasm prediction mainly overlaps with ovarian cancer, where bevacizumab is already established, so it is not a true new repurposing.

## Clinical Trial Evidence

Currently no related clinical trials registered for epiglottis neoplasm.

## Literature Evidence

Currently no related literature available for epiglottis neoplasm.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2700719 | Vegzelma | Inj 25 mg per ml, 16 ml vial | Not available (licence cancelled) |
| 2700468 | Baxter | Inj 1 mg for ECP | Not available (licence cancelled) |
| 2700700 | Vegzelma | Inj 25 mg per ml, 4 ml vial | Not available (licence cancelled) |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (anti-VEGF monoclonal antibody), not a conventional cytotoxic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: 22 interactions were recorded in the interaction database searched. **Major** interactions: deferiprone, samarium (153Sm) lexidronam, panitumumab, sunitinib, thalidomide. **Moderate** interactions include bisphosphonates (alendronic, risedronic, etidronic, pamidronic and tiludronic acids, ibandronate), filgrastim, pegfilgrastim, sargramostim, palifermin, idelalisib, romosozumab, radium-223, strontium-89 and phosphorus-32 sodium phosphate.

For warnings and contraindications, please refer to the package insert.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score for epiglottis neoplasm is very high (99.90%), but there are no trials or publications for this indication, so the evidence level is L5 (model prediction only). In addition, bevacizumab currently has no valid New Zealand licence.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe), which are required before safety screening can proceed
- Mechanism of action data (for example from DrugBank)
- Indication-specific evidence for epiglottis or laryngeal neoplasms (literature search, trial registries)
- Assessment of the similarity between the original and predicted indications
- Clarification of the current regulatory and supply status in New Zealand, given that all three licences are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

