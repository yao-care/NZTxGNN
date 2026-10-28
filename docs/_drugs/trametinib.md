---
layout: default
title: Trametinib
parent: Model Prediction Only (L5)
nav_order: 459
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
monitor_inn: "TRAMETINIB"
predicted_indications: ["choroideremia"]
description: "Trametinib: predicted new indication Choroideremia (evidence level L5). Approved use: Not recorded in the local data"
---

# Trametinib
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Trametinib: Choroideremia
{: .text-alpha }

## One-Sentence Summary

Trametinib is a MEK1/2 inhibitor originally used to treat BRAF-mutant metastatic melanoma.
The TxGNN model predicts it may be effective for **choroideremia**, a hereditary retinal degeneration.
This prediction currently has **0 clinical trials** and **0 publications** supporting it, so it rests on the model score alone.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Choroideremia |
| TxGNN Prediction Score | 0.9931 |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Trametinib targets mitogen-activated protein kinase kinase 1 and 2 (MEK1/MEK2, encoded by *MAP2K1* and *MAP2K2*). It is used against BRAF V600-mutant melanoma, where it is often combined with dabrafenib. The data compiled for this report did not include a full mechanism-of-action description, but the target information above is available.

The link between the original and predicted indications is weak. Choroideremia is an X-linked retinal degeneration caused by loss of the *CHM* gene (REP1 protein), which disrupts Rab protein prenylation. No established connection exists between this disease and MAPK/MEK pathway inhibition. The high TxGNN score (0.9931) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. There is no trial or literature support, so it is most likely a model artefact rather than a true therapeutic signal.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2483262 | Mekinist | Tab 2 mg | Valid |
| 2483254 | Mekinist | Tab 0.5 mg | Valid |


---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (MEK inhibitor, kinase inhibitor class) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions**: The interaction screen identified 15 moderate-level interactions. The interacting drugs are:
  - Fostamatinib
  - Elagolix
  - Apalutamide
  - Brigatinib
  - Dabrafenib
  - Encorafenib
  - Isavuconazonium
  - Lasmiditan
  - Midostaurin
  - Sarecycline
  - Enzalutamide
  - Erdafitinib
  - Gilteritinib
  - Idelalisib
  - Tucatinib

For other safety information, please refer to the package insert.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only support for choroideremia is a model score. There are no trials or publications, and the disease biology (defective Rab prenylation caused by *CHM* loss) has no recognised link to MEK inhibition. In addition, trametinib is marketed in New Zealand, with 2 of 2 authorizations valid.

Other predictions for trametinib have more support, though all of them stay within melanoma, close to the original indication. The most advanced is non-cutaneous melanoma, with a Phase 2 trial in BRAF non-V600-mutant melanoma (NCT02296112) and several dabrafenib/trametinib combination studies, for an evidence level of L2. Nodular malignant melanoma has observational literature (L3). Choroideremia was ranked first by TxGNN score only.

**To proceed, the following is needed:**
- Experimental or preclinical evidence that MEK/MAPK inhibition affects the choroideremia disease process (for example retinal pigment epithelium or photoreceptor models)
- Package insert warnings and contraindications for trametinib
- A review of ocular safety, since MEK inhibitors can cause retinal toxicity
- Confirmation of the regulatory pathway, given that the New Zealand authorizations are cancelled
- If prioritising by evidence, shift attention to the melanoma-related predictions above
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

