---
layout: default
title: Pembrolizumab
parent: Model Prediction Only (L5)
nav_order: 355
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Pembrolizumab: predicted new indication Fibromatosis, gingival (evidence level L5). Approved use: Non-small cell lung carcinoma, lung cancer, metastatic melanoma"
---

# Pembrolizumab
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Pembrolizumab: From Non-Small Cell Lung Cancer and Metastatic Melanoma to Fibromatosis, Gingival

## One-Sentence Summary

Pembrolizumab is a PD-1 immune checkpoint inhibitor, originally used to treat non-small cell lung cancer and metastatic melanoma.
The TxGNN model predicts it may be effective for **gingival fibromatosis** (score 99.4%), but **no clinical trials and no publications** support this prediction.
The result is a computational signal only, and it should not be treated as evidence of benefit.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Non-small cell lung carcinoma, lung cancer, metastatic melanoma (from the knowledge graph; the New Zealand licence records contain no indication text) |
| Predicted New Indication | Fibromatosis, gingival |
| TxGNN Prediction Score | 99.40% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From the pharmacology data, pembrolizumab is an antibody that binds the PD-1 receptor (PDCD1, CD279) on T cells. Blocking PD-1 releases the "brake" on T cells so they can attack tumour cells. It is approved in many cancers, including melanoma, lung cancer, head and neck cancer, Hodgkin lymphoma and others.

For gingival fibromatosis, no mechanistic link was identified. Gingival fibromatosis is a benign overgrowth of gum tissue, usually hereditary or drug-induced. It has no established relationship to the PD-1/PD-L1 pathway. The high model score most likely reflects network proximity in the knowledge graph rather than a biological rationale. Without trial or literature support, this prediction cannot be considered reasonable on current evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Both New Zealand licences for pembrolizumab are cancelled. Neither record contains approved indication text.

| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2547996 | Keytruda | Inj 25 mg per ml, 4 ml vial | Cancelled |
| 2508427 | Baxter | Inj 1 mg for ECP | Cancelled |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Immunotherapy (anti-PD-1 monoclonal antibody); not a conventional cytotoxic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions. Immune-related adverse events (for example thyroid, liver, adrenal and pituitary function) are described in the literature. |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Drug Interactions**: 13 interaction records were found.
  - **Major**: lenalidomide, pomalidomide, thalidomide.
  - **Moderate**: systemic corticosteroids, namely hydrocortisone, triamcinolone, dexamethasone, betamethasone, budesonide, prednisolone, prednisone, methylprednisolone and deflazacort. Corticosteroids are suspected of reducing immunotherapy efficacy (a cohort study of first-line pembrolizumab in NSCLC examined this).
  - The remaining record is the PD-1 (CD279) target, which is pharmacology information rather than an interaction.

Please refer to the package insert for warnings and contraindications. The package insert warnings are currently missing, which blocks formal safety screening.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score. There are no trials, no publications and no mechanistic rationale for gingival fibromatosis. Pembrolizumab also has no valid New Zealand licence at present, and immune-related toxicity would be hard to justify in a benign condition.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (currently a blocking gap)
- Mechanism of action data from DrugBank
- Any biological evidence linking PD-1 signalling to gingival fibromatosis
- Clarification of regulatory status, since both New Zealand licences are cancelled

---

### Note on other predictions

None of the other top-ranked predictions had direct clinical support. Most are rare genetic or benign conditions with no PD-1 rationale (inclusion body myopathy with Paget disease, ovarioleukodystrophy, lung fibroma, lung hamartoma, and a rare leukomelanoderma syndrome). Two are subsets of the approved lung cancer indication (lung hilum carcinoma and pulmonary sulcus neoplasm), which would be an extension of an existing indication rather than true repurposing. The large body of lung cancer trials and literature matched to "lung benign neoplasm" and "lung germ cell tumor" concerns malignant NSCLC and SCLC and general checkpoint-inhibitor research, so it should not be credited to those indications.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

