---
layout: default
title: Temozolomide
parent: Model Prediction Only (L5)
nav_order: 440
evidence_level: L5
indication_count: 1
monitor_inn: "TEMOZOLOMIDE"
predicted_indications: ["cauda equina neoplasm"]
last_modified_at: 2026-10-23
description: "Temozolomide: predicted new indication Cauda equina neoplasm (evidence level L5). Approved use: Not recorded in the local data"
---

# Temozolomide
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

## Temozolomide: Cauda equina neoplasm
{: .text-alpha }

## One-Sentence Summary

Temozolomide is an oral alkylating chemotherapy drug, with its original uses recorded in the knowledge graph as anaplastic astrocytoma, astrocytoma (excluding glioblastoma) and brain glioblastoma.
The TxGNN model predicts it may be effective for **cauda equina neoplasm**, but there are **0 registered clinical trials** and only **3 publications** (all case reports, only one of which relates directly to a tumour of this region). The evidence is therefore very limited.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Cauda equina neoplasm |
| TxGNN Prediction Score | 0.9930 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 8 of 8) |
| Number of Authorizations | 8 (valid: 8) |
| Number of Approvals | 8 licences (all single-ingredient products) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. From general pharmacological knowledge, temozolomide is an oral alkylating agent that methylates DNA (at O6-guanine, N7-guanine and N3-adenine), producing cytotoxic lesions in dividing tumour cells. It crosses the blood-brain barrier. This explanation comes from general knowledge, not from the supplied record.

The original uses recorded in the knowledge graph are tumours of the central nervous system (astrocytoma and glioblastoma). Tumours of the cauda equina are also neuroectodermal tumours of the nervous system and are mostly myxopapillary ependymomas. This gives a plausible, though indirect, rationale for a similar drug response. Activity is likely influenced by MGMT promoter methylation status, which is not reported in the supplied data.

The very high TxGNN score is a computational prediction only and does not replace clinical evidence.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [29270703](https://pubmed.ncbi.nlm.nih.gov/29270703/) | 2018 | Case report | Eur Spine J | Remarkable response to temozolomide in a relapsed spinal myxopapillary ependymoma with multiple recurrences and cerebrospinal dissemination (single case, with literature review) |
| [42760727](https://pubmed.ncbi.nlm.nih.gov/42760727/) | 2026 | Case report | Medicine | Nasal NUT carcinoma with repeated responses to multimodal treatment including radiotherapy (not a cauda equina tumour) |
| [18706663](https://pubmed.ncbi.nlm.nih.gov/18706663/) | 2009 | Case report | Neuro-Chirurgie | Atypical neuromeningeal cryptococcosis in a child treated for pontine glioma (infection case, not evidence for the predicted indication) |

Only the first report is closely related to the predicted disease. It involves a spinal myxopapillary ependymoma, a tumour type related to, but not the same as, the predicted disease. No cited study directly addresses cauda equina neoplasm itself.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2689413 | Temozolomide-Taro | Cap 5 mg | Not stated in the available records |
| 2572311 | Apo-Temozolomide | Cap 20 mg | Not stated in the available records |
| 2388243 | Temaccord | Cap 100 mg | Not stated in the available records |
| 2566044 | Temaccord | Cap 140 mg | Not stated in the available records |
| 2388278 | Temaccord | Cap 5 mg | Not stated in the available records |

There are 8 valid single-ingredient licences in total; the table shows the first 5.

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (alkylating agent) |
| Myelosuppression Risk | Medium to high. The knowledge graph records contraindication relations with neutropenia, thrombocytopenia, pancytopenia, aplastic anaemia and lymphopenia |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Handling Protection | Follow cytotoxic drug handling regulations |

## Safety Considerations

- **Relations recorded in the knowledge graph (not label text)**: liver failure, thrombocytopenia, neutropenia, agranulocytosis, anaemia, macrocytic anaemia, pneumocystosis, pancytopenia, lymphopenia, myelodysplastic syndrome, idiopathic aplastic anaemia, aplastic anaemia. These are not confirmed by the New Zealand package insert.
- **Drug Interactions**: The interaction query returned 100 records; the first 20 are shown in the data compiled for this report. The only graded ones are **Naltrexone** (Moderate) and **Levofloxacin** (Minor). Others listed with an unknown severity level include pantoprazole, omeprazole, lansoprazole, morphine, metformin, prednisone, hydrocortisone and amphotericin B.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trials are registered, and the only directly relevant publication is a single case report of a related spinal tumour (myxopapillary ependymoma). No cited study directly addresses cauda equina neoplasm, so the evidence level is L5. The high TxGNN score and the plausible mechanism justify further research but not clinical adoption.

**To proceed, the following is needed:**
- Clinical studies or case series in cauda equina tumours (especially myxopapillary ependymoma), including MGMT status
- Detailed mechanism of action data
- Medsafe package insert warnings and contraindications, to complete safety screening
- A myelosuppression monitoring plan and review of the interacting drugs
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

