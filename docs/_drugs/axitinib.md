---
layout: default
title: Axitinib
parent: Model Prediction Only (L5)
nav_order: 53
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Axitinib: predicted new indication Unclassified renal cell carcinoma (evidence level L5). Approved use: Renal cell carcinoma (clear cell and non-papillary renal cell carcino…"
monitor_inn: "AXITINIB"
predicted_indications: ["liposarcoma", "ovarian myxoid liposarcoma", "angiolipoma", "familial spontaneous pneumothorax"]
---

# Axitinib
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

## Axitinib: From Renal Cell Carcinoma to Unclassified Renal Cell Carcinoma
{: .text-alpha }

## One-Sentence Summary

Axitinib is a VEGFR-1/2/3 inhibitor originally used to treat renal cell carcinoma (clear cell and related subtypes).
The TxGNN model predicts it may be effective for **unclassified renal cell carcinoma**, but **no clinical trials or publications** were retrieved for this exact subtype, so the prediction rests on the model alone.
Related renal subtypes in the same analysis have far stronger support (for example, renal carcinoma with **40 registered trials** and **20 publications**), but that is mostly confirmation of the existing indication rather than new repurposing.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Renal cell carcinoma (clear cell and non-papillary renal cell carcinoma) |
| Predicted New Indication | Unclassified renal cell carcinoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, axitinib is a selective inhibitor of VEGF receptors 1, 2 and 3. It blocks angiogenesis, which drives tumour growth in renal cell carcinoma, where loss of the VHL gene leads to HIF-driven VEGF overexpression.

Unclassified renal cell carcinoma is a histologic subtype of renal cell carcinoma, so an anti-angiogenic mechanism is plausible. The efficacy of axitinib in clear cell disease is well established. However, no trials or literature were found for this specific subtype, and the high score reflects model prediction only.

### Other Predicted Indications Worth Noting

The model produced several other predictions. They are summarised here because they inform the overall decision.

| Predicted Indication | Evidence Level | Evidence Summary | Suggested Stance |
|------|------|------|------|
| Renal carcinoma (the original indication) | L1 | Multiple Phase 3 RCTs (AXIS, KEYNOTE-426, JAVELIN Renal 101, RENOTORCH) | Proceed with Guardrails; confirms existing use |
| Collecting duct carcinoma | L3 | One recruiting Phase 2 trial (n=30); mostly indirect clear cell evidence | Research question |
| Xp11.2/TFE3 translocation RCC | L2 | One Phase 2 trial (n=15, active, no results) | Research question |
| Liposarcoma | L4 | Preclinical activity in myxoid liposarcoma only | Research question |
| Childhood kidney cell carcinoma | L4 | A review and a single multi-drug case report | Hold |
| Renal cell carcinoma associated with neuroblastoma, ovarian myxoid liposarcoma, angiolipoma | L5 | Prediction only | Hold |
| Familial spontaneous pneumothorax | L5 | Likely a false positive (linked to renal tumour risk through FLCN, not to a treatment effect) | Hold |

## Clinical Trial Evidence

No clinical trials are registered for unclassified renal cell carcinoma. The most relevant trials for the closest, evidence-bearing indications (renal carcinoma overall and rare subtypes) are below.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00678392](https://clinicaltrials.gov/study/NCT00678392) | Phase 3 | Completed | 723 | AXIS: axitinib vs sorafenib as second-line therapy in metastatic RCC; the pivotal registration trial |
| [NCT02853331](https://clinicaltrials.gov/study/NCT02853331) | Phase 3 | Completed | 861 | KEYNOTE-426: pembrolizumab + axitinib vs sunitinib, first line |
| [NCT04394975](https://clinicaltrials.gov/study/NCT04394975) | Phase 3 | Completed | 421 | Toripalimab + axitinib vs sunitinib, first line |
| [NCT01693822](https://clinicaltrials.gov/study/NCT01693822) | Phase 2 | Completed | 65 | A-PREDICT: axitinib in metastatic RCC unsuitable for nephrectomy |
| [NCT00835978](https://clinicaltrials.gov/study/NCT00835978) | Phase 2 | Completed | 213 | Axitinib with or without dose titration in metastatic RCC |
| [NCT03595124](https://clinicaltrials.gov/study/NCT03595124) | Phase 2 | Active, not recruiting | 15 | Axitinib + nivolumab vs nivolumab in TFE/translocation RCC (non-clear-cell) |
| [NCT06211114](https://clinicaltrials.gov/study/NCT06211114) | Phase 2 | Recruiting | 30 | Immune checkpoint inhibitors + axitinib in previously treated collecting duct carcinoma |
| [NCT05969496](https://clinicaltrials.gov/study/NCT05969496) | Phase 2 | Recruiting | 17 | Neoadjuvant pembrolizumab + axitinib in RCC with IVC tumour thrombus |
| [NCT03494816](https://clinicaltrials.gov/study/NCT03494816) | Phase 2 | Completed | 24 | NAXIVA: neoadjuvant axitinib for venous tumour thrombus in clear cell RCC |
| [NCT05176288](https://clinicaltrials.gov/study/NCT05176288) | Phase 2 | Withdrawn | 0 | Avelumab + palbociclib + axitinib; no participants enrolled, no evidence |

## Literature Evidence

No publications were retrieved for unclassified renal cell carcinoma. The key publications for related indications are below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [30779529](https://pubmed.ncbi.nlm.nih.gov/30779529/) | 2019 | RCT | N Engl J Med | Pembrolizumab + axitinib vs sunitinib in advanced RCC |
| [30779531](https://pubmed.ncbi.nlm.nih.gov/30779531/) | 2019 | RCT | N Engl J Med | Avelumab + axitinib vs sunitinib in advanced RCC (JAVELIN Renal 101) |
| [40750932](https://pubmed.ncbi.nlm.nih.gov/40750932/) | 2025 | RCT (5-year follow-up) | Nat Med | KEYNOTE-426 5-year survival and biomarker analyses |
| [39706335](https://pubmed.ncbi.nlm.nih.gov/39706335/) | 2025 | RCT (final analysis) | Ann Oncol | Final analysis of JAVELIN Renal 101 including overall survival |
| [37872020](https://pubmed.ncbi.nlm.nih.gov/37872020/) | 2024 | RCT | Ann Oncol | RENOTORCH: toripalimab + axitinib vs sunitinib in intermediate/poor-risk advanced RCC |
| [39362847](https://pubmed.ncbi.nlm.nih.gov/39362847/) | 2024 | Single-arm Phase 2 | Signal Transduct Target Ther | NEOTAX: neoadjuvant toripalimab + axitinib in ccRCC with IVC tumour thrombus |
| [26487541](https://pubmed.ncbi.nlm.nih.gov/26487541/) | 2015 | Review | Drugs | Axitinib review in advanced RCC, summarising the AXIS trial |
| [35428926](https://pubmed.ncbi.nlm.nih.gov/35428926/) | 2022 | Review | Urologie | Collecting duct carcinoma: nivolumab + axitinib and radiotherapy appear to prolong survival |
| [27822137](https://pubmed.ncbi.nlm.nih.gov/27822137/) | 2016 | Preclinical | Sarcoma | Axitinib has antiangiogenic and antitumour activity in myxoid liposarcoma models |
| [26279736](https://pubmed.ncbi.nlm.nih.gov/26279736/) | 2015 | Case report | Can Urol Assoc J | Malignant epithelioid angiomyolipoma in a 12-year-old treated with sunitinib, everolimus and axitinib |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2432706 | Inlyta | Tab 5 mg | Not available (cancelled) |
| 2432692 | Inlyta | Tab 1 mg | Not available (cancelled) |

Both authorizations are cancelled, so no valid licence is currently listed in New Zealand.

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (VEGFR tyrosine kinase inhibitor), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert; a baseline and periodic review of blood counts, liver and renal function is generally advisable |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified. The most clinically important ones in the listed data are:
  - **Major**: clarithromycin, dexamethasone
  - **Moderate**: aprepitant, clotrimazole, miconazole, naltrexone
  - **Minor**: acid-reducing agents and antacids, including famotidine, ranitidine, cimetidine, nizatidine, rabeprazole, esomeprazole, lansoprazole, dexlansoprazole, aluminium hydroxide, calcium carbonate, magaldrate and magnesium salts

Package insert warnings and contraindications were not available in the data compiled for this report. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
For unclassified renal cell carcinoma, the evidence is a model prediction only (L5), with no trials or publications. Axitinib's established efficacy in clear cell RCC supports a plausible mechanism but does not confirm benefit in this subtype. In addition, both New Zealand licences are cancelled and no package insert safety data were available, so a safety screen cannot yet be completed.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Clinical or real-world data in unclassified RCC specifically
- Clarification of why the New Zealand licences were cancelled, and of current availability
- If a rare-subtype direction is desired, collecting duct carcinoma (NCT06211114) and TFE3-translocation RCC (NCT03595124) are the most evidence-supported candidates to follow as research questions
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

