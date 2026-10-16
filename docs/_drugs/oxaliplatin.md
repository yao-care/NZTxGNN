---
layout: default
title: Oxaliplatin
parent: Model Prediction Only (L5)
nav_order: 342
evidence_level: L5
indication_count: 4
last_modified_at: 2026-10-15
description: "Oxaliplatin: predicted new indication Malignant pleural mesothelioma (evidence level L5). Approved use: Metastasis from malignant tumour of colon"
monitor_inn: "OXALIPLATIN"
predicted_indications: ["malignant pleural mesothelioma", "malignant epithelioid mesothelioma", "sarcomatoid mesothelioma", "malignant visceral pleura tumor"]
---

# Oxaliplatin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **4** 
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

# Oxaliplatin: From Colon Cancer Metastasis to Malignant Pleural Mesothelioma

## One-Sentence Summary

Oxaliplatin is a platinum-based chemotherapy drug, originally used for metastatic colon cancer.
The TxGNN model predicts it may be effective for **malignant pleural mesothelioma**,
with **3 clinical trials** (2 directly relevant Phase 2 studies) and **20 publications** currently supporting this direction. The evidence consists mainly of small, single-arm Phase 2 studies, so it remains exploratory.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Metastasis from malignant tumour of colon (from the knowledge graph; no approved indication text is available from Medsafe) |
| Predicted New Indication | Malignant pleural mesothelioma |
| TxGNN Prediction Score | 99.68% |
| Evidence Level | L2 (one completed Phase 2 trial plus several published Phase 2 studies; no Phase 3 RCT) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 5 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, oxaliplatin is a third-generation platinum agent that forms DNA adducts (intra- and inter-strand crosslinks), blocking DNA replication and transcription. Its efficacy in metastatic colorectal cancer is established.

Platinum agents such as cisplatin and carboplatin are already backbone therapy in pleural mesothelioma, so a shared cytotoxic mechanism is plausible. Oxaliplatin's distinct DACH-ligand adducts may retain activity in some settings where other platinum agents are less effective. The very high TxGNN score is consistent with this mechanistic proximity, but it is a model prediction and not clinical proof.

Results are mixed. Several Phase 2 studies report activity for oxaliplatin combinations (with gemcitabine, raltitrexed or vinorelbine), while one small second-line study of raltitrexed plus oxaliplatin (14 patients) produced no objective responses.

Other mesothelioma-related predictions have weaker support:
- **Malignant epithelioid mesothelioma** (score 99.49%): no registered trials. The literature is indirect (peritoneal disease and HIPEC/PIPAC). Evidence level L3.
- **Sarcomatoid mesothelioma** (score 99.44%): no registered trials. This is the least chemosensitive subtype, and the only oxaliplatin-specific report is a surgery-plus-HIPEC series in peritoneal disease. Evidence level L4, Hold.
- **Malignant visceral pleura tumour** (score 99.26%): no registered trials. Most retrieved literature is not relevant to oxaliplatin efficacy. Evidence level L4, Hold.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00859469](https://clinicaltrials.gov/study/NCT00859469) | Phase 2 | Completed | 29 | Oxaliplatin plus gemcitabine as first- or second-line chemotherapy in pleural or peritoneal mesothelioma; primary question is response rate. Single-arm. |
| [NCT00996385](https://clinicaltrials.gov/study/NCT00996385) | Phase 2 | Unknown | 29 | Bortezomib plus oxaliplatin in previously treated pleural or peritoneal mesothelioma; two-stage, open-label. No confirmed results. |
| [NCT07532902](https://clinicaltrials.gov/study/NCT07532902) | Phase 1 | Recruiting | 60 | Basket trial of BMS-986504 with standard-of-care therapy in MTAP-deleted solid tumours; oxaliplatin may only be background therapy. Low relevance. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [14609447](https://pubmed.ncbi.nlm.nih.gov/14609447/) | 2003 | Phase II trial | Clin Lung Cancer | Gemcitabine plus oxaliplatin in 25 patients with malignant pleural mesothelioma |
| [12525529](https://pubmed.ncbi.nlm.nih.gov/12525529/) | 2003 | Phase II trial | J Clin Oncol | Raltitrexed plus oxaliplatin in 70 patients (15 pretreated, 55 chemotherapy-naive); reported as an active regimen |
| [15639727](https://pubmed.ncbi.nlm.nih.gov/15639727/) | 2005 | Phase II trial | Lung Cancer | Vinorelbine plus oxaliplatin as first-line therapy in untreated mesothelioma |
| [11989592](https://pubmed.ncbi.nlm.nih.gov/11989592/) | 2001 | Phase II pilot | Tumori | Oxaliplatin plus raltitrexed in inoperable pleural mesothelioma |
| [19091133](https://pubmed.ncbi.nlm.nih.gov/19091133/) | 2008 | Observational | J Occup Med Toxicol | Gemcitabine plus oxaliplatin in patients pretreated with pemetrexed |
| [15893013](https://pubmed.ncbi.nlm.nih.gov/15893013/) | 2005 | Phase II trial | Lung Cancer | Raltitrexed plus oxaliplatin as second-line therapy: 14 patients, no objective responses, trial closed |
| [15261443](https://pubmed.ncbi.nlm.nih.gov/15261443/) | 2004 | Review | Lung Cancer | Review of chemotherapy for pleural mesothelioma; response rates above 30% rarely achieved with conventional drugs |
| [12610498](https://pubmed.ncbi.nlm.nih.gov/12610498/) | 2003 | Review | Br J Cancer | Summary of Phase II-III chemotherapy studies in pleural mesothelioma |
| [25680983](https://pubmed.ncbi.nlm.nih.gov/25680983/) | 2015 | Cohort | Surg Oncol | Cytoreductive surgery plus HIPEC with oxaliplatin in peritoneal mesothelioma |
| [31455014](https://pubmed.ncbi.nlm.nih.gov/31455014/) | 2019 | Review | Int J Mol Sci | Effect of cisplatin, oxaliplatin and pemetrexed on immune checkpoint expression; rationale for combination with immunotherapy |

## New Zealand Market Information

All five licences are cancelled, and no approved indication text is available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2400553 | Oxaliplatin Actavis 100 | Inj 100 mg vial | Not available (cancelled) |
| 2339013 | Oxaliplatin Ebewe | Inj 100 mg vial | Not available (cancelled) |
| 2571242 | Oxaliplatin Accord | Inj 5 mg per ml, 20 ml vial | Not available (cancelled) |
| 2207621 | Baxter | Inj 1 mg for ECP | Not available (cancelled) |
| 2654830 | Alchemy Oxaliplatin | Inj 5 mg per ml, 20 ml vial | Not available (cancelled) |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (platinum-based DNA-crosslinking agent) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Full blood count, liver and renal function; neurological assessment for peripheral neuropathy |
| Handling Protection | Must follow cytotoxic drug handling regulations |

## Safety Considerations

- **Disease associations flagged in the knowledge graph** (not taken from label text): kidney disease, liver disease, gallbladder disease, peripheral nervous system disease, peripheral neuropathy, interstitial lung disease, pulmonary fibrosis, post-inflammatory pulmonary fibrosis, and pyoureter.
- **Drug Interactions**: 100 interactions were identified. Among those listed, **dolasetron** is rated Major. Most others are Moderate, including famotidine, amphotericin B (including the lipid complex), loperamide, mesalazine, balsalazide, bisacodyl, metronidazole, clarithromycin, palonosetron, vancomycin, kanamycin, levofloxacin, naltrexone, lactulose and lactitol, and bowel-preparation agents (picosulfuric acid, sodium sulfate, polyethylene glycol).

Label warnings and contraindications were not available. Please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for pleural mesothelioma is limited to small, single-arm Phase 2 studies with mixed results, and there is no Phase 3 RCT. In New Zealand, all oxaliplatin licences are cancelled, and the package insert safety review is blocked. The other predicted indications (epithelioid, sarcomatoid, visceral pleura) have no direct clinical evidence.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications, to complete safety screening
- Mechanism of action data from DrugBank
- Comparison against current standard-of-care regimens for mesothelioma, ideally with randomised data
- Review of the 2003 to 2005 Phase 2 results for consistency, given the negative second-line study
- Confirmation of product availability in New Zealand, since no licences are currently valid
- Safety monitoring plan, particularly for patients with kidney, liver, lung or peripheral nerve disease
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

