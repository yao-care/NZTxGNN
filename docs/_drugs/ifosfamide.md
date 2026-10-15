---
layout: default
title: Ifosfamide
parent: Model Prediction Only (L5)
nav_order: 229
evidence_level: L5
indication_count: 10
---

# Ifosfamide
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

# Ifosfamide: From Cancer Chemotherapy to Female Breast Carcinoma

## One-Sentence Summary

Ifosfamide is an alkylating chemotherapy drug used in cancer treatment. Its New Zealand licences are all cancelled, and the licence records compiled for this report contain no approved-indication text.
The TxGNN model predicts it may be effective for **female breast carcinoma**, with **6 clinical trials** (all Phase 1/2, mostly multi-agent regimens) and **20 publications** currently supporting this direction.
The strongest evidence in the model's top ten predictions is for **rhabdomyosarcoma (rank 7)**, which has several Phase 3 trials, although most of them are in soft tissue sarcoma generally.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (no approved-indication text in the New Zealand licence records) |
| Predicted New Indication | Female breast carcinoma |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L2 (as graded in the data compiled for this report; the breast trials are Phase 1/2, with no completed Phase 3 RCT) |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold (Research Question) for breast carcinoma; Proceed with Guardrails for rhabdomyosarcoma |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Ifosfamide is a well-known oxazaphosphorine alkylating agent and a prodrug. The liver activates it into metabolites that cross-link DNA, which kills rapidly dividing tumour cells. Mesna is routinely given alongside it to protect the bladder.

Breast carcinoma is a rapidly proliferating solid tumour, so a DNA-damaging agent is mechanistically plausible. Published reviews report single-agent response rates of roughly 7–30% in pretreated metastatic breast cancer, and higher rates in combination regimens. Ifosfamide-based regimens have also been used in metaplastic breast cancer and phyllodes tumours.

The supporting trials are mostly Phase 1/2 and use combination or high-dose regimens, so the contribution of ifosfamide alone is hard to isolate.

## Clinical Trial Evidence

Trials for female breast carcinoma (the top prediction):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00026078](https://clinicaltrials.gov/study/NCT00026078) | Phase 2 | Unknown | 42 | Docetaxel plus ifosfamide as first-line therapy in metastatic breast cancer; the most direct test of ifosfamide in this indication, with no randomized comparator |
| [NCT00012311](https://clinicaltrials.gov/study/NCT00012311) | Phase 2 | Unknown | Not reported | Multi-cycle high-dose versus optimized conventional chemotherapy in metastatic breast cancer; multi-agent, so the ifosfamide-specific effect is unclear |
| [NCT00002854](https://clinicaltrials.gov/study/NCT00002854) | Phase 1 | Completed | 33 | Sequential high-dose cisplatin, cyclophosphamide, etoposide and ifosfamide with stem cell support; safety and feasibility data |
| [NCT00006032](https://clinicaltrials.gov/study/NCT00006032) | Phase 2 | Terminated | Not reported | Intensive-dose topotecan, ifosfamide/mesna and etoposide with autologous stem cell rescue in metastatic breast cancer |
| [NCT00003086](https://clinicaltrials.gov/study/NCT00003086) | Phase 1/2 | Terminated | 12 | Samarium-153 within a double autologous transplant; ifosfamide is only a background component |
| [NCT00020722](https://clinicaltrials.gov/study/NCT00020722) | Phase 2 | Terminated | 7 | Activated T cells after stem cell transplant in stage IV breast cancer; ifosfamide is peripheral |

Selected Phase 3 trials supporting the rhabdomyosarcoma prediction (rank 7, score 99.67%):

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00003212](https://clinicaltrials.gov/study/NCT00003212) | Phase 3 | Completed | 780 | Two ifosfamide schedules versus standard-dose doxorubicin in advanced soft tissue sarcoma |
| [NCT00061984](https://clinicaltrials.gov/study/NCT00061984) | Phase 3 | Completed | 455 | Doxorubicin alone versus doxorubicin plus ifosfamide as first-line therapy in advanced soft tissue sarcoma |
| [NCT00003052](https://clinicaltrials.gov/study/NCT00003052) | Phase 3 | Completed | 340 | Neoadjuvant etoposide, ifosfamide and doxorubicin with or without regional hyperthermia in high-risk soft tissue sarcoma |
| [NCT00002641](https://clinicaltrials.gov/study/NCT00002641) | Phase 3 | Completed | 350 | Adjuvant high-dose doxorubicin and ifosfamide with lenograstim in high-grade soft tissue sarcoma |
| [NCT00025363](https://clinicaltrials.gov/study/NCT00025363) | Phase 2 | Completed | 150 | Randomized window study of irinotecan schedules with chemotherapy in relapsed rhabdomyosarcoma |

## Literature Evidence

Publications for female breast carcinoma:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [14586149](https://pubmed.ncbi.nlm.nih.gov/14586149/) | 2003 | Review | Oncology | Ifosfamide is active in breast cancer; single-agent response rates are 7–30% in pretreated patients and higher in combinations, and mesna reduces urotoxicity |
| [8918497](https://pubmed.ncbi.nlm.nih.gov/8918497/) | 1996 | Phase 2 trial | J Clin Oncol | Ifosfamide plus vinorelbine as first-line chemotherapy in metastatic breast cancer |
| [8445421](https://pubmed.ncbi.nlm.nih.gov/8445421/) | 1993 | Phase 2 trial | J Clin Oncol | Ifosfamide plus mitoxantrone as first-line chemotherapy in metastatic breast cancer |
| [11932893](https://pubmed.ncbi.nlm.nih.gov/11932893/) | 2002 | Phase 2 trial | Cancer | Paclitaxel (24-hour infusion) plus ifosfamide in anthracycline-resistant metastatic breast carcinoma |
| [17370037](https://pubmed.ncbi.nlm.nih.gov/17370037/) | 2007 | Phase 2 trial | Invest New Drugs | Docetaxel plus ifosfamide in HER2-non-overexpressing advanced breast cancer after anthracyclines |
| [39306877](https://pubmed.ncbi.nlm.nih.gov/39306877/) | 2024 | Cohort | Curr Probl Cancer | Experience with ifosfamide-based chemotherapy in metaplastic breast cancer |
| [9226029](https://pubmed.ncbi.nlm.nih.gov/9226029/) | 1997 | Clinical study | Tumori | Ifosfamide plus etoposide in previously treated advanced breast cancer |
| [1390313](https://pubmed.ncbi.nlm.nih.gov/1390313/) | 1992 | Review | Ann Oncol | Ifosfamide monotherapy in breast cancer shows about a 20% objective response; slight superiority over cyclophosphamide |
| [2112056](https://pubmed.ncbi.nlm.nih.gov/2112056/) | 1990 | Clinical study | Cancer Chemother Pharmacol | Ifosfamide/etoposide with mesna uroprotection in 44 patients with advanced breast cancer |
| [39074497](https://pubmed.ncbi.nlm.nih.gov/39074497/) | 2024 | Cohort | J Natl Compr Canc Netw | Adjuvant chemotherapy and long-term prognosis in malignant phyllodes tumours of the breast |

Most of this literature consists of older Phase 2 studies and reviews. No randomized comparison of ifosfamide in breast cancer was identified.

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 712760 | Holoxan | Inj 2 g | Not available (licence cancelled) |
| 712736 | Holoxan | Inj 1 g | Not available (licence cancelled) |
| 2204223 | Baxter | Inj 1 mg for ECP | Not available (licence cancelled) |

All three licences are cancelled, so ifosfamide currently has no valid New Zealand authorization.

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (oxazaphosphorine alkylating agent) |
| Myelosuppression Risk | High; leukopenia, anaemia and thrombocytopenia are expected, especially in combination or high-dose regimens |
| Emetogenicity Classification | Moderate to high, depending on dose |
| Monitoring Items | CBC with differential, renal function and electrolytes (including potassium), liver function, urinalysis, and neurological status |
| Handling Protection | Must follow cytotoxic drug handling regulations |

Please refer to the package insert warnings and precautions for full details.

## Safety Considerations

- **Contraindication signals from the knowledge graph** (drug-disease links, not label text): kidney disease, pyoureter, liver disease, gallbladder disease, thrombocytopenia, potassium deficiency disease, anaemia, macrocytic anaemia, hyperbilirubinaemia, hereditary hyperbilirubinaemia, Gilbert syndrome, renal tubular acidosis, leukopenia, and Fanconi renotubular syndrome.
- **Drug interactions**: 100 interactions were identified, and the first 20 retrieved were mostly Moderate, with one Minor (levofloxacin). Notable examples:
  - nephrotoxic agents: amphotericin B (including lipid complex), kanamycin, neomycin
  - sulfonylureas and other antidiabetic agents: chlorpropamide, glimepiride, glipizide, glyburide, tolazamide, acetohexamide, nateglinide, repaglinide
  - antiemetics and CNS-active drugs: aprepitant, metoclopramide, dronabinol, nabilone, scopolamine, bupropion, morphine
- **Guardrails from the literature**: neurotoxicity, nephrotoxicity, haemorrhagic cystitis (mesna is required), myelosuppression and secondary malignancy, including therapy-related myelodysplastic syndrome and leukaemia.

Package insert warnings and contraindications were not available for this report, so please refer to the package insert for label-based safety information.

## Conclusion and Next Steps

**Decision: Hold (Research Question) for female breast carcinoma**

**Rationale:**
- The breast cancer evidence consists of Phase 1/2 trials and older Phase 2 literature, mostly of combination regimens. No completed Phase 3 trial tests ifosfamide in breast cancer, and ifosfamide's own contribution is hard to separate.
- Ifosfamide has no valid New Zealand licence at present.
- Among the other predictions, **rhabdomyosarcoma** (rank 7) has the strongest support (L1, with multiple Phase 3 trials), and could be considered **Proceed with Guardrails**. However, the Phase 3 trials are largely in soft tissue sarcoma or in combination regimens. Ifosfamide is already established in rhabdomyosarcoma regimens, so this is more confirmation than new repurposing.
- Predictions for myelodysplastic syndromes, refractory cytopenia, aregenerative anaemia, sideroblastic anaemia, 5q deletion and monocytic leukaemia lack supporting evidence or conflict with the drug's myelosuppressive and secondary-malignancy profile, so they are **Hold**.
- The botryoid embryonal rhabdomyosarcoma of the vagina prediction (rank 10) is plausible by extension from rhabdomyosarcoma, but rests on one case report (Research Question).

**To proceed, the following is needed:**
- Package insert warnings and contraindications (Medsafe), which are currently missing and block safety screening
- Detailed mechanism of action data (DrugBank)
- Approved-indication text and the regulatory status of any replacement product in New Zealand
- For breast cancer: randomized or well-controlled comparative data isolating ifosfamide's contribution, particularly in metaplastic breast cancer and phyllodes tumours
- A safety monitoring plan covering neurotoxicity, nephrotoxicity, haemorrhagic cystitis and myelosuppression, and long-term follow-up for secondary malignancy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

