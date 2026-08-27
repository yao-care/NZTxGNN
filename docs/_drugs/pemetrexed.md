---
layout: default
title: Pemetrexed
parent: 僅模型預測 (L5)
nav_order: 272
evidence_level: L5
indication_count: 10
---

# Pemetrexed
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Pemetrexed: From Malignant Pleural Mesothelioma to Malignant Peritoneal Mesothelioma

## One-Sentence Summary

Pemetrexed is a multi-targeted antifolate whose established first-line indication (in combination with cisplatin) is malignant pleural mesothelioma. The TxGNN model predicts it may also be effective for **Malignant Peritoneal Mesothelioma**, a prediction currently supported by **11 clinical trials** and **20 publications**.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Malignant Pleural Mesothelioma (established first-line indication, in combination with cisplatin) |
| Predicted New Indication | Malignant Peritoneal Mesothelioma |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed structured mechanism-of-action data is not available for this drug record. Based on the evidence collected, pemetrexed is a multi-targeted antifolate that inhibits thymidylate synthase (TS), dihydrofolate reductase (DHFR), and glycinamide ribonucleotide formyltransferase (GARFT), blocking folate-dependent DNA synthesis in rapidly dividing tumour cells. This mechanism is well established in malignant pleural mesothelioma, where pemetrexed plus cisplatin is a globally accepted first-line regimen.

Malignant pleural mesothelioma and malignant peritoneal mesothelioma both arise from mesothelial cells and share the same underlying tumour biology — they differ mainly in the anatomic cavity of origin (pleura vs. peritoneum) rather than in cell-of-origin or driver biology. Because the antifolate mechanism targets a proliferation pathway common to mesothelial tumours regardless of location, extrapolation from pleural to peritoneal disease is mechanistically reasonable.

In practice, cisplatin plus pemetrexed is already used off-label as a standard systemic treatment option for peritoneal mesothelioma, and this is reflected in multiple ongoing prospective trials (e.g., ICARuS II, MESOTIP) evaluating its role alongside surgery/HIPEC. The absence of a completed disease-specific Phase 3 RCT — as opposed to the fully validated pleural indication — is the main gap separating this from a fully confirmed indication.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06057935](https://clinicaltrials.gov/study/NCT06057935) | Phase 2 | Recruiting | 64 | ICARuS II: randomized trial of intraperitoneal vs. intravenous chemotherapy after cytoreductive surgery + HIPEC for malignant peritoneal mesothelioma |
| [NCT05001880](https://clinicaltrials.gov/study/NCT05001880) | Phase 2 | Recruiting | 66 | Carboplatin/pemetrexed/bevacizumab ± atezolizumab as neoadjuvant/palliative therapy for peritoneal mesothelioma |
| [NCT03875144](https://clinicaltrials.gov/study/NCT03875144) | Phase 2 | Suspended | 66 | MESOTIP: PIPAC + systemic cisplatin/pemetrexed vs. systemic chemotherapy alone as 1st-line treatment |
| [NCT02535312](https://clinicaltrials.gov/study/NCT02535312) | Phase 1/2 | Active, not recruiting | 30 | TRC102 (methoxyamine) + cisplatin/pemetrexed in advanced solid tumours, including pemetrexed/platinum-refractory mesothelioma |
| [NCT06543069](https://clinicaltrials.gov/study/NCT06543069) | Phase 2 | Recruiting | 28 | Sintilimab + bevacizumab + pemetrexed/cisplatin in unresectable peritoneal mesothelioma |
| [NCT04462809](https://clinicaltrials.gov/study/NCT04462809) | Phase 2 | Unknown | 40 | Maintenance talazoparib following first-line platinum-based chemotherapy in pleural or peritoneal mesothelioma |
| [NCT02029690](https://clinicaltrials.gov/study/NCT02029690) | Phase 1 | Terminated | 85 | ADI-PEG 20 + pemetrexed/cisplatin (TRAP study) in arginine-dependent tumours, including advanced peritoneal mesothelioma |
| [NCT01353482](https://clinicaltrials.gov/study/NCT01353482) | Phase 1/2 | Withdrawn | 0 | Vorinostat + pemetrexed-cisplatin first-line therapy in mesothelioma (withdrawn before enrollment) |
| [NCT00402766](https://clinicaltrials.gov/study/NCT00402766) | Phase 1 | Completed | 19 | Cisplatin + pemetrexed + imatinib mesylate in unresectable/metastatic mesothelioma; maximum tolerated dose determination |
| [NCT03564691](https://clinicaltrials.gov/study/NCT03564691) | Phase 1 | Completed | 470 | MK-4830 ± pembrolizumab basket trial in advanced solid tumours (not mesothelioma-specific) |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26941986](https://pubmed.ncbi.nlm.nih.gov/26941986/) | 2016 | Review | Journal of Gastrointestinal Oncology | Diagnosis and management overview of malignant peritoneal mesothelioma (MPeM), including treatment landscape |
| [35407498](https://pubmed.ncbi.nlm.nih.gov/35407498/) | 2022 | Review | Journal of Clinical Medicine | Review of treatment approaches for MPeM, including CRS+HIPEC and systemic chemotherapy |
| [31417959](https://pubmed.ncbi.nlm.nih.gov/31417959/) | 2019 | Cohort/Case series | Pleura and Peritoneum | Bidirectional chemotherapy enabling surgical resectability in initially unresectable MPeM |
| [28594258](https://pubmed.ncbi.nlm.nih.gov/28594258/) | 2017 | Retrospective | Expert Review of Anticancer Therapy | First-line pemetrexed plus cisplatin efficacy in MPeM |
| [31287877](https://pubmed.ncbi.nlm.nih.gov/31287877/) | 2019 | Retrospective | Japanese Journal of Clinical Oncology | Efficacy and safety of first-line pemetrexed plus cisplatin in advanced MPeM |
| [41133016](https://pubmed.ncbi.nlm.nih.gov/41133016/) | 2025 | Retrospective | Clinical Medicine Insights: Oncology | Comparison of first-line pemetrexed-platinum vs. gemcitabine-platinum regimens in MPeM |
| [33743636](https://pubmed.ncbi.nlm.nih.gov/33743636/) | 2021 | Retrospective | BMC Cancer | Second-line treatment efficacy and prognostic factors in advanced MPeM |
| [38806763](https://pubmed.ncbi.nlm.nih.gov/38806763/) | 2024 | Multi-center cohort | Annals of Surgical Oncology | Treatment strategies and outcomes analysis across MPeM patients |
| [23291819](https://pubmed.ncbi.nlm.nih.gov/23291819/) | 2013 | Case report | BMJ Case Reports | Response to rechallenge with cisplatin and pemetrexed in MPeM |
| [34723916](https://pubmed.ncbi.nlm.nih.gov/34723916/) | 2022 | Case series | Journal of Immunotherapy | Chemoimmunotherapy in platinum-nonresponsive metastatic MPeM |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (multi-targeted antifolate/antimetabolite) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Standard cytotoxic drug handling precautions apply |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple Phase 2 trials (including RCTs) are actively investigating pemetrexed-based regimens specifically in malignant peritoneal mesothelioma, and one completed Phase 2 trial (NCT00061477) already included this population, supporting an L2 evidence level. However, no completed disease-specific Phase 3 RCT exists, and this is an off-label extrapolation from the approved pleural mesothelioma indication.

**To proceed, the following is needed:**
- TFDA/regulatory package insert warnings and contraindications (currently a blocking data gap)
- Confirmed mechanism of action (MOA) data from DrugBank
- Drug-drug interaction (DDI) data
- Route compatibility assessment (intravenous vs. intraperitoneal administration, given HIPEC/PIPAC use in this population)
- New Zealand market authorization status confirmation, given the drug is currently not marketed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

