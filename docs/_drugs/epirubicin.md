---
layout: default
title: Epirubicin
parent: Model Prediction Only (L5)
nav_order: 181
evidence_level: L5
indication_count: 7
---

# Epirubicin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **7** 
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

# Epirubicin: From Breast Cancer to Primary Pulmonary Lymphoma

## One-Sentence Summary

Epirubicin is an anthracycline chemotherapy drug originally used to treat breast cancer.
The TxGNN model predicts it may be effective for **primary pulmonary lymphoma** (the top-ranked prediction), with **no registered clinical trials** and **6 publications**, all indirect, supporting this direction.
The best-supported prediction in the data is **small cell lung carcinoma** (5 clinical trials, 20 publications), which is covered in the sections below.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Breast carcinoma / breast neoplasm (from the knowledge graph; no approved-indication text in the New Zealand licence records) |
| Predicted New Indication | Primary pulmonary lymphoma |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L4 (for primary pulmonary lymphoma); L1 for the alternative prediction, small cell lung carcinoma |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold for primary pulmonary lymphoma (research question only); Proceed with Guardrails for small cell lung carcinoma |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, epirubicin is an anthracycline, a class that damages DNA and inhibits topoisomerase II. Its efficacy in breast cancer is established, and mechanistically it may be applicable to lymphoma.

Anthracyclines are a core component of lymphoma regimens (CHOP-like regimens, including epirubicin variants), so a cytotoxic effect against pulmonary lymphoma is plausible. However, the supporting evidence is indirect: a general epirubicin review, case reports, and retrospective series of extranodal lymphoma. Doxorubicin is the established anthracycline in this setting, which weakens the case for epirubicin-specific use.

Among the other predictions, **small cell lung carcinoma** (score 99.69%) has the strongest support. It is a highly proliferative, chemosensitive tumour, and epirubicin-containing regimens have been tested in prospective trials. The Phase 3 data suggest at best modest incremental benefit over the platinum-etoposide standard. The remaining predictions (well-differentiated fetal adenocarcinoma of the lung, pulmonary blastoma, mixed small cell and squamous cell lung carcinoma, myeloid leukaemia, upper aerodigestive tract neoplasm) have little or only indirect evidence.

---

## Clinical Trial Evidence

**Primary pulmonary lymphoma:** Currently no related clinical trials registered.

**Small cell lung carcinoma (best-supported alternative prediction):**

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00003606](https://clinicaltrials.gov/study/NCT00003606) | Phase 3 | Completed | 216 | Cisplatin + etoposide with or without epirubicin and cyclophosphamide in extensive-stage SCLC; directly tests epirubicin's contribution |
| [NCT00658580](https://clinicaltrials.gov/study/NCT00658580) | Phase 3 | Completed | 361 | Cisplatin-etoposide vs etoposide regimen without platinum in extensive SCLC; epirubicin involvement unclear |
| [NCT00011921](https://clinicaltrials.gov/study/NCT00011921) | Phase 3 | Unknown | 430 | Sequential high-dose vs standard chemotherapy in SCLC; regimen likely contains epirubicin |
| [NCT03755115](https://clinicaltrials.gov/study/NCT03755115) | Phase 2 | Unknown | 40 | PD-1 antibody SHR-1210 plus epirubicin in extensive SCLC after first-line failure |
| [NCT00154739](https://clinicaltrials.gov/study/NCT00154739) | Phase 2 | Completed | 86 | Gemcitabine/cisplatin vs gemcitabine/epirubicin in stage IIIB/IV lung cancer (appears to be non-small cell, so limited relevance) |

---

## Literature Evidence

**Primary pulmonary lymphoma:**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [7686469](https://pubmed.ncbi.nlm.nih.gov/7686469/) | 1993 | Review | Drugs | Epirubicin has response rates and survival equivalent to doxorubicin regimens in several cancers |
| [39192408](https://pubmed.ncbi.nlm.nih.gov/39192408/) | 2024 | Retrospective cohort | Zhongguo Shi Yan Xue Ye Xue Za Zhi | Clinical features and prognosis of primary extranodal DLBCL in the rituximab era (not epirubicin-specific) |
| [40728626](https://pubmed.ncbi.nlm.nih.gov/40728626/) | 2025 | Subgroup analysis | Annals of Hematology | Pola-R-CHP in DLBCL (not epirubicin-specific) |
| [10526668](https://pubmed.ncbi.nlm.nih.gov/10526668/) | 1999 | Clinical study | Cancer Journal | VEBEP plus radiotherapy in advanced Hodgkin's disease |
| [36237246](https://pubmed.ncbi.nlm.nih.gov/36237246/) | 2022 | Case report | Translational Cancer Research | Misdiagnosed primary pleural MALT lymphoma |
| [1866500](https://pubmed.ncbi.nlm.nih.gov/1866500/) | 1991 | Case report | Rev Invest Clin | Primary non-Hodgkin lymphoma of the lung |

**Small cell lung carcinoma (selected from 20 publications):**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12488411](https://pubmed.ncbi.nlm.nih.gov/12488411/) | 2002 | Phase 3 RCT | J Clin Oncol | Cisplatin-etoposide was superior to cyclophosphamide-epirubicin-vincristine in SCLC (5-year follow-up) |
| [7812705](https://pubmed.ncbi.nlm.nih.gov/7812705/) | 1994 | Clinical trial | Lung Cancer | Cyclophosphamide, epirubicin and etoposide in 136 patients: 31% complete and 49% partial responses |
| [36000584](https://pubmed.ncbi.nlm.nih.gov/36000584/) | 2023 | Retrospective cohort | Cancer Med | Real-world second-line epirubicin-paclitaxel in relapsed SCLC (EpiTax) |
| [21831476](https://pubmed.ncbi.nlm.nih.gov/21831476/) | 2012 | Clinical study | Lung Cancer | Epirubicin plus ifosfamide in relapsed or refractory SCLC |
| [1317199](https://pubmed.ncbi.nlm.nih.gov/1317199/) | 1992 | Clinical trial | Eur J Cancer | Vincristine-epirubicin-cyclophosphamide with epirubicin dose escalation in limited SCLC |
| [2165738](https://pubmed.ncbi.nlm.nih.gov/2165738/) | 1990 | Phase 2 | Am J Clin Oncol | High-dose epirubicin in untreated SCLC: 33% overall response rate |
| [22530708](https://pubmed.ncbi.nlm.nih.gov/22530708/) | 2012 | Meta-analysis | Intern Med J | Cisplatin plus etoposide vs other platinum-based regimens in extensive SCLC |

---

## New Zealand Market Information

All four licences are cancelled, and the licence records contain no approved-indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2224127 | Epirubicin Ebewe | Inj 2 mg per ml, 25 ml vial | Not recorded (cancelled) |
| 2224119 | Epirubicin Ebewe | Inj 2 mg per ml, 5 ml vial | Not recorded (cancelled) |
| 2224143 | Epirubicin Ebewe | Inj 2 mg per ml, 100 ml vial | Not recorded (cancelled) |
| 2204177 | Baxter | Inj 1 mg for ECP | Not recorded (cancelled) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (anthracycline; topoisomerase II inhibitor) |
| Myelosuppression Risk | High (knowledge-graph contraindication edges include neutropenia, thrombocytopenia and agranulocytosis; the literature also reports secondary AML/MDS after epirubicin-based therapy) |
| Emetogenicity Classification | Moderate (based on drug class) |
| Monitoring Items | CBC with differential, liver and renal function, cardiac function (cumulative anthracycline cardiotoxicity) |
| Handling Protection | Must follow cytotoxic drug handling regulations |

---

## Safety Considerations

- **Contraindication signals (from the knowledge graph, not label text):** liver failure, liver disease, kidney disease, gallbladder disease, thrombocytopenia, neutropenia, agranulocytosis, myocardial infarction, coronary thrombosis, cardiomyopathy (including hypertrophic), myocardial disorder, endomyocardial fibrosis, heart conduction disease, sudden cardiac arrest, hemoglobinopathy, pyoureter and stomatitis.
- **Drug Interactions:** 100 interactions were identified in total. The two **Major** ones in the list reviewed are **cimetidine** and **dolasetron**. Most others are Moderate, including acarbose, famotidine, pioglitazone, loperamide, bupropion, clarithromycin, levofloxacin, minocycline, naltrexone and palonosetron.
- **Other signals from the literature:** cumulative cardiotoxicity of anthracyclines, and an increased risk of secondary AML/MDS.

The package insert warnings and contraindications could not be retrieved for this report. Please refer to the package insert for the full safety information.

---

## Conclusion and Next Steps

**Decision: Hold (primary pulmonary lymphoma); Proceed with Guardrails (small cell lung carcinoma only)**

**Rationale:**
- For primary pulmonary lymphoma, there are no trials and only indirect literature, so this remains a research question. Doxorubicin is the established anthracycline for lymphoma.
- Small cell lung carcinoma has Phase 3 evidence, but epirubicin offers at best modest benefit over the platinum-etoposide standard, and cardiotoxicity and myelosuppression are significant. In New Zealand all licences are cancelled, so any use would need a supply pathway.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking data gap for safety screening)
- Mechanism of action data (for example from DrugBank)
- Dedicated epirubicin data in pulmonary lymphoma, or a head-to-head comparison against doxorubicin-based regimens
- A cardiac monitoring plan with a cumulative dose limit
- Clarification of the New Zealand supply and licensing situation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

