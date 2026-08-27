---
layout: default
title: Lomustine
parent: 僅模型預測 (L5)
nav_order: 209
evidence_level: L5
indication_count: 10
---

# Lomustine
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

# Lomustine: From Brain Tumors & Hodgkin's Lymphoma to Lymphosarcoma

## One-Sentence Summary

Lomustine (CCNU, DrugBank DB01206) is a lipophilic nitrosourea alkylating agent originally used for primary/metastatic brain tumors and as a component of combination regimens for Hodgkin's lymphoma.
The TxGNN model predicts it may be effective for **Lymphosarcoma**,
with **17 clinical trials** and **20 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Primary/metastatic brain tumors; Hodgkin's lymphoma (secondary combination therapy) — not present in local licensing data |
| Predicted New Indication | Lymphosarcoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L2 |
| Taiwan Market Status | Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack (flagged as a High-severity data gap). Based on known pharmacology, lomustine is a lipophilic nitrosourea that alkylates and cross-links DNA, preferentially affecting rapidly dividing cells including lymphocytes — the same mechanism underlying its established role in Hodgkin lymphoma and non-Hodgkin lymphoma (NHL) combination regimens (e.g., MOPP/LOPP, LEMP, CAMP, PACET).

Lymphosarcoma is an older clinical term largely synonymous with non-Hodgkin lymphoma. Lomustine's original approved use already spans CNS tumors and Hodgkin's disease, both of which share pharmacological and disease-biology proximity to lymphosarcoma (lymphoid malignancy, CNS-penetrant regimens for CNS-involved lymphoma). This places the TxGNN prediction close to an *established, adjacent-indication extension* rather than a mechanistically novel hypothesis.

Multiple oral combination regimens containing lomustine (CCNU + etoposide + cyclophosphamide + procarbazine; CCNU + vincristine + procarbazine + prednisolone) have been studied specifically in NHL/lymphosarcoma populations, including AIDS-associated lymphoma and primary CNS lymphoma, reinforcing the biological plausibility of this prediction.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01775475](https://clinicaltrials.gov/study/NCT01775475) | Phase 2 | Completed | 7 | Randomized trial of CHOP vs. oral chemotherapy (incl. lomustine) with concomitant ART in HIV-associated lymphoma, Sub-Saharan Africa |
| [NCT00049439](https://clinicaltrials.gov/study/NCT00049439) | Phase 2 | Completed | 54 | Dose-modified oral combination chemotherapy (lomustine, etoposide, cyclophosphamide, procarbazine) in AIDS-related NHL |
| [NCT00003114](https://clinicaltrials.gov/study/NCT00003114) | Phase 2 | Completed | 5 | Oral combination chemotherapy with lomustine, etoposide, cyclophosphamide, procarbazine in AIDS-related Hodgkin's disease |
| [NCT00074191](https://clinicaltrials.gov/study/NCT00074191) | Phase 2 | Completed | 1 | MPV regimen (methotrexate, procarbazine, CCNU) ± intra-ocular chemo for primary CNS lymphoma |
| [NCT00989352](https://clinicaltrials.gov/study/NCT00989352) | Phase 2 | Unknown | 56 | Rituximab + high-dose methotrexate + lomustine + procarbazine, followed by maintenance, in elderly primary CNS lymphoma |
| [NCT05518383](https://clinicaltrials.gov/study/NCT05518383) | Phase 4 | Recruiting | 300 | Pediatric/adolescent B-cell mature NHL treatment protocol; lomustine inclusion not explicitly confirmed |
| [NCT00003113](https://clinicaltrials.gov/study/NCT00003113) | Phase 2 | Terminated | 6 | Oral combination chemo + G-CSF in elderly intermediate/high-grade NHL; terminated for poor accrual |
| [NCT01989052](https://clinicaltrials.gov/study/NCT01989052) | Phase 1 | Terminated | 9 | CTO ± lomustine in bevacizumab-naïve recurrent malignant glioma; efficacy endpoint not lymphoma-specific |
| [NCT04402073](https://clinicaltrials.gov/study/NCT04402073) | Phase 2 | Terminated | 20 | Personalized risk-adapted therapy for post-pubertal medulloblastoma; disease mismatch (not lymphoma) |
| [NCT02551718](https://clinicaltrials.gov/study/NCT02551718) | NA | Completed | 34 | Chemosensitivity/genomics-guided treatment in relapsed/refractory acute leukemia; population mismatch |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [348294](https://pubmed.ncbi.nlm.nih.gov/348294/) | 1978 | RCT (CALGB) | Cancer | Randomized comparison of CCNU vs. methyl-CCNU in Hodgkin's disease, lymphosarcoma, and reticulum cell sarcoma |
| [10711848](https://pubmed.ncbi.nlm.nih.gov/10711848/) | 1999 | Cohort | Drugs | Oral CCNU/etoposide/cyclophosphamide/procarbazine regimen in 38 patients with AIDS-related lymphoproliferative malignancies |
| [8436213](https://pubmed.ncbi.nlm.nih.gov/8436213/) | 1993 | Cohort | European Journal of Haematology | LEMP (lomustine, etoposide, methotrexate, prednisone) in 22 patients with relapsed/refractory NHL |
| [21303800](https://pubmed.ncbi.nlm.nih.gov/21303800/) | 2011 | Cohort | Annals of Oncology | Rituximab + methotrexate/procarbazine/lomustine (R-MCP) pilot trial in elderly primary CNS lymphoma |
| [15803492](https://pubmed.ncbi.nlm.nih.gov/15803492/) | 2005 | Cohort | Cancer | CIBO-P regimen (incl. lomustine) for poor-prognosis refractory/recurrent aggressive NHL |
| [33336792](https://pubmed.ncbi.nlm.nih.gov/33336792/) | 2021 | Cohort | British Journal of Haematology | DECC oral chemotherapy regimen (incl. lomustine) in relapsed/refractory diffuse large B-cell lymphoma |
| [8422281](https://pubmed.ncbi.nlm.nih.gov/8422281/) | 1993 | Cohort | European Journal of Cancer | PACET regimen (incl. lomustine) in 27 patients with relapsed/refractory NHL; 26% complete response |
| [2259920](https://pubmed.ncbi.nlm.nih.gov/2259920/) | 1990 | Phase 2 (Cohort) | Seminars in Oncology | CAMP regimen (lomustine, cytarabine, mitoxantrone, prednisone) in doxorubicin-resistant NHL |
| [30197327](https://pubmed.ncbi.nlm.nih.gov/30197327/) | 2018 | Cohort | Journal of Cancer Research and Therapeutics | LACE (lomustine-containing) conditioning for autologous HSCT in refractory/relapsed lymphoma |
| [35999255](https://pubmed.ncbi.nlm.nih.gov/35999255/) | 2022 | Cohort | Scientific Reports | Comparison of CEAC (lomustine-containing), BEAM, and IEAC conditioning regimens in autologous HSCT for PTCL |

---

## Market Information (Taiwan)

Lomustine currently has **no marketing authorization** in Taiwan (market status: 未上市 / Not Marketed; 0 licenses on record). No product/dosage form data is available.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (Alkylating agent, nitrosourea class) |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information. (TFDA package insert warnings/contraindications and drug-interaction data are flagged as an unresolved, Blocking-severity data gap in this evidence pack.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Lomustine's mechanism of action and its established, closely related use in Hodgkin lymphoma and CNS lymphoma combination regimens provide strong mechanistic plausibility for lymphosarcoma; evidence includes one completed randomized Phase 2 trial (NCT01775475) and multiple cohort studies, meeting L2 evidence criteria. However, the drug is not currently marketed in Taiwan and safety/labeling data are unresolved.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — currently a Blocking data gap
- Confirmed mechanism of action (MOA) data from DrugBank
- Drug-drug interaction (DDI) profile
- Local regulatory pathway assessment given current "Not Marketed" status in Taiwan
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

