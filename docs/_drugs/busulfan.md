---
layout: default
title: Busulfan
parent: 僅模型預測 (L5)
nav_order: 56
evidence_level: L5
indication_count: 10
---

# Busulfan
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

# Busulfan: From Leukemia to Myelodysplastic Syndrome

## One-Sentence Summary

Busulfan is a classical bifunctional alkylating agent historically used as myeloablative conditioning prior to allogeneic hematopoietic stem cell transplantation (HSCT) in leukemia and bone marrow failure syndromes.
The TxGNN model predicts it may be effective for **Myelodysplastic Syndrome (MDS)**, with **10+ clinical trials** and **20 publications** currently supporting this direction.
Evidence quality is rated L1, anchored by a completed Phase 3 RCT (*Lancet Haematology* 2020) that directly evaluated Busulfan+Fludarabine as the reference conditioning regimen in 476 older or comorbid AML/MDS patients.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | HSCT myeloablative conditioning for leukemia and bone marrow failure (no New Zealand registration found) |
| Predicted New Indication | Myelodysplastic Syndrome (MDS) |
| TxGNN Prediction Score | 99.62% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed (0 authorizations) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the Evidence Pack. Based on established pharmacological knowledge, Busulfan belongs to the sulfonylalkane class of bifunctional alkylating agents. It forms covalent cross-links between DNA strands, interfering with replication and triggering apoptosis in rapidly dividing hematopoietic progenitor cells. This selective marrow toxicity makes it the cornerstone drug in myeloablative conditioning regimens — it eradicates residual disease, ablates the recipient's immune system, and creates physical space within the bone marrow niche to enable donor stem cell engraftment.

Myelodysplastic syndrome (MDS) is a clonal disorder of hematopoietic stem cells characterized by dysplastic bone marrow morphology, ineffective hematopoiesis, and risk of transformation to acute myeloid leukemia. Allogeneic HSCT remains the only potentially curative option for eligible patients with higher-risk disease. In this context, Busulfan does not act as a standalone anti-MDS drug; rather, it serves as the conditioning backbone that eliminates abnormal MDS clones and establishes the immunological conditions needed for durable donor engraftment — a mechanistic role identical to its established use in leukemia HSCT.

The prediction is strongly supported by direct clinical evidence. A landmark Phase 3 RCT (PMID 31606445, *Lancet Haematology* 2020) used Busulfan+Fludarabine as the gold-standard comparator against Treosulfan+Fludarabine in 476 older/comorbid AML and MDS patients, confirming its established status in this population. A second Phase 3 RCT (PMID 36702138, *Lancet Haematology* 2023) used Busulfan-Cyclophosphamide as the standard-of-care control arm specifically for MDS-RAEB undergoing allo-HSCT. The high TxGNN prediction score of 99.62% is therefore well-grounded in the disease biology and the existing trial landscape.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT06477549](https://clinicaltrials.gov/study/NCT06477549) | Phase 2 | Recruiting | 220 | Randomized trial comparing Benadamustine vs Ruxolitinib combined with Fludarabine+Busulfan conditioning in haploidentical HSCT for MDS and relapsed hematologic malignancies; Flu+Bu is the core conditioning backbone under direct investigation |
| [NCT02250937](https://clinicaltrials.gov/study/NCT02250937) | Phase 2 | Active, Not Recruiting | 116 | Venetoclax combined with timed-sequential Busulfan, Cladribine, and Fludarabine before donor HSCT in AML/MDS; Busulfan is a central component of this novel conditioning regimen |
| [NCT00416598](https://clinicaltrials.gov/study/NCT00416598) | Phase 2 | Completed | 546 | Decitabine maintenance therapy post-HSCT for AML (MDS overlap); largest enrolled cohort using Busulfan-based pre-transplant conditioning, evaluating long-term post-conditioning outcomes |
| [NCT05027945](https://clinicaltrials.gov/study/NCT05027945) | Phase 2 | Recruiting | 54 | Allogeneic HSCT for VEXAS syndrome — a clonal disorder in which MDS is the primary hematologic comorbidity; Busulfan included in the conditioning regimen |
| [NCT00186342](https://clinicaltrials.gov/study/NCT00186342) | Phase N/A | Completed | 120 | Busulfan+Etoposide+Cyclophosphamide allogeneic HSCT for patients aged 51–60 with MDS or myeloproliferative disorders; assessed tolerability and efficacy in older patients |
| [NCT00301834](https://clinicaltrials.gov/study/NCT00301834) | Phase 2 | Completed | 35 | Fludarabine+Busulfan+Alemtuzumab reduced-toxicity ablative conditioning for pediatric patients with MDS and marrow failure syndromes; evaluates long-term toxicity-efficacy balance |
| [NCT02221310](https://clinicaltrials.gov/study/NCT02221310) | Phase 2 | Completed | 25 | Gemtuzumab ozogamicin+Busulfan+Cyclophosphamide combined immunochemotherapy and allogeneic HSCT for high-risk AML/MDS; explores adding CD33-targeted therapy to Busulfan conditioning |
| [NCT00863148](https://clinicaltrials.gov/study/NCT00863148) | Phase 2 | Completed | 30 | Clofarabine+IV Busulfan+Thymoglobulin (CBT) reduced-intensity conditioning for high-risk AML, MDS, or ALL prior to allogeneic HSCT; evaluated efficacy and tolerability in adults |
| [NCT01177371](https://clinicaltrials.gov/study/NCT01177371) | Phase 2 | Completed | 13 | High-dose Busulfan+Cyclophosphamide allogeneic BMT for MDS, leukemia, multiple myeloma, and lymphoma; long-running single-center study (1988–2010) establishing conditioning regimen tolerability |
| [NCT02626715](https://clinicaltrials.gov/study/NCT02626715) | Phase 2 | Completed | 21 | Head-to-head comparison of myeloablative vs reduced-intensity Busulfan-based conditioning before HSCT for AML/MDS in pediatric and young adult patients |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [31606445](https://pubmed.ncbi.nlm.nih.gov/31606445/) | 2020 | Phase 3 RCT | The Lancet Haematology | MC-FludT.14/L trial (n=476): Treosulfan+Flu vs **Busulfan+Flu** as reference conditioning in older/comorbid AML and MDS patients — establishes Busulfan+Flu as the definitive L1 standard comparator for MDS HSCT conditioning |
| [35617104](https://pubmed.ncbi.nlm.nih.gov/35617104/) | 2022 | Phase 3 RCT Final Analysis | American Journal of Hematology | Final confirmatory analysis of the above Phase 3 trial (476 patients): Busulfan-based conditioning arm demonstrates noninferiority event-free survival; confirms long-term durability of the Busulfan+Flu reference regimen |
| [36702138](https://pubmed.ncbi.nlm.nih.gov/36702138/) | 2023 | Phase 3 RCT | The Lancet Haematology | Open-label multicenter RCT: G-CSF+Decitabine+Bu/Cy vs **Bu/Cy alone** conditioning for MDS-RAEB and secondary AML undergoing allo-HSCT; Bu/Cy is the standard-of-care arm, directly validates Busulfan's central role in MDS transplant conditioning |
| [40079242](https://pubmed.ncbi.nlm.nih.gov/40079242/) | 2025 | Systematic Review | American Journal of Hematology | Contemporary review of allo-HSCT for MDS and myelofibrosis: confirms HSCT as the only curative option for eligible higher-risk MDS patients, with genomic profiling now guiding patient selection; Busulfan-based regimens discussed throughout as conditioning backbone |
| [37579918](https://pubmed.ncbi.nlm.nih.gov/37579918/) | 2023 | Prospective Cohort | Transplantation and Cellular Therapy | Myeloablative-dose Busulfan+Fludarabine with in vivo T-cell depletion for AML/MDS: safe and effective conditioning even beyond the traditional age 55 cutoff; challenges overly conservative patient selection criteria |
| [28380315](https://pubmed.ncbi.nlm.nih.gov/28380315/) | 2017 | Phase 3 RCT (CIBMTR) | Journal of Clinical Oncology | BMT-CTN 0901 randomized trial comparing myeloablative conditioning (MAC, Busulfan-based) vs reduced-intensity conditioning (RIC) in AML/MDS: MAC associated with lower relapse risk at the cost of higher non-relapse mortality |
| [33425740](https://pubmed.ncbi.nlm.nih.gov/33425740/) | 2020 | Systematic Review & Meta-analysis | Frontiers in Oncology | Meta-analysis synthesizing long-term outcomes of treosulfan- vs Busulfan-based conditioning for MDS/AML allo-HCT across multiple cohort studies |
| [38648898](https://pubmed.ncbi.nlm.nih.gov/38648898/) | 2024 | Retrospective Cohort | Transplantation and Cellular Therapy | Propensity score-matched comparison (n=138 MDS adults, Princess Margaret Hospital): treosulfan vs Busulfan conditioning; directly evaluates Busulfan-based regimen efficacy and safety outcomes in a pure MDS cohort |
| [34489555](https://pubmed.ncbi.nlm.nih.gov/34489555/) | 2021 | Retrospective Cohort | Bone Marrow Transplantation | Nationwide Japanese registry analysis: Flu/Bu4 (myeloablative) vs Bu4/Cy conditioning for MDS allo-HSCT using propensity score matching; provides real-world comparative effectiveness data |
| [35296446](https://pubmed.ncbi.nlm.nih.gov/35296446/) | 2022 | Retrospective Cohort | Transplantation and Cellular Therapy | Nationwide Japanese registry: MAC Flu/Bu4 vs RIC Flu/Bu2 in MDS HSCT; guides optimal Busulfan dose intensity selection for different patient risk profiles |

---

## New Zealand Market Information

Busulfan does not currently hold any Medsafe authorization in New Zealand (0 registered products). Patients requiring Busulfan-based HSCT conditioning in New Zealand would depend on hospital pharmacy importation under the Medicines Act unapproved special access pathway or use via clinical trial enrollment.

---

## Cytotoxicity

Busulfan is a conventional cytotoxic antineoplastic agent (alkylating drug). The following applies:

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Alkylating agent (sulfonylalkane/busulfane class) |
| Myelosuppression Risk | High — severe myelosuppression is the **intended therapeutic endpoint** as conditioning; complete bone marrow ablation is expected and required for HSCT engraftment |
| Emetogenicity Classification | Moderate (IV busulfan); antiemetic prophylaxis standard during conditioning |
| Monitoring Items | Complete blood count with differential (daily during conditioning and early engraftment), liver function tests (hepatic sinusoidal obstruction syndrome / venoocclusive disease is a major risk), renal function, and busulfan plasma pharmacokinetics (AUC-targeted therapeutic drug monitoring is recommended to optimize engraftment while limiting toxicity) |
| Handling Protection | Must follow cytotoxic drug handling regulations; parenteral busulfan (Busulfex) requires dedicated IV line, closed-system drug transfer devices, and full personal protective equipment per institutional cytotoxic handling protocols |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Busulfan-based conditioning for MDS HSCT is supported by L1-level evidence — a completed Phase 3 RCT (*Lancet Haematology* 2020, PMID 31606445, n=476) uses Busulfan+Fludarabine as the established reference conditioning regimen, while a second Phase 3 RCT (*Lancet Haematology* 2023, PMID 36702138) validates Busulfan-Cyclophosphamide as standard-of-care conditioning specifically for MDS-RAEB patients. The mechanistic rationale (eradication of dysplastic clones + marrow space creation for donor engraftment) is fully established.

**To proceed, the following is needed:**
- Full mechanism of action documentation from DrugBank (MOA data gap)
- Complete package insert safety data: warnings, contraindications, and drug interaction profile (TFDA/FDA/EMA)
- Medsafe New Zealand registration pathway assessment or unapproved access mechanism confirmation
- Institutional AUC-targeted therapeutic drug monitoring (TDM) protocol for busulfan dosing in MDS patients
- Formal cytotoxic drug handling, preparation, and administration standard operating procedure
- Patient-specific risk assessment prior to conditioning: age, performance status, comorbidity index (HCT-CI), MDS risk score (IPSS-R/IPSS-M), and availability of suitable HLA-matched donor
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

