---
layout: default
title: Emicizumab
parent: 僅模型預測 (L5)
nav_order: 132
evidence_level: L5
indication_count: 10
---

# Emicizumab
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

# Emicizumab: From Congenital Hemophilia A to Acquired Coagulation Factor Deficiency

## One-Sentence Summary

Emicizumab (Hemlibra) is a bispecific monoclonal antibody approved globally for prophylaxis in congenital Hemophilia A, currently not marketed in New Zealand.
The TxGNN model predicts it may be effective for **acquired coagulation factor deficiency** (principally acquired Hemophilia A), the highest-evidence candidate among all 10 predicted indications in this multi-indication analysis, supported by **1 observational cohort study** and **20 publications** — including multiple Phase 2 and Phase 3 clinical trials.
This indication carries an **L1 evidence level** and a "Proceed with Guardrails" recommendation, representing a compelling and mechanistically well-founded repurposing opportunity.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Congenital Hemophilia A prophylaxis (globally approved; not currently marketed in New Zealand) |
| Predicted New Indication | Acquired Coagulation Factor Deficiency (Acquired Hemophilia A) |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

> **Note on multi-indication analysis:** TxGNN's top-ranked prediction for emicizumab is pseudo-von Willebrand disease (score 99.99%), but this indication has no supporting clinical trials or literature (L5, Hold). This report focuses on acquired coagulation factor deficiency (rank #5, score 99.90%), which holds the strongest clinical evidence across all 10 predicted indications.

---

## Why is This Prediction Reasonable?

Emicizumab is a humanized bispecific antibody that simultaneously binds activated Factor IXa (FIXa) and Factor X (FX), physically bridging them in a way that mimics the cofactor function of activated Factor VIII (FVIIIa). In congenital Hemophilia A, genetic absence or dysfunction of FVIII disrupts this critical amplification step of the coagulation cascade; emicizumab restores thrombin generation entirely independent of FVIII protein.

Acquired Hemophilia A (AHA) arises when autoantibodies neutralise endogenous circulating FVIII in previously healthy individuals — producing the same functional FVIII deficiency as in congenital disease, but through an immunological rather than genetic mechanism. Because emicizumab does not rely on FVIII and is not recognised or inhibited by anti-FVIII autoantibodies, its hemostatic action is entirely preserved regardless of inhibitor titer. This represents a direct, high-confidence mechanistic fit. Conventional bypassing agents (recombinant FVIIa, activated prothrombin complex concentrates) provide only partial and short-lived hemostatic cover in AHA; emicizumab offers sustained prophylactic protection via subcutaneous self-administration — a meaningful clinical advantage in a patient population that is typically elderly and frail.

Multiple Phase 2 and Phase 3 clinical studies (GTH-AHA-EMI, AGEHA) have since validated this mechanistic hypothesis, demonstrating that emicizumab prevents bleeds in AHA while permitting deferral of immunosuppressive therapy and potentially reducing infection-related mortality. The mechanistic overlap between congenital and acquired Hemophilia A is nearly complete, making this one of the most compelling repurposing cases in rare haematology.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04398628](https://clinicaltrials.gov/study/NCT04398628) | N/A | Recruiting | 3,000 | ATHN Transcends: large multicentre observational cohort tracking long-term safety and real-world effectiveness across non-neoplastic haematologic disorders including acquired haemophilia; provides real-world safety background and patient population characterisation, but is not an interventional emicizumab efficacy study |

> The pivotal interventional evidence for emicizumab in acquired Hemophilia A (GTH-AHA-EMI Phase 2, AGEHA Phase 3) is documented in published literature below rather than as registered trials in the current database query.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [39134043](https://pubmed.ncbi.nlm.nih.gov/39134043/) | 2025 | Phase III Final Analysis | Thrombosis and Haemostasis | AGEHA study final analysis: emicizumab prophylaxis showed favourable benefit-risk in both IST-eligible (Cohort 1) and IST-ineligible (Cohort 2) AHA patients; supported long-term subcutaneous prophylaxis beyond 12 weeks |
| [37858328](https://pubmed.ncbi.nlm.nih.gov/37858328/) | 2023 | Phase II Open-Label | The Lancet Haematology | GTH-AHA-EMI: emicizumab prevented bleeds in AHA and allowed safe deferral of immunosuppression during first 12 weeks; established proof of concept for prophylactic use |
| [36696195](https://pubmed.ncbi.nlm.nih.gov/36696195/) | 2023 | Phase III Prospective Multicentre | Journal of Thrombosis and Haemostasis | First prospective Phase III study of emicizumab in AHA; demonstrated haemostatic efficacy regardless of inhibitor titer; supported regulatory consideration for off-label use |
| [40795229](https://pubmed.ncbi.nlm.nih.gov/40795229/) | 2025 | Prospective Follow-up Cohort | Blood Advances | GTH-AHA-EMI 2-year follow-up: sustained survival benefit confirmed with emicizumab and postponed immunosuppression; infection-related mortality reduced compared to historical IST-first data |
| [39361769](https://pubmed.ncbi.nlm.nih.gov/39361769/) | 2024 | Retrospective Multicentre Cohort | Blood Advances | Real-world US cohort (N=62, 12 centres): off-label emicizumab achieved high haemostatic success rates in AHA; reduced reliance on inpatient bypassing agents |
| [38049124](https://pubmed.ncbi.nlm.nih.gov/38049124/) | 2024 | Expert Consensus / Clinical Guidelines | Hämostaseologie | GTH-AHA Working Group consensus recommendations: recommends emicizumab for bleed prevention in AHA; provides structured guidance on dosing, monitoring, and IST integration |
| [39536818](https://pubmed.ncbi.nlm.nih.gov/39536818/) | 2025 | Narrative Review | Journal of Thrombosis and Haemostasis | Comprehensive management review in the emicizumab era; covers epidemiology, haemostatic strategies, IST timing, and evolving treatment algorithms |
| [38936699](https://pubmed.ncbi.nlm.nih.gov/38936699/) | 2024 | Comparative Analysis | Journal of Thrombosis and Haemostasis | Emicizumab versus immunosuppressive therapy head-to-head analysis in AHA; evaluates relative contributions to bleed control and inhibitor eradication in frail elderly patients |
| [36795341](https://pubmed.ncbi.nlm.nih.gov/36795341/) | 2023 | Review | Blood Transfusion | Balanced review of pros and cons of emicizumab in AHA; discusses off-label evidence base, thrombotic risk with concomitant aPCC, and patient selection criteria |
| [38066880](https://pubmed.ncbi.nlm.nih.gov/38066880/) | 2023 | Review | Hematology (ASH Education Program) | ASH Education Program: practical guidance on emicizumab in AHA covering patient selection, dosing, integration with immunosuppression, and monitoring for anti-drug antibodies |

---

## New Zealand Market Information

Emicizumab currently has no regulatory authorisations in New Zealand. There are no Medsafe-listed products, and the drug is not available through standard market channels. Access in New Zealand, if pursued, would require a Special Access Scheme (SAS) or PHARMAC funding application.

---

## Safety Considerations

Detailed New Zealand-specific safety data (package insert warnings and contraindications) are not available in this Evidence Pack due to the drug's non-marketed status.

Based on the global approved label and published literature, the following safety signals are clinically important for the acquired Hemophilia A repurposing context:

- **Thrombotic microangiopathy (TMA) and thromboembolic events**: A known serious risk when emicizumab is combined with activated prothrombin complex concentrates (aPCC, e.g., FEIBA). In AHA patients who may require bypassing agent rescue during emicizumab prophylaxis, aPCC should be avoided or used at low doses with extreme caution. Recombinant FVIIa is the preferred rescue agent.
- **Anti-drug antibodies**: Rare but documented in AHA patients (see PMID 39401737); may reduce haemostatic efficacy. Monitoring for ADAs is recommended in long-term prophylaxis.
- **Subcutaneous administration**: Generally well tolerated; injection site reactions reported.

Please refer to the full international package insert (FDA/EMA label) for complete warnings, contraindications, and dosing guidance pending New Zealand regulatory submission.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two Phase 3 and one Phase 2 clinical studies (GTH-AHA-EMI, AGEHA, and a Japanese prospective multicentre trial) demonstrate that emicizumab provides effective and sustained bleeding prophylaxis in acquired Hemophilia A, supported by real-world cohort data from 12 US centres. The mechanism of action is essentially identical to the approved congenital Hemophilia A indication — bypass of FVIII via FIXa-FX bridging — making this one of the most mechanistically coherent repurposing proposals available.

**To proceed, the following is needed:**

- **Regulatory pathway**: Initiate PHARMAC Special Access Scheme (SAS) or Named Patient Programme application, citing the GTH-AHA-EMI Phase 2 and AGEHA Phase 3 trial data
- **Safety protocol**: Obtain and review the full FDA/EMA approved package insert; establish an institutional protocol prohibiting concomitant aPCC use during emicizumab prophylaxis
- **Monitoring plan**: Define monitoring parameters — FVIII activity, inhibitor titer (Bethesda units), emicizumab plasma concentration, CBC, anti-drug antibody surveillance
- **Specialist referral requirement**: Mandate haematology specialist (preferably haemophilia centre) involvement for all AHA cases prior to initiating emicizumab
- **MOA documentation**: Complete DrugBank API query (DG002 data gap) to formally document the mechanism of action for institutional review submissions
- **TFDA/local label review**: Retrieve full package insert warnings and contraindications to resolve DG001 blocking data gap prior to S1 safety evaluation stage
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

