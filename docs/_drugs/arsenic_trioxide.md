---
layout: default
title: Arsenic Trioxide
parent: 僅模型預測 (L5)
nav_order: 34
evidence_level: L5
indication_count: 10
---

# Arsenic Trioxide
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

# Arsenic Trioxide: From Acute Promyelocytic Leukemia to Myelodysplastic Syndrome

## One-Sentence Summary

Arsenic trioxide (ATO) is a globally established antineoplastic agent best known for achieving >80% complete remission in relapsed Acute Promyelocytic Leukemia (APL), though it currently holds no New Zealand marketing authorization.
The TxGNN model predicts it may be effective for **Myelodysplastic Syndrome (MDS)** — the top-ranked prediction specifically targets **unclassified MDS** (score 99.93%), while the broader MDS indication (score 99.91%) is supported by **24 clinical trials** and **20 publications**, including a 2023 systematic review and a 2025 prospective randomized study.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acute Promyelocytic Leukemia (APL) — established global use; not registered in New Zealand |
| Predicted New Indication | Myelodysplastic Syndrome (MDS) |
| TxGNN Prediction Score | 99.91% (MDS); top-ranked prediction: Unclassified MDS at 99.93% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available from the Evidence Pack. Based on established pharmacology, arsenic trioxide is best characterized as a multi-target antineoplastic metalloid agent. In APL, ATO degrades the PML-RARα fusion oncoprotein through oxidative modification of its cysteine residues followed by proteasomal degradation, triggering terminal differentiation and apoptosis of leukemic blasts. This confirms ATO's capacity to selectively eliminate clonal bone marrow cells with aberrant myeloid differentiation — a hallmark equally central to MDS pathology.

APL and MDS are both clonal bone marrow disorders arising from dysregulated myeloid precursors. The mechanistic bridge from APL to MDS is supported by four complementary pathways: (1) ROS-mediated mitochondrial apoptosis (cytochrome-c / caspase-9/-3 cascade) that selectively purges dysplastic clonal progenitors; (2) inhibition of NF-κB and anti-apoptotic proteins BCL2/BCL-XL, which are upregulated in MDS marrow as shown in the Blood 2005 study (PMID 16105982); (3) downregulation of DNMT enzymatic activity, enabling synergistic demethylation when combined with hypomethylating agents such as decitabine; and (4) immunomodulatory expansion of CD4+CD25+Foxp3+ regulatory T cells that help restore immune homeostasis in the disordered marrow microenvironment.

A 2023 systematic review and component network meta-analysis (PMID 37908176) provided the first rigorous quantitative synthesis of ATO-containing regimens across MDS subtypes, and a 2025 prospective randomized study (PMID 40167011) confirmed superior outcomes with the decitabine+ATO combination over decitabine alone in elderly high-risk MDS patients. The TxGNN model's highest-scoring prediction specifically identifies **unclassified MDS** (TxGNN rank 929, score 99.93%) — a subtype where dedicated clinical trial data remain absent — but the mechanistic rationale and the substantial evidence base for MDS broadly make this prediction highly plausible.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06778187](https://clinicaltrials.gov/study/NCT06778187) | Phase 2 | Recruiting | 30 | Oral ATO (Arsenol®) + ascorbic acid ± investigator-choice low-intensity therapy in untreated or R/R TP53-mutated MDS, AML, and CMML; the most current frontline MDS-specific ATO trial (started February 2025) |
| [NCT02190695](https://clinicaltrials.gov/study/NCT02190695) | Phase 2 | Completed | 92 | Three-arm randomized study: decitabine vs. decitabine+carboplatin vs. decitabine+ATO in R/R and elderly AML/MDS; directly evaluates ATO combination efficacy and safety in MDS |
| [NCT00454480](https://clinicaltrials.gov/study/NCT00454480) | Phase 2/3 | Completed | 2,000 | Large-scale treatment development programme for elderly AML and high-risk MDS; provides extensive safety and efficacy background data including MDS subgroup |
| [NCT06670222](https://clinicaltrials.gov/study/NCT06670222) | Phase 1 | Recruiting | 24 | Dose-escalation oral ATO in low-risk MDS failing erythropoiesis-stimulating agents and luspatercept; evaluates emerging oral formulation in a treatment-refractory population (starting July 2025) |
| [NCT00671697](https://clinicaltrials.gov/study/NCT00671697) | Phase 1 | Completed | 13 | Decitabine + ATO + ascorbic acid triple combination in MDS and AML; establishes combination dosing safety data supporting the hypomethylating agent + ATO approach |
| [NCT00195104](https://clinicaltrials.gov/study/NCT00195104) | Phase 1/2 | Completed | 87 | ATO + low-dose cytarabine in intermediate-2 / high-risk MDS and poor-prognosis AML; complete remission achieved in 17%, including patients with unfavorable cytogenetics |
| [NCT00093366](https://clinicaltrials.gov/study/NCT00093366) | Phase 1/2 | Completed | 32 | ATO + etanercept (TNF-α inhibitor) in advanced-stage MDS; explores immunomodulatory combination approach targeting the inflammatory marrow microenvironment |
| [NCT00803530](https://clinicaltrials.gov/study/NCT00803530) | Phase 2 | Terminated | 55 | Prospective multicenter ATO + ascorbic acid in MDS; largest-enrollment ATO monotherapy-adjacent combination trial, safety data available despite early termination |
| [NCT00225992](https://clinicaltrials.gov/study/NCT00225992) | Phase 2 | Terminated | N/A | ATO monotherapy in MDS using a loading-dose regimen (0.30 mg/kg days 1–5 of cycle 1); primary endpoint: improvement in blood counts and reduction of transfusion dependence |
| [NCT00274820](https://clinicaltrials.gov/study/NCT00274820) | Phase 2 | Completed | 15 | TADA regimen (thalidomide + ATO + dexamethasone + ascorbic acid) in chronic idiopathic myelofibrosis and MDS/MPN overlap disorders; evaluates ATO in myeloid overlap syndromes |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37908176](https://pubmed.ncbi.nlm.nih.gov/37908176/) | 2023 | Systematic Review / Meta-analysis | Hematology | First comprehensive network meta-analysis of ATO-containing regimens in MDS; evaluates efficacy (overall response, haematological improvement) and adverse events; identifies optimal combination strategies |
| [40167011](https://pubmed.ncbi.nlm.nih.gov/40167011/) | 2025 | Prospective Randomized Study | Hematology | Decitabine+ATO demonstrates superior efficacy over decitabine alone in elderly high-risk MDS patients with acceptable toxicity; most recent prospective evidence supporting this combination |
| [38816179](https://pubmed.ncbi.nlm.nih.gov/38816179/) | 2024 | Comparative Clinical Study | Immunopharmacology and Immunotoxicology | Compares immunological effects of realgar (oral arsenic disulfide) vs. IV ATO in a murine MDS model; distinguishes route-specific immune modulation relevant to emerging oral ATO formulations |
| [30898879](https://pubmed.ncbi.nlm.nih.gov/30898879/) | 2019 | In vitro / Mechanistic | Journal of Investigative Medicine | Decitabine and ATO show synergistic apoptosis induction in MUTZ-1 and SKM-1 MDS cell lines via endoplasmic reticulum stress; provides cellular mechanistic basis for the combination |
| [22964015](https://pubmed.ncbi.nlm.nih.gov/22964015/) | 2012 | Ex vivo Clinical / Mechanistic | Journal of Hematology & Oncology | ATO + ascorbic acid modulates expression of 93 apoptotic genes including BCL2-family members in bone marrow of MDS patients; confirms in vivo mechanistic activity |
| [16105982](https://pubmed.ncbi.nlm.nih.gov/16105982/) | 2005 | In vitro / Mechanistic | Blood | NF-κB activity is elevated in high-grade MDS (RAEB) vs. low-grade (RA/RARS); ATO inhibits NF-κB and FLIP in MDS CD34+ cells, identifying key molecular targets |
| [20956016](https://pubmed.ncbi.nlm.nih.gov/20956016/) | 2011 | Phase I/II Clinical | Leukemia Research | ATO + low-dose cytarabine in 49 previously untreated int-2/high-risk MDS patients: CR 17%, including patients with poor-prognosis cytogenetics; 4-week mortality 8% |
| [31775455](https://pubmed.ncbi.nlm.nih.gov/31775455/) | 2019 | Retrospective Clinical Study | Zhonghua Nei Ke Za Zhi | Low-dose subcutaneous decitabine + ATO in 11 intermediate/high-risk MDS patients: CR 27%, haematological improvement 55%; median follow-up 413 days |
| [18282365](https://pubmed.ncbi.nlm.nih.gov/18282365/) | 2007 | Review / Clinical Data Summary | Clinical Lymphoma & Myeloma | Comprehensive review of ATO clinical data in APL and MDS; details response rates, dosing schedules, single-agent vs. combination performance, and rationale for MDS use |
| [15610661](https://pubmed.ncbi.nlm.nih.gov/15610661/) | 2005 | Clinical Review | Current Hematology Reports | Early clinical evidence establishing ATO as an MDS treatment candidate; reviews pro-apoptotic, antiproliferative, and anti-angiogenic mechanisms and early trial outcomes |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted cytotoxic (Metalloid / Arsenic-based agent); degrades PML-RARα oncoprotein in APL; induces ROS-mediated mitochondrial apoptosis and NF-κB inhibition in MDS and other haematological malignancies |
| Myelosuppression Risk | Low to Moderate (dose-dependent); at standard therapeutic doses, myelosuppression is less prominent than with conventional cytotoxics; differentiation syndrome / APL differentiation syndrome is the more characteristic acute toxicity in APL |
| Emetogenicity Classification | Low |
| Monitoring Items | QTc interval (12-lead ECG before initiation and twice weekly during induction), serum electrolytes (K⁺ and Mg²⁺ must be maintained ≥4.0 mEq/L and ≥1.8 mg/dL respectively), CBC with differential, liver function tests (AST/ALT), renal function (creatinine), serum arsenic levels if prolonged use |
| Handling Protection | Must follow cytotoxic drug handling regulations; intravenous preparation requires closed-system drug transfer device |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple Phase 1/2 clinical trials, a 2025 prospective randomized study, and a 2023 systematic review collectively provide an L2-level evidence base supporting ATO-containing regimens in MDS; the mechanistic rationale is strong and coherent, but the absence of a completed Phase 3 RCT and New Zealand marketing authorization means clinical use requires structured oversight and regulatory pathway planning.

**To proceed, the following is needed:**

- **Regulatory pathway**: ATO is not marketed in New Zealand; a Medsafe special clinical trial authorisation or Named Patient Supply application is required before any clinical use
- **Safety data gap**: Full warnings, contraindications, and DDI profile from the NZ-equivalent package insert and DrugBank must be retrieved and reviewed (Evidence Pack data gaps DG001/DG002)
- **Formulation decision**: Clarify whether IV (standard) or oral ATO (Arsenol® — actively recruiting in NCT06778187 and NCT06670222) is preferred; oral formulations may offer practical advantages for outpatient MDS management
- **Unclassified MDS subtype**: The TxGNN rank 1 prediction (unclassified MDS, score 99.93%) has no dedicated clinical trials or literature; a prospective case series or registry study specifically in this subtype is needed to validate the model prediction
- **QTc monitoring protocol**: Mandatory pre-treatment ECG and electrolyte management plan to be specified before any NZ patient receives ATO
- **Phase 3 evidence**: Current evidence is predominantly Phase 1/2 and retrospective; a completed Phase 3 RCT in MDS (e.g., NCT03377725 was withdrawn) remains a critical gap before broader recommendation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

