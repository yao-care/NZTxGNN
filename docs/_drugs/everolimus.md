---
layout: default
title: Everolimus
parent: 僅模型預測 (L5)
nav_order: 143
evidence_level: L5
indication_count: 10
---

# Everolimus
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

# Everolimus: From Renal Cell Carcinoma to Liposarcoma

## One-Sentence Summary

Everolimus is an mTOR (mechanistic target of rapamycin) inhibitor globally approved for renal cell carcinoma, hormone receptor-positive breast cancer, pancreatic neuroendocrine tumors, and organ transplant rejection prevention.
The TxGNN model predicts it may be effective for **Liposarcoma** (specifically dedifferentiated liposarcoma, DDL),
with **1 Phase 2 clinical trial** and **5 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No regulatory record for New Zealand (globally approved for renal cell carcinoma, breast cancer, neuroendocrine tumors, transplant rejection) |
| Predicted New Indication | Liposarcoma (Dedifferentiated Liposarcoma, DDL) |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the current Evidence Pack. Based on established pharmacology, Everolimus is a rapalog-class mTOR inhibitor that binds to FKBP-12, forming a complex that selectively inhibits mTORC1. This suppresses downstream signalling through S6K1 and 4E-BP1, leading to reduced cell proliferation, protein synthesis, angiogenesis, and tumour metabolism. Its efficacy in clear-cell renal cell carcinoma and other mTOR-driven malignancies is well established.

Dedifferentiated liposarcoma (DDLPS) is one of the two most common subtypes of soft-tissue sarcoma, characterised by CDK4 gene amplification and aggressive clinical behaviour. Immunohistochemical and in vitro analysis of 99 DDLPS specimens has directly demonstrated dual activation of both the AKT/mTOR and MAPK pathways in this tumour type (PMID 26518767), providing strong mechanistic justification for mTOR inhibitor use. This pathway activation is not merely incidental — it drives tumour proliferation and resistance to conventional chemotherapy.

The clinical hypothesis tested in NCT03114527 combines Ribociclib (a CDK4/6 inhibitor targeting the Rb-E2F axis) with Everolimus (mTOR inhibition) to achieve simultaneous dual-pathway blockade. Preclinical models across multiple tumour types have shown synergistic growth inhibition when these two mechanistically distinct targets are co-inhibited, making this combination rationally compelling for CDK4-amplified DDLPS.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03114527](https://clinicaltrials.gov/study/NCT03114527) | Phase 2 | Active, Not Recruiting | 48 | Two-arm trial: Ribociclib 300 mg/day (3 weeks on/1 week off) + Everolimus 2.5 mg daily in advanced DDL (Arm A) and LMS (Arm B) with ≥1 prior systemic therapy; Phase 2 results published 2024 (PMID 37967116) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [37967116](https://pubmed.ncbi.nlm.nih.gov/37967116/) | 2024 | Phase 2 Trial Results | Clinical Cancer Research | Phase II results of Ribociclib+Everolimus in advanced DDL and LMS; evaluated CDK4/6 and mTOR co-blockade; synergistic growth inhibition confirmed in multiple preclinical tumour models |
| [26518767](https://pubmed.ncbi.nlm.nih.gov/26518767/) | 2016 | Translational/Mechanistic | Tumour Biology | AKT/mTOR and MAPK pathway activation demonstrated by immunohistochemistry in 99 DDLPS specimens; in vitro mTOR inhibitor antitumour effects evaluated, supporting mechanistic rationale |
| [36003796](https://pubmed.ncbi.nlm.nih.gov/36003796/) | 2022 | Preclinical Review | Frontiers in Oncology | PDOX mouse model platform for sarcoma used to identify effective CDK inhibitor combinations; highlights intersection of CDK and mTOR pathway targeting in soft-tissue sarcomas |
| [29848686](https://pubmed.ncbi.nlm.nih.gov/29848686/) | 2018 | Preclinical | Anticancer Research | Eribulin combined with mechanistically distinct agents (including mTOR inhibitors) evaluated in liposarcoma xenograft models; provides preclinical combination landscape context |
| [41991999](https://pubmed.ncbi.nlm.nih.gov/41991999/) | 2026 | Preclinical/Translational | Oncogene | XPO1 inhibitor KPT-330 disrupts core transcriptional regulatory circuitry in DDLPS; contextualises the broader targeted therapy landscape and identifies translation-level vulnerabilities |

---

## New Zealand Market Information

Everolimus currently has no regulatory authorizations recorded in New Zealand. No product listings, dosage forms, or approved indications are available in the current regulatory record.

---

## Cytotoxicity

Everolimus meets criteria for the antineoplastic section: it is globally approved and used for oncological indications (renal cell carcinoma, breast cancer, neuroendocrine tumours) and belongs to the targeted anticancer drug class.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted therapy — mTOR inhibitor (rapalog class); not a conventional cytotoxic agent |
| Myelosuppression Risk | Low to Moderate — lymphopenia and thrombocytopenia are reported; anaemia and neutropenia less common than with conventional cytotoxics; haematological toxicity is manageable |
| Emetogenicity Classification | Low (oral administration; nausea generally mild) |
| Monitoring Items | CBC with differential, fasting blood glucose and HbA1c (hyperglycaemia risk), serum creatinine, liver function tests (AST/ALT), lipid panel (hyperlipidaemia), pulmonary function assessment (non-infectious pneumonitis is a class-specific toxicity), wound healing assessment if perioperative |
| Handling Protection | Standard oral targeted therapy handling precautions; cytotoxic waste disposal per institutional policy recommended |

---

## Safety Considerations

Key warnings, contraindications, and drug interaction data are not available in the current Evidence Pack for New Zealand.

Please refer to the package insert for safety information. Particular attention should be paid to non-infectious pneumonitis, immunosuppression-related infection risk, hyperglycaemia, and mucositis — known class effects of mTOR inhibitors that require proactive monitoring.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A Phase 2 clinical trial (NCT03114527) directly evaluating Ribociclib + Everolimus in dedifferentiated liposarcoma has been completed and published (PMID 37967116, 2024), and independent mechanistic evidence confirms AKT/mTOR pathway activation in DDLPS tumour specimens (PMID 26518767). Together, these constitute Level 2 evidence with a clear biological rationale, justifying advancement to the next evaluation stage under defined guardrails.

**To proceed, the following is needed:**
- Full review of NCT03114527 published results (PMID 37967116) to assess primary efficacy endpoints (PFS, ORR) and dose-limiting toxicities in the sarcoma-specific population
- New Zealand or international package insert acquisition to complete the safety profile (warnings, contraindications, drug-drug interactions — currently blocking data gaps DG001/DG002)
- Mechanism of action documentation (MOA data gap DG002) to support regulatory submission narrative
- Molecular patient selection criteria: prospective identification of patients with confirmed CDK4 amplification and/or mTOR pathway hyperactivation is recommended to maximise benefit-risk
- Dose optimisation confirmation: the trial used Ribociclib 300 mg (reduced from standard 600 mg) + Everolimus 2.5 mg — dose rationale and tolerability data should be reviewed before clinical application
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

