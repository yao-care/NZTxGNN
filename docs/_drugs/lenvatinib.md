---
layout: default
title: Lenvatinib
parent: 僅模型預測 (L5)
nav_order: 200
evidence_level: L5
indication_count: 10
---

# Lenvatinib
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

# Lenvatinib: From Thyroid Cancer to Liposarcoma

## One-Sentence Summary

Lenvatinib is a multi-target tyrosine kinase inhibitor originally developed for thyroid cancer and later extended to hepatocellular and renal cell carcinoma.
The TxGNN model predicts it may be effective for **Liposarcoma**, most notably in combination with eribulin,
with **1 completed clinical trial** and **4 supporting publications** currently backing this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Thyroid cancer (differentiated thyroid carcinoma) — no New Zealand license record on file to confirm official indication text |
| Predicted New Indication | Liposarcoma |
| TxGNN Prediction Score | 99.51% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed original mechanism-of-action documentation for this candidate is a flagged data gap. Based on known information drawn from the evidence pack's own rationale, Lenvatinib is a multi-target tyrosine kinase inhibitor acting on VEGFR1-3, FGFR1-4, PDGFRα, KIT, and RET, giving it a broad anti-angiogenic effect that underlies its established use in vascular-dependent solid tumors such as thyroid, hepatocellular, and renal cell carcinoma.

Liposarcoma and leiomyosarcoma are soft-tissue sarcomas with rich tumor vasculature and stroma, making them mechanistically plausible targets for an anti-angiogenic agent. The strongest evidence here is not for Lenvatinib monotherapy but for the combination of Lenvatinib with eribulin (a microtubule inhibitor), where the two agents act through complementary mechanisms — anti-angiogenesis plus mitotic disruption.

This mechanistic complementarity is further supported by biomarker research showing CDK4 amplification as a recurring molecular feature of dedifferentiated liposarcoma, offering a potential basis for future biomarker-guided patient selection. Overall, the rationale is reasonable but is specific to the Lenvatinib+eribulin combination rather than Lenvatinib used alone.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03526679](https://clinicaltrials.gov/study/NCT03526679) | Phase 1/2 | Completed | 30 | Single-arm study of Lenvatinib (anti-angiogenic) plus eribulin (chemotherapy targeting mitosis) in inoperable/metastatic adipocytic sarcoma and leiomyosarcoma; relevance graded A — direct evaluation of the combination in advanced liposarcoma with completed enrollment |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36129471](https://pubmed.ncbi.nlm.nih.gov/36129471/) | 2022 | Phase 1/2 (single-arm) | Clinical Cancer Research | Primary publication of the LEADER study (NCT03526679) reporting safety and efficacy of lenvatinib plus eribulin in advanced liposarcoma and leiomyosarcoma |
| [39103896](https://pubmed.ncbi.nlm.nih.gov/39103896/) | 2024 | Preclinical/Biomarker | Experimental Hematology & Oncology | CDK4 identified as a prognostic biomarker in soft tissue sarcoma, with synergistic effect of CDK4 inhibition in sequential treatment of dedifferentiated liposarcoma |
| [29848686](https://pubmed.ncbi.nlm.nih.gov/29848686/) | 2018 | Preclinical | Anticancer Research | Broad-spectrum preclinical activity of eribulin in combination with mechanistically different anticancer agents, including anti-angiogenic agents, across tumor models including liposarcoma |
| [34326745](https://pubmed.ncbi.nlm.nih.gov/34326745/) | 2021 | Case Report | Case Reports in Oncology | Individualized treatment (targeted therapy + surgery + chemotherapy) achieved notable tumor size reduction in a dedifferentiated liposarcoma patient with lung/abdominal metastasis |

---

## New Zealand Market Information

Lenvatinib is currently **not marketed in New Zealand** — no product licenses are on file (total authorizations: 0).

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (multi-target tyrosine kinase inhibitor; not a conventional cytotoxic chemotherapy agent) |
| Myelosuppression Risk | Not available in current data set; please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Not available in current data set; please refer to the package insert warnings and precautions |
| Monitoring Items | Not specified in current data set; TKI-class agents typically require monitoring of blood pressure, renal/hepatic function, thyroid function, and proteinuria — please refer to the package insert |
| Handling Protection | Not available in current data set; please refer to the package insert and institutional hazardous drug handling protocols |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A completed Phase 1/2 trial (n=30) plus four supporting publications establish a reasonable, though combination-specific (Lenvatinib+eribulin), rationale for liposarcoma — sufficient to advance under guardrails, but not sufficient to bypass a formal safety review, since New Zealand-specific labeling data (TFDA-equivalent warnings/contraindications) is a **Blocking** data gap that currently prevents entry into the S1 safety initial assessment.

**To proceed, the following is needed:**
- Package insert warnings, contraindications, and drug interaction data (currently a Blocking data gap)
- Confirmed original mechanism-of-action documentation (currently a High-severity data gap)
- Clarification that predicted efficacy applies to the Lenvatinib+eribulin combination, not Lenvatinib monotherapy
- Regulatory pathway assessment given the drug is not currently marketed in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

