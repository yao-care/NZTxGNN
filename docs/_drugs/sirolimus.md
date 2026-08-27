---
layout: default
title: Sirolimus
parent: 僅模型預測 (L5)
nav_order: 322
evidence_level: L5
indication_count: 10
---

# Sirolimus
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

# Sirolimus: From Renal Transplant Rejection Prevention to Liposarcoma

## One-Sentence Summary

> Sirolimus (rapamycin) is an mTOR inhibitor originally used as an immunosuppressant to prevent organ rejection after renal transplantation.
> The TxGNN model predicts it may be effective for **Liposarcoma**,
> with **5 clinical trials** and **12 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Renal transplant rejection prophylaxis (immunosuppressant) |
| Predicted New Indication | Liposarcoma |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known information, sirolimus is an mTOR (mammalian target of rapamycin) inhibitor, and its efficacy as an immunosuppressant in preventing renal transplant rejection has been well established for over two decades.

Dedifferentiated liposarcoma frequently shows activation of the Akt-mTOR and MAPK signalling pathways (PMID 26518767), which provides a direct mechanistic rationale for using an mTOR inhibitor such as sirolimus in this tumour type. This is reinforced by a growing body of evidence for the mTOR-inhibitor class in sarcomas more broadly: temsirolimus (sirolimus' ester prodrug), everolimus, and ridaforolimus have all shown activity in advanced sarcoma trials.

Importantly, sirolimus itself (not just its analogs) has already been tested directly in a completed Phase 2 trial combined with cyclophosphamide in metastatic/unresectable myxoid liposarcoma and chondrosarcoma (NCT02821507), which is the strongest piece of direct evidence supporting this repurposing signal. However, most of the remaining trial evidence involves related rapalogs rather than sirolimus itself, so the overall efficacy signal is moderate and mainly seen in combination regimens rather than as monotherapy.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02821507](https://clinicaltrials.gov/study/NCT02821507) | Phase 2 | Completed | 70 | Sirolimus + cyclophosphamide in metastatic/unresectable myxoid liposarcoma and chondrosarcoma; tests mTOR inhibition to prevent tumour growth |
| [NCT00949325](https://clinicaltrials.gov/study/NCT00949325) | Phase 1/2 | Completed | 24 | Torisel (temsirolimus, sirolimus ester prodrug) + liposomal doxorubicin in advanced soft tissue and bone sarcomas |
| [NCT01614795](https://clinicaltrials.gov/study/NCT01614795) | Phase 2 | Completed | 46 | Cixutumumab + temsirolimus in pediatric recurrent/refractory sarcoma |
| [NCT00093080](https://clinicaltrials.gov/study/NCT00093080) | Phase 2 | Completed | 216 | AP23573 (ridaforolimus), an mTOR inhibitor, in advanced sarcoma |
| [NCT03114527](https://clinicaltrials.gov/study/NCT03114527) | Phase 2 | Active, not recruiting | 48 | Ribociclib + everolimus in advanced dedifferentiated liposarcoma and leiomyosarcoma |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37967116](https://pubmed.ncbi.nlm.nih.gov/37967116/) | 2024 | Phase 2 trial report | Clin Cancer Res | Ribociclib + everolimus in dedifferentiated liposarcoma and leiomyosarcoma; synergistic mTOR/CDK4-6 targeting |
| [39796641](https://pubmed.ncbi.nlm.nih.gov/39796641/) | 2024 | Review | Cancers | Overview of novel therapeutics in soft tissue sarcoma including mTOR-pathway agents |
| [26518767](https://pubmed.ncbi.nlm.nih.gov/26518767/) | 2016 | Mechanistic/Translational | Tumour Biology | Activation of Akt-mTOR and MAPK pathways in dedifferentiated liposarcomas |
| [37400145](https://pubmed.ncbi.nlm.nih.gov/37400145/) | 2023 | Preclinical (PDX) | Cancer Genomics & Proteomics | Chloroquine + rapamycin combination effective in well-differentiated liposarcoma PDOX model |
| [36309387](https://pubmed.ncbi.nlm.nih.gov/36309387/) | 2022 | Preclinical (PDX) | In Vivo | Chloroquine + rapamycin arrests tumour growth in dedifferentiated liposarcoma PDOX model |
| [16434506](https://pubmed.ncbi.nlm.nih.gov/16434506/) | 2006 | Cohort (indirect epidemiology) | J Am Soc Nephrol | Sirolimus after cyclosporine withdrawal reduces cancer risk in renal transplant recipients |
| [37222206](https://pubmed.ncbi.nlm.nih.gov/37222206/) | 2023 | Review | Curr Opin Oncol | Rationale and trial results for molecular-targeted agents in advanced sarcomas |
| [26093731](https://pubmed.ncbi.nlm.nih.gov/26093731/) | 2015 | Cohort | Transplant Proc | Cancer screening in renal transplant patients on long-term immunosuppressive therapy |
| [25519700](https://pubmed.ncbi.nlm.nih.gov/25519700/) | 2015 | Preclinical | Mol Cancer Ther | MLN0128, an ATP-competitive mTOR kinase inhibitor, active in bone and soft-tissue sarcoma models |
| [20497911](https://pubmed.ncbi.nlm.nih.gov/20497911/) | 2010 | Review | Bull Cancer | Targeted treatment of rare connective tissue tumors and sarcomas |

---

## New Zealand Market Information

Sirolimus currently has no market authorizations registered in New Zealand (0 licenses; market status: 未上市／Not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A completed Phase 2 trial testing sirolimus itself (not only its analogs) in myxoid liposarcoma/chondrosarcoma, combined with a plausible Akt-mTOR mechanistic rationale and supportive class-wide rapalog data, justifies further investigation — but the drug is not currently marketed in New Zealand and a Blocking safety data gap (TFDA/package insert warnings and contraindications) must be resolved before any clinical use is considered.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert data on warnings and contraindications (Blocking gap, DG001)
- Confirmed mechanism-of-action and DrugBank classification data (High priority gap, DG002)
- Assessment of the regulatory pathway for New Zealand market entry, since sirolimus is not currently marketed there
- Additional randomized, sirolimus-specific (not just rapalog-class) trial data in liposarcoma to raise the evidence level beyond L2
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

