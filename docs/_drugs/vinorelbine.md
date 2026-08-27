---
layout: default
title: Vinorelbine
parent: 僅模型預測 (L5)
nav_order: 363
evidence_level: L5
indication_count: 10
---

# Vinorelbine
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

# Vinorelbine: From Non-Small Cell Lung Cancer to Ewing Sarcoma

## One-Sentence Summary

Vinorelbine is a semi-synthetic vinca alkaloid whose established use, as reflected throughout the literature evidence in this pack, is chemotherapy for **advanced non-small cell lung cancer (NSCLC)** and metastatic breast cancer.
The TxGNN model predicts it may also be effective for **Ewing Sarcoma**,
with **4 clinical trials** and **5 publications** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Non-Small Cell Lung Cancer (NSCLC) — established use per literature evidence; formal TFDA/Medsafe label text is a confirmed data gap (see below) |
| Predicted New Indication | Ewing Sarcoma |
| TxGNN Prediction Score | 99.999% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action (MOA) data for vinorelbine is not available in structured form from DrugBank (flagged as a High-severity data gap, DG002). Based on known information drawn from the evidence pack itself, vinorelbine is a semi-synthetic vinca alkaloid that binds tubulin and inhibits microtubule polymerization, thereby blocking mitotic spindle formation and arresting cancer cells in mitosis. Its efficacy in NSCLC (and breast cancer) is well documented across dozens of trials and reviews in this pack — it is repeatedly described as "an established treatment for advanced non-small cell lung cancer" and a standard chemotherapy backbone, particularly in combination with platinum agents.

Ewing sarcoma is a highly malignant small round-cell tumor of bone and soft tissue that is characteristically chemosensitive, especially to cytotoxic, microtubule-targeting agents. Vinorelbine combined with cyclophosphamide ("VC regimen") is already an established salvage regimen in pediatric oncology for relapsed/refractory sarcomas, including Ewing sarcoma, rhabdomyosarcoma, osteosarcoma, and neuroblastoma — this is not a novel mechanistic leap but an extension of existing clinical practice.

Mechanistically, the rationale is therefore consistent: a drug whose core activity is anti-microtubule cytotoxicity, already used off-label/on-protocol in pediatric refractory sarcomas via the VC regimen, is a reasonable candidate for further development in Ewing sarcoma specifically. This is supported by a completed Phase II trial in children with recurrent/refractory malignancies (NCT00003234) and a Phase II vinorelbine + cyclophosphamide trial explicitly enrolling Ewing tumor patients (NCT00180947), alongside preclinical work showing synergistic apoptosis induction when vinorelbine is combined with a PLK1 inhibitor in Ewing sarcoma cells (PMID 26260582).

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00003234](https://clinicaltrials.gov/study/NCT00003234) | Phase 2 | Completed | 50 | Phase II study of vinorelbine in children with recurrent or refractory malignancies — direct drug/pediatric-population evidence. |
| [NCT00180947](https://clinicaltrials.gov/study/NCT00180947) | Phase 2 | Unknown | 210 | Vinorelbine + cyclophosphamide in refractory/relapsed rhabdomyosarcoma, soft tissue tumors, **Ewing tumors**, osteosarcoma, neuroblastoma, and medulloblastoma; status is "Unknown" so currency should be verified. |
| [NCT05999994](https://clinicaltrials.gov/study/NCT05999994) | Phase 2 | Recruiting | 105 | CAMPFIRE master protocol for pediatric/young-adult oncology; Ewing sarcoma may be a sub-study, but the vinorelbine arm/sub-protocol is not explicitly confirmed in the title. |
| [NCT06451302](https://clinicaltrials.gov/study/NCT06451302) | N/A | Active, not recruiting | 100 | Multicenter cohort study of risk-stratified treatment outcomes in pediatric Ewing sarcoma (China); observational, not a vinorelbine-specific interventional trial — background epidemiology only. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [22633624](https://pubmed.ncbi.nlm.nih.gov/22633624/) | 2012 | Phase 2 trial report | European Journal of Cancer | Vinorelbine + continuous low-dose oral cyclophosphamide in children/young adults with relapsed or refractory solid tumors; good tolerability, efficacy demonstrated particularly in rhabdomyosarcoma. |
| [12115359](https://pubmed.ncbi.nlm.nih.gov/12115359/) | 2002 | Phase 2 trial report | Cancer | Vinorelbine in previously treated advanced childhood sarcomas; evidence of activity, notably in rhabdomyosarcoma. |
| [37637411](https://pubmed.ncbi.nlm.nih.gov/37637411/) | 2023 | Review | Frontiers in Pharmacology | Comprehensive review of chemotherapeutic drugs for soft tissue sarcomas, including vinorelbine-class agents. |
| [26260582](https://pubmed.ncbi.nlm.nih.gov/26260582/) | 2016 | Preclinical (mechanistic) | International Journal of Cancer | Synergistic induction of apoptosis by PLK1 inhibitor BI 6727 combined with microtubule-interfering drugs (including vinorelbine) specifically in Ewing sarcoma cells. |
| [36451163](https://pubmed.ncbi.nlm.nih.gov/36451163/) | 2022 | Case Report | BMC Urology | Case report/literature review of extraosseous Ewing's sarcoma/pPNET of the kidney; general disease-background reference, not vinorelbine-specific. |

---

## New Zealand Market Information

Vinorelbine currently holds **no authorizations in New Zealand** (market status: not marketed; total licenses: 0). No product records are available to summarize in table form.

---

## Cytotoxicity

Vinorelbine is a conventional cytotoxic chemotherapy agent (vinca alkaloid class), meeting the antineoplastic criteria for this section.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (Vinca alkaloid / anti-microtubule agent) |
| Myelosuppression Risk | High — dose-limiting toxicity for vinorelbine is myelosuppression (notably neutropenia), consistently reported across the included trial and review literature |
| Emetogenicity Classification | Low to moderate (consistent with the vinca alkaloid class) |
| Monitoring Items | CBC with differential (neutropenia surveillance), liver function tests, assessment for peripheral neuropathy, and injection-site monitoring |
| Handling Protection | Yes — vinorelbine is a vesicant and hazardous drug; standard cytotoxic drug handling and extravasation precautions are required |

*Note: No DrugBank-sourced toxicity monograph was available for this candidate; the above is derived from patterns consistently reported in the clinical trial and literature evidence within this pack. Please cross-check against the official package insert once available.*

---

## Safety Considerations

Please refer to the package insert for safety information. A dedicated data gap (DG001, Blocking severity) has been identified: TFDA/Medsafe label warnings and contraindications for vinorelbine are not yet available, which currently blocks formal S1 safety pre-assessment.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic rationale is strong and consistent with existing pediatric oncology practice (vinorelbine + cyclophosphamide as a salvage regimen in relapsed/refractory sarcomas including Ewing sarcoma), and is supported by two completed/ongoing Phase II trials plus preclinical mechanistic data (evidence level L2). However, vinorelbine is not currently marketed in New Zealand, and critical safety documentation (TFDA/Medsafe label) is missing, so progression should proceed only under defined guardrails rather than as an unconditional "Go."

**To proceed, the following is needed:**
- TFDA/Medsafe package insert with warnings, contraindications, and DDI data (DG001 — Blocking)
- Confirmed mechanism-of-action documentation from DrugBank (DG002 — High)
- Clarification of NCT05999994 (CAMPFIRE) sub-protocol status to confirm whether a vinorelbine/Ewing-sarcoma-specific arm exists
- Updated status check on NCT00180947 (currently "Unknown") to assess data currency
- Route-of-administration compatibility assessment (currently unassessed/pending)
- A defined regulatory pathway assessment for New Zealand market entry, given the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

