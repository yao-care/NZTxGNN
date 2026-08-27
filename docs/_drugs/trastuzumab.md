---
layout: default
title: Trastuzumab
parent: 僅模型預測 (L5)
nav_order: 348
evidence_level: L5
indication_count: 10
---

# Trastuzumab
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

# Trastuzumab: From HER2-Positive Breast Cancer to Normal Breast-Like Subtype of Breast Carcinoma

## One-Sentence Summary

Trastuzumab is a HER2-targeted monoclonal antibody originally developed for HER2-positive breast cancer.
The TxGNN model predicts it may also be relevant for the **normal breast-like subtype of breast carcinoma**,
but this is currently supported by only **12 clinical trials** (mostly indirect, HER2-positive-population trials) and **1 publication** (a descriptive/morphological study).

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HER2-positive breast cancer (based on established drug knowledge; not present in the supplied regulatory dataset) |
| Predicted New Indication | Normal breast-like subtype of breast carcinoma |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the source dataset (flagged as a data gap). Based on known pharmacology, trastuzumab is a humanized monoclonal antibody that binds the extracellular domain of the HER2/neu receptor, inhibiting proliferation of HER2-overexpressing tumor cells and inducing antibody-dependent cellular cytotoxicity (ADCC). Its efficacy in HER2-positive breast cancer is well established, which is the mechanistic basis TxGNN likely used to generate this prediction.

However, "normal breast-like" is a PAM50 intrinsic molecular subtype that is typically characterized by low proliferation and inconsistent — often negative — HER2 expression. This creates a direct mechanistic mismatch: trastuzumab's activity depends on HER2 overexpression, which is not a defining feature of this subtype.

Consistent with this, most of the clinical trials retrieved for this prediction actually enrolled HER2-positive breast cancer populations undergoing neoadjuvant therapy, rather than specifically validating trastuzumab in patients confirmed to have the normal breast-like subtype. The mechanistic link should therefore be regarded as an indirect inference from the broader HER2-positive breast cancer literature, not a direct, subtype-specific validation.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03168880](https://clinicaltrials.gov/study/NCT03168880) | Phase 3 | Active, not recruiting | 720 | Randomized comparison of neoadjuvant weekly paclitaxel vs. paclitaxel plus carboplatin in triple-negative breast cancer; large RCT relevant to the basal-like/normal-like subtype context, though not a trastuzumab-specific arm. |
| [NCT01796197](https://clinicaltrials.gov/study/NCT01796197) | Phase 2 | Completed | 23 | Paclitaxel combined with trastuzumab and pertuzumab as preoperative therapy for inflammatory breast cancer; trastuzumab used directly but not subtype-specific. |
| [NCT04329065](https://clinicaltrials.gov/study/NCT04329065) | Phase 2 | Recruiting | 25 | WOKVAC vaccine combined with neoadjuvant chemotherapy and HER2-targeted antibody therapy; trastuzumab is background treatment, not the primary study variable. |
| [NCT04759248](https://clinicaltrials.gov/study/NCT04759248) | Phase 2 | Active, not recruiting | 55 | ATREZZO study — atezolizumab combined with trastuzumab and vinorelbine in ER-negative or PAM50 non-luminal HER2-positive advanced/metastatic breast cancer. |
| [NCT05900206](https://clinicaltrials.gov/study/NCT05900206) | Phase 2 | Recruiting | 370 | ARIADNE — randomized trial of trastuzumab deruxtecan (an ADC, not the same molecule) vs. standard neoadjuvant treatment with biomarker-driven selection for HER2-positive breast cancer. |
| [NCT06348134](https://clinicaltrials.gov/study/NCT06348134) | Phase 2 | Recruiting | 74 | Efficacy and safety of optimal neoadjuvant-to-adjuvant anti-HER2-based therapy in Nigerian women with HER2-positive breast cancer. |
| [NCT04750122](https://clinicaltrials.gov/study/NCT04750122) | Phase 1/2 | Recruiting | 46 | Neoadjuvant therapy guided by in vitro drug screening of patient-derived tumor-like cell clusters for HER2-positive early breast cancer; not a direct efficacy test of trastuzumab in this subtype. |
| [NCT06585969](https://clinicaltrials.gov/study/NCT06585969) | Phase 3 | Withdrawn | 0 | Trastuzumab deruxtecan vs. CDK4/6 inhibitors in non-Luminal A, ER-positive/HER2-low metastatic breast cancer; trial withdrawn with zero enrollment, provides no evidence. |
| [NCT06328387](https://clinicaltrials.gov/study/NCT06328387) | Phase 1/2 | Unknown | 120 | Hydroxychloroquine combined with an antibody-drug conjugate vs. ADC alone for advanced breast cancer; mechanistic rationale unclear. |
| [NCT01670877](https://clinicaltrials.gov/study/NCT01670877) | Phase 2 | Completed | 56 | Neratinib alone and combined with fulvestrant in metastatic HER2 non-amplified but HER2-mutant breast cancer; population definition conflicts with trastuzumab's mechanism of action. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [19466513](https://pubmed.ncbi.nlm.nih.gov/19466513/) | 2009 | Descriptive/Morphology study | Breast cancer (Tokyo, Japan) | Describes morphological and cytopathological features of the basal-like breast carcinoma subtype, situating it among the five DNA-microarray-defined intrinsic subtypes (luminal A, luminal B, normal breast-like, HER2-overexpression, basal-like); does not directly assess trastuzumab efficacy. |

## New Zealand Market Information

Trastuzumab currently has no marketing authorization on record in New Zealand (market status: Not Marketed; 0 authorizations).

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (HER2-directed monoclonal antibody; not a conventional cytotoxic chemotherapeutic) |
| Myelosuppression Risk | Low as monotherapy; risk increases when combined with cytotoxic chemotherapy (e.g., taxanes, as seen in several trials above) |
| Emetogenicity Classification | Low (typical for monoclonal antibody monotherapy) |
| Monitoring Items | Cardiac function (LVEF/echocardiogram) is the primary monitoring parameter given trastuzumab's known cardiotoxicity risk; infusion-related reaction monitoring; CBC and organ function monitoring when combined with cytotoxic chemotherapy |
| Handling Protection | Standard biologic/monoclonal antibody handling precautions apply; detailed institutional handling requirements should follow the package insert, as formal safety labeling data is currently a blocking data gap (DG001) |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence specific to the normal breast-like subtype is indirect — nearly all identified trials enrolled HER2-positive populations broadly rather than validating efficacy in patients confirmed to have this subtype, and literature support is limited to a single descriptive morphology paper. Since this subtype is typically HER2-low/negative, the mechanistic rationale for trastuzumab is uncertain.

**To proceed, the following is needed:**
- Subtype-specific HER2 expression/amplification data confirming target presence in normal breast-like tumors
- TFDA/regulatory safety labeling data (currently a blocking data gap, DG001)
- Detailed mechanism of action documentation (currently a data gap, DG002)
- A clinical trial or biomarker study that directly stratifies outcomes by PAM50 normal breast-like subtype
- Note: within this same evidence pack, the closely related indications **progesterone-receptor positive breast cancer** and **progesterone-receptor negative breast cancer** carry substantially stronger evidence (L1, "Proceed with Guardrails") and may warrant separate, prioritized evaluation as the more actionable repurposing opportunities for trastuzumab.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

