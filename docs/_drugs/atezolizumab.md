---
layout: default
title: Atezolizumab
parent: 僅模型預測 (L5)
nav_order: 37
evidence_level: L5
indication_count: 10
---

# Atezolizumab
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

# Atezolizumab: From Urothelial Carcinoma to Prostatic Urethra Urothelial Carcinoma

## One-Sentence Summary

Atezolizumab is a humanized anti-PD-L1 monoclonal antibody (checkpoint inhibitor) with established clinical efficacy across multiple solid tumors — including urothelial carcinoma and NSCLC — in global markets, though it is currently not registered in Taiwan.
The TxGNN model predicts it may be effective for **Prostatic Urethra Urothelial Carcinoma**, a high-risk subtype of BCG-unresponsive non-muscle invasive bladder cancer (NMIBC) with prostatic urethral involvement,
with **2 clinical trials** (including 1 completed Phase 2 trial, n=172) currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not registered in Taiwan; globally approved for urothelial carcinoma, NSCLC, and others |
| Predicted New Indication | Prostatic Urethra Urothelial Carcinoma |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L2 |
| Taiwan Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Atezolizumab (brand name: Tecentriq) is an anti-PD-L1 monoclonal antibody that blocks the PD-L1/PD-1 and PD-L1/B7.1 signaling axes, thereby releasing the brake on T-cell anti-tumor immunity. While formal MOA documentation is not available in this Evidence Pack, the drug's mechanism is extensively characterized in the global literature and confirmed by multiple regulatory approvals in the US, EU, and Japan. Urothelial carcinoma is one of the tumor types with the highest PD-L1 expression rates and elevated tumor mutational burden (TMB-H) — both validated predictors of checkpoint inhibitor response — making this drug class biologically well-suited for this indication.

Prostatic urethra urothelial carcinoma sits within the high-risk tier of BCG-unresponsive NMIBC. Prostatic urethral involvement is a formally recognized adverse prognostic feature in NMIBC staging (EAU/AUA guidelines) that dramatically increases progression risk to muscle-invasive disease. For patients who have already failed BCG — the standard first-line intravesical therapy — radical cystectomy has historically been the only curative option. Atezolizumab's potential to offer a bladder-preserving systemic alternative in this population is clinically compelling, and the IMvigor series of trials has established atezolizumab's class-wide activity across the urothelial carcinoma spectrum (bladder, upper tract, urethra).

The most direct evidence comes from NCT02844816 (SWOG S1605), a completed Phase 2 trial that enrolled 172 BCG-unresponsive NMIBC patients and evaluated atezolizumab monotherapy — the precise population that overlaps with prostatic urethral involvement staging. This mechanistic coherence, combined with direct Phase 2 clinical trial data, provides a strong scientific foundation for the TxGNN prediction.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|-----------|--------------|
| [NCT02844816](https://clinicaltrials.gov/study/NCT02844816) | Phase 2 | Completed | 172 | Atezolizumab monotherapy for BCG-unresponsive NMIBC (SWOG S1605). Immunotherapy with this anti-PD-L1 antibody was evaluated to determine its ability to inhibit tumor cell growth and spread in patients with recurrent, BCG-refractory NMIBC — the population directly encompassing prostatic urethral involvement as a high-risk staging feature. |
| [NCT03170960](https://clinicaltrials.gov/study/NCT03170960) | Phase 1b | Active, Not Recruiting | 914 | Cabozantinib + Atezolizumab combination dose-escalation across multiple solid tumors, including an advanced urothelial carcinoma cohort (bladder, renal pelvis, ureter, urethra). Provides safety, PK, and preliminary efficacy data for combination immunotherapy in the broader urothelial carcinoma class; complements monotherapy evidence. |

---

## Taiwan Market Information

Atezolizumab has no approved registrations in Taiwan (TFDA). No authorization records are available for this drug in the local market. The drug is approved in the US (FDA), EU (EMA), and Japan (PMDA) for multiple solid tumor indications.

---

## Cytotoxicity

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted therapy / Immunotherapy — Anti-PD-L1 checkpoint inhibitor (humanized IgG1 monoclonal antibody; not a conventional cytotoxic agent) |
| Myelosuppression Risk | Low — checkpoint inhibitors do not cause classic cytotoxic myelosuppression; immune-mediated cytopenias (e.g., immune thrombocytopenia, aplastic anemia) are rare but possible |
| Emetogenicity Classification | Minimal — IV monoclonal antibody; emetogenic potential is negligible |
| Monitoring Items | LFTs (immune-mediated hepatitis), thyroid function (immune-mediated thyroiditis/hypothyroidism), CBC (rare immune cytopenias), creatinine (immune-mediated nephritis), chest imaging (immune-mediated pneumonitis), blood glucose (immune-mediated diabetes mellitus) |
| Handling Protection | Standard biologic drug handling procedures apply; no specialized cytotoxic drug handling regulations required |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
NCT02844816 (SWOG S1605) — a completed Phase 2 trial with 172 patients — directly addresses BCG-unresponsive NMIBC (the population group encompassing prostatic urethral involvement), providing L2-level evidence for atezolizumab in this setting; the strong biological rationale (high PD-L1 expression and TMB-H in urothelial carcinoma) further supports the feasibility of this application.

**To proceed, the following is needed:**
- Retrieve SWOG S1605 (NCT02844816) final published results: complete response rate, duration of response, and subgroup data specifically isolating patients with prostatic urethral involvement
- Obtain formal MOA documentation from DrugBank or TFDA package insert to complete the mechanistic dossier
- Define PD-L1 and TMB biomarker testing protocols for patient selection and stratification
- Develop a comprehensive immune-related adverse event (irAE) monitoring and management plan: pneumonitis, hepatitis, thyroiditis, nephritis, endocrinopathies
- Initiate TFDA regulatory strategy assessment — Taiwan has no current registration; a new drug application or orphan drug pathway evaluation is required before any clinical use
- Assess reimbursement feasibility via NHI Taiwan given the absence of approved local indication
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

