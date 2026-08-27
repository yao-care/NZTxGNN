---
layout: default
title: Mycophenolate Mofetil
parent: 僅模型預測 (L5)
nav_order: 236
evidence_level: L5
indication_count: 10
---

# Mycophenolate Mofetil
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

Using the report template above (no additional codebase skill applies — this is a direct content-generation task), here is the evaluation report:

---

# Mycophenolate Mofetil: From Transplant Rejection Prophylaxis to HIV Infectious Disease

## One-Sentence Summary

Mycophenolate mofetil (MMF) is an immunosuppressant internationally used to prevent organ transplant rejection. The TxGNN model predicts it may have adjunctive benefit in **HIV infectious disease**, with **10 clinical trials** and **20 publications** currently associated with this direction, though the strongest direct evidence is limited to a single completed Phase 2 pilot RCT and several small cohort/pharmacokinetic studies.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in New Zealand regulatory data (drug not marketed); internationally used as an immunosuppressant for prevention of organ transplant rejection |
| Predicted New Indication | HIV infectious disease |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L2 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data from DrugBank is not currently available for this evidence pack. Based on the mechanistic rationale extracted from the underlying evidence, MMF inhibits inosine monophosphate dehydrogenase (IMPDH), depleting the guanosine nucleotide pool inside activated T cells. Since HIV preferentially replicates in activated CD4+ T cells, this depletion may reduce the pool of susceptible target cells and dampen chronic immune hyperactivation — a recognized driver of HIV disease progression — giving MMF a plausible, if indirect, antiretroviral-adjunct rationale.

The connection between the drug's established use (transplant immunosuppression) and the predicted indication (HIV infection) is also clinically grounded: MMF is already the standard post-transplant immunosuppressant used in HIV-positive organ transplant recipients, so its pharmacology and safety behavior in HIV-positive populations is not novel. Multiple cohort and pharmacokinetic studies (e.g., PMID 12352149, 15355127) further support a mechanistic interaction with nucleoside reverse transcriptase inhibitors such as abacavir, where MMF-induced dGTP depletion appears to potentiate antiretroviral activity in vitro and in small clinical cohorts.

However, this remains a research hypothesis rather than an established therapeutic use: the only completed randomized trial (NCT00038272) is a small Phase 2 pilot, one Phase I/II study was withdrawn with zero enrollment (NCT00021489), and a related Phase 3/Phase 4 trial remains in "Unknown" status. The mechanism is biologically plausible but risk-benefit trade-offs between immunosuppression and antiviral effect require careful evaluation.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00120419](https://clinicaltrials.gov/study/NCT00120419) | Phase 4 | Unknown | 90 | MAN2 study: evaluates whether MMF treats chronic immune hyperactivation and preserves CD4+ T-cell count in chronically HIV-1-infected, ART-naive patients; also assesses plasma HIV-1 RNA and disease progression |
| [NCT00247494](https://clinicaltrials.gov/study/NCT00247494) | Phase 4 | Unknown | 90 | Substudy of MAN2; evaluates effects of MMF on cardiovascular surrogate markers in HIV-1-infected patients |
| [NCT00021489](https://clinicaltrials.gov/study/NCT00021489) | Phase 1/2 | Withdrawn | 0 | Intended to assess safety, tolerability and antiretroviral activity of MMF added to abacavir in treatment-experienced HIV patients; withdrawn with zero enrollment, no usable data |
| [NCT00038272](https://clinicaltrials.gov/study/NCT00038272) | Phase 2 | Completed | 56 | Randomized, double-blind, placebo-controlled pilot comparing DAPD vs. DAPD+MMF added to antiretroviral regimens in treatment-experienced HIV patients |
| [NCT00009009](https://clinicaltrials.gov/study/NCT00009009) | Phase 2 | Completed | 10 | Safety/efficacy of renal transplantation (with MMF as standard post-transplant immunosuppressant) in HIV-infected patients with end-stage renal disease |
| [NCT01453192](https://clinicaltrials.gov/study/NCT01453192) | Phase 3 | Completed | 27 | Multicenter follow-up of renal transplant outcomes in HIV-1-infected patients under a raltegravir-based regimen; MMF used as standard immunosuppression, not as HIV therapy |
| [NCT00112593](https://clinicaltrials.gov/study/NCT00112593) | N/A | Completed | 5 | Allogeneic stem cell transplant with post-transplant cyclosporine/MMF to induce mixed chimerism in HIV-1-infected patients, with or without malignancy |
| [NCT01288131](https://clinicaltrials.gov/study/NCT01288131) | Phase 3 | Terminated | 8 | RCT of cyclosporine+MMF vs. cyclophosphamide+prednisolone for anti-EPO-associated PRCA; not an HIV trial, included only via TxGNN drug-level linkage |
| [NCT02793544](https://clinicaltrials.gov/study/NCT02793544) | Phase 2 | Completed | 80 | HLA-mismatched unrelated donor bone marrow transplant with post-transplant cyclophosphamide, sirolimus and MMF for GVHD prophylaxis in hematologic malignancies; not an HIV trial |
| [NCT06869265](https://clinicaltrials.gov/study/NCT06869265) | Phase 2 | Recruiting | 56 | Thiotepa/busulfan/fludarabine conditioning for haplo-HSCT in elderly high-risk AML patients; not an HIV trial, included only via TxGNN drug-level linkage |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [17017956](https://pubmed.ncbi.nlm.nih.gov/17017956/) | 2006 | Review | Current Topics in Medicinal Chemistry | Reviews immunosuppressive drugs, including MMF, as strategies to target chronic immune hyperactivation in HIV disease progression |
| [15213566](https://pubmed.ncbi.nlm.nih.gov/15213566/) | 2004 | RCT (randomized pilot) | Journal of Acquired Immune Deficiency Syndromes | Randomized pilot study of MMF's effect on immune response and viral load during and after HAART interruption in chronic HIV infection |
| [16379601](https://pubmed.ncbi.nlm.nih.gov/16379601/) | 2005 | Cohort | AIDS Research and Human Retroviruses | Found no detrimental immunological effects when combining MMF with HAART in treatment-naive acute/chronic HIV-1 patients |
| [15871638](https://pubmed.ncbi.nlm.nih.gov/15871638/) | 2005 | Cohort | Clinical Pharmacokinetics | PK/PD study of low-dose MMF combined with abacavir, efavirenz and nelfinavir in HIV-infected patients |
| [15355127](https://pubmed.ncbi.nlm.nih.gov/15355127/) | 2004 | Cohort | Clinical Pharmacokinetics | MMF's effect on antiretroviral pharmacokinetics and intracellular nucleoside triphosphate pools |
| [12352149](https://pubmed.ncbi.nlm.nih.gov/12352149/) | 2002 | Cohort | Journal of Acquired Immune Deficiency Syndromes | Adding MMF to abacavir-containing ART depleted intracellular dGTP and decreased plasma HIV-1 RNA in 5 heavily treated patients |
| [15353978](https://pubmed.ncbi.nlm.nih.gov/15353978/) | 2004 | Cohort | AIDS | Compared HAART with vs. without MMF on plasma HIV-1 RNA decay rate and latent reservoir in treatment-naive patients |
| [11391161](https://pubmed.ncbi.nlm.nih.gov/11391161/) | 2001 | Cohort | Journal of Acquired Immune Deficiency Syndromes | Pilot study of MMF as a component of multidrug-resistant HIV-1 therapy in 7 heavily pretreated AIDS patients |
| [17885292](https://pubmed.ncbi.nlm.nih.gov/17885292/) | 2007 | Cohort | AIDS | Evaluated safety, tolerability and antiretroviral activity of DAPD with or without MMF in drug-resistant HIV infection |
| [16515490](https://pubmed.ncbi.nlm.nih.gov/16515490/) | 2006 | Review | Current Pharmaceutical Design | Reviews "virostatic" agents, including MMF, as a strategy to target residual HIV viremia despite HAART |

---

## New Zealand Market Information

Mycophenolate mofetil is not currently marketed in New Zealand, and no authorization records are available in the regulatory data reviewed.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Although the IMPDH-inhibition mechanism offers a biologically plausible rationale for an HIV adjunct role, and one completed Phase 2 pilot RCT plus several cohort/pharmacokinetic studies support feasibility, the evidence remains preliminary (L2) — several related trials were withdrawn, terminated, or left in unknown status. Critically, official safety labeling (warnings/contraindications) is a **blocking data gap**, and the drug is not currently marketed in New Zealand, so a full safety assessment cannot yet proceed.

**To proceed, the following is needed:**
- Official package insert / prescribing information (warnings, contraindications) — currently a blocking gap preventing initial safety review
- Confirmed mechanism-of-action documentation from DrugBank
- Updated status/results from the ongoing/unknown-status MAN2 trials (NCT00120419, NCT00247494)
- A larger, adequately powered controlled trial specifically testing MMF as HIV adjunct therapy, given the existing Phase 2 evidence is a small pilot
- Assessment of New Zealand market access or import pathway, since the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

