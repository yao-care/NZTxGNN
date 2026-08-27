---
layout: default
title: Ritonavir
parent: 僅模型預測 (L5)
nav_order: 309
evidence_level: L5
indication_count: 3
---

# Ritonavir
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Ritonavir: From HIV-1 Infection to Feline Acquired Immunodeficiency Syndrome

## One-Sentence Summary

Ritonavir is an HIV-1 protease inhibitor used as part of antiretroviral therapy (and as a pharmacokinetic booster in combination regimens); its own approved-indication and formulary records are not available in this evidence pack. The TxGNN model predicts a possible effect on **feline acquired immunodeficiency syndrome (FIV)** — a cat-only retroviral disease — but the only supporting clinical trial is a human HIV-1 study judged **low relevance (Grade C, likely an ontology mismatch)**, and no supporting literature was found. This is a model-prediction-only signal with essentially no direct evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV-1 infection (antiretroviral protease inhibitor) — inferred from trial/mechanistic context; no formal indication record found in the local registry |
| Predicted New Indication | Feline Acquired Immunodeficiency Syndrome (FIV) |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (`original_moa` is a data gap). Based on the contextual information present in this evidence pack, ritonavir is known as an HIV-1 aspartyl protease inhibitor that blocks viral particle maturation, and it is widely used in antiretroviral combination regimens (as reflected in the associated clinical trial, which studies ritonavir-boosted darunavir in HIV-1-infected patients).

The predicted new indication, FIV, is caused by a different lentivirus (feline immunodeficiency virus) that is only distantly related to HIV-1. While both are retroviruses of the same broad family, the structural homology between the FIV protease and the HIV-1 protease is limited, and cross-species inhibitory activity of ritonavir against FIV protease has not been demonstrated in this evidence pack.

The single retrieved clinical trial (NCT02770508) was explicitly graded **Relevance C** by the evidence pipeline, with the reasoning that it is a human HIV-1 trial mistakenly associated with this prediction (likely due to "AIDS" naming overlap between HIV/AIDS and feline "AIDS"), not a genuine FIV study. As such, the mechanistic rationale here is a same-drug-class analogy rather than direct evidence, and should be treated as hypothesis-generating only.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02770508](https://clinicaltrials.gov/study/NCT02770508) | Phase 4 | Completed | 145 | Compared ritonavir-boosted darunavir + lamivudine vs. ritonavir-boosted darunavir + tenofovir/emtricitabine (or lamivudine/tenofovir) in treatment-naïve **human HIV-1** infected adults. **Note: graded low relevance (Grade C)** — this is a human HIV-1 study, not an FIV study; the association is likely a naming-based mismatch and does not constitute direct evidence for feline AIDS. |

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Ritonavir is not currently marketed in New Zealand (0 authorizations on file), so no product license information is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked prediction (FIV) is a veterinary indication with only one associated clinical trial, and that trial is human-focused and graded as low/mismatched relevance — there is no genuine clinical or literature evidence linking ritonavir to FIV. Combined with missing MOA and safety/label data, the evidence base does not support proceeding at this time.

**To proceed, the following is needed:**
- TFDA/regulatory package insert (warnings, contraindications) — currently a blocking data gap
- Confirmed mechanism of action (MOA) data from DrugBank or equivalent source
- Genuine FIV-specific in vitro/in vivo protease-inhibition data (the current trial is not applicable)
- Re-validation of the disease-ontology mapping for this prediction to rule out a naming-based mismatch (human AIDS vs. feline AIDS)
- If pursuing the alternative rank-2 candidate (SIV infection), note it is also an animal-only indication with no human trial data — would require the same veterinary/translational evidence build-out
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

