---
layout: default
title: Posaconazole
parent: 僅模型預測 (L5)
nav_order: 280
evidence_level: L5
indication_count: 1
---

# Posaconazole
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Posaconazole: From [Indication Not on File] to Pneumocystosis

## One-Sentence Summary

Posaconazole is a broad-spectrum triazole antifungal agent; this evidence pack does not contain its original approved indication or mechanism-of-action data. The TxGNN model predicts it may be effective for **Pneumocystosis**, currently supported by **2 clinical trials** and **5 publications**, though none directly test posaconazole against this indication.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in evidence pack (no license or original-indication data on file) |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 99.77% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for posaconazole in this evidence pack. Based on general pharmacological knowledge, posaconazole is a broad-spectrum triazole antifungal that inhibits fungal 14-α-demethylase (Erg11/Cyp51), blocking ergosterol synthesis in the fungal cell membrane. It is established for prophylaxis and treatment of invasive fungal infections — notably *Aspergillus* and *Candida* species — in immunocompromised patients such as allogeneic haematopoietic stem cell transplant (HSCT) recipients and neutropenic patients.

Pneumocystosis (*Pneumocystis jirovecii* pneumonia) occurs in the same high-risk immunocompromised population — HSCT recipients, GVHD patients, and those on prolonged immunosuppression — that posaconazole's established antifungal-prophylaxis role already targets. This overlap in patient population is the main mechanistic rationale surfaced by the supporting literature.

However, the connection is largely indirect: the strongest clinical trial identified (NCT04368559) evaluates **rezafungin** against a "standard antimicrobial regimen" for invasive fungal disease prevention post-transplant, with posaconazole plausibly representing part of that standard-of-care comparator rather than being the tested intervention. *Pneumocystis* also has atypical fungal cell-wall/membrane biology (limited ergosterol dependence in some life stages), which is why azoles are not first-line PCP therapy in current clinical practice. This mechanistic caveat should be weighed against the TxGNN score.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04368559](https://clinicaltrials.gov/study/NCT04368559) | Phase 3 | Completed | 602 | Compared IV rezafungin vs. standard antimicrobial regimen for prevention of invasive fungal disease in allogeneic HSCT recipients; posaconazole is a likely component of the standard-of-care comparator arm rather than the studied agent |
| [NCT06859424](https://clinicaltrials.gov/study/NCT06859424) | Phase 2 | Recruiting | 358 | Platform trial comparing post-transplant cyclophosphamide-based GVHD prophylaxis regimens in mismatched unrelated donor PBSC transplant; not a direct posaconazole efficacy trial |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35596686](https://pubmed.ncbi.nlm.nih.gov/35596686/) | 2022 | Observational (retrospective) | Transplant Infectious Disease | Reviewed infectious complications, including fungal infection, in acute GVHD after liver transplantation at Mayo Clinic |
| [41232547](https://pubmed.ncbi.nlm.nih.gov/41232547/) | 2025 | Guideline/Review | The Lancet Infectious Diseases | UK best-practice update on diagnosis of serious fungal diseases, including Pneumocystis, reflecting shift to non-culture-based testing |
| [41362140](https://pubmed.ncbi.nlm.nih.gov/41362140/) | 2025 | Guideline/Review | Chinese Journal of Tuberculosis and Respiratory Diseases | 2025 clinical practice guideline for diagnosis/management of invasive pulmonary fungal disease |
| [26901377](https://pubmed.ncbi.nlm.nih.gov/26901377/) | 2016 | Review | Swiss Medical Weekly | Overview of invasive candidiasis, aspergillosis, cryptococcosis, and Pneumocystis pneumonia; notes posaconazole's role in reducing invasive candidiasis via antifungal prophylaxis |
| [21973267](https://pubmed.ncbi.nlm.nih.gov/21973267/) | 2011 | Review (pharmacokinetics) | Clinical Pharmacokinetics | Reviews pulmonary epithelial lining fluid penetration of antifungal and other anti-infective agents |

## New Zealand Market Information

No New Zealand market authorizations are on file — posaconazole is not currently marketed in New Zealand under this evidence pack (0 licenses recorded).

## Safety Considerations

Please refer to the package insert for safety information. No warnings, contraindications, or drug-interaction data were returned for posaconazole in this evidence pack (DDI query status: not found).

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The clinical trial and literature evidence linking posaconazole specifically to Pneumocystosis is indirect (posaconazole appears as a likely standard-of-care comparator, not the studied intervention), and mechanistic plausibility is uncertain given azoles' limited role in PCP treatment.
- A blocking data gap exists: no TFDA/package-insert safety data (warnings, contraindications, DDI) is available, so the candidate cannot pass initial safety screening (S1), and the drug is not currently marketed in New Zealand.

**To proceed, the following is needed:**
- TFDA package insert (warnings/contraindications) — blocking gap, obtain via TFDA official site
- Confirmed mechanism of action from DrugBank
- Direct evidence (trial or case series) of posaconazole efficacy specifically against *Pneumocystis jirovecii*, rather than as part of a broader prophylaxis regimen comparator
- Confirmation of original approved indication(s) and any existing regulatory filings, given none are currently on file
- New Zealand regulatory pathway assessment, since the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

