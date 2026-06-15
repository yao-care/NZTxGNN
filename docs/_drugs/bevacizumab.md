---
layout: default
title: Bevacizumab
parent: 僅模型預測 (L5)
nav_order: 48
evidence_level: L5
indication_count: 10
---

# Bevacizumab
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

# Bevacizumab: From Anti-VEGF Oncology Therapy to Epiglottis Neoplasm

## One-Sentence Summary

Bevacizumab is a recombinant humanized anti-VEGF-A monoclonal antibody globally approved for multiple solid tumors including colorectal cancer, non-small cell lung cancer, ovarian cancer, and glioblastoma.
The TxGNN model ranks **Epiglottis Neoplasm** as the top predicted new indication with a score of 99.90%,
however **no clinical trials** and **no publications** currently support this specific direction.
Given that epiglottis neoplasms are predominantly benign lesions with no anti-angiogenic evidence base, a **Hold** decision is recommended.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Anti-VEGF oncology therapy (colorectal cancer, NSCLC, ovarian cancer, glioblastoma, etc.) — specific indication data not recorded in this evidence pack |
| Predicted New Indication | Epiglottis Neoplasm |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known information, Bevacizumab is a recombinant humanized IgG1 monoclonal antibody that specifically binds to and neutralizes vascular endothelial growth factor A (VEGF-A), thereby inhibiting tumor-associated neovascularization. By starving tumors of their blood supply, it suppresses growth and metastatic spread across multiple solid tumor types. Its efficacy in colorectal cancer, ovarian cancer, and glioblastoma has been clinically validated, and mechanistically its anti-angiogenic action could theoretically apply to any highly vascularized neoplasm.

From an anatomical and biological perspective, the epiglottis is part of the laryngeal/hypopharyngeal region, which belongs to the head and neck. Head and neck tumors are known to overexpress VEGF, and anti-angiogenic strategies have been explored in head and neck squamous cell carcinoma (HNSCC). A mechanistic rationale therefore exists for malignant epiglottic tumors. However, the critical qualifier is that the predicted indication — **epiglottis neoplasm** — encompasses predominantly **benign** lesions such as cysts, fibromas, and papillomas, where aggressive systemic anti-VEGF therapy carries a clearly unfavorable risk-benefit profile.

No clinical trials or published literature specifically investigate Bevacizumab for epiglottis neoplasm, benign or malignant. Until direct clinical evidence is generated for this rare and predominantly benign anatomical site, the TxGNN prediction remains at the model-prediction-only tier (L5) and cannot be translated into a clinical repurposing recommendation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for epiglottis neoplasm.

---

## Literature Evidence

Currently no related literature available for epiglottis neoplasm.

---

## New Zealand Market Information

Bevacizumab is currently **not registered** in New Zealand based on the regulatory data in this evidence pack (0 licenses on record). No authorization table can be generated.

> **Note:** This finding may reflect a data pipeline gap. Bevacizumab (Avastin®) has received regulatory approval in multiple major markets (USA, EU, Japan, Australia). Verification against the Medsafe database directly is recommended before drawing conclusions about the New Zealand registration status.

---

## Cytotoxicity

Bevacizumab is a targeted antineoplastic agent (monoclonal antibody), meeting the cytotoxicity section criteria based on its oncology indication and drug class.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy — Anti-VEGF monoclonal antibody (not a conventional cytotoxic agent) |
| Myelosuppression Risk | Low — Bevacizumab does not directly cause myelosuppression; haematological toxicity is primarily associated with co-administered chemotherapy |
| Emetogenicity Classification | Low (monoclonal antibody; emetogenic risk is minimal when used as monotherapy) |
| Monitoring Items | Blood pressure (hypertension is the most common AE), urinalysis for proteinuria, wound healing assessment, thromboembolic event surveillance, gastrointestinal perforation signs |
| Handling Protection | Follows standard biologic/monoclonal antibody preparation guidelines; does not require cytotoxic drug handling precautions (e.g., closed-system transfer devices not mandated) in most jurisdictions, but institutional SOPs should be consulted |

---

## Safety Considerations

Please refer to the package insert for safety information. Key warnings, contraindications, and drug interaction data were not available in this evidence pack and must be sourced directly from the approved labelling before any clinical use.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a high TxGNN model prediction score (99.90%), the absence of any clinical trial or published literature evidence for Bevacizumab in epiglottis neoplasm places this candidate firmly at Evidence Level L5 — model prediction only. The predominantly benign nature of epiglottis neoplasms further undermines the clinical rationale for systemic anti-angiogenic therapy in this setting.

**Contextual note:** Among all 10 ranked predictions in this evidence pack, **Cystic Neoplasm (Rank 7)** carries the strongest evidence base: 1 large Phase III RCT (NCT00565851, n=1,052), multiple Phase II trials, a systematic review (PMID 37754507), and retrospective cohorts, yielding Evidence Level L2 and a "Proceed with Guardrails" recommendation. If prioritization of Bevacizumab repurposing candidates is the goal, Cystic Neoplasm (particularly ovarian low-grade serous carcinoma and pseudomyxoma peritonei) is the most actionable direction in this pack.

**To proceed with the epiglottis neoplasm candidate, the following is needed:**
- Histopathological clarification: confirm whether the target population is a **malignant** epiglottic tumour (squamous cell carcinoma, adenocarcinoma), not benign disease
- Literature search specifically for Bevacizumab in supraglottic/epiglottic malignancy or laryngeal VEGF-driven tumours
- Mechanism of action data retrieval from DrugBank API (DG002 remediation)
- Safety profile from TFDA/Medsafe package insert (DG001 remediation) — currently a Blocking data gap
- New Zealand regulatory status verification via Medsafe database direct query
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

