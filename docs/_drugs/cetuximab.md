---
layout: default
title: Cetuximab
parent: 僅模型預測 (L5)
nav_order: 70
evidence_level: L5
indication_count: 10
---

# Cetuximab
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

# Cetuximab: From Colorectal Cancer and Head and Neck Cancer to Non-Seminomatous Lesion

## One-Sentence Summary

Cetuximab is a chimeric IgG1 monoclonal antibody targeting EGFR, internationally approved for RAS wild-type colorectal cancer and head and neck squamous cell carcinoma (HNSCC), but not currently registered in New Zealand. The TxGNN model predicts it may have potential for **Non-Seminomatous Lesion** (global knowledge graph rank #729), with **no clinical trials** and **no supporting publications** identified for this specific direction. Given the absent mechanistic rationale and low EGFR expression profile of non-seminomatous tumours, the current recommendation is **Hold**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not registered in New Zealand; internationally approved for RAS wild-type colorectal cancer and head and neck squamous cell carcinoma |
| Predicted New Indication | Non-Seminomatous Lesion |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known information, Cetuximab is an anti-EGFR chimeric monoclonal antibody (IgG1 class) that competitively blocks EGFR ligand binding at the extracellular domain, thereby preventing receptor activation and downstream RAS/MAPK and PI3K/AKT signalling. This mechanism underpins its proven efficacy in KRAS/RAS wild-type colorectal cancer and EGFR-overexpressing HNSCC, where EGFR is a validated oncogenic driver.

Non-seminomatous germ cell tumours (NSGCTs) comprise embryonal carcinoma, yolk sac tumour, choriocarcinoma, and teratoma — a biologically diverse group driven primarily by germ cell developmental pathways rather than EGFR signalling. Published evidence reports EGFR expression in NSGCTs to be low and inconsistent, in marked contrast to the epithelial malignancies where Cetuximab is clinically effective. This biological mismatch substantially limits the mechanistic rationale for anti-EGFR therapy in this context.

NSGCTs are among the most chemosensitive solid tumours known: BEP (bleomycin, etoposide, cisplatin) achieves cure rates exceeding 90% in early-stage disease, leaving minimal unmet clinical need for alternative approaches. The absence of EGFR pathway dependency, combined with a highly effective existing standard of care, means the TxGNN prediction for this indication most likely reflects knowledge graph network proximity rather than direct biological plausibility.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Cetuximab has no Medsafe authorisations in New Zealand and is not currently marketed. Clinicians requiring access would need to apply through the Medsafe Special Access Scheme or a compassionate use pathway.

---

## Cytotoxicity

Cetuximab is an antineoplastic targeted therapy (anti-EGFR monoclonal antibody) used in the treatment of cancer.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy — Anti-EGFR monoclonal antibody (chimeric IgG1); not a conventional cytotoxic agent |
| Myelosuppression Risk | Low (monoclonal antibodies carry minimal myelosuppressive risk; isolated neutropenia has been reported) |
| Emetogenicity Classification | Low |
| Monitoring Items | Serum electrolytes (magnesium — hypomagnesaemia is a common and clinically significant adverse effect), infusion reaction signs during and after administration, skin toxicity (acneiform rash — severity correlates with response), CBC, liver and renal function |
| Handling Protection | Standard parenteral antineoplastic precautions; dedicated IV line required; resuscitation equipment must be available at bedside throughout each infusion |

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Clinical Note:** Cetuximab carries a well-documented risk of **severe infusion hypersensitivity reactions** (including anaphylaxis Grade 3–4), particularly in geographic regions where pre-existing IgE antibodies against alpha-1,3-galactose are prevalent. **Hypomagnesaemia** occurs in a significant proportion of patients on prolonged therapy and may require supplementation. **Acneiform skin rash** is the hallmark dermatologic toxicity and serves as a surrogate marker of biological activity. Pre-medication and on-site monitoring protocols per local oncology guidelines are essential before and during first administration.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Non-seminomatous testicular lesions express EGFR at low and inconsistent levels, and there is no established clinical or mechanistic evidence to support Cetuximab repurposing in this disease entity. The existing standard of care achieves high cure rates, and the TxGNN prediction is currently unsupported by any real-world data.

**To proceed, the following is needed:**

- Systematic immunohistochemical evaluation of EGFR expression and copy number in non-seminomatous germ cell tumour tissue samples to establish biological target presence
- Mechanistic in vitro studies in NSGCT cell lines to determine EGFR pathway dependency and sensitivity to EGFR blockade
- Review of the TxGNN knowledge graph pathway linking Cetuximab to non-seminomatous lesion to identify any indirect mechanistic connections not captured by literature alone
- Consider re-prioritising evaluation resources toward higher-evidence repurposing candidates identified in this same evidence pack:
  - **Cystic Neoplasm** (Rank #9, Evidence Level L2): Phase 1/2 trial NCT01192087 directly studying Cetuximab + IMRT + carbon ion boost in adenoid cystic carcinoma; Phase 2 clinical data published (PMID 18804410)
  - **Pre-Malignant Neoplasm** (Rank #10, Evidence Level L3): NCT00524017 — completed Phase 2 study of single-agent Cetuximab in high-risk pre-malignant upper aerodigestive lesions; supported by mechanistic review (PMID 24412287)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

