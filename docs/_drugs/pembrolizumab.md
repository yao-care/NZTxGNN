---
layout: default
title: Pembrolizumab
parent: 僅模型預測 (L5)
nav_order: 271
evidence_level: L5
indication_count: 10
---

# Pembrolizumab
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

Using the evidence pack as provided (no additional research needed — this is a direct report-generation task from structured data).

---

# Pembrolizumab: From Approved Oncology Indications to Gingival Fibromatosis

## One-Sentence Summary

Pembrolizumab is a PD-1 immune checkpoint inhibitor with established oncology use (referenced within this evidence pack as approved for PD-L1-positive NSCLC, among other cancers), though no original-indication or license data for New Zealand is on file. The TxGNN model's top-ranked prediction for this drug is **Gingival Fibromatosis**, a benign fibrotic gum condition — but this prediction is supported by **0 clinical trials** and **0 publications**, and the model's own rationale flags it as a likely false positive with no plausible mechanistic link to PD-1 blockade.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no license records or original-indication data on file for New Zealand |
| Predicted New Indication | Gingival Fibromatosis |
| TxGNN Prediction Score | 99.40% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data for pembrolizumab is not available in this evidence pack. Based on contextual information embedded in the evidence pack's own rationale text, pembrolizumab is a PD-1 immune checkpoint inhibitor that activates cytotoxic T-cell–mediated anti-tumour immunity, and is referenced as approved for PD-L1-positive non-small cell lung cancer (NSCLC) and other solid tumours.

Gingival fibromatosis is a benign fibrous overgrowth of gum tissue, typically caused by germline mutations (e.g., *SOS1*) or drug-induced fibroblast proliferation (cyclosporine, phenytoin, calcium channel blockers). It has no established connection to tumour immune evasion, PD-1/PD-L1 signalling, or T-cell exhaustion — the biological axis pembrolizumab targets. There is no known disease pathway linking checkpoint blockade to fibroblast regulation.

Given this mechanistic mismatch and the complete absence of clinical trials or literature (see below), this prediction should be treated as a high-score, low-plausibility artifact of the TxGNN model rather than a genuine repurposing candidate. The model's own generated rationale explicitly reaches the same conclusion.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## Safety Considerations

Please refer to the package insert for safety information.

## Cytotoxicity

Pembrolizumab is an immune checkpoint inhibitor (anti-PD-1 monoclonal antibody) used in oncology; no DrugBank category or original-indication data confirming this classification is present in this evidence pack, but its oncology use is corroborated by literature referenced elsewhere in this evidence pack (e.g., NSCLC, melanoma, hepatocellular carcinoma, head and neck cancer trials).

| Item | Content |
|------|------|
| Cytotoxicity Classification | Immunotherapy (PD-1 checkpoint inhibitor) — not a conventional cytotoxic agent |
| Myelosuppression Risk | Low — checkpoint inhibitors are not classically myelosuppressive; the dominant toxicity pattern is immune-related adverse events (irAEs) rather than bone marrow suppression |
| Emetogenicity Classification | Low |
| Monitoring Items | Thyroid function, liver and renal function, cortisol/ACTH (endocrine irAEs), pulmonary status (pneumonitis), skin exam, and neurologic/neuromuscular assessment given reported irAEs (hypophysitis, myocarditis/myositis, Stevens-Johnson syndrome) |
| Handling Protection | Standard IV biologic infusion precautions per institutional hazardous-drug policy; conventional cytotoxic handling regulations do not directly apply as this is a monoclonal antibody, not a small-molecule cytotoxic agent |

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Zero clinical trials and zero publications support pembrolizumab's use in gingival fibromatosis, and the drug's known mechanism (T-cell checkpoint blockade for tumour immune evasion) has no established relevance to this benign fibrotic condition. The evidence pack's own generated rationale independently identifies this as a likely false-positive prediction.

**To proceed, the following is needed:**
- Confirmed mechanism-of-action data for pembrolizumab (currently a data gap)
- TFDA/Medsafe package insert (warnings, contraindications, DDI) — currently unavailable
- If pursued at all, a preclinical/mechanistic rationale connecting PD-1 signalling to fibroblast proliferation, which does not currently exist

**Note:** This evidence pack contains nine other ranked candidates for pembrolizumab. Two — *lung hilum carcinoma* (rank 4) and *pulmonary sulcus neoplasm* (rank 8) — are anatomical subtypes of NSCLC, a cancer type pembrolizumab is already referenced as approved for; these are flagged "Research Question" rather than "Hold" and may warrant separate evaluation as label-extension questions rather than novel repurposing hypotheses.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

