---
layout: default
title: Flutamide
parent: 僅模型預測 (L5)
nav_order: 157
evidence_level: L5
indication_count: 10
---

# Flutamide
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

# Flutamide: From Prostate Cancer (Androgen Blockade) to Prostate Cancer/Brain Cancer Susceptibility

## One-Sentence Summary

Flutamide is a non-steroidal antiandrogen historically used as part of combined androgen blockade regimens for prostate cancer, though no approved-indication text or marketing authorization is on file for it in New Zealand. The TxGNN model's highest-scoring prediction for this drug is a composite **"prostate cancer / brain cancer susceptibility"** label (score **99.98%**), but this specific prediction currently has **zero registered clinical trials and zero publications** supporting it — it should be treated as a pure computational hypothesis rather than an actionable repurposing signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | No approved-indication text on file in New Zealand (drug not marketed); internationally, flutamide is used as a non-steroidal antiandrogen in combined androgen blockade for prostate cancer |
| Predicted New Indication | Prostate cancer / brain cancer susceptibility (shared-susceptibility composite label) |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for this evidence pack (flagged as a High-severity data gap, DG002). Based on known information, flutamide is a non-steroidal antiandrogen that competitively blocks the androgen receptor (AR), and its efficacy as part of combined androgen blockade for prostate cancer is well established internationally — this is the drug's core, long-recognized therapeutic role.

The predicted label in this evidence pack is not a simple "new disease" but a **composite susceptibility tag** that groups prostate cancer together with brain cancer under a shared-genetic-susceptibility relationship in the underlying knowledge graph, rather than a direct clinical indication. The prostate cancer component is mechanistically coherent with flutamide's known AR-antagonist activity; the brain cancer component rests only on a theoretical possibility that androgen receptor expression in some gliomas could, in principle, be modulated by AR-directed agents.

However, no clinical trial or literature evidence could be retrieved for this specific disease pairing — targeted searches against ClinicalTrials.gov, ICTRP, and PubMed for "flutamide + prostate cancer/brain cancer susceptibility" all returned zero results. Without any direct study of flutamide in CNS or brain tumor models, the cross-organ "susceptibility" link cannot currently be judged to have clinical meaning; it should be read as a graph-embedding signal rather than a validated pharmacological hypothesis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Flutamide currently holds no marketing authorization in New Zealand (0 licenses on file). The product is not marketed in this market, so no local dosage form or approved-indication text is available.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Hormonal antiandrogen (targeted endocrine therapy) — not a conventional cytotoxic chemotherapeutic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

No structured safety data (key warnings, contraindications, or drug-drug interactions) could be retrieved for this evidence pack. Retrieval of the official local package insert is flagged as a **Blocking** data gap (DG001), which must be resolved before any Stage-1 safety pre-assessment can proceed. Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction has the highest TxGNN score in the batch but is backed by zero clinical trials and zero publications; combined with missing MOA and package-insert data, there is currently no basis to advance it beyond a computational hypothesis.

**To proceed, the following is needed:**
- Obtain flutamide's official (TFDA/local) package insert to resolve the Blocking safety data gap (DG001)
- Obtain DrugBank mechanism-of-action detail to resolve the mechanistic data gap (DG002)
- Conduct a targeted literature search on androgen receptor signaling in CNS tumors/glioma specifically to test the plausibility of the "brain cancer susceptibility" component
- Note: other predictions generated for flutamide in this same evidence batch carry materially stronger evidence and may warrant separate evaluation — notably **"male reproductive organ cancer"** (Evidence Level L1, 40+ registered trials including several completed Phase 3 RCTs, recommendation "Proceed with Guardrails"), which reflects flutamide's already-established role in combined androgen blockade, and **"benign reproductive system neoplasm"** (BPH) (Evidence Level L3, recommendation "Research Question"), which has historical randomized-trial data (e.g., PMID 1722793) directly testing flutamide in benign prostatic hyperplasia.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

