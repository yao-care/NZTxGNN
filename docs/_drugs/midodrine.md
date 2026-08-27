---
layout: default
title: Midodrine
parent: 僅模型預測 (L5)
nav_order: 227
evidence_level: L5
indication_count: 10
---

# Midodrine
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

Using **superpowers:using-superpowers** — checked for a matching skill; this is a self-contained report-generation task fully specified by the prompt template itself, so no domain skill applies. Proceeding directly.

---

# Midodrine: From Hypotensive Disorder to Variably Protease-Sensitive Prionopathy

## One-Sentence Summary

Midodrine is a peripheral α1-adrenergic agonist whose established clinical use — reflected in this same evidence pack as the "hypotensive disorder" prediction (9 trials, 19 publications) — is treating orthostatic hypotension. The TxGNN model's **top-ranked** prediction, however, is **Variably Protease-Sensitive Prionopathy**, a rare prion disease, with a score of **99.99%** but **zero clinical trials and zero publications** supporting any connection, and no plausible mechanistic link to the drug's pharmacology.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in evidence pack (no NZ licenses on file); midodrine's known clinical role corresponds to "hypotensive disorder," which independently ranks #4 in this same prediction set with strong supporting evidence |
| Predicted New Indication | Variably Protease-Sensitive Prionopathy |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data was not returned for this drug (`original_moa: [Data Gap]`). Based on known pharmacology, midodrine is a prodrug converted to its active metabolite desglymidodrine, a peripheral α1-adrenergic receptor agonist that causes vasoconstriction and raises blood pressure — the basis for its established use in orthostatic/hypotensive disorders (consistent with the strong trial and literature base seen under the "hypotensive disorder" candidate elsewhere in this evidence pack).

Variably Protease-Sensitive Prionopathy (VPSPr) is a rare, sporadic human prion disease driven by abnormal, protease-sensitive misfolded prion protein and progressive neurodegeneration. There is no established pathway connecting peripheral α1-adrenergic vasoconstriction to prion protein misfolding or neurodegeneration, and the drug's evidence pack explicitly notes the pathology and MOA have no reasonable link.

This candidate therefore reflects a high raw model score with no accompanying mechanistic, preclinical, clinical, or literature support — the pattern the evidence level rubric classifies as L5 (model prediction only).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Midodrine is not currently marketed in New Zealand (0 authorizations on file), so no product/license table is available.

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data were all returned as data gaps or not found in this evidence pack.)

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The 99.99% TxGNN score is not corroborated by any clinical trial, publication, or plausible mechanism — this is a pure model artifact rather than a repurposing signal, and the evidence pack's own rationale confirms no known link between α1-agonism and prion disease pathology.
- Separately, a critical safety data gap (DG001, Blocking) means midodrine cannot yet enter S1 safety evaluation for *any* new indication regardless of efficacy signal.

**To proceed, the following is needed:**
- TFDA/official package insert (warnings, contraindications) to clear the Blocking safety gap (DG001) before any indication — including this one — can enter S1 review
- Confirmed mechanism-of-action documentation (DG002) via DrugBank or equivalent
- If pursuing repurposing for this drug, prioritize higher-evidence candidates already present in this same evidence pack — notably rank 4 "hypotensive disorder" (L4, 9 trials, 19 publications) — over this L5 candidate
- Preclinical or biological plausibility data specifically linking adrenergic agonism to prion disease, if this candidate is to be revisited
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

