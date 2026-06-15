---
layout: default
title: Candesartan Cilexetil
parent: 僅模型預測 (L5)
nav_order: 60
evidence_level: L5
indication_count: 5
---

# Candesartan Cilexetil
{: .fs-9 }

證據等級: **L5** | 預測適應症: **5** 個
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

# Candesartan Cilexetil: From Hypertension to Malignant Hypertensive Renal Disease

## One-Sentence Summary

Candesartan cilexetil is an angiotensin II receptor blocker (ARB) widely used in the treatment of hypertension and heart failure. The TxGNN model predicts it may be effective for **Malignant Hypertensive Renal Disease**, with a mechanistic rationale grounded in RAAS blockade and renal protection; however, **no clinical trials or dedicated literature** currently exist to directly confirm this specific indication, leaving the evidence at model-prediction level only.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Hypertension / Heart Failure (not approved in New Zealand) |
| Predicted New Indication | Malignant Hypertensive Renal Disease |
| TxGNN Prediction Score | 99.68% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this Evidence Pack. Based on known pharmacological information, candesartan cilexetil is a prodrug that is hydrolysed upon absorption to its active form, candesartan. Candesartan selectively and competitively antagonises the angiotensin II type 1 receptor (AT1R), thereby blocking angiotensin II-mediated renal vasoconstriction, aldosterone secretion, and downstream inflammatory signalling. The net effect is reduced intraglomerular pressure, decreased proteinuria, and slowed progression of renal fibrosis.

Malignant hypertensive renal disease is defined by fulminant overactivation of the renin-angiotensin-aldosterone system (RAAS), driving afferent arteriolar spasm, fibrinoid necrosis of the vessel wall, and rapidly progressive renal impairment. This pathological cascade — RAAS hyperactivation → afferent arteriolar spasm → fibrinoid necrosis — aligns closely with the known pharmacological targets of AT1R antagonism, making the biological plausibility of this prediction strong.

Despite this mechanistic fit, the evidence base is entirely model-derived (L5). No clinical trials specifically investigating candesartan in malignant hypertensive renal disease have been identified, and no direct primary literature supports this repurposing direction. The high TxGNN score (99.68%) likely reflects the structural proximity of disease nodes in the knowledge graph rather than direct empirical validation, and should be interpreted as a signal for further investigation rather than a clinical recommendation.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** A data gap has been identified for TFDA package insert warnings and contraindications (DG001, severity: Blocking). This gap must be resolved before any safety evaluation can proceed. Additionally, there are no drug-drug interaction records available for candesartan cilexetil in the current dataset. A special caution relevant to the predicted indication: ARBs (including candesartan) are known to carry risk of acute kidney injury in patients with bilateral renal artery stenosis or a solitary functioning kidney, as renal blood flow autoregulation in these patients depends on angiotensin II-maintained efferent arteriolar tone. This is particularly relevant when considering use in renovascular disease contexts.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
While the TxGNN model assigns a high prediction confidence (99.68%) and the mechanistic rationale linking AT1R blockade to malignant hypertensive renal disease is biologically sound, the evidence level is L5 (model prediction only) with no registered clinical trials and no dedicated literature. The absence of any empirical data, combined with unresolved safety data gaps, precludes advancement to clinical recommendation at this stage.

**To proceed, the following is needed:**
- Resolve DG001 (safety data gap): retrieve and parse the full package insert from the TFDA website to obtain contraindications and black-box warnings before any safety evaluation can begin
- Resolve DG002 (MOA data gap): retrieve confirmed mechanism of action from DrugBank API
- Conduct a targeted systematic literature review for candesartan (and class-level ARB evidence) in malignant hypertension with renal involvement and hypertensive nephropathy
- Evaluate the known contraindication risk (bilateral renal artery stenosis / ARB-induced AKI) to determine whether a safety guardrail is required for the predicted indication
- Assess New Zealand regulatory pathway (Medsafe) for any potential development or off-label use pathway, given that candesartan is currently not marketed in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

