---
layout: default
title: Sotalol
parent: 僅模型預測 (L5)
nav_order: 323
evidence_level: L5
indication_count: 7
---

# Sotalol
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Sotalol: From Ventricular Arrhythmia / Atrial Fibrillation to Sick Sinus Syndrome 2, Autosomal Dominant

## One-Sentence Summary

Sotalol is a Class III antiarrhythmic (combined β-blockade and potassium-channel blockade) established for ventricular arrhythmias and rhythm control of atrial fibrillation/flutter. The TxGNN model's top-ranked prediction links it to **Sick Sinus Syndrome 2, Autosomal Dominant**, but this pairing has **zero supporting clinical trials and zero literature**, and the underlying pharmacology points in the *opposite* therapeutic direction — sotalol's own mechanism would be expected to worsen this condition rather than treat it.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ventricular arrhythmias and atrial fibrillation/flutter rate/rhythm control (based on known pharmacology; no NZ license record on file) |
| Predicted New Indication | Sick sinus syndrome 2, autosomal dominant |
| TxGNN Prediction Score | 99.76% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for sotalol is not available in this evidence pack (flagged as a High-severity data gap). Based on known pharmacology, however, sotalol acts as a non-selective β-adrenergic blocker combined with a Class III potassium-channel (IKr) blocker, which prolongs cardiac repolarization. This mechanism is well established for suppressing ventricular arrhythmias and maintaining sinus rhythm in atrial fibrillation/flutter.

Sick sinus syndrome 2 (autosomal dominant) is a genetic sinus-node dysfunction, commonly linked to HCN4/SCN5A mutations, that already presents with abnormally slow or unreliable sinus node activity. Sotalol's β-blocking and K⁺-channel-blocking effects would be expected to further suppress sinus node automaticity and conduction — the opposite of what this condition requires. Consistent with this, sick sinus syndrome is typically listed as a contraindication (or requires prior pacemaker placement) on antiarrhythmic drug labels, rather than being a treatment target.

This prediction therefore appears to be a case where the TxGNN model surfaced a strong network-level association without capturing directionality — the drug-disease link exists, but pharmacologically in a risk/contraindication sense rather than a therapeutic one. No clinical trials or literature exist to support (or refute) this specific pairing.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## Safety Considerations

Please refer to the package insert for safety information. Note: based on the drug's known Class III/β-blocking mechanism, sinus node dysfunction (including sick sinus syndrome) is a recognized area of caution for antiarrhythmic agents of this class, independent of the specific safety fields on file (which are currently unpopulated for sotalol).

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication (sick sinus syndrome 2, autosomal dominant) has no clinical trial or literature support (L5, model prediction only), and the drug's known mechanism plausibly worsens rather than treats this condition. This should be treated as a likely false-positive/contraindication signal rather than a repurposing opportunity.

**To proceed, the following is needed:**
- TFDA/package insert warnings and contraindications for sotalol (currently a Blocking data gap — DG001)
- Confirmed mechanism of action from DrugBank (currently a High-severity data gap — DG002), to formally verify the directional mismatch described above
- If pursuing repurposing for this drug, evaluate the higher-evidence candidate in the same batch instead: **stroke disorder** (rank 4, L2/S1, "Research Question"), which is supported by multiple Phase 3 AF trials and cohort literature, albeit via an indirect AF-rhythm-control-to-stroke-prevention pathway rather than a direct anti-stroke mechanism
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

