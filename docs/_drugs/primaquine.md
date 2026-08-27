---
layout: default
title: Primaquine
parent: 僅模型預測 (L5)
nav_order: 288
evidence_level: L5
indication_count: 8
---

# Primaquine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Primaquine: From Malaria to Myiasis

## One-Sentence Summary

Primaquine is an 8-aminoquinoline originally used for the radical cure of *Plasmodium vivax*/*ovale* malaria (relapse prevention) and as a gametocytocide against *P. falciparum*; no New Zealand regulatory indication text is available because the drug is not currently marketed there. The TxGNN model predicts it may be effective for **Myiasis**, but this ranking is currently supported by **0 clinical trials** and **0 publications** — it is a pure model-score prediction with no literature or mechanistic corroboration.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Malaria (radical cure of *P. vivax*/*P. ovale*, gametocytocidal activity against *P. falciparum*) — no NZ-specific approved indication text available (not marketed) |
| Predicted New Indication | Myiasis |
| TxGNN Prediction Score | 99.76% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (flagged as a High-severity data gap). Based on known pharmacology, primaquine is an 8-aminoquinoline antimalarial that generates oxidative metabolites, disrupting mitochondrial function in the erythrocytic, hepatic, and gametocyte stages of *Plasmodium* parasites; this oxidative mechanism is also the basis of its hemolytic risk in G6PD-deficient patients.

Myiasis, however, is a dipteran larval tissue infestation (e.g. *Dermatobia hominis*, *Cochliomyia*) whose standard management is mechanical larva removal, occlusive/asphyxiation techniques, or ivermectin — none of which share a pharmacological target with primaquine's antiprotozoal oxidative-stress mechanism. The evidence pack's own rationale for this prediction explicitly states there is "no known association" between primaquine's mechanism and myiasis treatment, and that the ranking rests solely on the TxGNN model score (0.9976) with no supporting trial or literature data. This should be read as a low-confidence, exploratory signal rather than a mechanistically grounded hypothesis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Primaquine currently has no marketing authorization in New Zealand (0 licenses on file), so no product/dosage-form table can be generated.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA/Medsafe warnings and contraindications are flagged internally as a Blocking-severity data gap — a package insert was located but not yet parsed, so safety data could not be extracted for this evidence pack.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The myiasis prediction is evidence level L5 — a model score with zero supporting clinical trials or literature, and the stated repurposing rationale itself finds no mechanistic link to primaquine's known antiprotozoal activity. Combined with the unresolved Blocking-severity safety data gap (no parsed warnings/contraindications), this candidate cannot proceed past initial screening (S0).

**To proceed, the following is needed:**
- Parse the located TFDA/Medsafe package insert to populate warnings and contraindications (removes the Blocking gap)
- Confirm mechanism of action via DrugBank API query
- Obtain preclinical or in-vitro data testing primaquine activity against myiasis-causing larvae before further scoring
- Consider deprioritizing myiasis in favor of higher-evidence candidates already present in this same evidence pack — notably **pneumocystosis** (6 trials incl. completed Phase 3, 20 publications) and **toxoplasmosis** (L4, 7 literature records) — which have documented clinical use of primaquine (typically with clindamycin) and warrant separate, dedicated evaluation reports
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

