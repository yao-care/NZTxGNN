---
layout: default
title: Tolvaptan
parent: 僅模型預測 (L5)
nav_order: 344
evidence_level: L5
indication_count: 10
---

# Tolvaptan
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

# Tolvaptan: From Undocumented Original Indication to Polycystic Kidney Disease 3 (with or without Polycystic Liver Disease)

## One-Sentence Summary

Tolvaptan (DrugBank DB06212) is a vasopressin V2-receptor antagonist; its original approved indication is not recorded in the available regulatory data for this evidence pack. The TxGNN model predicts it may be effective for **Polycystic Kidney Disease 3 (with or without Polycystic Liver Disease)**, i.e., autosomal dominant polycystic kidney disease (ADPKD), a prediction strongly reinforced by two completed Phase 3 RCTs and multiple systematic reviews/consensus statements already establishing tolvaptan's clinical role in this exact disease area.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no Taiwan/NZ license records or original_indications data on file |
| Predicted New Indication | Polycystic Kidney Disease 3 (with or without Polycystic Liver Disease) |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, the drug-level mechanism of action field is a data gap. However, evidence embedded in the prediction record indicates tolvaptan is a **vasopressin V2-receptor antagonist**: it blocks V2 receptor–mediated cAMP signaling in renal tubular epithelium, a pathway that directly drives cyst epithelial proliferation and fluid secretion in polycystic kidney disease.

The predicted indication — "polycystic kidney disease 3" — falls within the autosomal dominant polycystic kidney disease (ADPKD) family (PKD1/PKD2/PKD3 genotypes), for which the V2-receptor/cAMP cystogenesis pathway is an already-validated drug target. This gives the TxGNN prediction strong mechanistic plausibility rather than a purely associative signal.

It is worth flagging a data-quality note directly from the evidence pack's own analysis: the record shows `market_status = 未上市 (Not Marketed)` and `original_moa = [Data Gap]`, which is inconsistent with the substantial Phase 3 RCT and consensus-guideline literature below (tolvaptan/Jynarque is an established, approved ADPKD therapy in multiple jurisdictions). This inconsistency should be manually verified against source registries before final sign-off.

## Clinical Trial Evidence

Currently no related clinical trials registered (structured `clinical_trials` field is empty; the Phase 3 RCT evidence below was captured through literature indexing, not trial-registry evidence).

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23121377](https://pubmed.ncbi.nlm.nih.gov/23121377/) | 2012 | RCT (Phase 3, TEMPO 3:4) | The New England Journal of Medicine | Landmark trial: tolvaptan slowed total kidney volume growth and eGFR decline vs. placebo in early ADPKD |
| [29105594](https://pubmed.ncbi.nlm.nih.gov/29105594/) | 2017 | RCT (Phase 3, REPRISE) | The New England Journal of Medicine | Confirmed efficacy/safety of tolvaptan in later-stage ADPKD; noted more frequent aminotransferase and bilirubin elevations |
| [38091246](https://pubmed.ncbi.nlm.nih.gov/38091246/) | 2024 | RCT (pediatric, NCT02964273) | Pediatric Nephrology | Evaluated tolvaptan safety and pharmacodynamics in children (5–17y) with ADPKD |
| [37150675](https://pubmed.ncbi.nlm.nih.gov/37150675/) | 2023 | Systematic Review / Meta-analysis | Nefrologia | Confirmed overall efficacy and safety of tolvaptan for delaying progression to ESRD in ADPKD |
| [39356039](https://pubmed.ncbi.nlm.nih.gov/39356039/) | 2024 | Cochrane Systematic Review | Cochrane Database of Systematic Reviews | Reviewed disease-modifying interventions (including tolvaptan) for preventing ADPKD progression |
| [35134221](https://pubmed.ncbi.nlm.nih.gov/35134221/) | 2022 | Consensus Statement/Review | Nephrology Dialysis Transplantation | ERA/ERKNet/PKD International consensus on tolvaptan use in ADPKD following TEMPO 3:4 |
| [40126492](https://pubmed.ncbi.nlm.nih.gov/40126492/) | 2025 | Review | JAMA | Comprehensive ADPKD overview covering epidemiology and treatment landscape |
| [40726372](https://pubmed.ncbi.nlm.nih.gov/40726372/) | 2025 | Review | Current Opinion in Nephrology and Hypertension | Notes tolvaptan as the only FDA-approved disease-modifying ADPKD therapy; reviews emerging alternatives |
| [35487607](https://pubmed.ncbi.nlm.nih.gov/35487607/) | 2022 | Review | Clinics in Liver Disease | Confirms tolvaptan slows deterioration of renal function and cyst growth in ADPKD/PLD |
| [35328738](https://pubmed.ncbi.nlm.nih.gov/35328738/) | 2022 | Review | International Journal of Molecular Sciences | Reviews ADPKD cystogenesis pathophysiology and treatment advances including tolvaptan |

## New Zealand Market Information

Tolvaptan is not currently marketed in New Zealand — no product authorization records are available (`total_licenses = 0`).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two completed Phase 3 RCTs (TEMPO 3:4, REPRISE) plus a Cochrane systematic review and an international consensus statement meet the L1 evidence bar for efficacy in ADPKD. However, regulatory safety data (MOA, package-insert warnings, contraindications, DDI) are all flagged as data gaps — one marked **Blocking** — so the candidate cannot yet clear the S1 safety pre-screen despite strong efficacy evidence.

**To proceed, the following is needed:**
- TFDA/NZ package insert warnings and contraindications (Blocking data gap DG001)
- Confirmed mechanism of action from DrugBank (High-priority data gap DG002)
- Verification of the market_status inconsistency (record shows "Not Marketed" despite established global ADPKD approval)
- Liver function monitoring plan — REPRISE trial data show increased aminotransferase/bilirubin elevations with tolvaptan use
- New Zealand/Taiwan regulatory license status confirmation before any market-entry planning
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

