---
layout: default
title: Mupirocin
parent: 僅模型預測 (L5)
nav_order: 235
evidence_level: L5
indication_count: 2
---

# Mupirocin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Mupirocin: From Topical Skin/Nasal Infection to Pleural Empyema

## One-Sentence Summary

Mupirocin is a topical antibacterial agent traditionally used for skin and nasal bacterial infections (e.g., impetigo, MRSA nasal decolonization); no original indication record was found in this evidence pack's regulatory data. The TxGNN model predicts it may be effective for **Pleural Empyema**, but this prediction is currently supported by **0 clinical trials** and **0 publications** — it is a model-only signal with no confirmatory evidence.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the evidence pack's license data (0 licenses on file); per the mechanistic rationale, Mupirocin is known clinically as a topical antibacterial for skin/nasal bacterial infections |
| Predicted New Indication | Pleural Empyema (disease) |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L5 (model prediction only — no clinical trials or literature identified) |
| New Zealand Market Status | ✗ Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the source database (DrugBank MOA field is a confirmed data gap, DG002). Based on known pharmacology, Mupirocin inhibits bacterial isoleucyl-tRNA synthetase, blocking protein synthesis in gram-positive organisms — including *Staphylococcus aureus* and MRSA. This is the basis for its established topical use in skin and nasal infections.

The proposed mechanistic link to pleural empyema is that empyema is frequently caused by gram-positive organisms such as *S. aureus*, against which Mupirocin has demonstrated antibacterial activity — creating a superficial pharmacological rationale for the prediction.

However, this link has a critical gap: Mupirocin has extremely low systemic bioavailability and is rapidly metabolized to inactive monic acid when absorbed. It is clinically formulated and used only as a topical (skin) or intranasal product, and there is no evidence it can reach therapeutic bactericidal concentrations in the pleural space. The mechanism (antibacterial activity against likely empyema pathogens) is plausible in isolation, but the route of administration and pharmacokinetics required to treat a deep pleural infection are fundamentally incompatible with the drug's current formulation. The same caveat applies to the secondary prediction, punctate epithelial keratoconjunctivitis (TxGNN score 99.10%): most cases are viral or idiopathic, Mupirocin has no antiviral activity, and no ophthalmic formulation or ocular penetration data exists for this drug. Both predictions should be treated as hypothesis-generating signals only, pending route-of-administration and pharmacokinetic feasibility assessment.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

No marketing authorization records were found for Mupirocin in New Zealand in this evidence pack (market status: 未上市 / Not Marketed; 0 total licenses on file).

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: the underlying TFDA/NZ package-insert warning and contraindication data are flagged as a Blocking data gap (DG001) in this evidence pack — this must be resolved before any S1 safety pre-assessment can proceed.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Both predicted indications are supported only by TxGNN model scores (Evidence Level L5), with zero clinical trials and zero literature identified, and the mechanistic rationale for each is undermined by a fundamental route-of-administration/pharmacokinetic mismatch (topical-only drug vs. deep pleural or ocular target tissue). Combined with a Blocking data gap on safety/label information and the drug's non-marketed status in New Zealand, there is currently insufficient basis to advance beyond an early screening stage.

**To proceed, the following is needed:**
- TFDA/NZ package insert warnings and contraindications (resolves Blocking gap DG001)
- Confirmed mechanism of action data via DrugBank or primary literature (resolves gap DG002)
- Pharmacokinetic/tissue-penetration data assessing whether any Mupirocin formulation (topical, or a hypothetical systemic/ocular formulation) could reach therapeutic concentrations in the pleural space or ocular surface
- Preclinical (in vitro/in vivo) evidence of efficacy specifically in empyema or keratoconjunctivitis models
- Formulation feasibility assessment if repurposing would require a new route of administration distinct from current topical/intranasal use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

