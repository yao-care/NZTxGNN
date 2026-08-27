---
layout: default
title: Nicorandil
parent: 僅模型預測 (L5)
nav_order: 241
evidence_level: L5
indication_count: 7
---

# Nicorandil
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

# Nicorandil: From Angina Pectoris (Vasodilator) to Benign Prostatic Hyperplasia

> **Note on data completeness**: The evidence pack flags two data gaps — original mechanism of action (DG002, High severity) and TFDA label/warnings (DG001, **Blocking** severity). "Angina Pectoris" above reflects Nicorandil's well-established pharmacological class (K-ATP channel opener / NO donor) rather than a value extracted from `taiwan_regulatory.licenses`, since that field is empty (drug not marketed in New Zealand).

---

## One-Sentence Summary

Nicorandil is a hybrid antianginal vasodilator (ATP-sensitive potassium channel opener with nitric oxide donor activity); its original indication text and formal MOA record are not present in this evidence pack. The TxGNN model predicts it may be effective for **Benign Prostatic Hyperplasia**, currently supported by **0 clinical trials** and **3 publications** (mechanistic/preclinical in nature, no completed clinical trials).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not captured in evidence pack (no NZ license data); Nicorandil is pharmacologically classified as a vasodilator/antianginal agent |
| Predicted New Indication | Benign Prostatic Hyperplasia (disease) |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed formal mechanism of action data is not available in this evidence pack (data gap DG002). Based on the mechanistic rationale captured for this prediction, Nicorandil is a K-ATP (ATP-sensitive potassium) channel opener that also carries nitric oxide (NO) donor activity, giving it a vasodilatory effect on both venous and arterial vasculature, including coronary vessels — the basis of its established use as an antianginal agent.

The link to Benign Prostatic Hyperplasia (BPH) is indirect and vascular rather than a direct anti-proliferative mechanism. Existing literature proposes that prostatic ischemia (impaired blood supply to the lower urinary tract) may be a contributing factor in the development of BPH/benign prostatic enlargement (BPH/BPE), and that lower urinary tract symptoms (LUTS) may in part reflect vascular dysfunction. A preclinical study in spontaneously hypertensive rats (SHR) found that nicorandil treatment improved prostatic blood flow and reduced markers associated with ischemic injury.

This is a plausible but indirect mechanistic hypothesis: it does not act through the pathways targeted by standard BPH therapies (e.g., 5-alpha-reductase inhibition or alpha-1 adrenergic blockade), and causality between prostatic ischemia and BPH development (versus symptom modulation only) remains unresolved. As such, the mechanistic plausibility supports further preclinical/translational investigation rather than an established therapeutic rationale.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31735753](https://pubmed.ncbi.nlm.nih.gov/31735753/) | 2019 | Review | Nihon Yakurigaku Zasshi (Folia Pharmacologica Japonica) | Proposes prostatic blood flow as a prominent therapeutic target in BPH; discusses clinical association between BPH/BPE, LUTS, and atherosclerotic/vascular disease |
| [26165338](https://pubmed.ncbi.nlm.nih.gov/26165338/) | 2015 | Cohort/Clinical observational (abstract unavailable) | Nihon Yakurigaku Zasshi (Folia Pharmacologica Japonica) | Frames LUTS as a manifestation of vascular dysfunction and discusses nicorandil's vasodilator effect as a potential therapeutic approach |
| [24448152](https://pubmed.ncbi.nlm.nih.gov/24448152/) | 2014 | Preclinical (animal model) | Scientific Reports | In spontaneously hypertensive rats, 6-week nicorandil treatment increased prostatic blood flow and reduced markers of ischemic/oxidative injury, supporting a prostatic-ischemia-driven mechanism for BPH that is reversible by vasodilation |

---

## New Zealand Market Information

Nicorandil currently has no registered product license in New Zealand (market status: 未上市 / Not Marketed; total licenses: 0). No dosage form or route information is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Key warnings, contraindications, and drug interaction data are all flagged as data gaps in this evidence pack. Notably, DG001 — missing TFDA label warnings/contraindications — is classified as Blocking severity, meaning a formal S1 safety assessment cannot yet be completed.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for the BPH indication is limited to L3-level mechanistic/preclinical and observational literature (one rodent study, one review, one observational report with no clinical trials registered), and the drug is not currently marketed in New Zealand. Critically, a **blocking** data gap (DG001 — missing TFDA/official label warnings and contraindications) prevents completion of even an initial (S1) safety assessment, so a "Proceed with Guardrails" recommendation cannot be responsibly made at this time.

**To proceed, the following is needed:**
- Official package insert / regulatory label data (warnings, contraindications) to resolve DG001 (Blocking)
- Confirmed mechanism of action documentation from DrugBank to resolve DG002 (High)
- Prospective or at minimum observational clinical evidence directly linking nicorandil exposure to BPH/LUTS outcomes in humans (currently zero registered trials)
- Clarification of original approved indication(s) and any New Zealand or comparable-market regulatory pathway, given the drug's current "not marketed" status
- Drug-drug interaction (DDI) data, currently unavailable (`query_status: not_found`)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

