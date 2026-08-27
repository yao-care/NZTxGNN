---
layout: default
title: Phenoxymethylpenicillin
parent: 僅模型預測 (L5)
nav_order: 275
evidence_level: L5
indication_count: 2
---

# Phenoxymethylpenicillin
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

# Phenoxymethylpenicillin: From Streptococcal Infections to Epiglottitis

## One-Sentence Summary

Phenoxymethylpenicillin (Penicillin V) is a narrow-spectrum, oral β-lactam antibiotic conventionally used against *Streptococcus* species infections. The TxGNN model predicts potential efficacy for **Epiglottitis** (score 99.90%), but this prediction is currently supported by **zero clinical trials and zero published literature** — it is a model-only signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this evidence pack (no NZ license/label text available); known pharmacologically as a narrow-spectrum agent for streptococcal infections |
| Predicted New Indication | Epiglottitis |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed formal mechanism-of-action data is flagged as a data gap in this evidence pack. Based on the mechanistic notes accompanying the prediction, phenoxymethylpenicillin is a narrow-spectrum, orally administered penicillin that inhibits bacterial cell wall synthesis via penicillin-binding protein (PBP) binding, with activity primarily limited to *Streptococcus* species.

This mechanism does not align well with epiglottitis. Epiglottitis is most commonly caused by *Haemophilus influenzae* type b, an organism that frequently produces β-lactamase and is therefore often resistant to penicillin. Epiglottitis is also a respiratory emergency requiring immediate intravenous, broad-spectrum antibiotic coverage (e.g., ceftriaxone) — not an orally administered agent with slow, variable absorption like penicillin V.

The evidence pack's own assessment concludes that the high TxGNN score most likely reflects a superficial "both are respiratory tract infections" association rather than a genuine mechanistic or clinical fit. No clinical trials or literature currently exist to support this specific application.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

Currently no related literature available

---

## New Zealand Market Information

Phenoxymethylpenicillin currently holds no market authorization in New Zealand (0 licenses on record; market status: not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The epiglottitis prediction is evidence level L5 — a model score only, with no supporting clinical trials or literature, and the mechanistic/pharmacokinetic rationale provided is unfavorable (antibacterial spectrum mismatch, oral route unsuitable for an airway emergency). Notably, a secondary TxGNN-predicted indication for this drug — laryngitis (score 99.85%) — does have direct clinical evidence, but that evidence (a double-blind RCT, PMID [3918495](https://pubmed.ncbi.nlm.nih.gov/3918495/), and multiple Cochrane systematic reviews including PMID [26002823](https://pubmed.ncbi.nlm.nih.gov/26002823/)) consistently found phenoxymethylpenicillin **ineffective**, reinforcing caution around broad respiratory-infection repurposing signals for this drug.

**To proceed, the following is needed:**
- TFDA/NZ package insert warnings and contraindications (currently blocking data gap)
- Verified mechanism-of-action documentation from DrugBank
- Direct clinical or preclinical evidence specific to epiglottitis (currently none identified)
- A regulatory pathway assessment, given the drug is not currently marketed in New Zealand (0 authorizations)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

