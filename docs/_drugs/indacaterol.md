---
layout: default
title: Indacaterol
parent: 僅模型預測 (L5)
nav_order: 173
evidence_level: L5
indication_count: 10
---

# Indacaterol
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

# Indacaterol: From COPD/Asthma Bronchodilation to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Indacaterol is a long-acting β2-adrenergic agonist (LABA) bronchodilator, established for COPD/asthma-type airway disease. TxGNN's top-ranked prediction proposes **Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD)** as a new indication (score **99.54%**), but currently **zero clinical trials and zero publications** support this link, and no plausible mechanism connects β2-agonism to V2-receptor-driven antidiuresis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Chronic Obstructive Pulmonary Disease (COPD) / asthma bronchodilation — inferred from drug class (LABA); not formally recorded in this evidence pack (`original_indications` empty; not marketed in New Zealand) |
| Predicted New Indication | Nephrogenic Syndrome of Inappropriate Antidiuresis |
| TxGNN Prediction Score | 99.54% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action (MOA) data is not available for indacaterol in this evidence pack. Based on known pharmacology, indacaterol is a selective, ultra-long-acting β2-adrenergic receptor agonist that relaxes bronchial smooth muscle to relieve airway obstruction — this is its well-established therapeutic role in obstructive airway disease.

Nephrogenic Syndrome of Inappropriate Antidiuresis, however, is caused by gain-of-function mutations in the vasopressin V2 receptor, leading to constitutive receptor activation independent of any ligand. There is no known crosstalk between the β2-adrenergic signaling pathway (Gs-coupled, targeting bronchial smooth muscle) and the renal V2-receptor/aquaporin-2 pathway responsible for NSIAD's pathophysiology.

The TxGNN model assigns this prediction a very high similarity score, but the evidence pack's own rationale explicitly flags the absence of any mechanistic, clinical, or literature support: this is a model-driven signal only, not a biologically grounded hypothesis at this stage.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Indacaterol is not currently registered or marketed in New Zealand (`market_status`: Not Marketed; 0 authorizations on file), so no product license records are available.

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: this evidence pack flags a Blocking data gap — TFDA/Medsafe package-insert warnings and contraindications — that must be resolved before any safety assessment can proceed.)*

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a high TxGNN similarity score, there is no mechanistic rationale, no clinical trial, and no published literature linking indacaterol to NSIAD — the two pathways (β2-adrenergic bronchodilation vs. V2-receptor-mediated antidiuresis) are pharmacologically unrelated. This is a pure model-prediction signal (L5) and does not meet the threshold to advance.

**To proceed, the following is needed:**
- Confirmed mechanism of action (MOA) data for indacaterol (currently a Blocking data gap)
- TFDA/Medsafe package-insert warnings and contraindications (currently a Blocking data gap)
- Preclinical/mechanistic evidence demonstrating any plausible interaction between β2-adrenergic signaling and renal V2-receptor/aquaporin-2 regulation
- At minimum, case-level or observational clinical signal before further investment is warranted

*Separate note for context: rank 7 in this dataset ("bronchial disease," L1, 37 trials, 20 publications, "Proceed with Guardrails") is not a novel repurposing candidate — it reflects indacaterol's already-established COPD/asthma indication, surfaced here because `original_indications` was empty in the source data.*
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

