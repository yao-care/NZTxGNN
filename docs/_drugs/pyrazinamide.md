---
layout: default
title: Pyrazinamide
parent: 僅模型預測 (L5)
nav_order: 296
evidence_level: L5
indication_count: 10
---

# Pyrazinamide
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

# Pyrazinamide: From Tuberculosis to Infectious Otitis Media

## One-Sentence Summary

Pyrazinamide is a first-line antimycobacterial agent used within standard combination therapy for tuberculosis. The TxGNN model predicts it may be effective for **Infectious Otitis Media**, but this ranking currently has **no clinical trials** and **no supporting literature** — it is a pure model-level association.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not captured in structured regulatory/DrugBank fields (data gap); literature within this evidence pack confirms pyrazinamide's established role in first-line tuberculosis combination therapy |
| Predicted New Indication | Infectious Otitis Media |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is currently unavailable (DG002, DrugBank query pending). Based on known pharmacology reflected in this evidence pack's own literature, pyrazinamide is a component of standard first-line anti-tuberculosis regimens (with isoniazid, rifampicin, and ethambutol) and is converted to its active form, pyrazinoic acid, selectively within the acidic, necrotic microenvironment created by *Mycobacterium tuberculosis* infection — this is a narrow, pathogen-specific bactericidal mechanism.

Infectious otitis media is most commonly caused by non-mycobacterial pathogens such as *Streptococcus pneumoniae* and *Haemophilus influenzae*, which fall outside pyrazinamide's activity spectrum. Consistent with this, no clinical trials or literature link pyrazinamide directly to this indication, and the repurposing rationale attached to this prediction explicitly flags it as lacking mechanistic or clinical support.

Notably, other lower-ranked predictions within this same evidence pack — chronic otitis media (rank 4) and suppurative otitis media (rank 5) — are supported by multiple case reports and reviews describing tuberculous otitis media, a rare extrapulmonary manifestation of TB treated with standard anti-TB regimens including pyrazinamide. This suggests the TxGNN model may be capturing a genuine TB–otitis media association at the disease-cluster level, but extrapolating it broadly to "infectious otitis media" without pathogen specificity. The top-ranked prediction evaluated here should therefore be interpreted with caution relative to those TB-confirmed subgroups.

## Clinical Trial Evidence

Currently no related clinical trials registered

## Literature Evidence

Currently no related literature available

## New Zealand Market Information

Pyrazinamide is not currently marketed in New Zealand (0 authorizations on file); no product-level market data is available.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trials or literature directly support pyrazinamide's efficacy against infectious otitis media, and its bactericidal mechanism is specific to *Mycobacterium tuberculosis* rather than the typical bacterial pathogens causing this condition. Evidence level is L5 (model prediction only).

**To proceed, the following is needed:**
- TFDA/Medsafe package insert warnings and contraindications (DG001, Blocking)
- Detailed mechanism of action data from DrugBank (DG002, High)
- Structured confirmation of original approved indication(s)
- If pursuing repurposing within the otitis media disease cluster, prioritize evaluation of the tuberculous-otitis-media-specific candidates (chronic otitis media, suppurative otitis media) instead, given their existing case-based evidence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

