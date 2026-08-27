---
layout: default
title: Olsalazine
parent: 僅模型預測 (L5)
nav_order: 256
evidence_level: L5
indication_count: 10
---

# Olsalazine
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

# Olsalazine: From Ulcerative Colitis to Myelodysplastic Syndrome

## One-Sentence Summary

Olsalazine is a 5-aminosalicylic acid (5-ASA/mesalazine) prodrug used for local intestinal anti-inflammatory therapy (ulcerative colitis).
The TxGNN model predicts it may be effective for **Myelodysplastic Syndrome (MDS)**,
but this prediction is currently supported by **0 clinical trials** and **0 publications** — it is a pure model-derived hypothesis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Ulcerative colitis (inferred from drug class — 5-ASA prodrug; formal record is a data gap, see DG001/DG002) |
| Predicted New Indication | Myelodysplastic syndrome |
| TxGNN Prediction Score | 99.91% |
| Evidence Level | L5 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (blocking data gap DG002). Based on the information available, olsalazine is split by colonic bacteria into two molecules of mesalazine (5-ASA), whose established efficacy is local intestinal anti-inflammatory action — inhibition of prostaglandin synthesis, modulation of the NF-κB pathway, and antioxidant effects — used to treat ulcerative colitis.

Myelodysplastic syndrome is a clonal hematopoietic stem cell disorder characterized by bone marrow failure and dysplastic blood cell production. There is no established pharmacological pathway connecting 5-ASA's local gut anti-inflammatory action to the somatic mutations and marrow failure mechanisms that drive MDS.

The TxGNN score is very high (99.91%, rank 1173), but the evidence pack's own rationale notes this most likely reflects indirect co-occurrence of inflammation/immune-related gene nodes within the knowledge graph, rather than a substantiated mechanistic or clinical connection. Nine of the ten top-ranked predictions for this drug (MDS subtypes, cytopenias, sideroblastic/aregenerative anemia, and unrelated hypotrichosis/alopecia conditions) show the same pattern — high model scores with zero corroborating trials or literature — reinforcing that this is a data-driven hypothesis requiring independent validation, not an evidence-backed signal.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Olsalazine is currently not marketed in New Zealand; no authorization records are available (0 licenses on file).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but the evidence level is L5 (model prediction only) with no supporting clinical trials, literature, or plausible mechanistic overlap between olsalazine's local intestinal anti-inflammatory action and MDS bone marrow pathology. Critical safety data (TFDA label warnings/contraindications) are also missing, blocking any S1 safety assessment.

**To proceed, the following is needed:**
- TFDA package insert data — warnings, precautions, contraindications (blocking gap DG001)
- Confirmed mechanism of action data from DrugBank (DG002)
- Preclinical or mechanistic studies linking 5-ASA compounds to hematopoietic stem cell function or bone marrow modulation
- Any case reports, in vitro, or animal studies in MDS or related myeloid/cytopenic disorders
- Formal confirmation of original approved indication(s) for olsalazine
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

