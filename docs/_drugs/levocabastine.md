---
layout: default
title: Levocabastine
parent: 僅模型預測 (L5)
nav_order: 203
evidence_level: L5
indication_count: 2
---

# Levocabastine
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

# Levocabastine: From Undocumented Original Indication to Allergic Urticaria

## One-Sentence Summary

Levocabastine is a highly selective H1 histamine receptor antagonist currently available only as a topical (nasal/ocular) formulation; the evidence pack does not document its original approved indication or MOA directly, but formulation and literature context point to allergic rhinitis/conjunctivitis as historical use. The TxGNN model predicts potential efficacy for **Allergic Urticaria**, but this is currently supported by **0 clinical trials** and only **2 indirectly relevant publications**, neither of which studies urticaria directly.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in evidence pack (original_indications field empty; formulation data suggests nasal/ocular topical use) |
| Predicted New Indication | Allergic Urticaria |
| TxGNN Prediction Score | 99.18% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed original mechanism-of-action data for levocabastine is not available in the evidence pack (`original_moa` is unrecorded). However, the model's own repurposing rationale identifies levocabastine as a highly selective H1 histamine receptor antagonist — H1 blockade is the established pharmacological class basis for treating urticaria, so the predicted mechanistic direction is plausible in principle.

That said, the supporting literature does not directly address urticaria: one study (PMID 8938880) evaluates intranasal levocabastine in an allergen nasal-challenge model for allergic rhinitis, and the other (PMID 1685361) is a general pharmacokinetic review of second-generation H1-antihistamines as a class, not levocabastine-specific efficacy data.

Critically, levocabastine currently exists only in topical (nasal/ocular) formulations. Urticaria is a dermatologic condition requiring systemic drug exposure to reach therapeutic skin-tissue concentrations, and no evidence in this pack demonstrates that a topical formulation can achieve this. This route–indication mismatch is the central gap limiting confidence in the prediction.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8938880](https://pubmed.ncbi.nlm.nih.gov/8938880/) | 1996 | RCT (allergen challenge, controlled) | Rhinology | Intranasal levocabastine significantly reduced sneezing (p<0.001) vs. placebo in an allergen-induced nasal challenge; evidence is specific to allergic rhinitis, not urticaria |
| [1685361](https://pubmed.ncbi.nlm.nih.gov/1685361/) | 1991 | Review | Clinical Pharmacokinetics | General PK/PD review of second-generation H1-antihistamines for allergic rhinoconjunctivitis and chronic urticaria as a drug class; not levocabastine-specific efficacy data |

---

## New Zealand Market Information

Levocabastine is not currently marketed in New Zealand, and no product authorization records are on file.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence level is L4 (mechanism/class-level rationale only), no clinical trials or urticaria-specific literature exist, and a fundamental route–indication mismatch (topical-only formulation vs. a condition requiring systemic exposure) is unresolved. The drug is also not currently marketed in New Zealand, and safety/labeling data are blocked pending source retrieval (per data gap DG001).

**To proceed, the following is needed:**
- TFDA/regulatory package insert (warnings, contraindications) — currently blocking (DG001)
- Confirmed original mechanism of action and approved indication(s) from DrugBank or equivalent primary source (DG002)
- Feasibility assessment of systemic exposure from topical levocabastine formulations, or evidence of a systemic dosage form
- Urticaria-specific preclinical or clinical data (current literature only covers allergic rhinitis and general antihistamine PK)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

