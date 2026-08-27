---
layout: default
title: Latanoprost
parent: 僅模型預測 (L5)
nav_order: 197
evidence_level: L5
indication_count: 10
---

# Latanoprost
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

# Latanoprost: From Open-Angle Glaucoma / Ocular Hypertension to Primary Hereditary Glaucoma

## One-Sentence Summary

Latanoprost is a prostaglandin F2α analogue whose established pharmacology lowers intraocular pressure in open-angle glaucoma and ocular hypertension. The TxGNN model predicts it may also be effective for **primary hereditary glaucoma**, a genetic subtype within the same disease family, with **1 completed Phase 2 clinical trial** and **0 publications** currently supporting this direction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not on file in structured fields (drug not marketed in New Zealand); established pharmacological use is open-angle glaucoma / ocular hypertension per mechanistic rationale |
| Predicted New Indication | Primary hereditary glaucoma |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the current evidence pack (flagged as a High-severity data gap, DG002). Based on known pharmacological information, latanoprost is a prostaglandin F2α (PGF2α) prodrug: after corneal esterase hydrolysis, it activates the FP receptor, increasing uveoscleral outflow of aqueous humor and lowering intraocular pressure. This is the standard mechanism by which the prostaglandin analogue class treats open-angle glaucoma and ocular hypertension.

Primary hereditary glaucoma is a genetic subtype within the broader glaucoma disease family. Because it shares the same underlying pathophysiology of elevated intraocular pressure and impaired aqueous outflow, the pharmacological rationale for extending latanoprost's use to this subtype is strong — this is an extension of an existing, well-characterized indication rather than a novel mechanistic hypothesis.

The supporting clinical trial (NCT01527682) tested a prostaglandin analogue in combination with a carbonic anhydrase inhibitor specifically in pediatric glaucoma refractory to surgery, reinforcing that this drug class is already used across glaucoma subtypes, including congenital/hereditary presentations. However, the trial does not explicitly confirm latanoprost as the tested agent versus another prostaglandin analogue, so this should be treated as class-level rather than drug-specific confirmation.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01527682](https://clinicaltrials.gov/study/NCT01527682) | Phase 2 | Completed | 37 | Assessed ocular hypotensive effect and safety of a prostaglandin analogue combined with dorzolamide (carbonic anhydrase inhibitor) in pediatric glaucoma patients refractory to surgical procedures. Relevance graded A: drug class and mechanism directly correspond to latanoprost, but the specific trial agent and hereditary-subtype enrollment are not fully confirmed from the available summary. |

## Literature Evidence

Currently no related literature available.

## New Zealand Market Information

Latanoprost is currently **not marketed** in New Zealand, and no authorization records are on file (0 licenses).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The predicted indication (primary hereditary glaucoma) is a mechanistically close extension of latanoprost's established glaucoma pharmacology, and one completed Phase 2 trial supports drug-class efficacy in a related pediatric/hereditary glaucoma population, but drug-specific and subtype-specific confirmation is still lacking.

**To proceed, the following is needed:**
- Full mechanism of action documentation from DrugBank (DG002)
- TFDA/regulatory package insert warnings and contraindications (DG001, currently Blocking for safety review)
- Confirmation that NCT01527682's tested agent was latanoprost specifically, and that hereditary glaucoma subtypes were enrolled
- Additional literature or trials specific to primary hereditary glaucoma to move beyond L2 evidence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

