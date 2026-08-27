---
layout: default
title: Hydrocortisone Acetate
parent: 僅模型預測 (L5)
nav_order: 166
evidence_level: L5
indication_count: 10
---

# Hydrocortisone Acetate
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

# Hydrocortisone Acetate: From Corticosteroid Anti-Inflammatory Therapy to Alopecia Areata

## One-Sentence Summary

Hydrocortisone acetate is a corticosteroid; the evidence pack does not document its original approved indication (no New Zealand licenses or original_indications data are recorded), but corticosteroids as a class are established anti-inflammatory/immunosuppressive agents. The TxGNN model predicts it may be effective for **Alopecia Areata**, supported by **1 completed Phase 3 clinical trial** and **2 supporting publications** — already reflecting existing clinical practice (topical/intralesional corticosteroid use) rather than a novel hypothesis.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented (no New Zealand licenses or original indication data available in the evidence pack) |
| Predicted New Indication | Alopecia Areata |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L2 (1 completed Phase 3 RCT involving the drug) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data for hydrocortisone acetate is not available in the evidence pack. Based on known pharmacology, hydrocortisone acetate belongs to the corticosteroid class, which acts through anti-inflammatory and local immunosuppressive effects — this is a well-characterized class mechanism, even though the specific documented MOA for this product is a data gap (flagged as High severity in the source data).

Because the original approved indication for this specific product is not documented, the relationship to alopecia areata cannot be assessed directly against a labeled indication. However, topical and intralesional corticosteroids — including hydrocortisone formulations — are already a standard, widely used treatment approach for alopecia areata in dermatology practice, which strengthens plausibility independent of the missing original-indication data.

Mechanistically, alopecia areata is driven by T-cell-mediated autoimmune attack on hair follicles. Corticosteroids suppress this local inflammatory/immune process, which is precisely why they are already used clinically (topical application, intralesional injection) for this condition. This makes the TxGNN prediction a reinforcement of established practice rather than a new mechanistic hypothesis. One caveat noted directly in the source rationale: this specific product (hydrocortisone acetate) is currently **not marketed** in New Zealand, so actual accessibility depends on confirming approved dosage forms and indication status.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01453686](https://clinicaltrials.gov/study/NCT01453686) | Phase 3 | Completed | 41 | Randomized controlled trial in children comparing clobetasol propionate 0.05% cream vs. hydrocortisone 1% cream for alopecia areata; addresses the lack of high-quality evidence on which topical steroid potency is safe and effective for this population. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [153470](https://pubmed.ncbi.nlm.nih.gov/153470/) | 1979 | Review | MMW, Münchener medizinische Wochenschrift | Reviews advances in topical dermatologic therapy; notes hydrocortisone acetate's anti-inflammatory potency is comparable to newer topical corticosteroids, informing its continued use as a reference-standard topical steroid. |
| [4755919](https://pubmed.ncbi.nlm.nih.gov/4755919/) | 1973 | Case series | Przegląd dermatologiczny | Describes treatment of severe alopecia areata using intralesional subcutaneous injections of hydrocortisone acetate suspension. |

## New Zealand Market Information

This product currently has no authorized license or marketed product in New Zealand (0 authorizations on record).

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
- One completed Phase 3 RCT directly involving hydrocortisone in alopecia areata, together with established clinical use of topical/intralesional corticosteroids for this condition, supports cautious advancement — but the drug's unmarketed status in New Zealand and missing MOA/safety data mean it cannot proceed without further verification. (Note: 9 additional lower-ranked predicted indications for this drug — including telogen effluvium, alopecia mucinosa, and others — currently have no supporting trials or literature and remain at Hold.)

**To proceed, the following is needed:**
- TFDA-equivalent package insert warnings and contraindications (currently a Blocking data gap; required before any safety pre-assessment)
- Detailed mechanism of action data via DrugBank (currently a High-severity data gap)
- Confirmation of New Zealand regulatory pathway and available dosage forms, given the product is currently unmarketed
- Additional controlled trial data to strengthen evidence beyond a single Phase 3 study
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

