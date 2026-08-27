---
layout: default
title: Ustekinumab
parent: 僅模型預測 (L5)
nav_order: 358
evidence_level: L5
indication_count: 10
---

# Ustekinumab
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

# Ustekinumab: From Plaque Psoriasis to Dermatitis

## One-Sentence Summary

Ustekinumab is a monoclonal antibody originally developed for plaque psoriasis and related inflammatory conditions (general background knowledge; the evidence pack contains no confirmed original-indication or license text).
The TxGNN model predicts it may be effective for **Dermatitis** (evidence points specifically toward atopic dermatitis),
with **7 clinical trials** currently supporting this direction and **no published literature** yet identified.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in evidence pack (drug is unlicensed in NZ; `original_indications` empty). Publicly known original use: plaque psoriasis/psoriatic arthritis. |
| Predicted New Indication | Dermatitis (atopic dermatitis) |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L2 |
| New Zealand Market Status | 未上市 (Not marketed) |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data from the drug record itself is marked as a data gap. However, based on the repurposing rationale attached to this prediction, ustekinumab's known pharmacology is an anti-IL-12/23 p40 monoclonal antibody that blocks the Th1/Th17 inflammatory pathway.

Psoriasis-family disease is strongly Th17-driven, and IL-12/23 blockade has well-established efficacy there — several trials in the evidence set (e.g., the CLEAR trial, NCT02074982) use ustekinumab as an active comparator in plaque psoriasis. Atopic dermatitis, by contrast, is primarily a Th2-driven disease, so the mechanistic link to "dermatitis" as predicted here is comparatively indirect. This raises a specific caution: the TxGNN score may be inflated by ustekinumab's strong association with psoriasis-spectrum disease being folded into the broader "dermatitis" concept, rather than reflecting confirmed efficacy in atopic dermatitis specifically.

The one directly relevant trial (NCT01806662) tested ustekinumab in chronic atopic dermatitis patients with sub-optimal response to prior therapy and completed with n=32 — supportive but small and pilot-scale, not yet sufficient to establish efficacy independently of the psoriasis mechanistic assumption.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01806662](https://clinicaltrials.gov/study/NCT01806662) | Phase 2 | Completed | 32 | Randomized pilot study of ustekinumab in chronic atopic dermatitis with sub-optimal response to prior therapy — the only trial directly testing ustekinumab in an atopic-dermatitis-labeled population. |
| [NCT01945086](https://clinicaltrials.gov/study/NCT01945086) | Phase 2 | Completed | 79 | Randomized, double-blind, placebo-controlled ustekinumab trial in adult Japanese subjects with severe atopic dermatitis. |
| [NCT05535738](https://clinicaltrials.gov/study/NCT05535738) | Phase 2/3 | Recruiting | 45 | Contact dermatitis suction-blister model studying skin inflammation with biologic therapies; ustekinumab's specific inclusion is unconfirmed. |
| [NCT02074982](https://clinicaltrials.gov/study/NCT02074982) | Phase 3 | Completed | 676 | CLEAR trial: secukinumab vs. ustekinumab in moderate-to-severe plaque psoriasis (ustekinumab as active comparator, not the test drug for dermatitis). |
| [NCT07352566](https://clinicaltrials.gov/study/NCT07352566) | Phase 4 | Not yet recruiting | 10 | Cutaneous microdevice testing FDA-approved atopic dermatitis/psoriasis drugs in situ; drug-to-indication mapping unclear. |
| [NCT01356758](https://clinicaltrials.gov/study/NCT01356758) | N/A | Completed | 126 | Observational cardiovascular risk assessment in severe psoriasis patients on biologics; indirect safety signal only. |
| [NCT07041112](https://clinicaltrials.gov/study/NCT07041112) | N/A | Completed | 1000 | Retrospective pharmacogenetic study on biologic therapy survival in cutaneous psoriasis; not a dermatitis efficacy study. |

## Literature Evidence

Currently no related literature available.

---

## Safety Considerations

Please refer to the package insert for safety information. A blocking data gap (DG001: TFDA/regulatory package-insert warnings and contraindications) currently prevents this candidate from entering the S1 safety evaluation stage.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence level is L2 on the strength of one completed but small Phase 2 pilot RCT (n=32) directly testing ustekinumab in atopic dermatitis, and the mechanistic link is plausible but indirect (Th17-oriented drug applied to a largely Th2-driven disease). More critically, a **Blocking**-severity data gap (missing TFDA/package-insert safety data) prevents this candidate from clearing initial safety evaluation (S1), so a Go or Guardrailed-Proceed decision cannot be responsibly made yet.

**To proceed, the following is needed:**
- TFDA/regulatory package insert (warnings, contraindications) to clear the blocking S1 safety gate
- Confirmed mechanism-of-action documentation from DrugBank to validate the Th17-vs-Th2 mechanistic concern
- Clarification of whether "dermatitis" evidence reflects atopic dermatitis specifically or is conflated with psoriasis-spectrum trials
- A larger, adequately powered trial beyond the existing n=32 pilot before advancing past the research-question stage
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

