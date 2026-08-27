---
layout: default
title: Fentanyl
parent: 僅模型預測 (L5)
nav_order: 149
evidence_level: L5
indication_count: 2
---

# Fentanyl
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

# Fentanyl: From Opioid Analgesia to Nephrogenic Syndrome of Inappropriate Antidiuresis

## One-Sentence Summary

Fentanyl is a potent synthetic μ-opioid receptor agonist used clinically for moderate-to-severe pain management and anesthesia.
The TxGNN model predicts a possible association with **Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD)**,
but this direction is currently supported by **0 clinical trials** and **0 publications**, and the accompanying mechanistic review flags the association as biologically implausible.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Moderate-to-severe pain management / anesthesia adjunct (general opioid-analgesic use; specific New Zealand label text is not available because the product is not currently marketed there) |
| Predicted New Indication | Nephrogenic Syndrome of Inappropriate Antidiuresis (NSIAD) |
| TxGNN Prediction Score | 99.46% (model rank 4,726) |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (blocked by data gap DG002). Based on known pharmacology, fentanyl is a member of the phenylpiperidine opioid class and acts as a full agonist at the μ-opioid receptor; its efficacy in analgesia and anesthesia is well established, and its downstream effects are concentrated in central pain modulation, sedation, and respiratory depression.

The proposed link to NSIAD does not hold up mechanistically. NSIAD is caused by gain-of-function mutations in the vasopressin V2 receptor (AVPR2) itself, producing constitutive antidiuretic signalling that is independent of circulating ADH levels — the defect is at the receptor, not at the level of ADH secretion. Opioids such as fentanyl are, if anything, reported to **increase** ADH release via the hypothalamic-pituitary axis, which is the opposite of what would be needed to counteract a constitutively active receptor. No mechanistic, preclinical, or clinical data currently support fentanyl as a modulator or antagonist of AVPR2 signalling.

Taken together, this prediction most plausibly reflects an indirect co-occurrence pattern in the model's embedding space (e.g., "opioid drugs" clustering near "fluid balance / renal disease" concepts) rather than a causally grounded relationship. A second candidate generated for this drug, Tourette syndrome (score 99.05%, rank 7,259), is similarly weak: existing hypotheses for Tourette syndrome implicate an *overactive* endogenous opioid system, for which opioid receptor **antagonists** (not agonists like fentanyl) have been explored — meaning fentanyl's pharmacology runs in the opposite direction of the proposed therapeutic rationale. Both candidates are model-only associations (L5) with no supporting trials or literature and should be treated as hypothesis-generating at most.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Currently no related literature available.

---

## New Zealand Market Information

Fentanyl is not currently marketed in New Zealand under this evidence pack (0 active authorizations, 0 registered licenses), so no product/authorization table is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: TFDA/label warnings and contraindications for fentanyl are a blocking data gap (DG001) — this omission by itself is sufficient to prevent progression to a safety pre-assessment, independent of the efficacy evidence above.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The predicted NSIAD indication is model-output only (L5), with zero supporting clinical trials or literature, and the mechanistic rationale runs counter to the known pathophysiology of NSIAD (receptor-level constitutive activation) and to fentanyl's known effect of increasing, not suppressing, ADH-related signalling.
- A blocking safety data gap (missing TFDA/package-insert warnings and contraindications) independently prevents this candidate from entering a safety pre-assessment.

**To proceed, the following is needed:**
- TFDA/regulatory package insert (warnings, contraindications) — currently blocking (DG001)
- Confirmed mechanism-of-action data from DrugBank or primary pharmacology sources (DG002)
- Preclinical or mechanistic evidence directly linking opioid receptor agonism to AVPR2 signalling or antidiuretic hormone regulation in NSIAD
- Any future clinical trial or case-report evidence for fentanyl in NSIAD or Tourette syndrome, should it emerge, to re-evaluate the evidence level
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

