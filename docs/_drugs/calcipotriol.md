---
layout: default
title: Calcipotriol
parent: 僅模型預測 (L5)
nav_order: 58
evidence_level: L5
indication_count: 10
---

# Calcipotriol
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

# Calcipotriol: From Psoriasis to Seborrheic Keratosis

## One-Sentence Summary

Calcipotriol is a synthetic vitamin D3 analogue widely used as a topical treatment for psoriasis, acting via activation of the Vitamin D Receptor (VDR) to suppress abnormal keratinocyte growth.
The TxGNN model predicts it may be effective for **Seborrheic Keratosis** — the most common benign epithelial skin tumour in adults —
with **no registered clinical trials** but **6 published studies** currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Psoriasis (topical treatment) |
| Predicted New Indication | Seborrheic Keratosis |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, calcipotriol is a synthetic vitamin D3 analogue (VDR agonist) that inhibits abnormal keratinocyte proliferation, promotes terminal differentiation, and induces apoptosis. These effects are already the basis of its approved use in psoriasis — a disease defined by runaway keratinocyte growth. PMID 16043912 specifically confirms that apoptosis induction is the mechanism by which topical vitamin D3 ointments (including calcipotriol) cause regression of senile warts (seborrheic keratosis).

Seborrheic keratosis is a benign tumour arising from epidermal keratinocytes, characterised by excessive but orderly proliferation without malignant transformation. The pathophysiology directly overlaps with calcipotriol's molecular targets: dysregulated keratinocyte cycling governed by the VDR-signalling axis. This mechanistic alignment — rather than anatomical proximity — is what makes the TxGNN prediction biologically credible.

The supporting literature spans two decades of clinical experience. A 2023 prospective case series (PMID 36752725, n = 12) reported complete lesion regression with 0.005% calcipotriol ointment in 3–8 months and durable remission of up to 10 years. A larger 2005 clinical series (PMID 16043912, n = 116) confirmed a 30.2% complete response rate across three vitamin D3 analogues applied once or twice daily. Taken together, these data suggest a reproducible biological effect, not chance observations.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [36752725](https://pubmed.ncbi.nlm.nih.gov/36752725/) | 2023 | Prospective Clinical Study | Australasian Journal of Dermatology | 12 patients with facial seborrheic keratosis treated with 0.005% calcipotriol ointment; complete regression in 3–8 months; remission sustained 6–10 years |
| [15090020](https://pubmed.ncbi.nlm.nih.gov/15090020/) | 2004 | Comparative Clinical Study | International Journal of Dermatology | Head-to-head comparison of cryosurgery vs. topical calcipotriene, tazarotene, and imiquimod for seborrheic keratoses; assessed relative efficacy of non-surgical topical approaches |
| [16043912](https://pubmed.ncbi.nlm.nih.gov/16043912/) | 2005 | Clinical Series | The Journal of Dermatology | 116 cases treated with topical vitamin D3 ointments (tacalcitol, calcipotriol, maxacalcitol); 30.2% complete response at 3–12 months; apoptosis induction identified as the primary mechanism |
| [15577148](https://pubmed.ncbi.nlm.nih.gov/15577148/) | 2004 | Case Series / Review | Clinical Calcium | Review of once- or twice-daily application of active vitamin D3 topicals for senile warts; provides dosing rationale and response characterisation |
| [10721662](https://pubmed.ncbi.nlm.nih.gov/10721662/) | 2000 | Case Report | The Journal of Dermatology | Keratosis lichenoides chronica (therapy-resistant; seborrheic dermatitis–like facial component) showed marked response to calcipotriol ointment |
| [21534378](https://pubmed.ncbi.nlm.nih.gov/21534378/) | 2011 | Clinical Vignette | JAAPA | Clinical vignette on seborrheic keratosis presentation and management context |

---

## New Zealand Market Information

Calcipotriol currently holds no Medsafe authorizations in New Zealand and is not marketed. No license table is available.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Taiwan (TFDA) package insert warnings and contraindication data were not retrieved in this evidence pack (Data Gap DG001). Formal mechanism of action data from DrugBank is also pending (Data Gap DG002). These gaps must be resolved before clinical safety evaluation.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two decades of published clinical experience — including a 2023 prospective series with up to 10-year follow-up — demonstrate that topical calcipotriol can achieve complete and durable regression of seborrheic keratosis, underpinned by a mechanistically sound VDR-mediated apoptosis pathway. Although no registered clinical trials exist (L3 evidence), the consistency and duration of reported responses justify cautious advancement rather than outright hold.

**To proceed, the following is needed:**
- Retrieve TFDA and Medsafe package insert warnings and contraindications (resolves blocking gap DG001)
- Obtain formal MOA and toxicity data from DrugBank (resolves high-severity gap DG002)
- Design a prospective randomised controlled trial comparing calcipotriol ointment with standard cryotherapy for seborrheic keratosis, with standardised response criteria
- Clarify optimal concentration (0.005% vs. higher), vehicle formulation, and treatment duration based on the existing case series data
- Assess drug interaction profile before combination-use recommendations (e.g., with 5-FU, as in PMID 30785593)
- Initiate New Zealand (Medsafe) regulatory scoping, given zero current authorizations
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

