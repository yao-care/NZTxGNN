---
layout: default
title: Ivermectin
parent: 僅模型預測 (L5)
nav_order: 185
evidence_level: L5
indication_count: 9
---

# Ivermectin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Ivermectin: From Parasitic Infections to Vulvovaginal Candidiasis

## One-Sentence Summary

Ivermectin is a long-established antiparasitic agent that paralyzes invertebrate parasites by binding glutamate-gated chloride channels; no formal original-indication record was provided in this evidence pack. The TxGNN model predicts potential efficacy for **Vulvovaginal Candidiasis**, but this direction is currently supported by **0 clinical trials** and **0 publications**, and no antifungal mechanism has been established for ivermectin.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Parasitic infections (inferred from known pharmacology; no structured original-indication data was provided) |
| Predicted New Indication | Vulvovaginal Candidiasis |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L5 |
| Taiwan Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in this evidence pack (`original_moa`: [Data Gap]). Based on the drug's known pharmacology, ivermectin binds invertebrate glutamate-gated chloride channels, causing parasite paralysis and death — this is the basis of its established antiparasitic use. No established antifungal (anti-*Candida*) mechanism has been demonstrated for ivermectin; only scattered in vitro reports suggest weak, non-specific antifungal activity, which is insufficient to constitute a mechanistic link to vulvovaginal candidiasis.

The 99.95% TxGNN score is therefore more plausibly explained by graph-embedding proximity — ivermectin and vulvovaginal candidiasis likely sit near shared "genitourinary/reproductive-tract infection" neighborhoods in the knowledge graph — rather than a genuine pharmacological relationship. Notably, 8 additional predicted indications for ivermectin in this evidence pack (esophageal candidiasis, HPV infection, vulvovaginitis, *C. glabrata*, congenital/neonatal candidiasis, postmenopausal atrophic vaginitis, invasive candidiasis) show the same pattern: high scores clustered around candida/genitourinary conditions, each with little or no supporting mechanism, trials, or literature. This systematic clustering further supports a graph-topology artifact rather than a true repurposing signal.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available.

## Taiwan Market Information

Ivermectin is currently **not marketed** in Taiwan, with 0 approved licenses on record — no product authorization data is available.

## Safety Considerations

Please refer to the package insert for safety information.

*Note: TFDA package insert warnings/contraindications data is currently a blocking data gap (DG001) — a preliminary safety assessment (S1) cannot proceed until this is resolved.*

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
There is no established antifungal mechanism for ivermectin, and no clinical trials or literature specifically support its use in vulvovaginal candidiasis — the high TxGNN score is best explained by graph proximity bias rather than genuine pharmacological relevance. This is compounded by a blocking gap in TFDA safety data, which prevents even a preliminary safety evaluation.

**To proceed, the following is needed:**
- TFDA package insert (warnings, contraindications) — currently blocking
- Confirmed mechanism-of-action data from DrugBank
- In vitro/preclinical evidence of genuine antifungal activity against *Candida* spp.
- At least early-phase clinical or observational data specific to vulvovaginal candidiasis
- A systematic review of the other 8 candida/genitourinary-clustered predictions before treating any of them as independent signals
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

