---
layout: default
title: Bicalutamide
parent: 僅模型預測 (L5)
nav_order: 49
evidence_level: L5
indication_count: 10
---

# Bicalutamide
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

# Bicalutamide: From Prostate Cancer to Hypertrichosis

## One-Sentence Summary

Bicalutamide is a non-steroidal androgen receptor (AR) antagonist, widely used in prostate cancer androgen deprivation therapy but not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Hypertrichosis** (excessive hair growth), supported by **0 clinical trials** and **1 commentary publication** — making this primarily a mechanistically plausible but clinically unvalidated hypothesis at this stage.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Prostate cancer (androgen deprivation therapy) |
| Predicted New Indication | Hypertrichosis |
| TxGNN Prediction Score | 99.69% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the current evidence pack. Based on established pharmaceutical knowledge, bicalutamide is a competitive non-steroidal androgen receptor (AR) antagonist. It binds AR without activating it, blocking testosterone and dihydrotestosterone (DHT) from triggering androgen-dependent gene transcription. This is the same mechanism exploited in prostate cancer treatment, where androgen signalling drives tumour growth.

Hypertrichosis is a broad term for excessive hair growth beyond what is considered normal for age, sex, and ethnicity. The androgen-dependent subset — including cases triggered or exacerbated by elevated androgens or heightened follicular AR sensitivity — shares direct mechanistic overlap with bicalutamide's pharmacology. In hair follicles responsive to androgens, AR activation promotes miniaturisation reversal and terminal hair production; blocking AR could theoretically suppress this pathway and reduce unwanted hair growth.

The sole supporting publication (PMID 35304167) is a commentary letter in the *Journal of the American Academy of Dermatology*, commenting on a retrospective review of 35 patients in whom bicalutamide appeared to improve minoxidil-induced hypertrichosis in the setting of female pattern hair loss. While this provides indirect mechanistic plausibility and a small clinical signal, it is tier-3 evidence — an opinion piece rather than a controlled study. The TxGNN model's high score (99.69%) likely reflects strong graph-topology similarity between AR-mediated hair biology nodes rather than accumulated clinical evidence.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35304167](https://pubmed.ncbi.nlm.nih.gov/35304167/) | 2022 | Commentary/Letter | Journal of the American Academy of Dermatology | Commentary on a retrospective review (n=35) in which bicalutamide was observed to improve minoxidil-induced hypertrichosis in female pattern hair loss; highlights AR-blockade as a mechanistic rationale for managing androgen-driven excess hair growth |

---

## Cytotoxicity

Bicalutamide is an antineoplastic agent (hormone therapy for prostate cancer) and is included in cytostatic handling frameworks in most institutional policies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy — Non-steroidal androgen receptor antagonist (hormone/endocrine therapy); not a conventional cytotoxic chemotherapy |
| Myelosuppression Risk | Low — myelosuppression is not a primary toxicity of bicalutamide; not expected as a dose-limiting concern |
| Emetogenicity Classification | Minimal — oral hormone therapy with very low emetogenic potential |
| Monitoring Items | Liver function tests (LFTs) — hepatotoxicity including fatal cases reported; PSA if used for prostate cancer; CBC at baseline |
| Handling Protection | Standard oral cytostatic/hormone therapy precautions recommended per institutional cytotoxic handling policy; avoid crushing tablets |

---

## Safety Considerations

Please refer to the package insert for safety information. No local (New Zealand/Taiwan) package insert data was available in this evidence pack; consult the originator SmPC or equivalent regulatory labelling from a registered market.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only supporting evidence is a single commentary letter describing indirect, retrospective clinical observations; there are no controlled trials and no prospective data specifically evaluating bicalutamide for hypertrichosis. Evidence level L4 is insufficient to advance this indication beyond a hypothesis-generation stage.

**To proceed, the following is needed:**
- Confirm hypertrichosis subtype: mechanistic link is only plausible for **androgen-dependent** variants (e.g., minoxidil-induced, idiopathic androgen-excess, hyperandrogenism-related); non-androgenic subtypes (Ambras syndrome, genetic hair shaft abnormalities) are out of scope
- Conduct a prospective pilot study or structured case series in patients with documented androgen-excess hypertrichosis, comparing bicalutamide to standard-of-care (e.g., spironolactone, flutamide)
- Obtain MOA data from DrugBank API to complete the mechanistic link analysis
- Clarify New Zealand (Medsafe) regulatory pathway: off-label prescribing conditions, prescriber obligations, and any data-exclusivity considerations
- Obtain full package insert (SmPC) to complete safety profiling — particularly hepatotoxicity monitoring requirements — before any clinical protocol is designed

> **Note:** Among all 10 TxGNN predictions reviewed in this evidence pack, **female breast carcinoma (AR+ TNBC)** carries substantially stronger evidence (L2, Phase 2 trial NCT03650894 active, 20 publications including 2025–2026 literature). If prioritisation across indications is being evaluated, the breast cancer signal warrants a separate, higher-priority report.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

