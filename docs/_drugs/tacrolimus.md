---
layout: default
title: Tacrolimus
parent: 僅模型預測 (L5)
nav_order: 329
evidence_level: L5
indication_count: 3
---

# Tacrolimus
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

# Tacrolimus: From Dermatitis to Seborrheic Dermatitis

## One-Sentence Summary

> Tacrolimus is a topical calcineurin inhibitor whose established, evidence-pack-referenced core use is dermatitis (atopic dermatitis, marketed as Protopic®). The TxGNN model predicts it may also be effective for **seborrheic dermatitis**, with **2 clinical trials** and **20 publications** currently supporting this direction. Note: formal original-indication and mechanism-of-action data are flagged as gaps in this evidence pack (see Data Gaps below), so the statements about tacrolimus's established use are drawn from context embedded in the evidence rationale rather than a structured indication field.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available as a structured field in this pack (data gap — see Conclusion). Evidence context references dermatitis/atopic dermatitis (Protopic®) as tacrolimus's core known use. |
| Predicted New Indication | Seborrheic Dermatitis |
| TxGNN Prediction Score | 99.26% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available as a structured field (DG002, High severity). Based on the mechanistic notes embedded in the evidence pack, tacrolimus inhibits calcineurin, blocking T-cell activation and downstream inflammatory cytokine release. This is the same pathway that underlies its established use in dermatitis (atopic dermatitis, Protopic®), where it downregulates the inflammatory cascade without causing the skin atrophy/telangiectasia associated with topical corticosteroids — a property that matters for chronic, face-predominant conditions requiring long-term maintenance therapy.

Seborrheic dermatitis shares this profile: it is a chronic, relapsing inflammatory dermatosis mainly affecting the face and scalp, driven in part by an abnormal immune (T-cell/inflammatory cytokine) response to *Malassezia* yeasts. Because tacrolimus's anti-inflammatory, non-atrophogenic mechanism is not specific to the atopic dermatitis trigger, it is mechanistically plausible that the same T-cell/calcineurin-NFAT blockade would suppress the inflammatory component of seborrheic dermatitis as well — and this has already been tested directly in two completed trials (Phase 3 and Phase 4) evaluating tacrolimus ointment specifically for facial seborrheic dermatitis maintenance therapy, giving this prediction stronger-than-typical clinical grounding for an L1 candidate.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02004860](https://clinicaltrials.gov/study/NCT02004860) | Phase 3 | Completed | 120 | Evaluated tacrolimus ointment (Protopic®) for maintenance treatment of severe facial seborrheic dermatitis in adults, aiming to reduce relapse frequency and topical steroid use. |
| [NCT01591070](https://clinicaltrials.gov/study/NCT01591070) | Phase 4 | Completed | 104 | Assessed whether proactive (once/twice weekly) use of 0.1% tacrolimus ointment maintains remission and reduces exacerbation incidence in adult facial seborrheic dermatitis. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33010323](https://pubmed.ncbi.nlm.nih.gov/33010323/) | 2021 | RCT | J Am Acad Dermatol | Multicenter, double-blind RCT comparing tacrolimus 0.1% vs ciclopiroxolamine 1% for maintenance therapy in severe facial seborrheic dermatitis. |
| [26512166](https://pubmed.ncbi.nlm.nih.gov/26512166/) | 2015 | Cohort | Annals of Dermatology | Evaluated maintenance therapy of facial seborrheic dermatitis with 0.1% tacrolimus ointment following successful topical calcineurin inhibitor use. |
| [39219446](https://pubmed.ncbi.nlm.nih.gov/39219446/) | 2024 | Review (Cochrane NMA) | Clin Exp Allergy | Network meta-analysis comparing relative effectiveness/safety of topical anti-inflammatory treatments (incl. calcineurin inhibitors) for eczema-spectrum disease. |
| [27804089](https://pubmed.ncbi.nlm.nih.gov/27804089/) | 2017 | Review | American Journal of Clinical Dermatology | Systematic review of topical treatments (antifungals, keratolytics, corticosteroids, calcineurin inhibitors) for facial seborrheic dermatitis. |
| [19222250](https://pubmed.ncbi.nlm.nih.gov/19222250/) | 2009 | Review | American Journal of Clinical Dermatology | Reviews pathophysiology, safety, and efficacy of topical calcineurin inhibitors (incl. tacrolimus) as a corticosteroid-sparing option for seborrheic dermatitis. |
| [24171300](https://pubmed.ncbi.nlm.nih.gov/24171300/) | 2013 | Clinical trial | Annals of Parasitology | Compared efficacy of sertaconazole 2% cream vs. tacrolimus 0.03% cream in 60 patients with seborrheic dermatitis. |
| [37067129](https://pubmed.ncbi.nlm.nih.gov/37067129/) | 2023 | Clinical trial | Indian J Dermatol Venereol Leprol | Compared oral itraconazole (2 days) plus topical tacrolimus vs. topical tacrolimus alone for maintenance treatment of seborrheic dermatitis in Vietnam. |
| [22101215](https://pubmed.ncbi.nlm.nih.gov/22101215/) | 2012 | RCT (single-blind) | J Am Acad Dermatol | Compared hydrocortisone 1% ointment with tacrolimus 0.1% ointment for facial seborrheic dermatitis in adults. |
| [12833030](https://pubmed.ncbi.nlm.nih.gov/12833030/) | 2003 | Open pilot study | J Am Acad Dermatol | Open-label pilot study of 18 patients; 0.1% tacrolimus produced complete clearance of seborrheic dermatitis in 61% of patients. |
| [19213227](https://pubmed.ncbi.nlm.nih.gov/19213227/) | 2009 | Review | Journal of Drugs in Dermatology | Reviews current status and therapeutic horizons for facial seborrheic dermatitis treatment, including calcineurin inhibitors. |

---

## New Zealand Market Information

Tacrolimus is currently **not marketed** in New Zealand per this evidence pack (0 authorizations on record; no license entries available).

---

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and drug-interaction data are flagged as gaps in this evidence pack — DG001, Blocking severity — and could not be sourced from a Medsafe/TFDA package insert at this time.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Two completed trials (Phase 3 and Phase 4) directly evaluating tacrolimus ointment for facial seborrheic dermatitis, backed by a large, consistent literature base including one double-blind RCT, support an L1 evidence level. However, the drug is not currently marketed in New Zealand and safety data (warnings/contraindications) is entirely missing, so this cannot yet proceed without guardrails.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert warnings and contraindications (DG001 — Blocking; required before S1 safety review can begin)
- Confirmed mechanism-of-action data from DrugBank (DG002 — High priority; needed to substantiate the mechanistic rationale beyond the evidence-pack narrative text)
- Formal original-indication/regulatory-status data, since `taiwan_regulatory.licenses` and `drug.original_indications` are currently empty
- A defined New Zealand market-entry pathway, given the drug's current unmarketed status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

