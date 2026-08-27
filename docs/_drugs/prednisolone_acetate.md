---
layout: default
title: Prednisolone Acetate
parent: 僅模型預測 (L5)
nav_order: 285
evidence_level: L5
indication_count: 10
---

# Prednisolone Acetate
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

# Prednisolone Acetate: From Steroid-Responsive Ocular Inflammation to Vernal Conjunctivitis

## One-Sentence Summary

Prednisolone acetate is a topical ophthalmic corticosteroid generally used to treat steroid-responsive ocular inflammatory conditions. The TxGNN model predicts it may also be effective for **Vernal Conjunctivitis** (vernal keratoconjunctivitis, VKC), with **1 clinical trial** and **19 publications** — including a comparative model study testing the drug directly — making this the best-supported candidate among 10 TxGNN-predicted indications for this drug.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not documented in this evidence pack (no market license on file); prednisolone acetate is generally used as a topical corticosteroid for ocular inflammation |
| Predicted New Indication | Vernal Conjunctivitis |
| TxGNN Prediction Score | 99.58% |
| Evidence Level | L2 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap, high severity). Based on known information, prednisolone acetate belongs to the corticosteroid (glucocorticoid) class and acts as a topical anti-inflammatory/immunosuppressive agent for ocular surface inflammation; its general efficacy in steroid-responsive ocular inflammatory conditions is well established in clinical practice.

Vernal conjunctivitis is a chronic, type 1/4-hypersensitivity–mediated allergic inflammation of the conjunctiva. Topical corticosteroids are already the recognized standard of care for acute VKC flares, sitting mechanistically close to the drug's established anti-inflammatory role — this is reflected in the literature, where severe VKC cases are routinely escalated to topical corticosteroids after antihistamines or mast-cell stabilizers fail (PMID 12931748).

Importantly, one tier-1 comparative study (PMID 24055903) tested **prednisolone acetate 1% directly** (not just the steroid class) against cyclosporine A and epinastine in an experimental allergic conjunctivitis model, demonstrating anti-inflammatory efficacy consistent with VKC pathophysiology. This is why vernal conjunctivitis stands out: among the 10 TxGNN-predicted indications for this drug, only vernal conjunctivitis (L2, Proceed with Guardrails) and papillary conjunctivitis (L2, Research Question) reach meaningful clinical evidence; the remaining candidates — including the top-ranked "chronic follicular conjunctivitis" — have only case-report-level or no supporting evidence and are recommended Hold.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04705584](https://clinicaltrials.gov/study/NCT04705584) | NA | Unknown | 180 | Comparative study of topical cyclosporine A 2% vs. tacrolimus 0.3% as steroid alternatives in resistant spring catarrh (VKC); directly targets the VKC population but does not test prednisolone acetate itself, and trial status is unknown (Grade A relevance to indication, limited by design/uncertain completion). |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24055903](https://pubmed.ncbi.nlm.nih.gov/24055903/) | 2013 | RCT/Comparative | Cornea | Direct comparison of prednisolone acetate 1%, cyclosporine A 0.05%/2%, and epinastine in an experimental allergic conjunctivitis model — anti-inflammatory efficacy data specific to this drug. |
| [12931748](https://pubmed.ncbi.nlm.nih.gov/12931748/) | 2003 | Cohort/Epidemiological | Asian Pac J Allergy Immunol | Prospective randomized cross-over study of VKC treatment in Thai patients; severe cases required topical corticosteroids after antihistamine/lodoxamide failure. |
| [22378107](https://pubmed.ncbi.nlm.nih.gov/22378107/) | 2012 | Clinical Study | Cornea | Evaluated topical calcineurin inhibitors as a steroid-sparing option for steroid-dependent atopic keratoconjunctivitis, implying corticosteroids remain first-line before escalation. |
| [26984315](https://pubmed.ncbi.nlm.nih.gov/26984315/) | 2016 | Cohort | Advances in Therapy | Review of intraocular pressure (IOP) elevation risk with topical ophthalmic corticosteroids in chronic ocular inflammation management — key safety consideration for the steroid class. |
| [9713785](https://pubmed.ncbi.nlm.nih.gov/9713785/) | 1998 | Cohort | Journal of Glaucoma | Long-term IOP monitoring during topical corticosteroid use for giant papillary/allergic conjunctivitis-type indications. |
| [21575117](https://pubmed.ncbi.nlm.nih.gov/21575117/) | 2012 | Case Series | Clin Exp Ophthalmol | Severe VKC requiring trabeculectomy for corticosteroid-induced glaucoma — highlights long-term steroid safety risk in this population. |
| [24021018](https://pubmed.ncbi.nlm.nih.gov/24021018/) | 2014 | Clinical Study | Cutaneous and Ocular Toxicology | Evaluated retinal nerve fiber layer thickness in VKC patients under long-term topical corticosteroid therapy — safety monitoring data. |
| [17102679](https://pubmed.ncbi.nlm.nih.gov/17102679/) | 2006 | Case Report | Cornea | Secondary bacterial keratitis/shield ulcer as a VKC complication, underscoring the need for concurrent anti-infective management alongside steroid use. |
| [18063892](https://pubmed.ncbi.nlm.nih.gov/18063892/) | 2007 | Case Report | Korean J Ophthalmol | Conjunctival inclusion cysts observed during 16-month follow-up of chronic VKC — long-term disease course data. |
| [8977483](https://pubmed.ncbi.nlm.nih.gov/8977483/) | 1996 | Preclinical | Invest Ophthalmol Vis Sci | Mouse model showing cyclosporine (a steroid alternative) inhibits mast-cell-mediated conjunctivitis, supporting the shared inflammatory mechanism corticosteroids also target. |

---

## New Zealand Market Information

No marketing authorizations are currently on file for New Zealand — prednisolone acetate is not marketed there (0 licenses recorded).

---

## Safety Considerations

Please refer to the package insert for safety information. Drug-label warnings and contraindications for this product are currently a **blocking data gap** (DG001) and must be resolved before any safety pre-screening (S1) can proceed. Note that literature on the broader topical corticosteroid class cited above (PMIDs 26984315, 9713785, 21575117, 24021018) consistently flags IOP elevation and corticosteroid-induced glaucoma as class-level risks relevant to long-term use in VKC.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A tier-1 comparative study directly testing prednisolone acetate in an allergic conjunctivitis model, combined with established literature support for topical corticosteroids as standard therapy for VKC flares, gives this indication the strongest evidence base (L2) among the 10 TxGNN candidates evaluated for this drug — but no clinical trial registers prednisolone acetate itself for VKC, and label-level safety data remains unresolved.

**To proceed, the following is needed:**
- TFDA/package-insert warnings and contraindications (DG001, blocking)
- Detailed mechanism of action / drug classification confirmation (DG002)
- New Zealand regulatory pathway assessment for this ophthalmic corticosteroid
- Long-term IOP/glaucoma monitoring protocol, given corticosteroid-class safety signals identified in the literature
- Consider parallel evaluation of papillary conjunctivitis (L2, Research Question stage) as a secondary candidate
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

