---
layout: default
title: Isoniazid
parent: 僅模型預測 (L5)
nav_order: 181
evidence_level: L5
indication_count: 1
---

# Isoniazid
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Isoniazid: From Tuberculosis to Conjunctivitis

## One-Sentence Summary

Isoniazid is a first-line antituberculosis agent, historically used to treat tuberculosis infection.
The TxGNN model predicts it may be effective for **Conjunctivitis**, with **1 clinical trial** (not directly relevant to conjunctivitis) and **20 publications** (mostly case reports and reviews of TB-related ocular disease) associated with this direction — the evidence itself is assessed as weak and likely confounded.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Tuberculosis (isoniazid is a well-established first-line antituberculous agent; formal regulatory label text is not available in this evidence pack) |
| Predicted New Indication | Conjunctivitis |
| TxGNN Prediction Score | 99.36% |
| Evidence Level | L5 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data for isoniazid is not available in this evidence pack. Based on known information, isoniazid inhibits mycolic acid synthesis in *Mycobacterium tuberculosis*, and its efficacy in tuberculosis treatment is well established.

However, the link between isoniazid and conjunctivitis appears to be an artifact of the underlying knowledge graph rather than a genuine pharmacological signal. Three confounded relationships likely drive the high TxGNN score: (1) conjunctivitis can itself be an ocular manifestation of active tuberculosis infection, so isoniazid treats the causative mycobacterial infection rather than conjunctival inflammation as an independent indication; (2) isoniazid has documented ocular toxicity (e.g., optic neuritis), and several of the retrieved case reports describe adverse ocular reactions rather than therapeutic benefit; (3) isoniazid is used to treat mycobacterial infection following intravesical BCG instillation, where a reactive arthritis–conjunctivitis syndrome can occur as a complication — again treating the underlying mycobacterial trigger, not acting mechanistically on conjunctival inflammation itself.

One partial exception is phlyctenular keratoconjunctivitis, a delayed-hypersensitivity reaction to tuberculoprotein, for which isoniazid prophylaxis has actually been studied (PMID 14253168, 1965). This represents a plausible, TB-specific mechanistic link, but it does not generalize to conjunctivitis as a broad indication.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04094012](https://clinicaltrials.gov/study/NCT04094012) | Phase 3 | Completed | 490 | Compared systemic adverse drug reaction rates between 3HP (rifapentine + isoniazid) and 1HP regimens for latent TB infection; primary endpoint was systemic reactions, with no direct relevance to conjunctivitis efficacy or safety (relevance grade C — largely unrelated) |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [14253168](https://pubmed.ncbi.nlm.nih.gov/14253168/) | 1965 | Prophylaxis study | The American Review of Respiratory Disease | Isoniazid prophylaxis studied in phlyctenular keratoconjunctivitis among Alaskan populations with high TB prevalence |
| [5103251](https://pubmed.ncbi.nlm.nih.gov/5103251/) | 1971 | Case series | Annales d'oculistique | Local (topical) use of isoniazid in treatment of ocular tuberculosis |
| [10641112](https://pubmed.ncbi.nlm.nih.gov/10641112/) | 1999 | Case series | Oftalmologia | 28 cases of tuberculous keratoconjunctivitis, mostly children with primary TB and positive tuberculin skin tests |
| [25433746](https://pubmed.ncbi.nlm.nih.gov/25433746/) | 2014 | Case report | Canadian Journal of Ophthalmology | Conjunctival phlyctenulosis re-emphasized as a presenting sign of impending clinical tuberculosis |
| [33607832](https://pubmed.ncbi.nlm.nih.gov/33607832/) | 2021 | Case report | Medicine | Pediatric case of primary sinonasal tuberculosis presenting with phlyctenular keratoconjunctivitis |
| [26692731](https://pubmed.ncbi.nlm.nih.gov/26692731/) | 2015 | Case report | Middle East African Journal of Ophthalmology | Tuberculous conjunctivitis in an anophthalmic socket following prior TB infection |
| [17133069](https://pubmed.ncbi.nlm.nih.gov/17133069/) | 2006 | Case report | Cornea | Mycobacterium tuberculosis presenting as chronic red eye / conjunctival TB |
| [14089390](https://pubmed.ncbi.nlm.nih.gov/14089390/) | 1964 | Case report | Archives of Ophthalmology | Primary tuberculosis of the conjunctiva |
| [1363080](https://pubmed.ncbi.nlm.nih.gov/1363080/) | 1992 | Review | Optometry Clinics | Review of ocular side effects of systemic drugs; notes conjunctivitis/blepharoconjunctivitis associated with several drug classes |
| [12226788](https://pubmed.ncbi.nlm.nih.gov/12226788/) | 2002 | Case report | Deutsche Medizinische Wochenschrift | Chronic reactive arthritis with conjunctivitis after intravesical BCG therapy; isoniazid used to treat underlying mycobacterial complication |

## New Zealand Market Information

Isoniazid is currently not marketed in New Zealand, and no authorization records are available in this evidence pack.

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted association between isoniazid and conjunctivitis is not supported by a direct pharmacological mechanism. The retrieved clinical trial is unrelated, and nearly all literature reflects confounded relationships — conjunctivitis as a manifestation of TB infection, isoniazid-related ocular toxicity, or treatment of a mycobacterial trigger rather than conjunctivitis itself. Combined with L5 evidence level and no New Zealand market presence, the evidence does not support progression at this time.

**To proceed, the following is needed:**
- Isoniazid MOA and TFDA/regulatory label data (currently flagged as Blocking/High data gaps)
- A dedicated mechanistic or preclinical study on isoniazid's direct anti-inflammatory or antimicrobial effect on non-tuberculous conjunctivitis
- Clarification of whether the intended indication is conjunctivitis broadly or specifically TB-associated phlyctenular keratoconjunctivitis (the one subtype with plausible rationale)
- Safety and DDI data, currently unavailable (query status: not found)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

