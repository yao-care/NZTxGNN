---
layout: default
title: Flucloxacillin
parent: 僅模型預測 (L5)
nav_order: 153
evidence_level: L5
indication_count: 10
---

# Flucloxacillin
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

# Flucloxacillin: From Staphylococcal Infections to Conjunctivitis

## One-Sentence Summary

Flucloxacillin is a penicillinase-resistant, narrow-spectrum β-lactam antibiotic traditionally used to treat infections caused by *Staphylococcus aureus* and other susceptible Gram-positive bacteria (e.g., skin/soft tissue, bone/joint infections). The TxGNN model predicts it may be effective for **Conjunctivitis**, but this direction is currently supported only by **0 clinical trials** and **3 publications**, none of which directly studies flucloxacillin as a conjunctivitis treatment.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not marketed in New Zealand — no local approved indication text on file. Internationally, flucloxacillin is indicated for infections caused by penicillinase-producing staphylococci (skin/soft tissue, bone/joint, respiratory) |
| Predicted New Indication | Conjunctivitis |
| TxGNN Prediction Score | 99.84% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (DG002, High severity data gap). Based on known information, flucloxacillin is an isoxazolyl penicillin that inhibits bacterial cell wall synthesis by binding penicillin-binding proteins (PBPs), and its resistance to staphylococcal β-lactamase makes it a first-line agent for *S. aureus* infections. Its efficacy against staphylococcal skin, soft-tissue, and bone/joint infections is well established internationally.

Bacterial conjunctivitis can, in a subset of cases, be caused by *S. aureus*, which provides a superficial mechanistic rationale for why a model trained on drug-disease association data might link flucloxacillin to conjunctivitis — anti-staphylococcal antibiotics are pharmacologically plausible candidates for staphylococcal ocular surface infection. Staphylococcal scalded skin syndrome (SSSS), for which systemic flucloxacillin is standard treatment, can also present with conjunctivitis as a prodromal feature, which likely contributes to the co-occurrence signal the model picked up.

However, this rationale should be read critically: of the three supporting publications, none is a direct efficacy study of flucloxacillin in conjunctivitis. The SSSS review supports treatment of the underlying staphylococcal disease (not conjunctivitis itself, which is a secondary symptom); the gonococcal dacryoadenitis case report involves *Neisseria gonorrhoeae*, a pathogen not covered by flucloxacillin's spectrum; and the HSV atypical-presentation article is unrelated to bacterial infection or to flucloxacillin's mechanism entirely. The evidence base is indirect and low-specificity rather than a confirmed treatment signal. It is also worth noting that conjunctivitis was the least implausible of ten TxGNN candidates for this drug — the other nine (rheumatoid arthritis, leprosy, Prinzmetal angina, thrombotic disease, and several rare genetic syndromes) show no credible mechanistic link and are considered model noise or literature-cooccurrence artifacts rather than genuine repurposing candidates.

---

## Clinical Trial Evidence

Currently no related clinical trials registered

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12627992](https://pubmed.ncbi.nlm.nih.gov/12627992/) | 2003 | Review | American Journal of Clinical Dermatology | Describes staphylococcal scalded skin syndrome (SSSS), which can present with conjunctivitis as a prodromal symptom; standard treatment targets the underlying *S. aureus* infection (indirect support only — treats SSSS, not conjunctivitis itself) |
| [1286123](https://pubmed.ncbi.nlm.nih.gov/1286123/) | 1992 | Review | International Journal of STD & AIDS | Discusses atypical presentations of herpes simplex virus infection; viral etiology, unrelated to flucloxacillin's antibacterial mechanism |
| [41884366](https://pubmed.ncbi.nlm.nih.gov/41884366/) | 2026 | Case Report | Case Reports in Ophthalmology | Case of gonococcal dacryoadenitis causing hyperacute bacterial conjunctivitis; caused by *Neisseria gonorrhoeae*, a pathogen outside flucloxacillin's spectrum |

---

## New Zealand Market Information

Flucloxacillin is not currently marketed in New Zealand (0 authorizations on file); no product registration data is available.

---

## Safety Considerations

Please refer to the package insert for safety information. Note: TFDA/regulatory label warnings and contraindications for this drug are currently unresolved (DG001 — Blocking severity), which by itself prevents progression to a formal S1 safety review.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The conjunctivitis signal rests on mechanistic plausibility and indirect literature (case reports/reviews that do not directly test flucloxacillin against conjunctivitis) rather than direct efficacy evidence, and no clinical trials exist for this indication. Combined with a blocking data gap in regulatory safety information and the drug's current non-marketed status in New Zealand, the evidence does not yet support advancing beyond a research question.

**To proceed, the following is needed:**
- Resolution of the blocking TFDA package insert data gap (warnings/contraindications, DG001)
- Confirmed mechanism-of-action data from DrugBank (DG002)
- Preclinical or clinical evidence specifically evaluating flucloxacillin (systemic or topical/ophthalmic) against staphylococcal conjunctivitis
- Assessment of route compatibility, since conjunctivitis treatment typically requires topical/ophthalmic formulations not currently confirmed as available
- A regulatory pathway assessment for New Zealand market entry, given the drug is not currently marketed there
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

