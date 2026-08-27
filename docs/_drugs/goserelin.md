---
layout: default
title: Goserelin
parent: 僅模型預測 (L5)
nav_order: 163
evidence_level: L5
indication_count: 3
---

# Goserelin
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

# Goserelin: From Hormone-Sensitive Cancers and Endometriosis to Amenorrhea

## One-Sentence Summary

Goserelin is a GnRH agonist historically used for hormone-responsive conditions such as prostate cancer, breast cancer, and endometriosis. The TxGNN model predicts it may be effective for **Amenorrhea**, with **7 clinical trials** (including three completed Phase 3 RCTs) and **19 publications** currently supporting this direction — largely reflecting the drug's own well-documented pharmacological effect of inducing reversible medical ovarian suppression.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from regulatory license data (drug not marketed in New Zealand); goserelin is an established GnRH agonist used for hormone-sensitive prostate cancer, breast cancer, and endometriosis |
| Predicted New Indication | Amenorrhea (disease) |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available from DrugBank in this evidence pack. However, based on the supporting evidence itself, goserelin is a gonadotropin-releasing hormone (GnRH) agonist. Continuous dosing desensitizes pituitary GnRH receptors, suppressing LH/FSH secretion and consequently ovarian steroidogenesis, producing a reversible, medically-induced state of amenorrhea (ovarian suppression). This is a direct, well-characterized pharmacological effect of the drug rather than an indirect inference.

Amenorrhea is not a "new" indication in the conventional sense — it is the intended physiological consequence of goserelin's mechanism, already exploited clinically for chemotherapy-induced ovarian protection in premenopausal breast cancer patients and for symptom control in endometriosis. The extensive Phase 3 trial and RCT literature base (IBCSG Trial VIII, OPTION trial, ZEBRA study, etc.) confirms this link was established through direct clinical investigation rather than a novel repurposing hypothesis, which is why the evidence level here reaches L1.

By contrast, the model's lower-ranked predictions (renal hypoplasia, bilateral renal hypoplasia) have zero supporting trials or literature and no plausible mechanistic link to the hypothalamic-pituitary-gonadal axis — these should be treated as likely knowledge-graph noise rather than credible signals.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00068601](https://clinicaltrials.gov/study/NCT00068601) | Phase 3 | Completed | 257 | LHRH analog (goserelin) during chemotherapy vs. chemotherapy alone to prevent ovarian failure/early menopause in hormone-receptor-negative early breast cancer |
| [NCT02483767](https://clinicaltrials.gov/study/NCT02483767) | Phase 3 | Completed | 98 | Goserelin + chemotherapy vs. chemotherapy alone to preserve ovarian function in premenopausal breast cancer |
| [NCT00427245](https://clinicaltrials.gov/study/NCT00427245) | Phase 3 | Completed | 400 | OPTION trial: goserelin to prevent early menopause during chemotherapy for Stage I–III breast cancer |
| [NCT01218581](https://clinicaltrials.gov/study/NCT01218581) | Phase 2/3 | Completed | 32 | Aromatase inhibitor vs. GnRH agonist for uterine adenomyosis management, with induced amenorrhea as an intermediate mechanism |
| [NCT03475758](https://clinicaltrials.gov/study/NCT03475758) | Phase 2 | Unknown | 100 | Goserelin for ovarian protection during cyclophosphamide-containing chemotherapy; menstruation outcome endpoint |
| [NCT02132390](https://clinicaltrials.gov/study/NCT02132390) | Phase 3 | Unknown | 300 | Adjuvant toremifene ± goserelin in premenopausal hormone-receptor-positive breast cancer, with chemotherapy-induced amenorrhea as a secondary outcome |
| [NCT00488722](https://clinicaltrials.gov/study/NCT00488722) | N/A | Unknown | N/A | Single-arm study of Zoladex 3.6mg + CEF neoadjuvant chemotherapy in hormone-responsive premenopausal breast cancer, noting reversible amenorrhea as an effect |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [17159194](https://pubmed.ncbi.nlm.nih.gov/17159194/) | 2007 | RCT | J Clin Oncol | IBCSG Trial VIII: chemotherapy followed by goserelin vs. either modality alone — effects on amenorrhea, hot flashes, and quality of life |
| [12488406](https://pubmed.ncbi.nlm.nih.gov/12488406/) | 2002 | RCT | J Clin Oncol | ZEBRA study: goserelin vs. CMF chemotherapy as adjuvant therapy in node-positive premenopausal breast cancer |
| [25187267](https://pubmed.ncbi.nlm.nih.gov/25187267/) | 2015 | RCT/Cohort | Cancer Res Treat | Goserelin-induced ovarian ablation improves survival in Stage II/III HR-positive breast cancer without chemotherapy-induced amenorrhea |
| [28472240](https://pubmed.ncbi.nlm.nih.gov/28472240/) | 2017 | RCT | Ann Oncol | Anglo Celtic OPTION trial: GnRH agonist for protection against chemotherapy-induced ovarian toxicity/premature ovarian insufficiency |
| [8513962](https://pubmed.ncbi.nlm.nih.gov/8513962/) | 1993 | RCT | Fertil Steril | Goserelin vs. low-dose oral contraceptive for pelvic pain associated with endometriosis |
| [14679153](https://pubmed.ncbi.nlm.nih.gov/14679153/) | 2003 | RCT | J Natl Cancer Inst | IBCSG Trial VIII: chemotherapy followed by goserelin vs. either modality alone in node-negative premenopausal breast cancer |
| [1533675](https://pubmed.ncbi.nlm.nih.gov/1533675/) | 1992 | Review | J R Army Med Corps | Goserelin as an effective agent for therapeutic induction of amenorrhoea |
| [12734855](https://pubmed.ncbi.nlm.nih.gov/12734855/) | 2003 | Review | Br J Surg | Review of methods for achieving ovarian ablation, including GnRH agonists, in adjuvant breast cancer treatment |
| [12353820](https://pubmed.ncbi.nlm.nih.gov/12353820/) | 2002 | Review | Breast Cancer Res Treat | Overview of LHRH agonists (goserelin) for reversible ovarian ablation in early breast cancer |
| [26951320](https://pubmed.ncbi.nlm.nih.gov/26951320/) | 2016 | Cohort | J Clin Oncol | Clinical management discussion on estradiol monitoring during ovarian suppression for breast cancer |

---

## New Zealand Market Information

Goserelin is currently **not marketed** in New Zealand under this evidence pack (0 authorizations, no license records available), so no market authorization table can be produced.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The predicted amenorrhea indication is backed by an L1 evidence level — three completed Phase 3 RCTs plus multiple additional RCTs and reviews directly evaluating goserelin's ovarian-suppressive effect — making the mechanistic link highly credible. However, the drug is not currently marketed in New Zealand and critical safety documentation (label warnings, contraindications, DDI data) is missing, so guardrails are required before any clinical or regulatory action.

**To proceed, the following is needed:**
- Official package insert / label data covering warnings, contraindications, and precautions (currently a Blocking data gap)
- DrugBank-sourced mechanism of action and drug category confirmation
- Drug-drug interaction (DDI) profile (currently not found)
- Confirmation of New Zealand Medsafe registration pathway and status
- Assessment of whether the amenorrhea indication requires new therapeutic-use registration versus reliance on existing goserelin labeling (e.g., breast cancer/endometriosis indications) in comparable markets
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

