---
layout: default
title: Icatibant
parent: 僅模型預測 (L5)
nav_order: 169
evidence_level: L5
indication_count: 7
---

# Icatibant
{: .fs-9 }

證據等級: **L5** | 預測適應症: **7** 個
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

# Icatibant: From Hereditary Angioedema (HAE) to C1 Inhibitor Deficiency

## One-Sentence Summary

Icatibant (marketed elsewhere as Firazyr®) is a synthetic decapeptide bradykinin B2-receptor antagonist used for acute attacks of hereditary angioedema (HAE) caused by C1-inhibitor deficiency. The TxGNN model's top prediction — **C1 inhibitor deficiency** — is in fact the disease the drug is already globally recognized for treating, and is supported by **23 clinical trials** (including at least 3 completed Phase 3 RCTs) and **20 publications**. The drug is currently **not marketed in Taiwan**, so this evaluation is best read as a confirmatory validation of a well-established indication rather than a novel repurposing hypothesis.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the Taiwan regulatory dataset (0 licenses on file). Trial/literature context indicates icatibant's established global indication is Hereditary Angioedema (HAE) due to C1-inhibitor deficiency (brand: Firazyr®). |
| Predicted New Indication | C1 inhibitor deficiency |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L1 (≥2 completed Phase 3 RCTs) |
| Taiwan Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Structured mechanism-of-action data from DrugBank is currently a data gap (DG002, High severity). However, the evidence pack's own literature entries repeatedly describe icatibant as a **selective bradykinin B2 receptor antagonist**. In HAE due to C1-inhibitor deficiency, insufficient C1-INH activity allows uncontrolled activation of the kallikrein-kinin cascade, leading to excess bradykinin and the characteristic episodes of subcutaneous/submucosal swelling. By blocking the B2 receptor, icatibant interrupts this pathway directly at the point where bradykinin causes vascular permeability and edema.

The "original" and "predicted" indications here are not mechanistically distant — they describe the same disease process. This is reflected in the trial evidence: pivotal Phase 3 RCTs (e.g., NCT00097695, NCT00912093, NCT00500656) and the large multinational Icatibant Outcome Survey (NCT01034969, n=1,761) were conducted specifically in HAE/C1-INH-deficiency patients. The literature further extends this rationale to **acquired** C1-inhibitor deficiency (as opposed to hereditary), where off-label icatibant use is documented in real-world cohorts (e.g., PMID 22686628, PMID 35871284), suggesting the bradykinin-blockade mechanism is applicable across both hereditary and acquired forms of the deficiency.

Because this is essentially the drug's known, extensively studied indication rather than a novel cross-disease hypothesis, the "reasonableness" of the prediction is high — but the actionable repurposing opportunity for this evaluation is narrower: it is about **bringing an already-validated therapy to the Taiwan market**, and secondarily about the **acquired C1-INH deficiency** off-label use pattern seen in the literature.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00097695](https://clinicaltrials.gov/study/NCT00097695) | Phase 3 | Completed | 84 | Randomized, double-blind, placebo-controlled pivotal trial establishing efficacy/safety of subcutaneous icatibant for acute cutaneous/abdominal HAE attacks. |
| [NCT00912093](https://clinicaltrials.gov/study/NCT00912093) | Phase 3 | Completed | 98 | Randomized, double-blind, placebo-controlled study confirming efficacy and safety of icatibant in acute HAE attacks. |
| [NCT00500656](https://clinicaltrials.gov/study/NCT00500656) | Phase 3 | Completed | 85 | Randomized, controlled comparison of subcutaneous icatibant vs. oral tranexamic acid for HAE attacks; icatibant showed faster time to symptom relief. |
| [NCT00997204](https://clinicaltrials.gov/study/NCT00997204) | Phase 3 | Completed | 151 | Open-label study on safety, tolerability, and efficacy of self-administered subcutaneous icatibant for acute HAE attacks. |
| [NCT01386658](https://clinicaltrials.gov/study/NCT01386658) | Phase 3 | Completed | 32 | Pharmacokinetics, tolerability, safety, and reproductive hormone effects of single-dose icatibant in pediatric/adolescent HAE patients. |
| [NCT03888755](https://clinicaltrials.gov/study/NCT03888755) | Phase 3 | Completed | 8 | Open-label study of efficacy, PK, and safety of icatibant in Japanese patients with acute HAE attacks. |
| [NCT04654351](https://clinicaltrials.gov/study/NCT04654351) | Phase 3 | Completed | 2 | Safety, efficacy, and PK of subcutaneous icatibant in Japanese pediatric/adolescent HAE patients. |
| [NCT01034969](https://clinicaltrials.gov/study/NCT01034969) | N/A (registry) | Completed | 1,761 | Icatibant Outcome Survey (IOS) — large multinational prospective registry documenting real-world safety and outcomes of icatibant in angioedema. |
| [NCT01457430](https://clinicaltrials.gov/study/NCT01457430) | Phase 4 | Completed | 19 | Real-world evaluation of self-administered icatibant vs. facility-administered treatment for acute HAE attacks. |
| [NCT07290855](https://clinicaltrials.gov/study/NCT07290855) | Phase 4 | Completed | 5 | Taiwan-based study of icatibant injection (Icanticure®) in bradykinin-induced angioedema; notes NHI reimbursement currently limited to diagnosed HAE. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [33602658](https://pubmed.ncbi.nlm.nih.gov/33602658/) | 2021 | Review | J Investig Allergol Clin Immunol | Overview of current C1-INH-HAE therapy including icatibant and kallikrein-kinin pathway inhibition. |
| [37898409](https://pubmed.ncbi.nlm.nih.gov/37898409/) | 2024 | Review | J Allergy Clin Immunol | Disease burden of C1-inhibitor deficiency in the Asia-Pacific region, including treatment access gaps. |
| [29757016](https://pubmed.ncbi.nlm.nih.gov/29757016/) | 2018 | Review | Expert Rev Clin Immunol | Efficacy and safety of icatibant specifically in adolescents and children (≥2 years) with C1-INH-HAE. |
| [24925394](https://pubmed.ncbi.nlm.nih.gov/24925394/) | 2014 | Review | Chem Immunol Allergy | Mechanistic review of bradykinin-mediated angioedema, including C1-inhibitor deficiency pathophysiology. |
| [26106828](https://pubmed.ncbi.nlm.nih.gov/26106828/) | 2015 | Review | Curr Opin Allergy Clin Immunol | Italian diagnostic and therapeutic experience in C1-INH-HAE management. |
| [34965883](https://pubmed.ncbi.nlm.nih.gov/34965883/) | 2021 | Observational | Allergy Asthma Clin Immunol | Real-world icatibant treatment outcomes from the Icatibant Outcome Survey (Spain cohort). |
| [22686628](https://pubmed.ncbi.nlm.nih.gov/22686628/) | 2012 | Observational | Allergy | Real-world use of icatibant in **acquired** C1-inhibitor deficiency (off-label), 8 patients, 48 treated attacks. |
| [35871284](https://pubmed.ncbi.nlm.nih.gov/35871284/) | 2023 | Observational | J Clin Pharmacol | Retrospective study showing predominant off-label prescribing of C1-INH concentrates and icatibant beyond hereditary C1-INH deficiency. |
| [37146882](https://pubmed.ncbi.nlm.nih.gov/37146882/) | 2023 | Observational | J Allergy Clin Immunol Pract | National UK survey characterizing HAE and acquired C1-inhibitor deficiency populations. |
| [30280305](https://pubmed.ncbi.nlm.nih.gov/30280305/) | 2018 | Case report | J Clin Immunol | Case series on icatibant and recombinant C1 inhibitor use for HAE attacks during pregnancy. |

---

## Taiwan Market Information

Icatibant is **not currently licensed or marketed in Taiwan** — the regulatory dataset shows 0 authorizations on file. One Taiwan-based Phase 4 study (NCT07290855) notes that a product (Icanticure®) is reimbursed by Taiwan's National Health Insurance Agency for diagnosed HAE, but formal license details were not present in this evidence pack.

---

## Safety Considerations

Please refer to the package insert for safety information. (TFDA label/warnings and contraindication data are currently unavailable — flagged as a **Blocking** data gap, DG001 — and DDI records were not found.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The predicted indication is supported by L1-level evidence (multiple completed Phase 3 RCTs plus a 1,761-patient outcomes registry) and is mechanistically well understood — but this largely reflects icatibant's existing, globally established indication rather than a novel disease link. The blocking gap is regulatory/safety readiness for the Taiwan market, not efficacy evidence.

**To proceed, the following is needed:**
- Resolve DG001 (Blocking): obtain and parse the TFDA package insert / warnings and contraindications before any S1 safety assessment.
- Resolve DG002 (High): confirm structured MOA/classification via DrugBank API rather than literature inference.
- Assess the Taiwan market-entry/import pathway, since the drug currently has zero local authorizations.
- If pursuing the acquired C1-inhibitor deficiency use case (seen as off-label in the literature), scope this as a separate, lower-evidence-level sub-indication requiring its own review.

---

### Note: Other TxGNN-Predicted Indications (Screened Out)

Six additional candidates (serpinopathy with toxic serpin polymerization, pseudo-von Willebrand disease, primary release disorder of platelets, immune-mediated necrotizing myopathy, antisynthetase syndrome, Glanzmann thrombasthenia) received high TxGNN scores but returned **zero clinical trials and zero literature** on targeted searches. The evidence pack's own mechanistic review attributes these to knowledge-graph embedding proximity (e.g., C1-INH being a serpin-family protein) rather than a plausible bradykinin-pathway link, and all are marked **Hold / L5**. No further action is recommended on these unless new evidence emerges.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

