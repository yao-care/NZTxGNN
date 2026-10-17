---
layout: default
title: Acitretin
parent: Moderate Evidence (L3-L4)
nav_order: 19
evidence_level: L3
indication_count: 4
last_modified_at: 2026-10-15
description: "Acitretin: predicted new indication Acne (disease) (evidence level L3). Approved use: Not available"
monitor_inn: "ACITRETIN"
predicted_indications: ["acne (disease)", "pediatric systemic lupus erythematosus", "fetal erythroblastosis", "familial cutaneous telangiectasia and oropharyngeal predisposition cancer syndrome"]
---

# Acitretin
{: .fs-9 }

Evidence Level: **L3** | Predicted Indications: **4** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

# Acitretin: From an Unspecified Original Indication to Acne

## One-Sentence Summary

Acitretin is a second-generation oral retinoid. The data compiled for this report do not list an approved indication for it in New Zealand, although in practice it is best known for severe psoriasis and other keratinisation disorders.
The TxGNN model predicts it may be effective for **acne (disease)**, with **1 clinical trial** and **18 publications** currently associated with this prediction.
The clinical signal comes mainly from hidradenitis suppurativa (acne inversa), not acne vulgaris, so the evidence is indirect.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (the Medsafe licence records contain no indication text) |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L3 |
| New Zealand Market Status | Cancelled only (no currently valid authorization) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on general pharmacology, acitretin is a systemic retinoid that influences keratinocyte differentiation and proliferation. It also reduces sebaceous gland activity and follicular hyperkeratinisation, both of which are plausible mechanisms in acne-type disease.

The available clinical signal comes from hidradenitis suppurativa (acne inversa), a related follicular occlusion disorder, and not from acne vulgaris. Isotretinoin remains the preferred retinoid for acne vulgaris. The high TxGNN score therefore reflects a biologically plausible but indirect link.

A major constraint is safety. Acitretin is teratogenic and has a very long effective washout period, because it can be converted to etretinate in the presence of alcohol. This is especially important for women of childbearing potential.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07708480](https://clinicaltrials.gov/study/NCT07708480) | Phase 4 | Completed | 70 | Randomised comparison of apremilast (30 mg twice daily) and acitretin (25 mg twice daily) for 12 weeks in moderate-to-severe hidradenitis suppurativa. The primary outcome was the IHS4 severity score. No results were provided, and this is not an acne vulgaris trial. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [25640693](https://pubmed.ncbi.nlm.nih.gov/25640693/) | 2015 | Guideline | J Eur Acad Dermatol Venereol | European guideline for treating hidradenitis suppurativa/acne inversa |
| [29234829](https://pubmed.ncbi.nlm.nih.gov/29234829/) | 2018 | Review | Der Hautarzt | Drug therapy of acne inversa: antibiotics, clindamycin plus rifampicin, and TNF-α inhibitors |
| [41692081](https://pubmed.ncbi.nlm.nih.gov/41692081/) | 2026 | Review | Clin Dermatol | Vitamin A and retinoids in dermatology, including oral acitretin |
| [9074840](https://pubmed.ncbi.nlm.nih.gov/9074840/) | 1997 | Review | Drugs | Current and potential uses of retinoids, including severe acne and acne-related dermatoses |
| [1617858](https://pubmed.ncbi.nlm.nih.gov/1617858/) | 1992 | Review | Clin Pharmacokinet | Pharmacokinetics and efficacy of retinoids; isotretinoin helps severe acne, while acitretin is mainly used in psoriasis |
| [8573927](https://pubmed.ncbi.nlm.nih.gov/8573927/) | 1995 | Review | Dermatology | Retinoids and sebaceous gland activity; whether new oral retinoids have an anti-acne effect |
| [20874789](https://pubmed.ncbi.nlm.nih.gov/20874789/) | 2011 | Clinical study | Br J Dermatol | Long-term results of acitretin in hidradenitis suppurativa |
| [12080949](https://pubmed.ncbi.nlm.nih.gov/12080949/) | 2002 | Case report | Cutis | Nodulocystic acne and hidradenitis suppurativa treated with acitretin |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2384175 | Novatretin | Cap 25 mg | Not recorded (cancelled) |
| 2384167 | Novatretin | Cap 10 mg | Not recorded (cancelled) |

## Safety Considerations

- **Known contraindication signals (from the TxGNN knowledge graph, not label text):** liver failure, drug-induced liver injury, pancreatitis, hypertriglyceridemia, diabetes mellitus and diabetic ketoacidosis, obesity disorders, anxiety and dysthymic disorders, substance abuse/dependence, pseudotumor cerebri/intracranial hypertension, vision disorders including night blindness and blindness, hypoalphalipoproteinemia, and hyperostosis.
- **Drug Interactions (100 interactions found; main ones listed):**
  - **Major:** doxycycline, minocycline, tetracycline (risk of raised intracranial pressure) and vitamin A (additive toxicity).
  - **Moderate:** sulfonylureas (acetohexamide, chlorpropamide, glimepiride, glipizide, glyburide, tolazamide, tolbutamide) and naltrexone.
- **Teratogenicity:** acitretin is a known human teratogen with a very long washout period, which is the most serious constraint on use.

Please also refer to the package insert, since no label warnings were available in the compiled data.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only clinical signal is indirect, coming from hidradenitis suppurativa rather than acne vulgaris. Isotretinoin is already the preferred retinoid for acne, and acitretin carries serious teratogenicity and long-washout risks. No New Zealand authorization is currently valid, and package-insert safety data are missing. At present this is a research question, not an actionable repurposing candidate.

**To proceed, the following is needed:**
- The Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Controlled trial data in acne vulgaris, or at least a clear comparison with isotretinoin
- A pregnancy prevention and risk management plan for women of childbearing potential
- Clarification of the New Zealand regulatory status, since all licences are cancelled

---

**Other predicted indications (all Hold, evidence level L5, no trials or literature):**

| Predicted Indication | TxGNN Score | Comment |
|------|------|------|
| Pediatric systemic lupus erythematosus | 99.35% | Computational prediction only. Acitretin has some use in cutaneous lupus, but paediatric bone and growth effects and hepatotoxicity make the risk-benefit poor. |
| Fetal erythroblastosis | 99.28% | No plausible mechanism, and acitretin is contraindicated in pregnancy, so the score is likely a model artefact. |
| Familial cutaneous telangiectasia and oropharyngeal predisposition cancer syndrome | 99.10% | Only a speculative link through retinoid chemoprevention, with no supporting evidence. |
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

