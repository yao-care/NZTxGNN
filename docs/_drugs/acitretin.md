---
layout: default
title: Acitretin
parent: 僅模型預測 (L5)
nav_order: 15
evidence_level: L5
indication_count: 4
---

# Acitretin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Acitretin: From Psoriasis to Acne

## One-Sentence Summary

Acitretin is a second-generation aromatic retinoid, primarily established in dermatology for psoriasis and hyperkeratotic skin disorders.
The TxGNN model predicts it may be effective for **acne (disease)**, supported by **1 registered clinical trial** and **18 publications** — though the trial involves a related retinoid rather than acitretin directly, and most literature is at the case report or review level.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Psoriasis and hyperkeratotic skin disorders (based on literature evidence; no NZ regulatory record) |
| Predicted New Indication | Acne (disease) |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Acitretin is a second-generation systemic retinoid that acts as an agonist at nuclear retinoid receptors RAR-α and RAR-γ. Through this mechanism, it normalises follicular keratinisation, suppresses sebaceous gland activity, and exerts anti-inflammatory effects by modulating cytokines including IL-1β and TNF-α. These are precisely the pathological drivers of acne — excessive sebum production, abnormal follicular cornification, and perifollicular inflammation — making the mechanistic basis for this prediction coherent and biologically grounded.

Isotretinoin (13-cis-retinoic acid), a first-generation retinoid with a closely related mechanism, is the established gold standard for severe nodulocystic acne. Acitretin shares the sebosuppressive and anti-keratinising properties of isotretinoin, though its direct effect on sebaceous glands is considered comparatively weaker. Acitretin's established clinical niche has been psoriasis and keratotic genodermatoses, but case-level evidence documents its successful use in nodulocystic acne and hidradenitis suppurativa (acne inversa) — particularly in patients refractory to full courses of isotretinoin.

The TxGNN prediction is therefore mechanistically plausible given this shared retinoid biology. The key caveat is that the available clinical evidence remains at the case report and review level for acitretin specifically in classic acne vulgaris. Most controlled acne trial data involves isotretinoin, not acitretin. This distinction is critical when translating the model's prediction into a clinical development question.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04663906](https://clinicaltrials.gov/study/NCT04663906) | N/A | Unknown | 300 | Observational study examining whether nasal mucosal dryness from oral **isotretinoin** (not acitretin) increases COVID-19 infection risk in dermatology patients — indirect retinoid class safety reference only; not an efficacy study for acne |

*No registered clinical trials were identified evaluating acitretin directly in acne.*

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [12080949](https://pubmed.ncbi.nlm.nih.gov/12080949/) | 2002 | Case Report | Cutis | Acitretin successfully controlled severe nodulocystic acne and hidradenitis suppurativa in a patient refractory to two full isotretinoin courses — first direct report of acitretin use in acne |
| [20874789](https://pubmed.ncbi.nlm.nih.gov/20874789/) | 2011 | Observational Review | British Journal of Dermatology | Long-term acitretin therapy for hidradenitis suppurativa (acne inversa) across 25 years; scattered case reports showed promising results where isotretinoin had limited effect |
| [25640693](https://pubmed.ncbi.nlm.nih.gov/25640693/) | 2015 | Clinical Guideline | JEADV | European S1 guideline for hidradenitis suppurativa/acne inversa; acitretin included among therapeutic options for this severe acne-spectrum disorder |
| [29234829](https://pubmed.ncbi.nlm.nih.gov/29234829/) | 2018 | Review | Der Hautarzt | Drug therapy review for acne inversa; discusses acitretin alongside biologics and antibiotics, highlighting retinoid role in follicular disease |
| [41692081](https://pubmed.ncbi.nlm.nih.gov/41692081/) | 2026 | Review | Clinics in Dermatology | Comprehensive review of vitamin A and retinoids in dermatology; oral retinoids including acitretin covered across psoriasis, acne-spectrum, and keratotic conditions |
| [9074840](https://pubmed.ncbi.nlm.nih.gov/9074840/) | 1997 | Review | Drugs | Retinoid use across skin diseases; acitretin explicitly listed for severe acne and acne-related dermatoses alongside psoriasis and keratotic disorders |
| [8573927](https://pubmed.ncbi.nlm.nih.gov/8573927/) | 1995 | Mechanistic Review | Dermatology | Retinoids and sebaceous gland activity — mechanistic analysis of why isotretinoin dominates acne treatment, and whether acitretin's sebosuppressive effect is comparable |
| [1617858](https://pubmed.ncbi.nlm.nih.gov/1617858/) | 1992 | Review | Clinical Pharmacokinetics | Pharmacokinetics and efficacy of retinoids; isotretinoin for acne versus acitretin/etretinate for psoriasis — key PK differences explaining different clinical niches |
| [26617362](https://pubmed.ncbi.nlm.nih.gov/26617362/) | 2016 | Review | Dermatologic Clinics | Medical treatments for hidradenitis suppurativa; evidence levels assessed — highlights the gap in randomised controlled data for acitretin |
| [2112772](https://pubmed.ncbi.nlm.nih.gov/2112772/) | 1990 | Mechanistic Study | Prostaglandins | Retinoid-induced inhibition of eosinophil LTC4 production — anti-inflammatory mechanism shared across acitretin, isotretinoin, and other retinoids relevant to acne pathogenesis |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic link between acitretin and acne is scientifically coherent — shared retinoid receptor biology, sebosuppression, and anti-keratinising action directly address acne pathophysiology — and case-level literature confirms real-world use in refractory nodulocystic acne and hidradenitis suppurativa. However, no randomised controlled trial data exists for acitretin specifically in acne, and the evidence base sits at L3.

**To proceed, the following is needed:**
- A prospective controlled study evaluating acitretin in nodulocystic or cystic acne refractory to isotretinoin, with standardised endpoints (IGA, lesion count, DLQI)
- Head-to-head or dose-comparison data against isotretinoin in acne indications
- A full safety and teratogenicity risk assessment, including a pregnancy prevention programme protocol aligned with retinoid prescribing requirements (retinoids are highly teratogenic — risk management infrastructure is mandatory before any clinical use)
- Regulatory pre-consultation on New Zealand market entry pathway, given no current NZ registration
- Retrieval and formal review of the package insert for complete contraindications, key warnings, and drug interactions (currently unavailable in this evidence pack)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

