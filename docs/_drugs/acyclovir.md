---
layout: default
title: Acyclovir
parent: Model Prediction Only (L5)
nav_order: 20
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Acyclovir: predicted new indication Punctate epithelial keratoconjunctivitis (evidence level L5). Approved use: Herpes simplex infection, herpes zoster, chickenpox, herpes labialis…"
monitor_inn: "Acyclovir"
predicted_indications: ["punctate epithelial keratoconjunctivitis", "common wart", "post-infectious neuralgia", "hepatitis C induced liver cirrhosis", "eosinophilic pustular folliculitis", "sequela of COVID-19", "vulvovaginal candidiasis", "papular urticaria", "disease of orbital region", "epidemic keratoconjunctivitis"]
---

# Acyclovir
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Acyclovir: From Herpesvirus Infections to Punctate Epithelial Keratoconjunctivitis

## One-Sentence Summary

Acyclovir is an antiviral drug originally used for herpes simplex infections, herpes zoster, chickenpox, herpes labialis, herpes simplex encephalitis and genital herpes.
The TxGNN model's top-ranked prediction is **punctate epithelial keratoconjunctivitis**, but there are **no clinical trials** and only **2 publications** (case series and case reports, neither showing acyclovir benefit) behind it.
The best-supported repurposing direction in the data is **common wart** (4 clinical trials and several randomized comparisons of intralesional acyclovir), so this report also covers it.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Herpes simplex infection, herpes zoster, chickenpox, herpes labialis, herpes simplex encephalitis, genital herpes (the New Zealand licence records contain no indication text) |
| Predicted New Indication | Punctate epithelial keratoconjunctivitis (top-ranked); common wart (best evidence, rank 2) |
| TxGNN Prediction Score | 99.67% (punctate epithelial keratoconjunctivitis); 99.46% (common wart) |
| Evidence Level | L5 for punctate epithelial keratoconjunctivitis; L2 for common wart |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold for punctate epithelial keratoconjunctivitis; Proceed with Guardrails (research question) for common wart |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Acyclovir is a guanosine analogue known to inhibit herpesvirus DNA polymerase after activation by viral thymidine kinase. Its efficacy in herpes simplex and varicella-zoster infections is well established.

**Punctate epithelial keratoconjunctivitis (rank 1).** This condition can have a herpetic or adenoviral component, which explains the model's high score. However, the literature retrieved concerns microsporidial keratoconjunctivitis and drug-induced corneal lipidosis in AIDS, neither of which responds to acyclovir. The score is a model prediction without clinical support.

**Common wart (rank 2).** Warts are caused by HPV, which lacks the viral thymidine kinase needed to activate acyclovir, so the standard mechanism does not apply. Proposed mechanisms include inhibition of host or viral DNA synthesis and immunomodulation, but these remain uncertain. Even so, several small randomized trials of intralesional acyclovir report clinical responses, which is why this is the most credible direction.

**Other predictions.** Most lower-ranked predictions are weak. Acyclovir has no antifungal activity (vulvovaginal candidiasis), no activity against HCV (hepatitis C cirrhosis), and no expected activity against adenovirus (epidemic keratoconjunctivitis). Post-infectious neuralgia, eosinophilic pustular folliculitis and papular urticaria have no supporting studies. "Disease of orbital region" largely overlaps with existing herpetic labels (herpes zoster ophthalmicus, herpetic keratitis), so it is an extension to ocular sites rather than novel repurposing. Sequelae of COVID-19 are supported only indirectly, through acyclovir treating herpesvirus reactivation.

## Clinical Trial Evidence

No clinical trials are registered for the top-ranked prediction, punctate epithelial keratoconjunctivitis. The table below shows trials for the best-supported prediction, common wart.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06261684](https://clinicaltrials.gov/study/NCT06261684) | Phase 2/3 | Unknown | 92 | Randomized trial of intralesional acyclovir vs cryotherapy in plantar warts; most direct efficacy evidence, active comparator |
| [NCT05429151](https://clinicaltrials.gov/study/NCT05429151) | Phase 4 | Unknown | 40 | Intralesional acyclovir vs Candida antigen in plantar warts |
| [NCT05324904](https://clinicaltrials.gov/study/NCT05324904) | Not applicable | Unknown | 44 | Intralesional vitamin D3 vs intralesional acyclovir in plantar warts |
| [NCT07448844](https://clinicaltrials.gov/study/NCT07448844) | Phase 4 | Recruiting | 40 | Intralesional vitamin D3 vs acyclovir in cutaneous viral warts; no results yet |

## Literature Evidence

The two publications for punctate epithelial keratoconjunctivitis (PMIDs 21934222 and 7825685) are a microsporidial case series and an AIDS corneal lipidosis report and do not support the prediction. The table below shows the most relevant literature for common wart.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [40889709](https://pubmed.ncbi.nlm.nih.gov/40889709/) | 2026 | RCT | J Am Acad Dermatol | Intralesional acyclovir vs cryotherapy in plantar warts |
| [38822848](https://pubmed.ncbi.nlm.nih.gov/38822848/) | 2024 | RCT | Arch Dermatol Res | Intralesional acyclovir vs hepatitis B vaccine in resistant plantar warts (48 patients) |
| [41686619](https://pubmed.ncbi.nlm.nih.gov/41686619/) | 2026 | Systematic review / network meta-analysis | Medicine | Compares treatments for cutaneous warts; intralesional acyclovir described as a novel option |
| [40488756](https://pubmed.ncbi.nlm.nih.gov/40488756/) | 2025 | Comparative study | Arch Dermatol Res | Intralesional acyclovir vs 5-fluorouracil in recalcitrant warts |
| [40056255](https://pubmed.ncbi.nlm.nih.gov/40056255/) | 2025 | Comparative clinical trial | Arch Dermatol Res | Intralesional acyclovir vs quadrivalent HPV vaccine in recalcitrant warts (60 patients) |
| [37303436](https://pubmed.ncbi.nlm.nih.gov/37303436/) | 2023 | Comparative study | Cureus | Intralesional acyclovir vs PPD immunotherapy in viral warts |
| [34412535](https://pubmed.ncbi.nlm.nih.gov/34412535/) | 2022 | Clinical study | J Cutan Med Surg | Intralesional acyclovir as a potential option for cutaneous warts |
| [38793636](https://pubmed.ncbi.nlm.nih.gov/38793636/) | 2024 | Case reports and review | Viruses | Remission of HPV-related disease (e.g. penile condyloma) with acyclovir or valaciclovir |
| [26885794](https://pubmed.ncbi.nlm.nih.gov/26885794/) | 2016 | Case report | J Drugs Dermatol | Persistent plantar warts resolved after oral acyclovir given for herpes zoster |

## New Zealand Market Information

All four licences are cancelled, and no indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 471690 | Lovir | Dispersible tablet 400 mg | Cancelled |
| 471704 | Lovir | Dispersible tablet 200 mg | Cancelled |
| 471712 | Lovir | Dispersible tablet 800 mg | Cancelled |
| 2490803 | ViruPOS | Eye ointment 3% | Cancelled |

## Safety Considerations

- **Disease associations from the knowledge graph (not label text):** The knowledge graph links acyclovir to contraindication edges with conditions including kidney disease and impaired renal function, hypertension, diabetes mellitus and diabetic ketoacidosis, hyperglycemia, adrenocortical insufficiency (Addison disease), obesity, stroke and cerebrovascular disorders, myocardial infarction, hemolytic-uremic syndrome, thrombocytopenic purpura, hemoglobinopathy, peripheral vascular disease and related vascular conditions, and skin atrophy. These are model-derived associations and need verification against the official label.
- **Drug interactions:** The database lists 100 interactions. Moderate-level interactions include balsalazide, mesalazine, olsalazine, sulfasalazine and trospium. Others, such as amphotericin B, metformin, morphine and omeprazole, have unclassified severity.

Please refer to the package insert for the official warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold for punctate epithelial keratoconjunctivitis; Proceed with Guardrails (research question) for common wart**

**Rationale:**
- The top-ranked prediction, punctate epithelial keratoconjunctivitis, rests only on a model score (L5), with no trials and no supportive literature. Common wart is the best-supported direction (L2): one Phase 2/3 trial and several randomized comparisons, but the trials are small, mostly single-centre, use active comparators rather than placebo, and the mechanism is uncertain.
- All New Zealand licences are cancelled, so any use would require re-establishing a marketed product.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Placebo-controlled, adequately powered trials of intralesional acyclovir in warts, with published results for the Phase 2/3 trial
- Confirmation of which intralesional formulation and route would be used, given that the former licences cover only oral dispersible tablets and an eye ointment
- Review of the knowledge-graph contraindication associations (especially renal impairment) against official labelling
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

