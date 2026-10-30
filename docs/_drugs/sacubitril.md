---
layout: default
title: Sacubitril
parent: Model Prediction Only (L5)
nav_order: 417
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-30
description: "Sacubitril: predicted new indication Top-ranked: brain small vessel disease 1 with or without ocular anomalies (evidence level L5). Approved use: Not stated in the New Zealand licence data (the combinat…"
monitor_inn: "sacubitril"
predicted_indications: ["brain small vessel disease 1 with or without ocular anomalies", "autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome", "diabetic nephropathy", "rheumatoid arthritis", "hemoglobinopathy"]
---

# Sacubitril
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Sacubitril: From Heart Failure to brain small vessel disease 1 with or without ocular anomalies
{: .text-alpha }

## One-Sentence Summary

Sacubitril is the neprilysin-inhibitor component of the sacubitril/valsartan combination (Entresto). The licence records compiled for this report do not state an approved indication, but the combination is known as a heart failure treatment.
The TxGNN model ranks **brain small vessel disease 1 with or without ocular anomalies** first, but the best-supported prediction is **Diabetic Nephropathy** (rank 3), with **1 clinical trial** (Phase 4, status unknown) and **15 publications**.
Overall evidence for the top-ranked prediction is only model-based, so this report focuses on the best-supported candidate and notes the others briefly.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the New Zealand licence data (the combination is generally used for heart failure) |
| Predicted New Indication | Top-ranked: brain small vessel disease 1 with or without ocular anomalies (best-supported: diabetic nephropathy) |
| TxGNN Prediction Score | 0.9958 (top-ranked); 0.9950 (diabetic nephropathy) |
| Evidence Level | L5 (top-ranked); L3 (diabetic nephropathy) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold (top-ranked); diabetic nephropathy is a research question |

**Other predictions, for context**

| Rank | Predicted Indication | Score | Evidence Level | Data Available |
|------|------|------|------|------|
| 1 | Brain small vessel disease 1 with or without ocular anomalies | 0.9958 | L5 | 1 unrelated paper (systemic right ventricular failure) |
| 2 | Autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome | 0.9957 | L5 | None |
| 3 | Diabetic nephropathy | 0.9950 | L3 | 1 trial, 15 publications |
| 4 | Rheumatoid arthritis | 0.9935 | L5 | 1 unrelated case report |
| 5 | Hemoglobinopathy | 0.9918 | L5 | None |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available. Based on known pharmacology, sacubitril is a prodrug whose active metabolite inhibits neprilysin, an enzyme that breaks down natriuretic peptides. It is marketed together with valsartan, an angiotensin II receptor blocker.

For the top-ranked prediction (a rare COL4A1-related brain small vessel disease), the data compiled for this report show no supported mechanistic link. The only paper retrieved concerns systemic right ventricular failure, a different condition. The same applies to the rank 2, rank 4 and rank 5 predictions, where the high model scores are not corroborated by any relevant evidence.

Diabetic nephropathy is the most plausible candidate. Valsartan's blockade of the renin-angiotensin system is an established cornerstone of diabetic kidney disease care. Neprilysin inhibition raises natriuretic peptides, which may add renal and vascular benefit. Preclinical studies in rats and mice report reduced oxidative stress, reduced NF-κB-mediated inflammation, NLRP3 inflammasome inhibition and gut microbiota modulation. Human data are mostly from reviews and from heart failure populations with diabetes. Note that the benefit may largely reflect the valsartan component, and the contribution of sacubitril itself is not isolated.

## Clinical Trial Evidence

Clinical trials were found only for diabetic nephropathy (rank 3). No trials were registered for the other four predictions.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06501651](https://clinicaltrials.gov/study/NCT06501651) | Phase 4 | Unknown | 297 | Randomised (2:1), controlled, multicentre 12-week comparison of sacubitril/valsartan vs valsartan in essential hypertension with type 2 diabetic nephropathy; no results available |

## Literature Evidence

Literature for diabetic nephropathy (rank 3), with the most clinically relevant items listed first.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37549515](https://pubmed.ncbi.nlm.nih.gov/37549515/) | 2023 | Clinical study (randomised, 112 patients; design unclear) | Int Immunopharmacol | Nifedipine plus sacubitril/valsartan vs nifedipine plus valsartan in diabetic nephropathy with hypertension |
| [29661699](https://pubmed.ncbi.nlm.nih.gov/29661699/) | 2018 | Secondary analysis of RCT | Lancet Diabetes Endocrinol | PARADIGM-HF secondary analysis of neprilysin inhibition and renal function in type 2 diabetes with heart failure |
| [40416927](https://pubmed.ncbi.nlm.nih.gov/40416927/) | 2025 | Clinical | Diabetes Metab Syndr Obes | BOLD-MRI assessment of renal protection by sacubitril/valsartan in type 2 diabetes |
| [37625003](https://pubmed.ncbi.nlm.nih.gov/37625003/) | 2023 | Review | Diabetes Care | Update on the pillars of therapy slowing diabetic kidney disease progression |
| [35165832](https://pubmed.ncbi.nlm.nih.gov/35165832/) | 2022 | Review | Curr Hypertens Rep | Newer drugs for blood pressure and hypertensive target organ damage |
| [35992034](https://pubmed.ncbi.nlm.nih.gov/35992034/) | 2022 | Preclinical (rat) | Diabetes Metab Syndr Obes | Improved early diabetic nephropathy via NLRP3 inflammasome inhibition |
| [36589853](https://pubmed.ncbi.nlm.nih.gov/36589853/) | 2022 | Preclinical (mouse) | Front Endocrinol | Improved diabetic kidney disease and modulated gut microbiota |
| [32596035](https://pubmed.ncbi.nlm.nih.gov/32596035/) | 2020 | Preclinical (rat) | PeerJ | LCZ696 reduced oxidative stress, inflammation and glomerulosclerosis |
| [33870733](https://pubmed.ncbi.nlm.nih.gov/33870733/) | 2021 | Preclinical (mouse) | Am J Physiol Renal Physiol | Differential effects vs valsartan in db/db and KKAy mice |
| [37202215](https://pubmed.ncbi.nlm.nih.gov/37202215/) | 2023 | Preclinical (mouse) | Nephrol Dial Transplant | Improved tubulointerstitial injury by increasing renal plasma flow |

For the other predictions, the only literature retrieved was PMID 37169551 (rank 1; a cohort study of sacubitril/valsartan in systemic right ventricular failure) and PMID 42266390 (rank 4; a case report of golimumab-associated myocarditis). Neither supports the predicted indication.

## New Zealand Market Information

All three authorisations are valid, and no approved indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2554720 | Entresto 24/26 | Tablet 24.3 mg with valsartan 25.7 mg | Not recorded |
| 2554739 | Entresto 49/51 | Tablet 48.6 mg with valsartan 51.4 mg | Not recorded |
| 2554747 | Entresto 97/103 | Tablet 97.2 mg with valsartan 102.8 mg | Not recorded |

## Safety Considerations

- **Drug Interactions**: Six moderate-level interactions were identified: nitisinone, rosuvastatin, simvastatin, cobicistat, revefenacin and alpelisib.

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The top-ranked prediction and the other low-evidence predictions (ranks 2, 4 and 5) rest on model scores alone, with no supporting evidence. Diabetic nephropathy has a plausible mechanism and a Phase 4 trial, but that trial has unknown status and no results, and the human renal evidence is limited. Also, the drug is marketed in New Zealand, with 3 valid licences of 3.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe (a blocking gap for safety screening)
- Detailed mechanism of action data
- Results or status confirmation for NCT06501651, and a completed randomised trial with renal outcomes
- Evidence separating the contribution of sacubitril from that of valsartan in diabetic nephropathy
- Clarification of the regulatory pathway, given that the drug is marketed in New Zealand with 3 valid licences of 3
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

