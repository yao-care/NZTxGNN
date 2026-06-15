---
layout: default
title: Captopril
parent: 僅模型預測 (L5)
nav_order: 62
evidence_level: L5
indication_count: 4
---

# Captopril
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

# Captopril: From Hypertension to Malignant Renovascular Hypertension

## One-Sentence Summary

Captopril is a first-generation ACE (angiotensin-converting enzyme) inhibitor, classically used globally to treat hypertension, heart failure, post-myocardial infarction left ventricular dysfunction, and diabetic nephropathy.
The TxGNN model predicts it may be effective for **Malignant Renovascular Hypertension**, with **0 clinical trials** and **20 publications** currently supporting this direction.
Evidence is predominantly observational and mechanistic (L3), but the pharmacological rationale is exceptionally strong given that the RAAS axis is the direct pathological driver of this condition.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not registered in New Zealand; internationally approved for hypertension and heart failure |
| Predicted New Indication | Malignant Renovascular Hypertension |
| TxGNN Prediction Score | 99.28% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Captopril was the first oral ACE inhibitor developed, designed to block the conversion of angiotensin I to angiotensin II (Ang II) within the renin-angiotensin-aldosterone system (RAAS). By reducing circulating Ang II, captopril simultaneously lowers systemic vascular resistance, reduces aldosterone-driven sodium retention, and blunts the vasopressor cascade — making it one of the most mechanistically direct antihypertensive agents available.

Malignant renovascular hypertension is caused by renal artery stenosis triggering runaway RAAS activation: ischemic kidneys hypersecrete renin → Ang II surges → aldosterone rises → severe hypertension with end-organ damage ensues. Captopril sits precisely at the central node of this cascade. Captopril renal scintigraphy (the "captopril renogram") is, in fact, a standard diagnostic tool that *exploits this very mechanism* — captopril administration unmasks the renin-dependent nature of renovascular hypertension by precipitating a fall in GFR on the affected side, producing a characteristic scan pattern. This dual diagnostic-therapeutic relationship is strong mechanistic evidence that captopril acts directly on the disease pathway.

The primary safety concern in this indication is bilateral renal artery stenosis: when both kidneys depend on Ang II to maintain glomerular filtration pressure, ACE inhibition can precipitate acute kidney injury. Clinical use therefore requires pre-screening by renal imaging and close monitoring of creatinine and potassium after initiation. Within these guardrails, the mechanistic and observational evidence strongly supports captopril's role in managing this condition.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for captopril in malignant renovascular hypertension.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [6145432](https://pubmed.ncbi.nlm.nih.gov/6145432/) | 1984 | Prospective Case Series | Bulletin of the All-Union Cardiological Research Center | Direct clinical experience with captopril in both stable-phase and malignant-phase arterial hypertension; one of the earliest reports evaluating captopril specifically in the malignant course |
| [232024](https://pubmed.ncbi.nlm.nih.gov/232024/) | 1979 | Clinical Study | Clinical Science | Captopril induced PRA elevation >14 ng/h/ml in 43 of 44 untreated renovascular hypertension patients; response was absent in normal-renin essential hypertension — confirming captopril's selectivity for renin-driven disease |
| [2887673](https://pubmed.ncbi.nlm.nih.gov/2887673/) | 1987 | Mechanistic Study | Japanese Heart Journal | Serial measurements of PRA, angiotensin I/II, catecholamines, and vasopressin during benign-to-malignant transition in 2K2C Goldblatt hypertensive dogs; delineates the RAAS trajectory that captopril targets |
| [3894732](https://pubmed.ncbi.nlm.nih.gov/3894732/) | 1985 | Observational Cohort | Japanese Journal of Medicine | Captopril evaluated as both diagnostic and therapeutic tool in renovascular hypertension and primary aldosteronism; sodium balance highlighted as critical variable |
| [8070421](https://pubmed.ncbi.nlm.nih.gov/8070421/) | 1994 | Case Series + Review | Endocrinology and Metabolism Clinics of North America | In renin-secreting tumors causing severe malignant hypertension with hypokalemia, blood pressure dropped markedly with converting enzyme inhibitor treatment; supports captopril in renin-mediated malignant hypertension |
| [11334320](https://pubmed.ncbi.nlm.nih.gov/11334320/) | 2001 | Case Report + Review | Clinical Nephrology | Two NF1 cases with renovascular hypertension; basal PRA 2.8 ng/ml/h rose to 12.6 ng/ml/h post-captopril, confirming RAAS-dependent hypertension; captopril challenge used as confirmatory test |
| [10955932](https://pubmed.ncbi.nlm.nih.gov/10955932/) | 2000 | Case Series | Pediatric Nephrology | 27 NF1 pediatric patients studied with captopril test and Doppler ultrasonography for renovascular hypertension screening; captopril test central to diagnostic algorithm |
| [17008836](https://pubmed.ncbi.nlm.nih.gov/17008836/) | 2006 | Clinical Review | Minerva Medica | Renovascular hypertension review: treatment must target the renin-angiotensin system; mere anatomical stenosis without functional RAAS activation is insufficient for diagnosis or treatment |
| [2040938](https://pubmed.ncbi.nlm.nih.gov/2040938/) | 1991 | Review | Journal of Pediatrics | Review of malignant hypertension including pathophysiology and treatment considerations in pediatric populations |
| [1572120](https://pubmed.ncbi.nlm.nih.gov/1572120/) | 1992 | Case Report | Clinical Nuclear Medicine | Patient with malignant hypertension showed positive captopril renogram without angiographic renal artery stenosis; highlights diagnostic complexity and that captopril renography reflects renin-dependency, not anatomy alone |

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note for clinical teams:** Based on the pharmacological mechanism, particular vigilance is warranted for:
> - **Acute kidney injury risk** in bilateral renal artery stenosis or solitary functioning kidney — creatinine and potassium should be checked within 1–2 weeks of initiation
> - **First-dose hypotension** — particularly in volume-depleted or high-renin states, which characterize this indication
> - **Angioedema** — class effect of ACE inhibitors; contraindicated in prior ACE inhibitor-related angioedema

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The pharmacological mechanism of captopril is a near-perfect match for the pathophysiology of malignant renovascular hypertension — RAAS overactivation from renal artery stenosis is the disease's core driver, and ACE inhibition is the most direct pharmacological intervention available. Multiple observational studies and mechanistic data support its use, and the captopril renogram's diagnostic role is itself evidence of on-target drug action in this condition. The L3 evidence level reflects the lack of formal RCTs, not absence of mechanistic or clinical support.

**To proceed, the following is needed:**

- **Renal vascular imaging before initiation** (duplex ultrasound or CTA) to rule out bilateral renal artery stenosis or stenosis in a solitary kidney — these are relative contraindications requiring specialist nephrology input
- **Baseline and follow-up renal function monitoring** (serum creatinine, eGFR, potassium at 1 and 4 weeks)
- **Formal MOA documentation** from DrugBank — the data gap in original_moa should be resolved to complete regulatory filing materials
- **Safety profile documentation** from the package insert (TFDA or Medsafe equivalent) — DG001 is still a blocking gap for the full S1 safety assessment
- **Consideration of a prospective registry or observational study** in this patient population, given the absence of registered clinical trials — an L3→L2 upgrade would substantially strengthen the evidence base
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

