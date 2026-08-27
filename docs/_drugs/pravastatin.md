---
layout: default
title: Pravastatin
parent: 僅模型預測 (L5)
nav_order: 283
evidence_level: L5
indication_count: 9
---

# Pravastatin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

# Pravastatin: From Hypercholesterolemia to Homozygous Familial Hypercholesterolemia

## One-Sentence Summary

> Pravastatin is a well-established HMG-CoA reductase inhibitor (statin) used to manage hypercholesterolemia and dyslipidemia.
> The TxGNN model predicts it may be effective for **Homozygous Familial Hypercholesterolemia (HoFH)**,
> with **1 clinical trial** (indirect, drug-mismatched) and **13 publications** currently associated with this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypercholesterolemia / mixed dyslipidemia (general statin-class indication; no New Zealand label text available — drug is not marketed there) |
| Predicted New Indication | Homozygous Familial Hypercholesterolemia (HoFH) |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known pharmacological information, pravastatin belongs to the statin (HMG-CoA reductase inhibitor) class, which lowers LDL cholesterol by inhibiting hepatic cholesterol synthesis and upregulating LDL receptor expression on hepatocyte surfaces. Its efficacy in primary hypercholesterolemia and mixed dyslipidemia has been well proven over decades of clinical use.

HoFH is caused by near-complete loss of functional LDL receptors, which severely limits the effectiveness of statins as monotherapy, since the LDL receptor upregulation pathway that statins depend on is largely unavailable in these patients. Nonetheless, statins including pravastatin are still commonly used as background/adjunct therapy alongside LDL receptor-independent treatments (e.g., PCSK9 inhibitors, apheresis) to reduce residual LDL-C burden, which is the mechanistic basis for this prediction. The relationship is therefore mechanistically plausible but capped in magnitude — statins are unlikely to be disease-modifying as monotherapy in HoFH.

Given the lack of pravastatin-specific MOA data and the absence of any direct pravastatin trial in HoFH patients, the mechanistic rationale should be regarded as supportive but not confirmatory.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03510715](https://clinicaltrials.gov/study/NCT03510715) | Phase 3 | Completed | 18 | Evaluated alirocumab (PCSK9 inhibitor), not pravastatin, in children/adolescents with HoFH on top of background therapy — assessed LDL-C reduction at 12/24/48 weeks. Relevance to pravastatin is indirect (same disease population, different drug). |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31696945](https://pubmed.ncbi.nlm.nih.gov/31696945/) | 2019 | Cochrane Review | Cochrane Database Syst Rev | Systematic review of statin use in children with familial hypercholesterolemia (including HoFH), covering efficacy and short-term safety data |
| [28685504](https://pubmed.ncbi.nlm.nih.gov/28685504/) | 2017 | Cochrane Review | Cochrane Database Syst Rev | Earlier version of the same Cochrane review on statins for pediatric familial hypercholesterolemia |
| [28437620](https://pubmed.ncbi.nlm.nih.gov/28437620/) | 2017 | Guideline | Endocr Pract | AACE/ACE dyslipidemia management guideline; positions statins within broader lipid-lowering strategy including severe FH phenotypes |
| [34425670](https://pubmed.ncbi.nlm.nih.gov/34425670/) | 2021 | Case Report | Iran Biomed J | Describes a novel LDLRAP1 splice-site variant causing familial hypercholesterolemia; genetic/mechanistic background relevant to LDL receptor pathway biology |
| [31358055](https://pubmed.ncbi.nlm.nih.gov/31358055/) | 2019 | Basic Research (iPSC model) | Stem Cell Res Ther | LDL receptor-deficient iPSC-derived hepatocyte model of FH, used for gene correction and mechanistic study of LDLR pathway |
| [15531000](https://pubmed.ncbi.nlm.nih.gov/15531000/) | 2004 | Review | Clin Ther | Reviews rosuvastatin's role across hypercholesterolemia subtypes including homozygous familial hypercholesterolemia |
| [12269853](https://pubmed.ncbi.nlm.nih.gov/12269853/) | 2002 | Review | Drugs | Reviews rosuvastatin efficacy versus atorvastatin, simvastatin, and pravastatin in dyslipidemia trials |
| [14727947](https://pubmed.ncbi.nlm.nih.gov/14727947/) | 2003 | Review | Am J Cardiovasc Drugs | Reviews ezetimibe as a cholesterol absorption inhibitor, relevant as a background combination agent in severe hypercholesterolemia |
| [9793596](https://pubmed.ncbi.nlm.nih.gov/9793596/) | 1998 | Review | Ann Pharmacother | Reviews atorvastatin efficacy/safety in primary hypercholesterolemia and mixed dyslipidemias |
| [9129869](https://pubmed.ncbi.nlm.nih.gov/9129869/) | 1997 | Review | Drugs | Reviews atorvastatin pharmacology and therapeutic potential in hyperlipidemia management |

---

## New Zealand Market Information

Pravastatin currently holds no marketing authorization in New Zealand (0 licenses on record).

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The mechanistic rationale for statin use as adjunct therapy in HoFH is well accepted in clinical guidelines (evidence level L2), but no clinical trial or publication directly tests pravastatin in HoFH — the only trial identified evaluates a different drug (alirocumab) in the same disease population, and pravastatin's efficacy as monotherapy is inherently limited by the LDL receptor deficiency that defines HoFH.

**To proceed, the following is needed:**
- Medsafe/TFDA-sourced package insert data (warnings, contraindications) — currently a blocking data gap (DG001)
- Confirmed mechanism of action data specific to pravastatin (DG002)
- Direct clinical evidence of pravastatin (not just statin-class) use in confirmed HoFH patients, ideally as adjunct to PCSK9 inhibitors or apheresis
- A regulatory pathway assessment, since pravastatin currently has no New Zealand marketing authorization
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

