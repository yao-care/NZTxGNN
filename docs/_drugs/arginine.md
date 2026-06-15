---
layout: default
title: Arginine
parent: 僅模型預測 (L5)
nav_order: 32
evidence_level: L5
indication_count: 1
---

# Arginine
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

# ARGININE: From Amino Acid Supplement to Gastroparesis

## One-Sentence Summary

L-Arginine is an endogenous amino acid and nutritional supplement with no currently registered therapeutic indications in New Zealand or Taiwan.
The TxGNN model predicts it may be effective for **Gastroparesis**, based on its role as the sole substrate for neuronal nitric oxide synthase (nNOS) — the key enzyme controlling pyloric relaxation and gastric emptying.
This prediction is currently supported by **1 peripherally related clinical trial** and **10 preclinical publications**, placing the evidence at an early mechanistic stage.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No registered therapeutic indications |
| Predicted New Indication | Gastroparesis |
| TxGNN Prediction Score | 99.42% |
| Evidence Level | L4 (Preclinical / mechanistic studies only) |
| New Zealand Market Status | Not currently marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

L-Arginine is the sole endogenous substrate for neuronal nitric oxide synthase (nNOS). In the enteric nervous system, nitric oxide (NO) serves as the primary inhibitory neurotransmitter, responsible for pyloric sphincter relaxation and coordinated gastric emptying. When NO production is impaired — whether through nNOS depletion, cofactor deficiency, or substrate exhaustion — the pylorus remains in a contracted state, resulting in the delayed gastric emptying that characterises gastroparesis.

The mechanistic link is directly supported by PMID 25057793 (Reichardt et al., 2014), which demonstrated that oral dexamethasone depletes L-arginine levels in mice, triggering gastroparesis, and that this effect was abolished in GR(dim) mutant mice — establishing a causal chain from arginine depletion to gastroparesis. Additional preclinical evidence shows that nNOS dysfunction is a common pathway in diabetic gastroparesis (PMID 18312542), Parkinson's disease-associated gastroparesis (PMID 35380456), and tetrahydrobiopterin (BH4)-deficient models (PMID 23639814), all conditions where the nitrergic pathway fails to provide adequate pyloric relaxation.

TxGNN's high confidence score (99.42%) is biologically coherent: if insufficient substrate supply is a root cause of nNOS-mediated gastroparesis, then supplementing L-arginine represents a pharmacologically rational intervention. However, no human clinical trials have directly tested this hypothesis, and the leap from mechanistic rodent data to clinical efficacy remains unvalidated.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT01702051](https://clinicaltrials.gov/study/NCT01702051) | N/A | Unknown | 150 | Observational study of autologous pancreatic islet transplantation after pancreatectomy to prevent diabetes. Indirectly relevant: post-pancreatectomy patients can develop gastroparesis, and the study population overlaps with a gastroparesis-prone group, but the primary endpoint is glycaemic control, not gastric motility. Arginine is not the intervention. |

> **Note:** No clinical trials directly testing L-arginine for gastroparesis were identified. The trial above has only indirect (Grade C) relevance.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [25057793](https://pubmed.ncbi.nlm.nih.gov/25057793/) | 2014 | Animal study (mechanistic) | Endocrinology | Glucocorticoid-induced L-arginine depletion directly causes gastroparesis in mice; effect abolished in GR(dim) mutants — establishes causal arginine→nNOS→NO→gastroparesis pathway |
| [35380456](https://pubmed.ncbi.nlm.nih.gov/35380456/) | 2022 | Animal study (6-OHDA PD rat) | Am J Physiol Gastrointest Liver Physiol | Impaired nitrergic relaxation of pyloric sphincter in Parkinson's disease rat model; nNOS dysfunction links PD neurodegeneration to delayed gastric emptying |
| [23639814](https://pubmed.ncbi.nlm.nih.gov/23639814/) | 2013 | Animal study (BH4-deficient mouse) | Am J Physiol Gastrointest Liver Physiol | BH4 deficiency (nNOS cofactor) induces gastroparesis in newborn mice; confirms nitrergic pathway dependence on substrate/cofactor availability |
| [18312542](https://pubmed.ncbi.nlm.nih.gov/18312542/) | 2008 | Animal study (diabetic BB-rat) | Neurogastroenterol Motil | Decreased nNOS expression in myenteric neurons of diabetic BB-rats; supports nNOS downregulation as mechanism of diabetic gastroparesis |
| [18322959](https://pubmed.ncbi.nlm.nih.gov/18322959/) | 2008 | Animal study (diabetic mouse) | World J Gastroenterol | Ghrelin and GHRP-6 improve gastric motility in diabetic gastroparesis mice; contextualises therapeutic targets in nitrergic-deficient gastric dysmotility |
| [19023028](https://pubmed.ncbi.nlm.nih.gov/19023028/) | 2009 | Animal study (vagotomy dog) | Am J Physiol Gastrointest Liver Physiol | Synchronized gastric electrical stimulation improves impaired accommodation via nitrergic pathway in vagotomised dogs; validates NO signalling as modifiable target |
| [21193530](https://pubmed.ncbi.nlm.nih.gov/21193530/) | 2011 | Animal study (hyperglycaemia rat) | Am J Physiol Gastrointest Liver Physiol | Hyperglycaemia inhibits gastric motility via nodose ganglia KATP channels; mechanistic context for metabolic gastroparesis |
| [31984783](https://pubmed.ncbi.nlm.nih.gov/31984783/) | 2020 | Animal study (rat, SNS) | Am J Physiol Gastrointest Liver Physiol | Sacral nerve stimulation improves gastric accommodation via spinal-vagal pathway; identifies neuromodulation as adjunct approach to nitrergic dysfunction |
| [8194696](https://pubmed.ncbi.nlm.nih.gov/8194696/) | 1994 | Animal study (rat, anaphylaxis) | Gastroenterology | Antigen challenge causes delayed gastric emptying in sensitised rats; characterises gastroparesis mediators in immune-mediated settings |
| [33867519](https://pubmed.ncbi.nlm.nih.gov/33867519/) | 2021 | Case report (MELAS) | Am J Case Rep | Lifestyle normalisation of lactate in m.3243A>G mitochondrial disorder carrier with MELAS; arginine is a recognised MELAS therapy but gastroparesis connection here is indirect |

---

## Safety Considerations

Please refer to the package insert for safety information. No key warnings, contraindications, or drug interaction data were identified in this evidence pack.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic case for L-arginine in gastroparesis is scientifically compelling — arginine depletion has been causally linked to gastroparesis in animal models, and the nNOS/NO pathway is well-established as the pharmacological target. However, all current evidence is preclinical (L4), with no human clinical trials directly evaluating L-arginine supplementation for gastroparesis. This gap is too large to proceed without additional validation.

**To proceed, the following is needed:**

- **Proof-of-concept human study**: A small Phase 1/2 trial measuring gastric emptying time (scintigraphy or breath test) before and after oral or IV L-arginine supplementation in patients with documented gastroparesis
- **Baseline arginine profiling**: Plasma L-arginine and citrulline levels in gastroparesis patients compared to controls, to confirm substrate deficiency is present in the target population
- **Mechanistic MOA data (DrugBank)**: Formal documentation of L-arginine's pharmacology, including nNOS affinity, bioavailability by route, and dose-response for NO production
- **Safety data**: Package insert review for relevant warnings, particularly in patients with hepatic impairment (urea cycle disorders), renal insufficiency, or herpes simplex infections (arginine is a known viral replication promoter)
- **Drug interaction data**: Arginine may potentiate vasodilatory effects of antihypertensives and PDE5 inhibitors; formal DDI screening is needed before clinical use
- **Route and dose optimisation**: Determine whether oral supplementation achieves sufficient enteric tissue concentrations to restore nNOS substrate supply, or whether IV administration is required
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

