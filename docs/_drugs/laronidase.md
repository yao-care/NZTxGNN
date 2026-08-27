---
layout: default
title: Laronidase
parent: 僅模型預測 (L5)
nav_order: 196
evidence_level: L5
indication_count: 2
---

# Laronidase
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Laronidase: From Mucopolysaccharidosis Type I to Lysosomal Storage Disease with Skeletal Involvement

## One-Sentence Summary

Laronidase is a recombinant human α-L-iduronidase enzyme replacement therapy (ERT), originally developed for Mucopolysaccharidosis type I (MPS I; Hurler / Hurler-Scheie / Scheie syndrome). The TxGNN model predicts efficacy for **lysosomal storage disease with skeletal involvement**, a disease-ontology label that clinically overlaps with MPS I itself, supported by **4 publications** but **no dedicated clinical trials** registered under this specific label.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Mucopolysaccharidosis type I (MPS I; Hurler / Hurler-Scheie / Scheie syndrome) |
| Predicted New Indication | Lysosomal storage disease with skeletal involvement |
| TxGNN Prediction Score | 99.31% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, structured mechanism-of-action data (DrugBank MOA field) is not available. Based on the evidence pack's literature and rationale, Laronidase is recombinant human α-L-iduronidase — it directly supplies the lysosomal enzyme that MPS I patients lack, degrading accumulated glycosaminoglycans (heparan sulfate and dermatan sulfate). In vitro studies confirm the enzyme is taken up by fibroblasts and osteoblasts primarily via mannose-6-phosphate receptors, then trafficked to lysosomes and processed to its mature, active form.

Importantly, the predicted indication "lysosomal storage disease with skeletal involvement" is not a genuinely distinct new disease — skeletal involvement (dysostosis multiplex, joint stiffness, short stature) is a core clinical manifestation of MPS I itself. This prediction is therefore best understood as a **re-confirmation of the already-approved use** rather than a novel repurposing hypothesis. Because the enzymatic mechanism is direct and already clinically validated in MPS I, mechanistic applicability is sound, but the incremental "repurposing" value is limited.

A second candidate in this evidence pack, **Sanfilippo syndrome (MPS III)**, was also evaluated and rejected (Hold, L4). Sanfilippo syndrome results from deficiency of SGSH, NAGLU, HGSNAT, or GNS — none of which is α-L-iduronidase — so Laronidase cannot compensate for the underlying enzymatic defect. Sanfilippo syndrome is also predominantly neurodegenerative rather than skeletal, and Laronidase does not cross the blood-brain barrier, further weakening the case. This is flagged as a likely false positive from TxGNN's disease-embedding similarity rather than a valid mechanistic hypothesis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered under "lysosomal storage disease with skeletal involvement" specifically.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [12196045](https://pubmed.ncbi.nlm.nih.gov/12196045/) | 2002 | Review | BioDrugs | BioMarin's recombinant α-L-iduronidase ERT for MPS I; received US/EU orphan drug designation and FDA fast-track status; Phase I trial in 10 patients |
| [25345091](https://pubmed.ncbi.nlm.nih.gov/25345091/) | 2014 | Review | Pediatric Endocrinology Reviews | MPS I caused by α-L-iduronidase deficiency leading to GAG accumulation; spans Hurler, Scheie, and Hurler-Scheie phenotypes; diagnosed via urine GAG pattern and enzyme assay |
| [18758061](https://pubmed.ncbi.nlm.nih.gov/18758061/) | 2008 | Cohort (in vitro mechanism) | Biological & Pharmaceutical Bulletin | Laronidase taken up dose-dependently by MPS I fibroblasts and osteoblasts mainly via mannose-6-phosphate receptors, then processed to its mature lysosomal form |
| [23127271](https://pubmed.ncbi.nlm.nih.gov/23127271/) | 2012 | Cohort | Pediatric Neurology | 6.5-year follow-up of ERT in an attenuated (Scheie syndrome) case; despite treatment, the patient showed decline in overall status and disease progression |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The underlying enzyme-replacement mechanism is well-validated in MPS I, giving the prediction strong mechanistic footing (L1), but this specific disease label ("lysosomal storage disease with skeletal involvement") has no dedicated clinical trials, and the product is currently **not marketed in New Zealand**. A related candidate (Sanfilippo syndrome) was independently assessed and correctly rejected, indicating the model's broader predictions for this drug need case-by-case mechanistic screening rather than blanket acceptance.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) — currently a blocking data gap
- Structured DrugBank mechanism-of-action data
- A New Zealand market entry/authorization pathway assessment, given current unmarketed status
- Clarification on whether "lysosomal storage disease with skeletal involvement" is to be regulated as equivalent to the existing MPS I indication or requires separate clinical substantiation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

