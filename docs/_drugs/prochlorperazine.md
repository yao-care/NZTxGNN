---
layout: default
title: Prochlorperazine
parent: 僅模型預測 (L5)
nav_order: 291
evidence_level: L5
indication_count: 10
---

# Prochlorperazine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Prochlorperazine: From Nausea/Vomiting (Antiemetic) to Retinal Dystrophy with or without Extraocular Anomalies

## One-Sentence Summary

> Prochlorperazine is a phenothiazine-class D2-dopamine antagonist, conventionally used to treat nausea, vomiting, vertigo, and (historically) as an antipsychotic/anxiolytic.
> The TxGNN model's top-ranked prediction is **Retinal Dystrophy with or without Extraocular Anomalies**, with a prediction score of **99.9998%**,
> but **no clinical trials and no drug-specific literature** support this link — the retrieved publications are general ophthalmology reviews that do not mention prochlorperazine, and the evidence pack itself flags this as a likely **false-positive** association.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Nausea, vomiting and vertigo (phenothiazine antiemetic/antipsychotic class; not confirmed via NZ regulatory license, as the drug is not currently marketed there) |
| Predicted New Indication | Retinal Dystrophy with or without Extraocular Anomalies |
| TxGNN Prediction Score | 99.9998% |
| Evidence Level | L5 (model prediction only, no supporting studies) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action (MOA) data for prochlorperazine is not available in the source records. Based on general pharmacological knowledge, prochlorperazine belongs to the phenothiazine class and acts primarily as a D2-dopamine receptor antagonist (with additional H1-antihistaminic and anticholinergic activity), an action well established for nausea/vomiting, vertigo, and psychotic symptom control.

For the top-ranked predicted indication — retinal dystrophy with or without extraocular anomalies — **the mechanistic case is weak to absent**. Other phenothiazines (chlorpromazine, thioridazine) are known to cause drug-*induced* pigmentary retinopathy as an **adverse effect**, which creates an indirect, negative (toxicity-related) association between "phenothiazine" and "retina" in the literature — not a therapeutic one. None of the retrieved publications for this indication mention prochlorperazine or any pharmacological treatment of retinal dystrophy; they are general ophthalmology reviews and case reports on congenital orbital/ocular anomalies. This pattern is consistent with a **keyword co-occurrence false positive** in the TxGNN embedding space rather than a genuine repurposing signal.

Of note, a lower-ranked candidate in this same evidence pack — **manic bipolar affective disorder** (rank 10, score 99.98%) — has a more coherent mechanistic story: phenothiazines were used historically for acute mania/agitation, and one retrieved case report (PMID 13617778) directly documents a clinical effect of prochlorperazine in a manic-depressive patient. That signal, while still preliminary (L4/S1), is mechanistically far more plausible than the top-ranked retinal indication and may be a more productive direction for follow-up research than the disease formally reported here.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9416661](https://pubmed.ncbi.nlm.nih.gov/9416661/) | 1997 | Review | Semin Ultrasound CT MR | General review of orbital infections (sinusitis-related cellulitis); no mention of prochlorperazine or retinal dystrophy treatment |
| [20127583](https://pubmed.ncbi.nlm.nih.gov/20127583/) | 2010 | Review | Semin Neurol | Clinical approach to diplopia; unrelated to drug therapy of retinal disease |
| [38321238](https://pubmed.ncbi.nlm.nih.gov/38321238/) | 2024 | Review | Pediatr Radiol | Imaging differential diagnosis of pediatric congenital ocular pathologies; no drug relevance |
| [38249493](https://pubmed.ncbi.nlm.nih.gov/38249493/) | 2023 | Review | Taiwan J Ophthalmol | Congenital anomalies of lens shape; developmental, not pharmacological |
| [22241537](https://pubmed.ncbi.nlm.nih.gov/22241537/) | 2012 | Review | Klin Monbl Augenheilkd | Congenital ptosis pathophysiology; no drug relevance |
| [109006](https://pubmed.ncbi.nlm.nih.gov/109006/) | 1979 | Case Report | Am J Ophthalmol | Case series of unilateral cryptophthalmia; structural anomaly, no treatment discussed |
| [7035111](https://pubmed.ncbi.nlm.nih.gov/7035111/) | 1981 | Review | Doc Ophthalmol | Wagner-Stickler syndrome vitreoretinal degeneration; genetic condition, no drug link |
| [33806565](https://pubmed.ncbi.nlm.nih.gov/33806565/) | 2021 | Case Series | Int J Mol Sci | Optic nerve/retinal abnormalities in congenital fibrosis of extraocular muscles (genetic, KIF21A/TUBB3) |
| [30196776](https://pubmed.ncbi.nlm.nih.gov/30196776/) | 2018 | Review | J Binocul Vis Ocul Motil | Review of congenital cranial dysinnervation disorders; no pharmacotherapy discussed |
| [24932988](https://pubmed.ncbi.nlm.nih.gov/24932988/) | 2014 | Review | Am J Ophthalmol | Pathogenesis/treatment of maculopathy from cavitary optic disc anomalies (surgical, not drug-based) |

None of the above literature discusses prochlorperazine directly; all were retrieved via disease-term overlap rather than drug-specific evidence.

---

## New Zealand Market Information

Prochlorperazine is **not currently marketed in New Zealand** — no Medsafe authorizations are on record for this product.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication (retinal dystrophy with or without extraocular anomalies) has L5 evidence only — no clinical trials and no drug-specific literature — and the available literature plus known phenothiazine pharmacology suggest this is more likely a false-positive association (retina-related toxicity keyword overlap) than a genuine therapeutic signal.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a blocking data gap (DG001)
- Confirmed mechanism of action data from DrugBank — currently a high-severity data gap (DG002)
- If pursuing repurposing research for this drug, consider redirecting attention to the **manic bipolar affective disorder** signal (rank 10, L4/S1, "Research Question" stage), which has a more plausible mechanistic basis and at least one directly relevant case report
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

