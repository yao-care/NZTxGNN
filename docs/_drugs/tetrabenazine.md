---
layout: default
title: Tetrabenazine
parent: 僅模型預測 (L5)
nav_order: 337
evidence_level: L5
indication_count: 10
---

# Tetrabenazine
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

# Tetrabenazine: From Huntington's Disease Chorea to Polycystic Kidney Disease 3

## One-Sentence Summary

Tetrabenazine is a VMAT2 (vesicular monoamine transporter 2) inhibitor historically used to control chorea in Huntington's disease; formal original-indication and licensing data are not present in this evidence pack (`original_indications` empty, `original_moa` marked as Data Gap). TxGNN predicts potential efficacy in **Polycystic Kidney Disease 3 (with or without Polycystic Liver Disease)** with a **99.90% score**, but this is supported by **zero clinical trials** and **20 background literature items that discuss the disease itself, not the drug-disease relationship**. The drug's own repurposing rationale flags this as a likely graph-topology artifact rather than a mechanistically grounded hypothesis.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from formal licensing data (no NZ market authorization on file); drug class context (VMAT2 inhibitor / monoamine-depleting agent) points to Huntington's disease chorea management |
| Predicted New Indication | Polycystic Kidney Disease 3, with or without Polycystic Liver Disease |
| TxGNN Prediction Score | 99.90% (rank 1353) |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available for this evaluation (`original_moa` = Data Gap). The evidence pack's own repurposing rationale, however, notes that tetrabenazine acts as a VMAT2 inhibitor, depleting presynaptic monoamines (dopamine, serotonin, norepinephrine) — a pathway used therapeutically to suppress chorea and other hyperkinetic movement symptoms.

Polycystic Kidney Disease 3 and polycystic liver disease arise from a fundamentally different biology: mutations affecting primary cilia function and cyst-related genes (e.g., *PKD1*, *PKD2*, *PKHD1*), driving progressive cystogenesis in the kidney and liver. There is no established pharmacological or physiological link between monoamine vesicular transport and ciliopathy-driven cyst formation.

The evidence pack's own assessment is explicit on this point: the high TxGNN score most likely reflects **graph co-occurrence or topological similarity within the knowledge graph rather than genuine mechanistic support**. This prediction should be treated as a hypothesis-generation signal only, not as evidence of biological plausibility.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

None of the retrieved literature discusses tetrabenazine directly — all 20 items describe the biology, diagnosis, or management of polycystic kidney/liver disease in general, with no drug-specific findings. The top entries by type priority are listed below.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38958301](https://pubmed.ncbi.nlm.nih.gov/38958301/) | 2024 | Review/Guideline | Am J Gastroenterol | ACG guideline on focal liver lesions, including polycystic liver disease management |
| [35728731](https://pubmed.ncbi.nlm.nih.gov/35728731/) | 2022 | Guideline | J Hepatol | EASL clinical practice guidelines on cystic liver diseases |
| [30819518](https://pubmed.ncbi.nlm.nih.gov/30819518/) | 2019 | Review | Lancet | Overview of autosomal dominant polycystic kidney disease (ADPKD) pathophysiology and management |
| [35487607](https://pubmed.ncbi.nlm.nih.gov/35487607/) | 2022 | Review | Clin Liver Dis | Clinical course of ADPKD and associated polycystic liver disease (PLD) |
| [29038287](https://pubmed.ncbi.nlm.nih.gov/29038287/) | 2018 | Review | JASN | Genetic overlap and shared pathogenesis between ADPKD and ADPLD |
| [38097330](https://pubmed.ncbi.nlm.nih.gov/38097330/) | 2023 | Review | Adv Kidney Dis Health | Genetic spectrum of PKD1/PKD2 mutations and resulting phenotypes |
| [34034501](https://pubmed.ncbi.nlm.nih.gov/34034501/) | 2022 | Review | Rev Esp Enferm Dig | Diagnosis and management of liver hydatid cyst (differential diagnosis context) |
| [36047551](https://pubmed.ncbi.nlm.nih.gov/36047551/) | 2022 | Review | Rev Med Suisse | Overview of polycystic liver disease subtypes and clinical course |
| [37266470](https://pubmed.ncbi.nlm.nih.gov/37266470/) | 2023 | Case Report | Maedica | Rare case of ADPKD/PLD associated with advanced gastric cancer |
| [40296340](https://pubmed.ncbi.nlm.nih.gov/40296340/) | 2025 | Cohort | Ann Transplant | Outcomes of combined liver-kidney transplantation in 9 PLD/PKD patients |

---

## New Zealand Market Information

No New Zealand market authorization is currently on file — tetrabenazine is not marketed in New Zealand under this evidence pack's data (0 licenses).

---

## Safety Considerations

Please refer to the package insert for safety information. Note: the TFDA/Medsafe package insert (warnings/contraindications) is flagged in this evidence pack as a **Blocking data gap (DG001)**, meaning a formal safety pre-screen (S1) cannot currently be completed for this drug.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
This prediction is supported only by the TxGNN model score (L5, no clinical trials, no drug-specific literature), and the evidence pack's own mechanistic analysis concludes there is no known biological link between VMAT2 inhibition and cystic kidney/liver disease pathogenesis — the high score is more likely a graph-topology artifact than a genuine repurposing signal. This is also the strongest-evidenced candidate among the 10 predicted indications for tetrabenazine in this pack; ranks 2–10 have no clinical trials and little to no literature at all, and several (e.g., thoracic malformation, rank 5) rely on tangential or unrelated evidence.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data to resolve the Blocking safety gap (DG001)
- Confirmed original indication and mechanism of action (DrugBank MOA currently a Data Gap, DG002)
- Preclinical or mechanistic studies directly linking VMAT2/monoamine pathways to ciliopathy-driven cystogenesis, if such a hypothesis is to be pursued further
- Drug-specific clinical or case evidence connecting tetrabenazine to renal/hepatic cystic disease outcomes
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

