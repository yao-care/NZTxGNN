---
layout: default
title: Allopurinol
parent: Moderate Evidence (L3-L4)
nav_order: 29
evidence_level: L4
indication_count: 10
---

# Allopurinol
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **10** 
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

# Allopurinol: From Gout to Hepatic Porphyria

## One-Sentence Summary

Allopurinol is a xanthine oxidase inhibitor originally used for gout, hyperuricaemia and acute urate nephropathy.
The TxGNN model predicts it may be effective for **hepatic porphyria**, but there are **0 clinical trials** and only **2 publications** (one hypothesis paper and one old rat study), neither of which shows a clinical benefit.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gout, hyperuricaemia, acute urate nephropathy (from the knowledge graph; no approved indication text in the New Zealand licence data) |
| Predicted New Indication | Hepatic porphyria |
| TxGNN Prediction Score | 99.95% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, allopurinol is a xanthine oxidase inhibitor whose efficacy in gout and hyperuricaemia is established, and it may have some biological connection to heme metabolism relevant to porphyria.

Older preclinical work suggests allopurinol can interact with heme metabolism, for example by degrading hepatic cytochrome P450 heme and affecting regulation of ALA synthase. A 2019 hypothesis paper discusses metabolic targeting of 5-aminolevulinate synthase in acute hepatic porphyrias, and a 1992 rat study examines drug effects on hepatic heme metabolism. Together these give a plausible but indirect link.

The direction of effect is unresolved. The same heme-related effects could worsen porphyria rather than help it. No human clinical data are available, so the prediction should be treated as a hypothesis only.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31443750](https://pubmed.ncbi.nlm.nih.gov/31443750/) | 2019 | Review (hypothesis) | Medical Hypotheses | Proposes targeting liver 5-ALAS, via tryptophan or inhibition of heme use by tryptophan 2,3-dioxygenase, as a therapy for acute hepatic porphyrias; does not test allopurinol clinically |
| [1567472](https://pubmed.ncbi.nlm.nih.gov/1567472/) | 1992 | Preclinical (animal) | Biochemical Pharmacology | Acute carbamazepine administration altered haem metabolism in rat liver, relevant to how drugs exacerbate hepatic porphyrias; not a study of allopurinol benefit |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2666863 | Ipca-Allopurinol (cancelled) | Tab 300 mg | Not available |
| 2666855 | Ipca-Allopurinol (cancelled) | Tab 100 mg | Not available |

## Safety Considerations

- **Disease-level contraindication signals (from the knowledge graph, not label text)**: liver disease, gallbladder disease, impaired renal function, hypersensitivity vasculitis, cutaneous leukocytoclastic angiitis, allergic cutaneous vasculitis, and anti-glomerular basement membrane disease. The liver disease signal is particularly relevant to a hepatic porphyria indication.
- **Drug Interactions**: 100 interactions were found in the DDI screen. The main ones rated Moderate are:
  - Aluminum hydroxide
  - Attapulgite
  - Kaolin
  - Magaldrate
  - Sucralfate

  Minor interactions include amoxicillin and chlorpropamide. Many others (for example metformin, omeprazole, pantoprazole, glimepiride, morphine and doxycycline) have an unrated severity.

Please refer to the package insert for label warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a high model score plus indirect preclinical and hypothesis-level literature, with no clinical trials. The heme-related mechanism could plausibly be harmful in porphyria, and allopurinol has no current valid licence in New Zealand.

**To proceed, the following is needed:**
- Mechanism of action data, in particular clarifying the direction of effect on heme biosynthesis and ALA synthase
- Targeted preclinical or case-level evidence showing benefit rather than exacerbation in porphyria
- Package insert warnings and contraindications for safety screening
- Clarification of the regulatory pathway, since both New Zealand licences are cancelled
- A hepatic safety assessment, given the liver disease contraindication signal
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

