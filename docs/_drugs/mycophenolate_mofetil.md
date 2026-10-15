---
layout: default
title: Mycophenolate Mofetil
parent: Model Prediction Only (L5)
nav_order: 314
evidence_level: L5
indication_count: 10
---

# Mycophenolate Mofetil
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

# Mycophenolate Mofetil: From Transplant Immunosuppression to HIV Infectious Disease

## One-Sentence Summary

Mycophenolate mofetil (MMF) is an immunosuppressant that inhibits lymphocyte proliferation; the data compiled for this report do not list a formal approved indication for it in New Zealand.
The TxGNN model predicts it may be effective for **HIV infectious disease**,
with **4 clinical trials** (only 1 directly relevant, and it was withdrawn with no participants) and **20 publications** (including 1 RCT and several small cohort studies) currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available (approved indication text is empty in the New Zealand licence records) |
| Predicted New Indication | HIV infectious disease |
| TxGNN Prediction Score | 99.86% |
| Evidence Level | L2 (as assigned in the compiled data; see the note under Clinical Trial Evidence) |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the DrugBank record compiled for this report. Based on the repurposing analysis, MMF inhibits inosine monophosphate dehydrogenase (IMPDH), which depletes guanosine nucleotide pools in lymphocytes.

This has two plausible consequences for HIV. First, it reduces T-cell proliferation and the pool of nucleotide substrate available for HIV reverse transcription. Second, it potentiates nucleoside analogues such as abacavir and didanosine, probably by depleting intracellular dGTP. A small case series reported a fall in plasma HIV-1 RNA when MMF was added to an abacavir-containing regimen. Targeting proliferating infected CD4+ T-cell clones is also a plausible "virostatic" strategy.

The main concern is that immunosuppression is a safety issue in people with HIV. Small studies report no detrimental immunological effects when MMF is combined with HAART, but this is not firmly established. The original indication is not listed in the compiled data, so a direct comparison with the new indication cannot be made.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00038272](https://clinicaltrials.gov/study/NCT00038272) | Phase 2 | Completed | 56 | DAPD vs DAPD plus MMF added to anti-HIV regimens in treatment-experienced patients; no results summarised in the data |
| [NCT00021489](https://clinicaltrials.gov/study/NCT00021489) | Phase 2 | Withdrawn | 0 | MMF added to abacavir in treatment-failure HIV; withdrawn, so no data |
| [NCT01288131](https://clinicaltrials.gov/study/NCT01288131) | Phase 3 | Terminated | 8 | Cyclosporine plus MMF vs cyclophosphamide plus prednisolone for anti-erythropoietin pure red cell aplasia; link to HIV is indirect |
| [NCT00112593](https://clinicaltrials.gov/study/NCT00112593) | Not applicable | Completed | 5 | Stem cell transplant with MMF in the immunosuppression regimen in HIV-positive patients; not direct evidence of antiviral efficacy |

**Note:** No trial provides direct efficacy data for MMF in HIV. The Phase 3 trial is unrelated to HIV treatment and was terminated early. The L2 level in the compiled data is therefore generous; on trial evidence alone, the case is closer to L3-L5.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [15353978](https://pubmed.ncbi.nlm.nih.gov/15353978/) | 2004 | RCT | AIDS | HAART with or without MMF in treatment-naive HIV-1 patients; assessed effect on plasma HIV-1 RNA decay and the latent reservoir |
| [15871638](https://pubmed.ncbi.nlm.nih.gov/15871638/) | 2005 | Cohort | Clin Pharmacokinet | Pharmacokinetics and pharmacodynamics of low-dose MMF with abacavir, efavirenz and nelfinavir; monitoring recommended |
| [16379601](https://pubmed.ncbi.nlm.nih.gov/16379601/) | 2005 | Cohort | AIDS Res Hum Retroviruses | No detrimental immunological effects of MMF plus HAART in treatment-naive patients |
| [12352149](https://pubmed.ncbi.nlm.nih.gov/12352149/) | 2002 | Case series | J Acquir Immune Defic Syndr | MMF added to abacavir-containing regimens in 5 patients associated with dGTP depletion and lower HIV-1 RNA |
| [17017956](https://pubmed.ncbi.nlm.nih.gov/17017956/) | 2006 | Review | Curr Top Med Chem | Immunosuppressive drugs in HIV disease and the rationale of targeting immune activation |
| [16515490](https://pubmed.ncbi.nlm.nih.gov/16515490/) | 2006 | Review | Curr Pharm Des | Virostatics as a strategy for targeting HIV infection |
| [41118390](https://pubmed.ncbi.nlm.nih.gov/41118390/) | 2025 | Preclinical | J Clin Invest | Selective targeting of HIV-infected clones with antiproliferative drugs |
| [20840478](https://pubmed.ncbi.nlm.nih.gov/20840478/) | 2010 | Retrospective study | Am J Transplant | Kidney transplantation in 27 HIV-infected patients on MMF-based regimens; a transplant setting, not antiviral use |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 350516 | Cellcept | Cap 250 mg | Not listed (cancelled) |
| 2124912 | Cellcept | Powder for oral liq 1 g per 5 ml | Not listed (cancelled) |
| 2093774 | Cellcept | Tab 500 mg | Not listed (cancelled) |

All three authorizations are cancelled, so there is currently no valid marketed MMF product in the data compiled for this report.

---

## Safety Considerations

- **Disease-level cautions (from a knowledge graph, not label text):** The TxGNN knowledge graph links MMF with contraindication-type relationships to: neutropenia, agranulocytosis, aplastic anaemia and related marrow failure syndromes (e.g. Fanconi anaemia, Diamond-Blackfan anaemia); active peptic ulcer disease; liver and gallbladder disease; cancers including lymphoma, primary CNS lymphoma and skin cancer; and infections such as cytomegalovirus, herpes simplex, herpes zoster and progressive multifocal leukoencephalopathy. The infection and malignancy links are especially relevant to an immunosuppressant used in people with HIV.
- **Drug Interactions:** 92 interactions were identified in total. The most notable is **activated charcoal (Major)**. Many **Moderate** interactions involve proton pump inhibitors (omeprazole, esomeprazole, lansoprazole, dexlansoprazole, rabeprazole), antacids and minerals (magnesium oxide, aluminium hydroxide, calcium carbonate) and antibiotics (amoxicillin, clarithromycin, doxycycline, minocycline, tetracycline, vancomycin, kanamycin, paromomycin). **Minor** interactions include zinc salts and iron.

Package insert warnings and contraindications were not available for this report; please refer to the New Zealand package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic rationale (IMPDH inhibition, dGTP depletion, synergy with abacavir) is credible, and a few small human studies exist. However, there is no completed trial with direct efficacy data in HIV, the only directly relevant trial was withdrawn, and the immunosuppression risk is a real safety concern. All New Zealand authorizations are cancelled, and package insert safety data are missing, so the case cannot advance to safety screening yet.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap)
- Detailed mechanism of action data from DrugBank
- A critical review of the RCT (PMID 15353978) and the smaller cohort data, in particular the effect on viral load and the latent reservoir
- A safety assessment of immunosuppression in HIV-positive patients (opportunistic infections, CD4 counts, malignancy)
- Clarification of the original approved indication and the New Zealand regulatory pathway, given that all authorizations are cancelled
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

