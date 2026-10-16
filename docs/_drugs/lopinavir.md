---
layout: default
title: Lopinavir
parent: Moderate Evidence (L3-L4)
nav_order: 277
evidence_level: L4
indication_count: 3
last_modified_at: 2026-10-15
description: "Lopinavir: predicted new indication Simian immunodeficiency virus infection (evidence level L4). Approved use: HIV infectious disease"
monitor_inn: "lopinavir"
predicted_indications: ["simian immunodeficiency virus infection", "feline acquired immunodeficiency syndrome", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
---

# Lopinavir
{: .fs-9 }

Evidence Level: **L4** | Predicted Indications: **3** 
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

# Lopinavir: From HIV Infection / AIDS to Simian Immunodeficiency Virus Infection

## One-Sentence Summary

Lopinavir is an HIV-1 protease inhibitor, originally used to treat HIV infection and AIDS.
The TxGNN model predicts it may be effective for **simian immunodeficiency virus (SIV) infection**, but there are **0 clinical trials** and only **3 animal-model publications** supporting this direction. This is an animal-model (veterinary/research) indication rather than a human therapeutic use, and it is essentially an extension of the original antiretroviral indication.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HIV infectious disease; AIDS (from the knowledge graph, since the New Zealand licence record has no indication text) |
| Predicted New Indication | Simian immunodeficiency virus infection |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L4 |
| New Zealand Market Status | Cancelled only (no currently valid licence) |
| Number of Authorizations | 1 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, lopinavir is an HIV-1 protease inhibitor, marketed in a fixed-dose combination with ritonavir (a pharmacokinetic booster). Its efficacy in HIV infection is established, and mechanistically it may be applicable to SIV.

SIV is a closely related primate lentivirus, and its protease is structurally similar to HIV-1 protease, so activity is biologically plausible. The available literature consists of macaque studies of combination antiretroviral regimens including lopinavir, plus a chimeric SHIV engineered with an HIV-1 protease gene so that macaques become susceptible to HIV protease inhibitors.

This evidence is indirect and preclinical. The prediction mostly reflects network proximity to the original HIV/AIDS indication, so it is not a true repurposing opportunity for a new human disease.

The other two predictions in the dataset are weaker:
- **Feline acquired immunodeficiency syndrome** (score 99.90%): FIV protease differs from HIV-1 protease in substrate specificity and inhibitor sensitivity, so activity cannot be assumed. No trials or literature are available (L5).
- **A rare neurodevelopmental disorder with ataxic gait, absent speech and decreased cortical white matter** (score 99.90%): no plausible mechanistic link was found, and the prediction is likely a graph artifact (L5).

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16973590](https://pubmed.ncbi.nlm.nih.gov/16973590/) | 2006 | Animal study | Journal of Virology | Four SIVmac251-infected cynomolgus macaques received 7 days of quadruple antiretroviral therapy; rapid viral decay was observed, informing comparison of viral dynamics between SIV and HIV-1 |
| [12951220](https://pubmed.ncbi.nlm.nih.gov/12951220/) | 2003 | Animal study | Journal of Virological Methods | Two SHIV 89.6P-infected rhesus macaques received oral AZT, 3TC and lopinavir/ritonavir for 28 days; the study assessed effects on the CD8 subset |
| [17350308](https://pubmed.ncbi.nlm.nih.gov/17350308/) | 2007 | Animal model development | Microbes and Infection | A novel SHIV carrying the HIV-1 protease gene was built; its growth was blocked by a protease inhibitor in cell culture, and it persisted weakly in macaques, making it a tool for testing protease inhibitors in vivo |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2613549 (cancelled) | Lopinavir/Ritonavir Mylan | Tab 200 mg with ritonavir 50 mg | Not recorded |

## Safety Considerations

- **Contraindications / conditions of concern (from knowledge graph, not label text)**: long QT syndrome (including familial), torsades de pointes, sudden cardiac arrest, heart conduction disease, atrioventricular block, cardiomyopathy, myocardial ischaemia and coronary artery disease; pancreatitis; liver disease, cirrhosis, and chronic hepatitis B or C; hyperglycaemia, diabetes mellitus and diabetic ketoacidosis; hypertriglyceridaemia and familial combined hyperlipidaemia; hyperbilirubinaemia, including Gilbert syndrome; gallbladder disease; potassium deficiency; and haemophilia.
- **Drug Interactions**: 65 interactions recorded. Major-level interactions include **dolasetron** and **papaverine**. Moderate-level interactions include acarbose, dapagliflozin, metformin, famotidine, levofloxacin and ondansetron, among others. Minor-level interactions include cimetidine and clotrimazole.

Package insert warnings and contraindications were not available, so please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The only supporting evidence is three preclinical macaque studies, with no registered clinical trials. SIV infection is an animal-model indication rather than a human therapeutic use, so the prediction extends the original antiretroviral indication rather than opening a new treatment opportunity. The product's only New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Clarification of whether a research or veterinary use case justifies further work
- If a human indication is pursued instead, new mechanistic work and a safety review given the cardiac, hepatic, metabolic and interaction concerns listed above
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

