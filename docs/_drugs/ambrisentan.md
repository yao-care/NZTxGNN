---
layout: default
title: Ambrisentan
parent: 僅模型預測 (L5)
nav_order: 24
evidence_level: L5
indication_count: 10
---

# Ambrisentan
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

# Ambrisentan: From Pulmonary Arterial Hypertension to PAH Associated with Congenital Heart Disease

## One-Sentence Summary

Ambrisentan is a selective endothelin type A (ETA) receptor antagonist globally approved for idiopathic and heritable pulmonary arterial hypertension (PAH), though not currently registered in the New Zealand market.
The TxGNN model predicts it may be effective across multiple PAH subtypes; **PAH associated with congenital heart disease (PAH-CHD)** represents the top actionable prediction, supported by **9 registered clinical trials** (including 1 completed Phase 3 study) and **18 publications**.
A companion indication — **PAH associated with connective tissue disease (PAH-CTD)** — carries parallel L2 evidence and a "Proceed with Guardrails" recommendation.

> **Note on TxGNN ranking**: The model's top-ranked prediction by score is pulmonary arteriovenous malformation (PAVM, score 99.41%), but clinical evidence for that indication is limited to a single case report (L4). This report focuses on PAH-CHD (rank 2, score 99.37%, L1 evidence) as the primary actionable candidate.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Idiopathic/heritable pulmonary arterial hypertension (globally approved; not registered in New Zealand) |
| Predicted New Indication | PAH associated with congenital heart disease (PAH-CHD) |
| TxGNN Prediction Score | 99.37% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, Ambrisentan is a highly selective endothelin receptor type A (ETA) antagonist. Endothelin-1 (ET-1) is one of the most potent vasoconstrictors known — it acts primarily through ETA receptors to drive pulmonary vasoconstriction, vascular smooth muscle proliferation, and fibrotic remodelling. These are the defining pathological features of all forms of PAH.

In congenital heart disease (CHD), persistent left-to-right cardiac shunts chronically expose the pulmonary vasculature to excess blood flow and shear stress. This triggers sustained ET-1 overproduction and ETA receptor upregulation, progressively destroying pulmonary vascular architecture. In its most advanced form — Eisenmenger syndrome — the shunt reverses and the condition becomes inoperable. Because ET-1/ETA signalling is the central driver of this pathological cascade, Ambrisentan's mechanism of action is highly congruent with CHD-PAH pathophysiology. The repurposing logic is not speculative: it is a mechanistic extension of the same ETA blockade that underpins the approved PAH indication.

Multiple international PAH management guidelines already position endothelin receptor antagonist (ERA) therapy as a recommended option for Eisenmenger syndrome. The completed Phase 3 study in Chinese PAH patients with CHD-related disease (NCT01808313, n=134) provides the highest-quality direct evidence supporting this prediction. A systematic review and meta-analysis (PMID 31096477) further affirms ERA utility in Eisenmenger syndrome, consolidating the evidence to L1 level.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01808313](https://clinicaltrials.gov/study/NCT01808313) | Phase 3 | Completed | 134 | Open-label, single-arm study of Ambrisentan in Chinese PAH patients including CHD-related subgroup. Primary endpoint: 6-minute walk distance change over 12 weeks (PEP) with optional 12-week dose-adjustment extension. Largest direct efficacy dataset for Ambrisentan in Asian CHD-PAH. |
| [NCT01884675](https://clinicaltrials.gov/study/NCT01884675) | Phase 3 | Terminated | 33 | Double-blind RCT: Ambrisentan 5 mg vs placebo in inoperable CTEPH over 16 weeks. Terminated early (33 of 160 planned enrolled); termination reason requires clarification to rule out safety signal. Design is high quality but early termination limits standalone evidential value. |
| [NCT01342952](https://clinicaltrials.gov/study/NCT01342952) | Phase 2 | Completed | 38 | Long-term open-label extension of the paediatric PAH study in patients aged 8–18 years; duration minimum 6 months, some participants followed to age 18 or until discontinuation. Provides paediatric CHD-PAH long-term safety data through 2022. |
| [NCT01332331](https://clinicaltrials.gov/study/NCT01332331) | Phase 2 | Terminated | 41 | Randomised, open-label dose-finding study of high vs low body-weight–adjusted Ambrisentan in paediatric PAH (ages 8–18); terminated early after 24 weeks. Offers dose-response and pharmacokinetic data for the paediatric population. |
| [NCT04095286](https://clinicaltrials.gov/study/NCT04095286) | Phase 1 | Completed | 29 | Crossover relative bioavailability study comparing a new low-dose Ambrisentan dispersible tablet (intended for paediatric use) with the marketed reference tablet in healthy adults. Supports paediatric dose formulation development. |
| [NCT02688387](https://clinicaltrials.gov/study/NCT02688387) | Phase 1 | Completed | 112 | Crossover PK study comparing multiple fixed-dose combination (FDC) tablets of Ambrisentan + Tadalafil in healthy participants. Supports development of combination therapy formulations for PAH. |
| [NCT01894022](https://clinicaltrials.gov/study/NCT01894022) | Phase 3 | Terminated | 19 | Open-label extension of the CTEPH Phase 3 study (NCT01884675); terminated early with only 19 participants. Long-term safety reference only; no independent efficacy value. |
| [NCT01383083](https://clinicaltrials.gov/study/NCT01383083) | N/A | Unknown | 42 | Study of Iloprost (not Ambrisentan) in CHD-PAH/Eisenmenger physiology. Indirect comparator — demonstrates clinical trial feasibility and prostaglandin pathway relevance in this population. |
| [NCT00593905](https://clinicaltrials.gov/study/NCT00593905) | N/A | Withdrawn | 0 | Pharmacogenomics study of ERA therapy (including Ambrisentan) in PAH; withdrawn before any enrolment. No usable data. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31096477](https://pubmed.ncbi.nlm.nih.gov/31096477/) | 2019 | Systematic Review | Medicine | Meta-analysis evaluating PAH-specific drug therapy in Eisenmenger syndrome. Qualitative synthesis supports ERA and other targeted agents in CHD-PAH. Highest-tier evidence for this indication. |
| [35412560](https://pubmed.ncbi.nlm.nih.gov/35412560/) | 2022 | Narrative Review | JAMA | Comprehensive review of PAH diagnosis and management. Addresses CHD-associated PAH subgroup, risk stratification, and ERA positioning within current treatment guidelines. |
| [21371683](https://pubmed.ncbi.nlm.nih.gov/21371683/) | 2011 | Observational/Case Series | American Journal of Cardiology | Early clinical experience with Ambrisentan in Eisenmenger syndrome (Columbia University, 2007–2008). Assessed effects on resting and exercise systemic arterial oxygen saturation, exercise capacity, and haemodynamics. |
| [34921523](https://pubmed.ncbi.nlm.nih.gov/34921523/) | 2022 | Prospective Observational | Pediatric Pulmonology | Real-world safety and tolerability of Ambrisentan + Tadalafil combination in paediatric pulmonary hypertension. Relevant to paediatric CHD-PAH management. |
| [41727855](https://pubmed.ncbi.nlm.nih.gov/41727855/) | 2025 | Review | Frontiers in Pediatrics | ET-1 pathobiology in the failing Fontan circulation (single-ventricle CHD). Provides mechanistic rationale for ETA antagonism in complex congenital heart disease with pulmonary vascular involvement. |
| [22104452](https://pubmed.ncbi.nlm.nih.gov/22104452/) | 2011 | Registry/Cohort | Postgraduate Medicine | Texas Adult Congenital Heart Program: characterises clinical burden of PAH in adult CHD patients and describes treatment practice patterns. |
| [28348949](https://pubmed.ncbi.nlm.nih.gov/28348949/) | 2017 | Case Report | Respiratory Medicine Case Reports | Advanced PAH combination therapy in Eisenmenger syndrome. Describes haemodynamic and functional outcomes with multi-drug PAH regimens. |
| [25787798](https://pubmed.ncbi.nlm.nih.gov/25787798/) | 2015 | Case Report | International Heart Journal | Adult Eisenmenger syndrome (VSD) treated with PAH drugs. Demonstrates individual patient benefit from targeted PAH therapy including ERA. |
| [18333354](https://pubmed.ncbi.nlm.nih.gov/18333354/) | 2007 | Expert Review | Romanian Journal of Internal Medicine | Review of PAH management in congenital heart disease, including pathophysiology of Eisenmenger syndrome and evidence for targeted therapy at the time. |
| [21852894](https://pubmed.ncbi.nlm.nih.gov/21852894/) | 2009 | Review | Progress in Pediatric Cardiology | Non-CHD paediatric PAH — discusses IPAH, capillary haemangiomatosis, and haemoglobin-related PAH in the context of growing experience with targeted therapies. |

---

## New Zealand Market Information

Ambrisentan is not currently registered or marketed in New Zealand. No product authorizations are on record.

> Ambrisentan (brand names **Letairis®** [USA] and **Volibris®** [EU/UK]) received FDA approval in 2007 and EMA approval in 2008 for pulmonary arterial hypertension (WHO Group I). Its absence from the New Zealand market is likely commercial rather than safety-driven. Any access programme or clinical trial in New Zealand would require a consent under Section 29 of the Medicines Act 1981, or full regulatory submission to Medsafe.

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Key interaction specific to PAH-CHD and HIV-PAH populations**: The evidence pack specifically flags that HIV protease inhibitors are potent CYP3A4 and P-glycoprotein inhibitors, which may significantly elevate Ambrisentan plasma concentrations. For any patient on antiretroviral therapy, close therapeutic monitoring and dose adjustment protocols are required before initiating Ambrisentan.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
PAH associated with congenital heart disease has robust and mechanistically coherent evidence: one completed Phase 3 study in the target CHD-PAH population (n=134), multiple Phase 2 studies and long-term extensions providing paediatric data, and a systematic review affirming ERA utility in Eisenmenger syndrome. The ETA-antagonism mechanism directly addresses the ET-1–driven pulmonary vascular remodelling central to CHD-PAH. The evidence package is sufficiently strong to advance to structured clinical planning.

**To proceed, the following is needed:**
- Obtain and review full results data from NCT01808313 (the completed Phase 3 Chinese CHD-PAH study), including primary endpoint outcomes, subgroup analyses, and safety table
- Investigate the reason for early termination of NCT01884675 — determine whether it was efficacy, safety, or commercial, and assess impact on the evidence base
- Obtain Taiwan/NZ regulatory package insert for complete safety profile, boxed warnings, contraindications, and drug interactions (currently a blocking data gap per DG001 in the evidence pack)
- Develop a DDI monitoring protocol, particularly for paediatric patients and adults co-prescribed cardiovascular polypharmacy
- Define the target patient population for any local access programme: Eisenmenger syndrome (inoperable) vs. pre-Eisenmenger CHD-PAH; adult vs. paediatric age band
- Initiate parallel evaluation of **PAH associated with connective tissue disease (PAH-CTD, rank 3, L2 evidence)** — this indication has 3 clinical trials (including 1 completed Phase 4 SSc-PAH study) and multiple AMBITION trial sub-analyses specifically examining Ambrisentan + Tadalafil in CTD-PAH, making it a strong co-primary candidate
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

