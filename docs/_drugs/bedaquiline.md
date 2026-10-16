---
layout: default
title: Bedaquiline
parent: Model Prediction Only (L5)
nav_order: 58
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Bedaquiline: predicted new indication Inactive tuberculosis (evidence level L5). Approved use: Pulmonary tuberculosis"
monitor_inn: "BEDAQUILINE"
predicted_indications: ["tuberculosis, bovine", "tuberculous ascites", "tuberculoma", "inactive tuberculosis", "tuberculosis, avian", "vulvovaginal candidiasis", "fascioliasis", "urea cycle disorder", "cutaneous tuberculosis", "esophageal candidiasis"]
---

# Bedaquiline
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

# Bedaquiline: From Pulmonary Tuberculosis to Inactive (Latent) Tuberculosis

## One-Sentence Summary

Bedaquiline was originally developed to treat pulmonary tuberculosis and multidrug-resistant tuberculosis.
The TxGNN model predicts it may be effective for **inactive tuberculosis** (preventive treatment), with **3 clinical trials** and **19 publications** currently related to this direction. The only trial that directly tests this use is still recruiting and has no results yet.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Pulmonary tuberculosis; multidrug-resistant tuberculosis (the New Zealand licence record contains no approved indication text) |
| Predicted New Indication | Inactive tuberculosis (the top-ranked prediction by score is bovine tuberculosis; inactive tuberculosis is the best-supported one and is the focus of this report) |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L2 (a Phase 2/3 trial is registered but not completed, so this level is provisional) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the compiled data. Based on known information, bedaquiline inhibits the F-ATP synthase (subunit c) of mycobacteria, which impairs the bacterium's energy metabolism. Its efficacy in drug-resistant pulmonary tuberculosis is established.

Inactive (latent) tuberculosis is a persistent, low-replication infection with *Mycobacterium tuberculosis*. Because bedaquiline acts on energy production and is active against non-replicating bacilli, it may be applicable to preventive therapy. This is especially relevant for contacts of patients with multidrug-resistant TB, who cannot be protected by standard preventive regimens.

The prediction should still be read cautiously. The score reflects a model prediction, and no efficacy results for this use are yet available.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT06568484](https://clinicaltrials.gov/study/NCT06568484) | Phase 2/3 | Recruiting | 2530 | BREACH-TB: 4 weeks of bedaquiline vs a standard regimen to prevent TB disease over 72 weeks in people living with HIV and high-risk close contacts (non-inferiority design). No results yet. |
| [NCT05766267](https://clinicaltrials.gov/study/NCT05766267) | Phase 2/3 | Active, not recruiting | 288 | CRUSH-TB: 17-week bedaquiline-moxifloxacin-pyrazinamide regimens (plus rifabutin or delamanid) vs the standard 6-month regimen. Tests active pulmonary TB, not inactive TB. |
| [NCT07069582](https://clinicaltrials.gov/study/NCT07069582) | Phase 1 | Not yet recruiting | 60 | Single-dose pharmacokinetic study of anti-TB drugs, including bedaquiline, in plasma and breast milk of breastfeeding women. Exposure data only. |

---

## Literature Evidence

The retrieved literature consists mainly of reviews and general TB articles. No randomized controlled trial was supplied.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28625141](https://pubmed.ncbi.nlm.nih.gov/28625141/) | 2018 | Review | Recent Patents on Anti-Infective Drug Discovery | Bedaquiline as a basis for shorter and better anti-TB regimens |
| [39766559](https://pubmed.ncbi.nlm.nih.gov/39766559/) | 2024 | Review | Antibiotics (Basel) | *M. tuberculosis* F-ATP synthase inhibitors and targets, the mechanistic basis of bedaquiline |
| [39887565](https://pubmed.ncbi.nlm.nih.gov/39887565/) | 2025 | Review | Respirology | Clinical update on TB, including its disease spectrum and subclinical TB |
| [36982277](https://pubmed.ncbi.nlm.nih.gov/36982277/) | 2023 | Review | Int J Mol Sci | TB pathogenesis, current regimens and new drug targets |
| [33837535](https://pubmed.ncbi.nlm.nih.gov/33837535/) | 2021 | Review | Clin Pharmacol Ther | Treatment of latent and active TB, and the roles of individual drugs |
| [33319660](https://pubmed.ncbi.nlm.nih.gov/33319660/) | 2021 | Review | Curr Top Med Chem | Pathophysiology, drug resistance mechanisms, newer anti-TB drugs and host-directed therapies |
| [37316702](https://pubmed.ncbi.nlm.nih.gov/37316702/) | 2023 | Review | Innere Medizin | What is new in TB treatment |
| [39301910](https://pubmed.ncbi.nlm.nih.gov/39301910/) | 2025 | Review | Infect Disord Drug Targets | Bedaquiline-based delivery systems; notes activity against persistent or latent TB |
| [36915977](https://pubmed.ncbi.nlm.nih.gov/36915977/) | 2022 | Review | Zhejiang Da Xue Xue Bao Yi Xue Ban | Progress in diagnosis and treatment of latent TB infection |
| [28776518](https://pubmed.ncbi.nlm.nih.gov/28776518/) | 2017 | Review | Int J Mycobacteriol | Drugs targeting non-replicating TB bacilli |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2659956 | Sirturo | Tab 100mg | Not recorded (licence status: cancelled) |

---

## Safety Considerations

- **Drug Interactions**: 100 interactions were recorded. Among those listed, the **Major** interactions are with dexamethasone, clarithromycin, picosulfuric acid, polyethylene glycol (3350 with electrolytes), dolasetron, palonosetron and sodium sulfate. **Moderate** interactions include acarbose, famotidine, doxycycline, pioglitazone, loperamide, bupropion, bisacodyl, chenodeoxycholic acid, minocycline, oxandrolone, oxymetholone, tetracycline and naltrexone.
- **Monitoring**: The prediction analysis recommends QTc monitoring and a full interaction review before any use.

Please refer to the package insert for warnings and contraindications.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
A Phase 2/3 trial (BREACH-TB, n=2530) directly tests bedaquiline for TB prevention, and the mechanism is plausible for non-replicating bacilli. However, it is still recruiting, and the supporting literature is indirect. The licence in New Zealand is cancelled. Use should be limited to trial settings or carefully selected high-risk contacts, such as those of MDR-TB patients.

**To proceed, the following is needed:**
- Efficacy and safety results from BREACH-TB
- Package insert warnings and contraindications (from Medsafe), which are needed before safety screening
- Confirmed mechanism of action data from DrugBank
- QTc monitoring and interaction management plan
- Clarification of the cancelled New Zealand licence and the supply route

**Other predictions (for reference):** Tuberculoma has case reports of BPaL/BPaLM regimens with radiographic resolution, but evidence is limited to case reports and CNS penetration is limited (Research Question). Bovine TB, tuberculous ascites and cutaneous TB have only indirect or in vitro evidence (Hold). Avian TB, vulvovaginal and esophageal candidiasis, fascioliasis and urea cycle disorder have no evidence or no plausible mechanism, and are likely model artifacts (Hold).
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

