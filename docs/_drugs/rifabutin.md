---
layout: default
title: Rifabutin
parent: 僅模型預測 (L5)
nav_order: 303
evidence_level: L5
indication_count: 10
---

# Rifabutin
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

# Rifabutin: From Mycobacterial Infection to HIV Infectious Disease

## One-Sentence Summary

Rifabutin is a rifamycin-class antimycobacterial, established as standard therapy for preventing/treating *Mycobacterium avium* complex (MAC) bacteremia and for treating tuberculosis in patients co-infected with HIV. The TxGNN model predicts a link to **HIV infectious disease**, but this is supported almost entirely by evidence around rifabutin's existing role managing HIV-associated opportunistic infections — with **39 clinical trials** and **20 publications** in the evidence base, not new antiretroviral activity.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Mycobacterial infections (MAC prophylaxis/treatment, TB in HIV co-infection) — formal DrugBank/regulatory indication text is a data gap (see below) |
| Predicted New Indication | HIV infectious disease |
| TxGNN Prediction Score | 99.88% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (DrugBank MOA query returned a data gap). Based on known information, rifabutin is part of the rifamycin antibiotic class (structurally related to rifampicin), inhibiting bacterial DNA-dependent RNA polymerase. Its efficacy against *Mycobacterium avium* complex and *M. tuberculosis* has been proven over decades of use, and it is specifically favored over rifampicin in HIV-positive patients because it is a weaker CYP3A4 inducer, causing fewer disruptive interactions with protease inhibitors and integrase inhibitors used in antiretroviral therapy (ART).

The predicted link to "HIV infectious disease" should be read carefully: rifabutin has **no direct antiretroviral activity** and does not treat HIV itself. Its relevance to HIV infection is as the standard-of-care agent for managing HIV-associated opportunistic mycobacterial disease — MAC bacteremia prophylaxis/treatment and TB treatment in HIV/TB co-infected patients — which is why the overwhelming majority of trials and literature in this evidence pack concern rifabutin's use *in* HIV-positive populations (often as pharmacokinetic drug-drug interaction studies with ART) rather than a genuinely novel repurposing signal. The evidence pack's own reviewer notes flag this as a potentially misleading label, and the correct framing is "HIV-related opportunistic infection management" rather than treatment of HIV infection itself.

Mechanistically, this is why the evidence level still reaches L1: multiple completed Phase 3 RCTs (MAC prevention/treatment trials, several hundred to over a thousand patients each) substantiate rifabutin's established efficacy in this HIV-adjacent clinical context, even though it does not represent a new mechanism against HIV.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00002080](https://clinicaltrials.gov/study/NCT00002080) | N/A | Completed | N/A | Daily rifabutin monotherapy to prevent/delay MAC bacteremia in HIV+ patients with CD4 ≤200 — core evidence for rifabutin's established HIV-adjacent indication (relevance grade A) |
| [NCT00001030](https://clinicaltrials.gov/study/NCT00001030) | Phase 3 | Completed | 1,100 | Clarithromycin vs. rifabutin vs. combination for prevention of MAC bacteremia/disseminated disease in advanced HIV; also assessed survival, toxicity, QoL |
| [NCT00002101](https://clinicaltrials.gov/study/NCT00002101) | Phase 3 | Completed | 450 | Clarithromycin/ethambutol ± rifabutin (300mg or 450mg) vs. placebo for treatment of MAC bacteremia; measured CFU reduction and survival |
| [NCT00002122](https://clinicaltrials.gov/study/NCT00002122) | Phase 3 | Completed | 720 | Azithromycin and rifabutin, alone and combined, for prevention of disseminated MAC in HIV-infected patients |
| [NCT00001047](https://clinicaltrials.gov/study/NCT00001047) | Phase 3 | Completed | 400 | Clarithromycin + ethambutol with rifabutin or clofazimine for disseminated MAC disease in AIDS patients |
| [NCT00002032](https://clinicaltrials.gov/study/NCT00002032) | N/A | Completed | 750 | Double-blind, placebo-controlled trial of oral rifabutin for prevention of MAC bacteremia in AIDS patients with CD4 ≤200 |
| [NCT00002267](https://clinicaltrials.gov/study/NCT00002267) | N/A | Completed | 750 | Double-blind, placebo-controlled trial assessing rifabutin monotherapy safety/efficacy in delaying MAC bacteremia incidence |
| [NCT00640887](https://clinicaltrials.gov/study/NCT00640887) | Phase 2 | Completed | 48 | Rifabutin as replacement for rifampicin in combined TB/HIV treatment across different ART regimens (South Africa) |
| [NCT00023400](https://clinicaltrials.gov/study/NCT00023400) | Phase 4 | Completed | 20 | TBTC study of nelfinavir-rifabutin PK interaction in HIV-related TB patients on a rifabutin-based regimen |
| [NCT04518228](https://clinicaltrials.gov/study/NCT04518228) | N/A | Completed | 205 | PK properties of antiretroviral and anti-TB drugs, including rifabutin, during pregnancy and postpartum in HIV/TB co-infected women |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [23828580](https://pubmed.ncbi.nlm.nih.gov/23828580/) | 2013 | Systematic Review (Cochrane) | Cochrane Database Syst Rev | Rifamycins (rifampicin/rifabutin/rifapentine) vs. isoniazid for preventing active TB in people at risk of latent infection |
| [21726477](https://pubmed.ncbi.nlm.nih.gov/21726477/) | 2009 | Review | BMJ Clinical Evidence | Overview of TB treatment in HIV-infected patients, including rifamycin-based regimens |
| [28233512](https://pubmed.ncbi.nlm.nih.gov/28233512/) | 2017 | Review | Microbiology Spectrum | Bidirectional TB-HIV disease relationship and treatment considerations, incl. rifabutin as lower-interaction rifamycin |
| [40310456](https://pubmed.ncbi.nlm.nih.gov/40310456/) | 2025 | Review | PNAS | Next-generation rifamycins for mycobacterial infections; discusses rifabutin's CYP3A4 induction and limitations |
| [33294914](https://pubmed.ncbi.nlm.nih.gov/33294914/) | 2021 | Cohort | J Antimicrob Chemother | Rifabutin PK and safety in TB/HIV-coinfected children on lopinavir/ritonavir-based second-line ART; neutropenia noted |
| [31139825](https://pubmed.ncbi.nlm.nih.gov/31139825/) | 2019 | Cohort | J Antimicrob Chemother | Safety and efficacy of rifabutin in HIV/TB-coinfected children on lopinavir/ritonavir-based ART |
| [25281400](https://pubmed.ncbi.nlm.nih.gov/25281400/) | 2015 | Cohort | J Antimicrob Chemother | PK and safety of rifabutin in young HIV-infected children co-treated with lopinavir/ritonavir |
| [26832753](https://pubmed.ncbi.nlm.nih.gov/26832753/) | 2016 | Population PK Analysis | J Antimicrob Chemother | Pooled analysis of rifabutin-HIV protease inhibitor drug-drug interactions to guide dosing |
| [36385424](https://pubmed.ncbi.nlm.nih.gov/36385424/) | 2023 | Population PK Model | Br J Clin Pharmacol | Rifabutin-dolutegravir interaction model, supporting rifabutin as an alternative to rifampicin in HIV/TB co-treatment |
| [32979587](https://pubmed.ncbi.nlm.nih.gov/32979587/) | 2020 | Retrospective Observational | Int J Infect Dis | Tenofovir alafenamide + rifabutin co-administration did not compromise HIV-1 viral suppression |

---

## New Zealand Market Information

RIFABUTIN currently holds **no marketing authorizations in New Zealand** (0 licenses on file; market status: not marketed).

---

## Safety Considerations

Please refer to the package insert for safety information — key warnings, contraindications, and DDI data are all unavailable in this evidence pack (query returned no TFDA/DDI records).

One point worth flagging despite the data gap: the trial and literature base above is dominated by pharmacokinetic drug-drug interaction studies with antiretrovirals (protease inhibitors, integrase inhibitors, NNRTIs), reflecting rifabutin's known CYP3A4-mediated interaction profile — this should be a priority focus once formal DDI/label data is obtained.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple completed Phase 3 RCTs (several hundred to over a thousand patients) substantiate rifabutin's established efficacy for MAC bacteremia prophylaxis/treatment and HIV/TB co-treatment, supporting the L1 evidence level. However, the "HIV infectious disease" label is potentially misleading — rifabutin manages HIV-associated opportunistic infection rather than HIV itself — and the drug is not currently marketed in New Zealand, with two outstanding data gaps blocking a full safety assessment.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert warnings and contraindications (DG001, blocking — required before any S1 safety evaluation)
- Detailed mechanism of action data from DrugBank (DG002, high priority)
- Reframing of the target indication to "HIV-related opportunistic infection (MAC/TB) management" to avoid mislabeling as direct anti-HIV therapy
- A regulatory pathway assessment for New Zealand market entry, given current non-marketed status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

