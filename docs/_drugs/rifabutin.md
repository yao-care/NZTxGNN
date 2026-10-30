---
layout: default
title: Rifabutin
parent: High Evidence (L1-L2)
nav_order: 402
evidence_level: L1
indication_count: 10
last_modified_at: 2026-10-30
description: "Rifabutin: predicted new indication HIV infectious disease (evidence level L5). Approved use: Not stated in the licence record (the Medsafe indication text is empt…"
monitor_inn: "RIFABUTIN"
predicted_indications: ["HIV infectious disease", "multiple endocrine neoplasia", "sclerosing cholangitis", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter", "conjunctivitis", "leprosy", "endocarditis", "pneumocystosis", "feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection"]
---

# Rifabutin
{: .fs-9 }

Evidence Level: **L1** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Rifabutin: From Mycobacterial Infection to HIV Infectious Disease
{: .text-alpha }

## One-Sentence Summary

Rifabutin is a rifamycin antibiotic that inhibits bacterial RNA polymerase. It has been used against mycobacterial infections, and the New Zealand licence record compiled for this report does not state an approved indication.
The TxGNN model predicts it may be effective for **HIV infectious disease**. Rifabutin has no direct anti-HIV activity, so this is best read as **HIV-associated mycobacterial disease** (disseminated MAC and tuberculosis).
Support comes from **33 listed clinical trials** and **20 publications** compiled for this prediction.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not stated in the licence record (the Medsafe indication text is empty). Rifabutin is known as an antimycobacterial agent. |
| Predicted New Indication | HIV infectious disease (best framed as HIV-associated mycobacterial disease) |
| TxGNN Prediction Score | 0.9988 |
| Evidence Level | L1 (see note below) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (1 of 1 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Proceed with Guardrails |

*Note on evidence level:* The L1 rating follows the data compiled for this report. Three completed Phase 3 trials (NCT00001047, NCT00002101, NCT00001030) were run in HIV-positive patients with MAC. They all concern HIV-associated mycobacterial disease and do not show anti-HIV efficacy. Only NCT00001047 and NCT00002101 are directly relevant treatment trials, and in both rifabutin is part of the regimen rather than the sole variable.

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the structured drug record. Rifabutin belongs to the rifamycin class, which inhibits bacterial DNA-dependent RNA polymerase (rpoB). Its efficacy in mycobacterial infection is established.

Rifabutin has no direct activity against HIV. Its value is in treating and preventing the mycobacterial opportunistic infections that occur in people with advanced HIV, mainly disseminated *Mycobacterium avium* complex (MAC) and tuberculosis. The TxGNN model (deep learning) score for HIV infectious disease (0.9988) may reflect similarities the model learned between related diseases, not biological evidence.

Rifabutin is also a weaker CYP3A4 inducer than rifampin. It lowers protease inhibitor levels less, so it is often preferred for patients on protease inhibitor-based antiretroviral therapy, with dose adjustment. The indication should therefore be framed as HIV-associated mycobacterial disease, not HIV itself.

## Clinical Trial Evidence

The table lists the most relevant trials, ordered by relevance grade and then by phase. The data set contains 33 trials in total.

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00002101](https://clinicaltrials.gov/study/NCT00002101) | Phase 3 | Completed | 450 | Clarithromycin/ethambutol plus rifabutin (two doses) vs placebo for MAC bacteremia in AIDS |
| [NCT00001047](https://clinicaltrials.gov/study/NCT00001047) | Phase 3 | Completed | 400 | Four regimens for disseminated MAC in AIDS (clarithromycin at two doses with ethambutol and either rifabutin or clofazimine) |
| [NCT00001995](https://clinicaltrials.gov/study/NCT00001995) | Not labeled | Completed | 200 | Double-blind trial of a rifabutin regimen for MAC bacteremia in AIDS |
| [NCT00001030](https://clinicaltrials.gov/study/NCT00001030) | Phase 3 | Completed | 1100 | Clarithromycin vs rifabutin vs the combination for MAC prevention in HIV patients with CD4 ≤100 |
| [NCT00002122](https://clinicaltrials.gov/study/NCT00002122) | Phase 3 | Completed | 720 | Azithromycin and rifabutin, alone and combined, for preventing disseminated MAC |
| [NCT00002267](https://clinicaltrials.gov/study/NCT00002267) | Not labeled | Completed | 750 | Rifabutin vs placebo for preventing MAC bacteremia in AIDS (CD4 ≤200) |
| [NCT00001058](https://clinicaltrials.gov/study/NCT00001058) | Phase 2 | Completed | 246 | Clarithromycin combined with rifabutin, ethambutol, or both for disseminated MAC |
| [NCT00023361](https://clinicaltrials.gov/study/NCT00023361) | Not labeled | Completed | 215 | Intermittent rifabutin-based regimen for HIV-related tuberculosis (failure and relapse rates) |
| [NCT00651066](https://clinicaltrials.gov/study/NCT00651066) | Phase 2 | Completed | 47 | Rifabutin pharmacokinetics with antiretroviral therapy in TB/HIV (Vietnam) |
| [NCT00640887](https://clinicaltrials.gov/study/NCT00640887) | Phase 2 | Completed | 48 | Rifabutin pharmacokinetics with antiretroviral therapy in TB/HIV (South Africa) |

Many other trials in the data set are pharmacokinetic drug-interaction studies of rifabutin with antiretrovirals such as indinavir, nelfinavir, efavirenz, nevirapine, maraviroc, dolutegravir and cabotegravir. They inform dosing rather than efficacy. One trial listed (NCT07066722) concerns a different investigational agent and is not relevant to rifabutin.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [21819645](https://pubmed.ncbi.nlm.nih.gov/21819645/) | 2012 | Review | Int J Tuberc Lung Dis | Rifabutin as an alternative to rifampicin when co-administered with protease inhibitor-based ART |
| [23828580](https://pubmed.ncbi.nlm.nih.gov/23828580/) | 2013 | Systematic review | Cochrane Database Syst Rev | Rifamycins vs isoniazid for TB prevention in HIV-negative people at risk |
| [31139825](https://pubmed.ncbi.nlm.nih.gov/31139825/) | 2019 | Cohort | J Antimicrob Chemother | Rifabutin in HIV/TB-coinfected children on lopinavir/ritonavir; earlier work reported treatment-limiting neutropenia |
| [33294914](https://pubmed.ncbi.nlm.nih.gov/33294914/) | 2021 | Cohort | J Antimicrob Chemother | Rifabutin pharmacokinetics and safety in TB/HIV-coinfected children on lopinavir/ritonavir ART |
| [39037240](https://pubmed.ncbi.nlm.nih.gov/39037240/) | 2024 | Pharmacokinetic study | Antimicrob Agents Chemother | Population pharmacokinetics and dose optimisation of rifabutin in children on lopinavir/ritonavir |
| [36385424](https://pubmed.ncbi.nlm.nih.gov/36385424/) | 2023 | Pharmacokinetic model | Br J Clin Pharmacol | Rifabutin–dolutegravir interaction; rifabutin as a less potent inducer than rifampicin |
| [36377436](https://pubmed.ncbi.nlm.nih.gov/36377436/) | 2023 | Modelling study | Clin Infect Dis | Managing interactions between long-acting cabotegravir/rilpivirine and inducers such as rifabutin |
| [10456381](https://pubmed.ncbi.nlm.nih.gov/10456381/) | 1999 | Review | Drug Saf | Risk-benefit assessment of MAC therapies, including rifabutin |
| [17181373](https://pubmed.ncbi.nlm.nih.gov/17181373/) | 2006 | Review | Drugs | Practical approach to TB and HIV co-infection treatment |
| [7736687](https://pubmed.ncbi.nlm.nih.gov/7736687/) | 1995 | Pharmacokinetic review | Clin Pharmacokinet | Rifabutin pharmacokinetics; effective MAC prophylaxis in HIV patients with low CD4 counts |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 219924 | Mycobutin | Cap 150 mg | Not recorded (licence valid) |

## Cytotoxicity

Rifabutin is not an antineoplastic drug, so no cytotoxicity assessment applies.

## Safety Considerations

- **Key Warnings and Contraindications**: Package insert data were not available for this report. Please refer to the package insert.
- **Knowledge-graph contraindication signals** (from a drug–disease database, not label text): thrombocytopenia, neutropenia, agranulocytosis, *Clostridium difficile* colitis, and perinatal necrotizing enterocolitis. Neutropenia is consistent with the paediatric reports above.
- **Drug Interactions**: The interaction screen returned 100 interacting drugs. Only one of the first 20 listed is rated Major: **eliglustat**. Most others are Moderate, including doxycycline, clarithromycin, metronidazole, pioglitazone, aprepitant, morphine, saxagliptin, linagliptin, and several corticosteroids (hydrocortisone, dexamethasone, betamethasone, triamcinolone). Dolasetron is rated Minor.
- **HIV-specific interaction concerns** (from the literature): protease inhibitors, cabotegravir/rilpivirine, dolutegravir and other antiretrovirals need dose adjustment and, where possible, therapeutic drug monitoring.
- **Other concerns noted in the data**: uveitis, neutropenia and hepatotoxicity require monitoring.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Phase 3 trials in HIV patients with MAC support rifabutin as part of treatment and prophylaxis regimens, and it is an established option for TB/HIV co-treatment alongside protease inhibitors. The evidence concerns HIV-associated mycobacterial disease, not HIV itself, and the New Zealand licence is cancelled.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (the main gap, which blocks safety screening)
- Mechanism of action data from the drug database
- The approved indication text, since the Medsafe record is blank
- Dose adjustment and therapeutic drug monitoring guidance for protease inhibitors, cabotegravir/rilpivirine and other antiretrovirals
- A monitoring plan for uveitis, neutropenia and hepatotoxicity
- Confirmation of product availability in New Zealand, given the cancelled licence

**Other predicted indications (not recommended at this stage):**
- Leprosy (L4, research question): evidence is limited to preclinical work, a resistance-focused simulation study and a single case report, with likely cross-resistance with rifampicin.
- Endocarditis and pneumocystosis (L4, Hold): the literature does not establish efficacy.
- Multiple endocrine neoplasia, sclerosing cholangitis, conjunctivitis, a rare neurodevelopmental disorder, feline AIDS and simian immunodeficiency virus infection (L5, Hold): no clinical evidence, and the TxGNN model (deep learning) scores may reflect similarities the model learned between related diseases, not biological evidence. For conjunctivitis, rifabutin-associated uveitis is also a safety concern.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

