---
layout: default
title: Dapsone
parent: High Evidence (L1-L2)
nav_order: 134
evidence_level: L1
indication_count: 1
last_modified_at: 2026-10-15
description: "Dapsone: predicted new indication Pneumocystosis (evidence level L1). Approved use: Dermatitis herpetiformis, acne, leprosy"
monitor_inn: "DAPSONE"
predicted_indications: ["pneumocystosis"]
---

# Dapsone
{: .fs-9 }

Evidence Level: **L1** | Predicted Indications: **1** 
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

## Dapsone: From Dermatitis Herpetiformis, Acne and Leprosy to Pneumocystosis
{: .text-alpha }

## One-Sentence Summary

Dapsone is a sulfone drug originally used for dermatitis herpetiformis, acne and leprosy.
The TxGNN model predicts it may be effective for **pneumocystosis** (Pneumocystis pneumonia, PCP),
with **10 clinical trials** and **20 publications** currently supporting this direction, including several completed Phase 3 trials.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Dermatitis herpetiformis, acne, leprosy (the New Zealand licence records contain no indication text) |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 99.73% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on established pharmacology, dapsone is a sulfone that inhibits dihydropteroate synthase in the folate synthesis pathway. This is the same target as sulfonamides such as sulfamethoxazole. This mechanistic explanation comes from general pharmacology, not from the supplied data.

Dapsone's established uses are leprosy and dermatitis herpetiformis, and it also has anti-inflammatory effects. *Pneumocystis jirovecii* depends on the folate pathway, so a drug that acts on it is mechanistically plausible for PCP. Published reviews describe dapsone as useful for PCP prophylaxis in patients with HIV disease. The high TxGNN score is consistent with this picture.

Several trials directly compare dapsone with other PCP regimens. The data compiled for this report also show that dapsone causes dose-related haemolysis and methemoglobinemia, which shapes the safety guardrails below.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00000802](https://clinicaltrials.gov/study/NCT00000802) | Phase 3 | Completed | 700 | Daily dapsone vs daily atovaquone for PCP prophylaxis in HIV patients intolerant of trimethoprim/sulfonamides |
| [NCT00001028](https://clinicaltrials.gov/study/NCT00001028) | Phase 3 | Completed | 400 | Monthly aerosolized pentamidine vs thrice-weekly dapsone for PCP prophylaxis in sulfonamide-intolerant patients |
| [NCT00000640](https://clinicaltrials.gov/study/NCT00000640) | Phase 3 | Completed | 290 | Dapsone/trimethoprim and clindamycin/primaquine vs TMP-SMX for mild-to-moderate PCP in AIDS (exact comparator to be confirmed) |
| [NCT00000991](https://clinicaltrials.gov/study/NCT00000991) | Phase 3 | Completed | 600 | Three anti-Pneumocystis regimens plus zidovudine for primary prevention in advanced HIV (dapsone arm to be confirmed) |
| [NCT00000739](https://clinicaltrials.gov/study/NCT00000739) | Phase 1 | Completed | 96 | Daily vs weekly oral dapsone for PCP prophylaxis in HIV-infected children; toxicity and pharmacokinetics |
| [NCT00002120](https://clinicaltrials.gov/study/NCT00002120) | Phase 1 | Completed | 20 | Trimetrexate with leucovorin plus dapsone vs TMP-SMX for moderately severe PCP; safety and pharmacokinetics |
| [NCT00002043](https://clinicaltrials.gov/study/NCT00002043) | NA | Completed | Not reported | Dapsone 100 mg vs 50 mg as primary PCP prophylaxis in patients with AIDS-related complex |
| [NCT00002283](https://clinicaltrials.gov/study/NCT00002283) | NA | Completed | Not reported | Randomized comparison of dapsone plus trimethoprim vs TMP-SMX for first-episode PCP in AIDS |
| [NCT02550080](https://clinicaltrials.gov/study/NCT02550080) | Phase 4 | Unknown | 3130 | HLA-B*1301 genetic screening to reduce dapsone hypersensitivity syndrome (relevance to PCP not confirmed) |
| [NCT00636935](https://clinicaltrials.gov/study/NCT00636935) | Phase 4 | Withdrawn | 0 | Corticosteroids in mild PCP; not a dapsone study and no data produced |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38583518](https://pubmed.ncbi.nlm.nih.gov/38583518/) | 2024 | Systematic review / network meta-analysis | Clin Microbiol Infect | Compares PCP prophylaxis regimens (TMP-SMX, dapsone-based regimens, aerosolized pentamidine, atovaquone) in people living with HIV |
| [39732393](https://pubmed.ncbi.nlm.nih.gov/39732393/) | 2025 | Systematic review / network meta-analysis | Clin Microbiol Infect | Compares PCP treatment regimens in people living with HIV; TMP-SMX remains the primary treatment |
| [27550992](https://pubmed.ncbi.nlm.nih.gov/27550992/) | 2016 | Guideline | J Antimicrob Chemother | ECIL guidelines for PCP prophylaxis in haematological malignancy and stem cell transplant; TMP-SMX is the drug of choice |
| [33870843](https://pubmed.ncbi.nlm.nih.gov/33870843/) | 2021 | Review | Expert Opin Pharmacother | Review of *P. jirovecii* with a focus on prevention and treatment |
| [11155588](https://pubmed.ncbi.nlm.nih.gov/11155588/) | 2001 | Review | Dermatol Clin | Dapsone is the most important drug for leprosy and is useful for PCP prophylaxis in HIV disease |
| [18971152](https://pubmed.ncbi.nlm.nih.gov/18971152/) | 2008 | Review | J Formos Med Assoc | Overview of Pneumocystis pneumonia |
| [7937279](https://pubmed.ncbi.nlm.nih.gov/7937279/) | 1994 | Review | Pharmacotherapy | PCP prophylaxis during pregnancy, including dapsone options |
| [9606476](https://pubmed.ncbi.nlm.nih.gov/9606476/) | 1998 | Case report | Ann Pharmacother | Dapsone-induced methemoglobinemia during PCP prophylaxis |
| [33280223](https://pubmed.ncbi.nlm.nih.gov/33280223/) | 2021 | Retrospective review | Pediatr Transplant | Methemoglobinemia in paediatric kidney transplant recipients on dapsone for PJP prophylaxis |
| [31631707](https://pubmed.ncbi.nlm.nih.gov/31631707/) | 2021 | Review (safety) | Drug Chem Toxicol | Dapsone-induced hepatic complications beyond methemoglobinemia |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2212226 | Dapsone | Tab 25 mg | Not recorded (licence cancelled) |
| 712914 | Dapsone | Tab 100 mg | Not recorded (licence cancelled) |

Both authorizations are cancelled, so product availability in New Zealand should be verified.

## Safety Considerations

- **Disease-based cautions (from the knowledge graph, not label text)**: Dapsone is linked to contraindications in liver disease, gallbladder disease, anemia (including macrocytic anemia), G6PD deficiency, glycogen storage disease, cystinuria, galactosemia, pyruvate dehydrogenase E3 deficiency, and methemoglobinemia due to methemoglobin reductase deficiency.
- **Haematological risk**: The literature documents dapsone-induced methemoglobinemia, including hypoxia from methemoglobinemia (PMID 9606476, 32714715, 33280223). Other reported adverse effects include hepatic complications (PMID 31631707) and acute pancreatitis (PMID 14519046).
- **Drug Interactions**: 100 interactions were identified. The main moderate-level ones are aprepitant, metronidazole, picosulfuric acid, nateglinide, nitisinone, rosuvastatin, simvastatin, tinidazole, activated charcoal and troglitazone. Other listed interactions of unknown severity include calcitriol, pantoprazole, glimepiride, morphine, metformin, omeprazole, sucralfate, rosiglitazone, lansoprazole and vancomycin.

Please also refer to the package insert for label warnings and contraindications, which were not available in the data compiled for this report.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Four completed Phase 3 trials (700, 400, 290 and 600 participants) evaluate dapsone-containing regimens for PCP prophylaxis or treatment, and network meta-analyses include dapsone-based regimens. This supports Evidence Level L1. TMP-SMX remains the first-line agent, so dapsone is best viewed as an alternative for patients who cannot tolerate it. Both New Zealand licences are cancelled, and the haematological risks are significant.

**To proceed, the following is needed:**
- Confirm product availability in New Zealand, since both licences are cancelled
- Obtain the Medsafe package insert warnings and contraindications
- Confirm the arms of NCT00000991 and NCT00000640, whose titles are truncated
- Mechanism of action data from DrugBank
- G6PD screening before use and a methemoglobin and haemoglobin monitoring plan
- A review of concomitant medicines against the interaction list
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

