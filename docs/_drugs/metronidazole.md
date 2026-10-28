---
layout: default
title: Metronidazole
parent: Model Prediction Only (L5)
nav_order: 297
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
monitor_inn: "METRONIDAZOLE"
predicted_indications: ["pneumocystosis", "punctate epithelial keratoconjunctivitis", "ulcerative proctosigmoiditis", "nocardiosis", "postmenopausal atrophic vaginitis", "Cryptococcal meningitis", "papillary conjunctivitis", "myiasis", "cap polyposis", "ulceration of vulva"]
description: "Metronidazole: predicted new indication Pneumocystosis (evidence level L5). Approved use: Not recorded in the local data"
---

# Metronidazole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

## Metronidazole: Pneumocystosis
{: .text-alpha }

## One-Sentence Summary

Metronidazole is an antimicrobial originally used for anaerobic bacterial infections, giardiasis, bacterial vaginosis and related conditions.
The TxGNN model predicts it may be effective for **pneumocystosis** with a very high score, but there are **no clinical trials** and **no supporting publications** that show metronidazole works against this disease.
The data compiled for this report show that the best-supported alternative signal is **cap polyposis**, and even that rests only on case reports.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 0.9999 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 4 of 4) |
| Number of Authorizations | 4 (valid: 4) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Metronidazole is a nitroimidazole whose nitro group is reduced in anaerobic bacteria and protozoa, producing DNA-damaging radicals.

For pneumocystosis, this rationale does not hold. *Pneumocystis* is a fungus and has no comparable anaerobic nitroreductase target. The very high TxGNN score (0.9999) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. The retrieved literature consists of general antiparasitic reviews and HIV opportunistic infection case reports. None shows metronidazole efficacy against *Pneumocystis*. In the case reports, metronidazole was given for a coexisting amoebic or diarrhoeal illness.

**Other predicted indications (ranks 2-10)**

| Rank | Disease | Score | Evidence Level | Assessment |
|------|------|------|------|------|
| 2 | Punctate epithelial keratoconjunctivitis | 0.9993 | L5 | Prediction only; no trials or literature |
| 3 | Ulcerative proctosigmoiditis | 0.9987 | L5 | Plausible indirect link through gut dysbiosis; no controlled evidence |
| 4 | Nocardiosis | 0.9986 | L5 | Weak; *Nocardia* is aerobic and standard therapy is sulfonamide-based |
| 5 | Postmenopausal atrophic vaginitis | 0.9984 | L5 | Caused by oestrogen deficiency; the only paper concerns oestrogen cream |
| 6 | Cryptococcal meningitis | 0.9983 | L5 | No mechanistic link; metronidazole has no antifungal activity |
| 7 | Papillary conjunctivitis | 0.9976 | L5 | Prediction only; no trials or literature |
| 8 | Myiasis | 0.9975 | L4 | Indirect; metronidazole is an adjunct for wound anaerobes, not larvicidal |
| 9 | Cap polyposis | 0.9973 | L4 | Best-supported candidate; case reports with a proposed dysbiosis or anti-inflammatory mechanism |
| 10 | Ulceration of vulva | 0.9972 | L5 | Context-dependent (vulvar Crohn's disease, cutaneous amebiasis) |

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

The ten publications retrieved for pneumocystosis are listed below. None provides evidence that metronidazole is effective against this disease.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [1545596](https://pubmed.ncbi.nlm.nih.gov/1545596/) | 1992 | Review | Mayo Clinic Proceedings | General review of antiparasitic agents; no data on metronidazole for *Pneumocystis* |
| [7355683](https://pubmed.ncbi.nlm.nih.gov/7355683/) | 1980 | Review | American Family Physician | Lists metronidazole for amoebiasis and trichomoniasis; trimethoprim-sulfamethoxazole is the drug of choice for pneumocystis pneumonia |
| [1782741](https://pubmed.ncbi.nlm.nih.gov/1782741/) | 1991 | Review | Clinical Pharmacokinetics | Pharmacokinetic rationale for antiprotozoal therapy in normal and immunocompromised hosts |
| [2996829](https://pubmed.ncbi.nlm.nih.gov/2996829/) | 1985 | Review | Clinical Pharmacy | Treatment of infectious complications of AIDS, including *Pneumocystis carinii* pneumonia |
| [26518395](https://pubmed.ncbi.nlm.nih.gov/26518395/) | 2015 | Review | Topics in Antiviral Medicine | Overview of HIV-related opportunistic infections |
| [6282154](https://pubmed.ncbi.nlm.nih.gov/6282154/) | 1982 | Case report | Am Rev Respir Dis | *Pneumocystis* and cytomegalovirus pneumonia in a patient previously treated with metronidazole and tetracycline for diarrhoea |
| [2338506](https://pubmed.ncbi.nlm.nih.gov/2338506/) | 1990 | Case report | Kansenshogaku Zasshi | Two AIDS patients; metronidazole was used for amoebic dysentery and liver abscess |
| [6771863](https://pubmed.ncbi.nlm.nih.gov/6771863/) | 1980 | Review | Reviews of Infectious Diseases | Critique of antimicrobial prophylaxis trials |
| [16496064](https://pubmed.ncbi.nlm.nih.gov/16496064/) | 2005 | Case report | J Formos Med Assoc | Colon perforation with peritonitis in an AIDS patient with amoebic colitis and CMV infection |
| [42178576](https://pubmed.ncbi.nlm.nih.gov/42178576/) | 2026 | Case report | J Med Case Rep | *S. intermedius* cerebellar abscess as first presentation of sarcoidosis |

**Best-supported alternative signal (cap polyposis):** one case study proposes that metronidazole acts through anti-inflammatory rather than antibiotic effects ([PMID 12141801](https://pubmed.ncbi.nlm.nih.gov/12141801/)). Other reports describe remission after antibiotic regimens including metronidazole, usually with *H. pylori* eradication ([24949613](https://pubmed.ncbi.nlm.nih.gov/24949613/)), while another case was refractory to drug treatment including infliximab ([38873995](https://pubmed.ncbi.nlm.nih.gov/38873995/)). No controlled data exist.

---

## New Zealand Market Information

Four of the four authorizations are valid, and no indication text is recorded for any of them.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2684446 | Metronidamed | Tab 200 mg | Not recorded |
| 729426 | Flagyl-S | Oral liq benzoate 200 mg per 5 ml | Not recorded |
| 2684454 | Metronidamed | Tab 400 mg | Not recorded |
| 250511 | Flagyl | Suppos 500 mg | Not recorded |

---

## Safety Considerations

Package insert warnings and contraindications were not available in the data compiled for this report, so please refer to the package insert for them.

- **Disease-level contraindication signals (from the knowledge graph, not label text):** liver failure, liver disease, kidney disease, epilepsy, brain disease, peripheral neuropathy and other peripheral nervous system disease, optic nerve and visual pathway disease, blood disorders (haematologic disease, neutropenia, agranulocytosis, leukopenia, blood coagulation disease), congestive heart failure, diabetes mellitus, diabetic ketoacidosis, gout, gallbladder disease, dermatitis, non-infectious meningitis, pyoureter and substance abuse/dependence.
- **Drug Interactions:** 100 interactions were found; the first 20 reviewed include:
  - **Major:** ethanol.
  - **Moderate:** paclitaxel, adalimumab, trastuzumab emtansine, ethinylestradiol, estradiol, butalbital, amobarbital, amiodarone, celecoxib and loperamide.
  - **Minor:** abiraterone, tramadol, famotidine, salbutamol, alfuzosin, amisulpride, amitriptyline, anagrelide and propofol.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The pneumocystosis prediction has no trials, no supporting literature and no plausible mechanism. The high score (0.9999) comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence.

**To proceed, the following is needed:**
- Do not pursue pneumocystosis, cryptococcal meningitis, nocardiosis or atrophic vaginitis as repurposing targets.
- Consider **cap polyposis** as a research question, using a case series or registry design, since the disease is rare.
- Consider **ulcerative proctosigmoiditis**, **myiasis** and **vulvar ulceration** only as secondary research questions, because metronidazole's independent effect is not isolated in the available evidence.
- Obtain the Medsafe package insert to complete the safety review. This gap is blocking for safety screening.
- Obtain mechanism-of-action data, for example from DrugBank.
- Confirm the reason for cancellation of the New Zealand authorizations and the availability of any alternative metronidazole products.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

