---
layout: default
title: Phenoxymethylpenicillin
parent: Model Prediction Only (L5)
nav_order: 362
evidence_level: L5
indication_count: 2
last_modified_at: 2026-11-12
monitor_inn: "PHENOXYMETHYLPENICILLIN"
predicted_indications: ["epiglottitis", "laryngitis"]
description: "Phenoxymethylpenicillin: predicted new indication Epiglottitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Phenoxymethylpenicillin
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **2** 
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

## Phenoxymethylpenicillin: Epiglottitis
{: .text-alpha }

## One-Sentence Summary

Phenoxymethylpenicillin (penicillin V) is an oral, narrow-spectrum penicillin originally used for streptococcal infections, pharyngitis, otitis media and other upper respiratory tract infections.
The TxGNN model predicts it may be effective for **epiglottitis**, but there are **0 clinical trials** and **0 publications** supporting this prediction. The evidence is model-based only.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Epiglottitis |
| TxGNN Prediction Score | 0.9990 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (4 Community Medicines items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 4 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not currently available. Based on known information, phenoxymethylpenicillin is a narrow-spectrum, beta-lactamase-labile penicillin with established activity against streptococci. Its efficacy in streptococcal infections and upper respiratory tract infections is well recognised, and it may be mechanistically applicable to bacterial epiglottitis.

Epiglottitis (supraglottitis) is most often bacterial. Historically the main cause was *Haemophilus influenzae* type b, but *Streptococcus pyogenes*, *Streptococcus pneumoniae* and *Staphylococcus aureus* are also implicated. The link to the drug's original uses is therefore plausible only for streptococcal cases. The very high TxGNN score is probably driven by the drug's known streptococcal and upper respiratory tract indications.

There are important limits to this reasoning:
- The drug has no activity against beta-lactamase-producing *H. influenzae* or MRSA.
- It is an oral agent, whereas epiglottitis is an airway emergency that needs parenteral therapy (for example third-generation cephalosporins) and airway management.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for epiglottitis.

For reference, the second-ranked prediction, laryngitis (score 0.9985, evidence level L4), has one linked publication. It is only indirect evidence:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [1632252](https://pubmed.ncbi.nlm.nih.gov/1632252/) | 1992 | RCT (inferred from the title) | Acta Oto-Laryngologica Supplementum | 106 adults with acute laryngitis received erythromycin or placebo. Erythromycin reduced nasopharyngeal *B. catarrhalis* isolation. The abstract notes that phenoxymethylpenicillin had no effect on the clinical course in earlier work. |

## New Zealand Market Information

No approved indication text is available.

<!-- review:begin nz-lic-20261106-phenoxymethylpenicillin-1 -->

> **Review correction (2026-11-06)**: Originally stated: "All four licences are valid. No approved indication text is available." Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 4 listings for this drug, 4 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-phenoxymethylpenicillin-1 -->

<!-- licenses:begin -->

| Pharmacode | Brand | Chemical | Presentation | Fully subsidised |
|------|------|------|------|------|
| 2602865 | Cilicaine VK | Phenoxymethylpenicillin (Penicillin V) | Cap 250 mg | Yes |
| 2602873 | Cilicaine VK | Phenoxymethylpenicillin (Penicillin V) | Cap 500 mg | Yes |
| 2029219 | AFT | Phenoxymethylpenicillin (Penicillin V) | Grans for oral liq 125 mg per 5 ml | Yes |
| 2029227 | AFT | Phenoxymethylpenicillin (Penicillin V) | Grans for oral liq 250 mg per 5 ml | Yes |

Source: Pharmac Pharmaceutical Schedule, Community Medicines (funding list). These are not Medsafe licences, and Medsafe licence status is not included in this data.

<!-- licenses:end -->

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (not taken from package insert text): kidney disease, pyoureter, *Clostridium difficile* colitis, perinatal necrotizing enterocolitis, infectious mononucleosis, and chronic Epstein-Barr virus infection syndrome.
- **Drug Interactions**: 32 interactions were identified. The main ones by severity are:
  - Moderate: doxycycline, minocycline, tetracycline, neomycin, balsalazide, picosulfuric acid, warfarin, dicoumarol, iodide I-123, iodide I-131
  - Minor: clarithromycin
  - Severity unknown: omeprazole, lansoprazole, simvastatin, prednisolone, dexamethasone, acetylsalicylic acid, clopidogrel, heparin, ibuprofen

For package insert warnings and contraindications, please refer to the package insert. The Medsafe package insert data could not be obtained for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a model score alone, with no clinical trials or literature for epiglottitis. Clinically, an oral, beta-lactamase-labile penicillin is unsuitable for an airway emergency whose major pathogens it does not cover.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (currently a blocking data gap)
- Detailed mechanism of action data
- Clinical or microbiological evidence supporting use in streptococcal epiglottitis, and a defined role relative to standard parenteral therapy
- Route compatibility assessment (oral drug versus parenteral requirement)
- Clarification of regulatory status

<!-- review:begin nz-lic-20261106-phenoxymethylpenicillin-2 -->

> **Review correction (2026-11-06)**: Originally stated: "Clarification of regulatory status, since all 4 New Zealand licences are valid" Removed: the number or status of Schedule listings stated here did not match the Pharmac Schedule data (the Pharmaceutical Schedule as of 2026-11-06). The Pharmaceutical Schedule lists 4 listings for this drug, 4 current; the Quick Overview and the table under New Zealand Market Information show the current figures. Source: [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/).

<!-- review:end nz-lic-20261106-phenoxymethylpenicillin-2 -->

<!-- review:begin log -->

## Review Log

Below is the review log for this page: statements checked against official Pharmac data (the New Zealand Pharmaceutical Schedule). Corrections are limited to basic factual statements; model predictions, evidence levels, and conclusions were not rewritten.

| Review date | Item | Action | Source |
|------|------|------|------|
| 2026-11-06 | Removed: "All four licences are valid" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |
| 2026-11-06 | Removed: "since all 4 New Zealand licences are valid" | Corrected | [Pharmac Pharmaceutical Schedule](https://schedule.pharmac.govt.nz/) |

<!-- review:end log -->

## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

