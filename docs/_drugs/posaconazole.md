---
layout: default
title: Posaconazole
parent: Model Prediction Only (L5)
nav_order: 372
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-30
description: "Posaconazole: predicted new indication Pneumocystosis (evidence level L5). Approved use: Fungal infections (chromomycosis, coccidioidomycosis, aspergillosis…"
monitor_inn: "POSACONAZOLE"
predicted_indications: ["pneumocystosis"]
---

# Posaconazole
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
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

## Posaconazole: From Invasive Fungal Infections to Pneumocystosis
{: .text-alpha }

## One-Sentence Summary

Posaconazole is a triazole antifungal originally used for fungal infections such as aspergillosis, candidiasis, histoplasmosis and sporotrichosis.
The TxGNN model predicts it may be effective for **pneumocystosis**, but this is a model prediction only, with **1 clinical trial** (of a different drug) and **5 publications** (none directly testing posaconazole in this disease).
Mechanistically the prediction is doubtful, and the evidence does not support it.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Fungal infections (chromomycosis, coccidioidomycosis, aspergillosis, candidiasis, histoplasmosis, sporotrichosis, fusariosis, mycetoma and others) |
| Predicted New Indication | Pneumocystosis |
| TxGNN Prediction Score | 0.9977 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Pharmac Schedule Items | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Posaconazole belongs to the triazole antifungal class. Triazoles generally inhibit fungal lanosterol 14-alpha-demethylase (CYP51), which blocks ergosterol synthesis in the fungal cell membrane.

The link to pneumocystosis is weak. *Pneumocystis jirovecii* has very little ergosterol in its membrane and relies mainly on cholesterol and other sterols, so the azole target is largely absent or not functionally relevant. The high TxGNN score most likely reflects the drug's general antifungal class association rather than a true mechanistic fit. This could not be verified from the available data.

Posaconazole is established for prevention and treatment of invasive mould and yeast infections, but that does not carry over to Pneumocystis.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04368559](https://clinicaltrials.gov/study/NCT04368559) | Phase 3 | Completed | 602 | Rezafungin (an echinocandin, not posaconazole) versus the standard antimicrobial regimen to prevent invasive fungal diseases after allogeneic blood and marrow transplantation. It gives context for antifungal prophylaxis only, not direct evidence for posaconazole in pneumocystosis. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26901377](https://pubmed.ncbi.nlm.nih.gov/26901377/) | 2016 | Review | Swiss Med Wkly | Overview of invasive candidiasis, aspergillosis, cryptococcosis and Pneumocystis pneumonia. It notes that mould-active posaconazole prophylaxis reduced invasive fungal disease in high-risk haemato-oncological patients. |
| [41232547](https://pubmed.ncbi.nlm.nih.gov/41232547/) | 2026 | Guideline | Lancet Infect Dis | British Society for Medical Mycology 2025 update on diagnosis of serious fungal diseases. It addresses diagnosis, not posaconazole treatment. |
| [41362140](https://pubmed.ncbi.nlm.nih.gov/41362140/) | 2025 | Guideline | Zhonghua Jie He He Hu Xi Za Zhi | Chinese guidelines for diagnosis and management of invasive pulmonary fungal diseases. |
| [21973267](https://pubmed.ncbi.nlm.nih.gov/21973267/) | 2011 | Review | Clin Pharmacokinet | Penetration of antifungal and other anti-infective agents into pulmonary epithelial lining fluid. |
| [35596686](https://pubmed.ncbi.nlm.nih.gov/35596686/) | 2022 | Retrospective cohort | Transplant Infect Dis | Infectious complications of acute graft-versus-host disease after liver transplantation. |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2631008 | Posaconazole Juno | Tab modified-release 100 mg | Not available (valid) |
| 2645181 | Devatis | Oral liq 40 mg per ml | Not available (valid) |

## Safety Considerations

- **Drug Interactions**: 100 interactions were identified in total. The 20 listed in the data compiled for this report include:
  - **Major**: loperamide, triamcinolone, budesonide
  - **Moderate**: famotidine, ranitidine, cimetidine, omeprazole, rabeprazole, hydrocortisone, dexamethasone, betamethasone, aprepitant, clarithromycin, saxagliptin, bisacodyl, picosulfuric acid, polyethylene glycol (3350 with electrolytes), and nasal and ophthalmic corticosteroid forms

Please refer to the package insert for warnings and contraindications.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on the model score alone (L5). The only trial found tests a different drug, and no literature directly supports posaconazole in pneumocystosis. Pneumocystis lacks the main azole target, ergosterol, so the high score is probably a class-association artefact. Both New Zealand authorizations are cancelled.

**To proceed, the following is needed:**
- Direct preclinical or clinical evidence of posaconazole activity against *Pneumocystis*
- Mechanism of action data and confirmation of a plausible target
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Clarification of New Zealand availability, since both authorizations are cancelled
- A review of the interaction profile for the intended patient population
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

