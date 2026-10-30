---
layout: default
title: Ethambutol
parent: Model Prediction Only (L5)
nav_order: 185
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-30
monitor_inn: "Ethambutol"
predicted_indications: ["epiglottitis", "laryngitis", "meningococcal infection", "peritonitis", "infectious otitis media"]
description: "Ethambutol: predicted new indication Epiglottitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Ethambutol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
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

## Ethambutol: Epiglottitis
{: .text-alpha }

## One-Sentence Summary

Ethambutol is an anti-mycobacterial drug originally used to treat pulmonary tuberculosis.
The TxGNN model predicts it may be effective for **epiglottitis**,
but there are **0 registered clinical trials** and only **1 publication** (a case series of laryngeal tuberculosis) supporting this direction, so the evidence is weak.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Epiglottitis |
| TxGNN Prediction Score | 0.9990 |
| Evidence Level | L5 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (2 of 2 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Ethambutol is an anti-mycobacterial agent that inhibits arabinosyl transferase, which blocks synthesis of arabinogalactan in the mycobacterial cell wall. Detailed mechanism of action data was not available in the data compiled for this report, so this description is based on general pharmacological knowledge.

The only linked publication is a case series of laryngeal tuberculosis, in which the epiglottis was one of the affected sites and patients were treated with multi-drug anti-TB regimens that included ethambutol. This is tuberculosis at an adjacent anatomical site, not epiglottitis in the usual sense. Typical epiglottitis is caused by *Haemophilus influenzae* or other bacteria, which ethambutol does not cover.

The high TxGNN score therefore most likely comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence, rather than a true new indication. Any plausible link is limited to rare tuberculous involvement of the epiglottis or larynx, which is an extension of the original indication.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [2806495](https://pubmed.ncbi.nlm.nih.gov/2806495/) | 1989 | Case series | The European Respiratory Journal | 41 cases of laryngeal tuberculosis (1975-1985); the epiglottis was the second most frequently affected site after the true vocal cords; patients were treated with isoniazid, rifampicin and ethambutol-containing regimens |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2540746 | EMB Fatol | Tab 100 mg | Status: valid |
| 2244969 | Myambutol | Tab 400 mg | Status: valid |

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug-disease relationship data, not label text): kidney disease, pyoureter, gout, diabetic retinopathy, diabetic cataract, optic neuritis.
- **Drug Interactions**: 100 interactions were identified in total. Moderate-level interactions include metronidazole, picosulfuric acid, naltrexone, rosuvastatin, simvastatin, tinidazole and aluminum hydroxide. Others, such as proton pump inhibitors (omeprazole, pantoprazole, lansoprazole), corticosteroids (prednisone, prednisolone, hydrocortisone), morphine, vancomycin and ranitidine, have an unclassified severity level.

Please also refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No clinical trials exist, and the only supporting publication describes tuberculosis involving the larynx, which is a manifestation of the original indication rather than a true repurposing.

**To proceed, the following is needed:**
- Detailed mechanism of action data
- Package insert warnings and contraindications, to allow safety screening
- Evidence of benefit in non-tuberculous epiglottitis (none currently available), or a decision to treat tuberculous epiglottitis as part of the existing tuberculosis indication
- Clarification of the regulatory pathway, since the drug is marketed in New Zealand with 2 valid licences of 2
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

