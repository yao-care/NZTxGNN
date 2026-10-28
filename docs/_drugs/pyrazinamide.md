---
layout: default
title: Pyrazinamide
parent: Model Prediction Only (L5)
nav_order: 393
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-28
description: "Pyrazinamide: predicted new indication Infectious otitis media (evidence level L5). Approved use: Pulmonary tuberculosis, meningeal tuberculosis, tuberculous meningitis"
monitor_inn: "PYRAZINAMIDE"
predicted_indications: ["infectious otitis media", "middle ear disease", "otosalpingitis", "chronic otitis media", "suppurative otitis media", "non-suppurative otitis media", "middle ear cholesterol granuloma", "allergic otitis media", "Lyme disease", "infectious otitis interna"]
---

# Pyrazinamide
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

## Pyrazinamide: From Tuberculosis to Infectious Otitis Media
{: .text-alpha }

## One-Sentence Summary

Pyrazinamide is an anti-tuberculosis drug, originally used for pulmonary tuberculosis and tuberculous meningitis.
The TxGNN model predicts it may be effective for **infectious otitis media**, but **no clinical trials and no publications** currently support this specific prediction.
The prediction rests on model output alone (evidence level L5).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Pulmonary tuberculosis, meningeal tuberculosis, tuberculous meningitis (from the knowledge graph; the New Zealand licence has no indication text recorded) |
| Predicted New Indication | Infectious otitis media |
| TxGNN Prediction Score | 0.9996 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 1 of 1) |
| Number of Authorizations | 1 (valid: 1) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this report. Based on known information, pyrazinamide is a first-line anti-tuberculosis agent whose activity is specific to *Mycobacterium tuberculosis* (acid-dependent activity through pyrazinoic acid). It is usually given as part of a multidrug regimen.

The link to otitis media is weak. Common otitis media pathogens (*S. pneumoniae*, *H. influenzae*, *M. catarrhalis*) are not susceptible to pyrazinamide. The prediction is mechanistically plausible only for the rare tuberculous subset of otitis media, where standard anti-TB regimens that include pyrazinamide would be used. That is an extension of its existing TB use to an extrapulmonary site, not true repurposing.

Other high-ranked predictions are in the same disease area: middle ear disease, otosalpingitis, chronic otitis media, suppurative otitis media, non-suppurative otitis media, middle ear cholesterol granuloma, allergic otitis media and infectious otitis interna. Lyme disease also appears among the top 10. For non-infectious conditions (allergic otitis media, cholesterol granuloma) there is no plausible antimycobacterial rationale.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for infectious otitis media.

---

## Literature Evidence

Currently no related literature available for infectious otitis media itself.

For related ear conditions, only indirect evidence was found. It is all case reports or low-tier studies, and pyrazinamide was given within multidrug regimens, so no benefit can be attributed to it alone:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [26785789](https://pubmed.ncbi.nlm.nih.gov/26785789/) | 2013 | Case report | Int J Mycobacteriol | Nasal and middle ear tuberculosis in a 55-year-old woman, treated with rifampicin, isoniazid and pyrazinamide for 9 months; no recurrence at 1 year (middle ear disease) |
| [15168420](https://pubmed.ncbi.nlm.nih.gov/15168420/) | 2004 | Case report | Am J Kidney Dis | Tuberculous otitis media in a renal transplant recipient after repeated empiric antibiotic treatment for chronic otitis media |
| [41783873](https://pubmed.ncbi.nlm.nih.gov/41783873/) | 2026 | Case report | Cureus | Tuberculous otitis media presenting as otorrhea, with delayed diagnosis in a low-prevalence setting |
| [8011030](https://pubmed.ncbi.nlm.nih.gov/8011030/) | 1994 | Case series | Laryngo-rhino-otologie | 12 patients with head and neck tuberculosis found incidentally, including one with middle ear involvement |
| [21439219](https://pubmed.ncbi.nlm.nih.gov/21439219/) | 2011 | Animal study | J Am Assoc Lab Anim Sci | Torticollis in *M. tuberculosis*-infected mice, including some on pyrazinamide-containing regimens; not a test of pyrazinamide for middle ear disease |

Several other retrieved items (orbitofacial tuberculosis, sacroiliitis in multiorgan tuberculosis, Lyme disease reports) are off-topic or only tangentially relevant and are not listed.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2142023 | AFT-Pyrazinamide | Tab 500 mg | Not recorded (licence valid) |

---

## Safety Considerations

- **Contraindications and disease associations (from the knowledge graph, not label text)**: liver failure, acute liver failure, liver disease, hepatitis, inherited porphyria, porphyria, porphyrin metabolism disease, gallbladder disease, diabetes mellitus, diabetic ketoacidosis, gout, peripheral nervous system disease, peripheral neuropathy, substance abuse/dependence, slow acetylator status.
- **Drug Interactions**: 56 interactions were identified. Two are rated Moderate: **picosulfuric acid** and **naltrexone**. The remainder, including pantoprazole, omeprazole, morphine, prednisone, vancomycin, amphotericin B, metronidazole, glyburide and streptomycin, have an unclassified severity level.

Package insert warnings and contraindications were not available for this report. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction for infectious otitis media is model-based only, with no trials or literature, and pyrazinamide has no activity against the usual causative bacteria. The indirect evidence covers only rare tuberculous otitis media, which is already treated with standard anti-TB regimens. Its New Zealand licence is also cancelled.

**To proceed, the following is needed:**
- Package insert warnings and contraindications, since safety screening cannot proceed without them
- Mechanism of action data
- Evidence that the target population (tuberculous otitis media) differs from existing TB indications, or a clear mechanistic case for non-tuberculous disease
- Confirmation of whether pyrazinamide is currently available in New Zealand, given the cancelled licence
- Assessment of drug penetration into the middle and inner ear, and of hepatic and neuropathy risks
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

