---
layout: default
title: Isosorbide Mononitrate
parent: Model Prediction Only (L5)
nav_order: 246
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "ISOSORBIDE MONONITRATE"
predicted_indications: ["hypertrichosis (disease)", "Ambras type hypertrichosis universalis congenita", "malformation syndrome with odontal and/or periodontal component", "syndrome with a Dandy-Walker malformation as major feature", "alopecia", "isolated genetic hair shaft abnormality", "hypotrichosis simplex of the scalp", "congenital hypotrichosis milia", "diffuse alopecia areata", "pulmonary arterial hypertension"]
description: "Isosorbide Mononitrate: predicted new indication Hypertrichosis (disease) (evidence level L5). Approved use: Not recorded in the local data"
---

# Isosorbide Mononitrate
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

## Isosorbide Mononitrate: Hypertrichosis (disease)
{: .text-alpha }

## One-Sentence Summary

Isosorbide mononitrate is a nitric oxide (NO)-donating vasodilator. Its original approved indication text is not available in the data compiled for this report.
The TxGNN model's top-ranked prediction is **hypertrichosis (disease)**, but there are **0 clinical trials** and **0 publications** supporting it. The only prediction with any supporting literature is **pulmonary arterial hypertension**, with **2 indirect publications** and no registered trials.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Hypertrichosis (disease) |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, isosorbide mononitrate is an NO donor. NO activates soluble guanylate cyclase, raises cGMP, and causes vasodilation.

For the top-ranked prediction, hypertrichosis, **no established mechanistic link exists**. Minoxidil, a vasodilator, can cause hypertrichosis, but it acts through potassium channel opening, not NO donation. The high score is therefore best treated as a model output with no supporting clinical or literature data. The other highly ranked predictions (Ambras syndrome, malformation syndromes, Dandy-Walker–related syndrome, hereditary hair shaft disorders, hypotrichosis, and alopecia areata) are mostly congenital, genetic, or autoimmune conditions. They are unlikely to respond to a vasodilator, and their scores come from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, not biological evidence. Alopecia has only a speculative link through scalp microcirculation.

The **most biologically plausible** prediction is **pulmonary arterial hypertension (PAH)** (score 0.9994, rank 10). The NO–sGC–cGMP pathway is the same one targeted by approved PAH drugs such as sildenafil and riociguat. However, organic nitrates are not an established PAH therapy. Tolerance and the severe hypotension risk with PDE5 inhibitors are important concerns.

---

## Clinical Trial Evidence

Currently no related clinical trials registered for the top-ranked prediction (hypertrichosis), nor for any of the other predictions, including pulmonary arterial hypertension.

---

## Literature Evidence

No literature is available for the top-ranked prediction (hypertrichosis). The only literature in the data relates to the pulmonary arterial hypertension prediction:

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16422873](https://pubmed.ncbi.nlm.nih.gov/16422873/) | 2005 | Clinical hemodynamic study | The Journal of Sexual Medicine | Hemodynamic effects of sildenafil combined with isosorbide mononitrate in men with coronary artery disease and erectile dysfunction (not a PAH population) |
| [29705351](https://pubmed.ncbi.nlm.nih.gov/29705351/) | 2018 | Preclinical animal study | Life Sciences | Stimulation of NO-sensitive soluble guanylate cyclase in monocrotaline-induced pulmonary hypertensive rats; examined whether NO-enhancing drugs can halt disease progression |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2447355 | Ismo 40 Retard | Tab long-acting 40 mg | Not available (valid) |
| 2202964 | Duride | Tab long-acting 60 mg | Not available (valid) |
| 214833 | Ismo 20 | Tab 20 mg | Not available (valid) |

---

## Safety Considerations

- **Knowledge-graph contraindication signals** (from drug–disease contraindication edges, not label text): hypotensive disorder, hypertrophic cardiomyopathy, myocardial infarction, coronary thrombosis, hemoglobinopathy, anaemia (including macrocytic anaemia), hyperthyroidism, Graves disease, thyrotoxicosis, and intracerebral haemorrhage.
- **Drug Interactions**: 100 interactions were identified. Moderate-level interactions include bupropion, morphine, canagliflozin, dapagliflozin, empagliflozin, ertugliflozin, dronabinol, nabilone, opium, sapropterin, and rosiglitazone. Minor: omeprazole. Several others (e.g., metformin, glimepiride, pantoprazole, lansoprazole, doxycycline) have an unclassified level.
- **Additional concern for the PAH direction**: the nitrate–PDE5 inhibitor interaction can cause severe hypotension, and nitrate tolerance may limit use.

Package insert warnings and contraindications were not available; please refer to the package insert for label safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top prediction (hypertrichosis) is supported only by the model score, with no trials, no literature, and no plausible mechanism. Pulmonary arterial hypertension is the only biologically plausible direction, but its evidence is indirect (L4) and best treated as a research question rather than a development candidate.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (a blocking gap for safety screening)
- Original indication and mechanism of action data
- For PAH: targeted mechanistic and preclinical work, and a safety assessment of tolerance and the PDE5 inhibitor–nitrate interaction
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

