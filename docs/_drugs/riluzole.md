---
layout: default
title: Riluzole
parent: Model Prediction Only (L5)
nav_order: 405
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Riluzole: predicted new indication Bilateral parasagittal parieto-occipital polymicrogyria (evidence level L5). Approved use: Amyotrophic lateral sclerosis"
monitor_inn: "RILUZOLE"
predicted_indications: ["bilateral parasagittal parieto-occipital polymicrogyria", "axial spondylometaphyseal dysplasia", "lower motor neuron syndrome with late-adult onset", "trichomegaly-retina pigmentary degeneration-dwarfism syndrome", "lethal arthrogryposis-anterior horn cell disease syndrome", "monomelic amyotrophy", "Mills syndrome", "autosomal dominant mitochondrial myopathy with exercise intolerance", "amyotrohpic lateral sclerosis type 22"]
---

# Riluzole
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

## Riluzole: From Amyotrophic Lateral Sclerosis to Bilateral Parasagittal Parieto-occipital Polymicrogyria
{: .text-alpha }

## One-Sentence Summary

Riluzole is a glutamate-modulating drug originally used to treat amyotrophic lateral sclerosis (ALS).
The TxGNN model predicts it may be effective for **bilateral parasagittal parieto-occipital polymicrogyria**, a rare cortical malformation,
but **no clinical trials and no publications** currently support this specific prediction, so it rests on model output alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Amyotrophic lateral sclerosis (the approved indication text in the New Zealand licence record is empty; this is taken from the knowledge-graph indication data) |
| Predicted New Indication | Bilateral parasagittal parieto-occipital polymicrogyria |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this drug in the data compiled for this report. Based on general knowledge, riluzole is thought to reduce glutamate-mediated excitotoxicity and block sodium channels. Reviews retrieved for ALS describe it as a drug thought to affect glutamate metabolism, which modestly prolongs survival in ALS.

Polymicrogyria is a cortical malformation often associated with neuronal hyperexcitability. Glutamate modulation and sodium-channel blockade could plausibly be relevant to that hyperexcitability. However, this link is speculative. No trials or literature were retrieved for this condition, and the high graph score should be treated as a hypothesis, not as evidence of benefit.

Among the other top-ranked predictions, several (for example lower motor neuron syndrome with late-adult onset, Mills syndrome, monomelic amyotrophy) are motor neuron disorders that are biologically closer to ALS. These also have no direct clinical evidence. The ALS-susceptibility and ALS type 22 entries fall within riluzole's existing indication, so they are not true repurposing candidates.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

Currently no related literature available for the predicted indication.

For context, 20 publications were retrieved for the related entity "amyotrophic lateral sclerosis, susceptibility to". These are mostly reviews and mechanistic papers on ALS, not studies of riluzole in the predicted indication. Examples include [9178165](https://pubmed.ncbi.nlm.nih.gov/9178165/) (1997, Review, glutamate excitotoxicity in ALS) and [42161327](https://pubmed.ncbi.nlm.nih.gov/42161327/) (2026, association study, CYP2D6 variants and ALS risk and survival).

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 238732 (cancelled) | Rilutek | Tab 50 mg | Not recorded |

## Safety Considerations

- **Contraindications / risk conditions (from knowledge-graph drug-disease links, not label text):** neutropenia, agranulocytosis, substance abuse/dependence, hyperbilirubinemia (including hereditary forms and Gilbert syndrome), interstitial lung disease, and drug-induced liver injury.
- **Drug Interactions:** 44 interactions were identified in total. Moderate-level interactions include obeticholic acid, naltrexone, deferasirox, osilodrostat, anagrelide, asparaginase, brentuximab vedotin, clofarabine, methotrexate and epirubicin. Minor-level interactions include omeprazole, caffeine, theophylline and aminophylline. Interactions with metformin, simvastatin, ranitidine, cyanocobalamin, salbutamol and ibuprofen are of unknown severity.
- **Liver:** A published modelling study reports that about 50% of ALS patients taking oral riluzole have ALT above the upper limit of normal, 8% above 3× ULN and 2% above 5× ULN.

Please refer to the package insert for full warnings. The Medsafe label text was not available for this report.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction is supported only by a model score, with no trials or literature for this condition. The only New Zealand licence is cancelled, and the mechanistic link is speculative. The prediction does not currently justify further development investment.

**To proceed, the following is needed:**
- Systematic literature and trial searches specific to polymicrogyria and riluzole, including preclinical epilepsy or cortical-excitability models
- Detailed mechanism of action data
- Medsafe package insert warnings and contraindications for a formal safety review
- Clarification of the cancelled New Zealand licence status and the availability of any current supply route
- Consideration of the closer motor neuron disease candidates (such as Mills syndrome and lower motor neuron syndrome), which have a more plausible rationale but also lack direct evidence
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

