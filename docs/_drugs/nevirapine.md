---
layout: default
title: Nevirapine
parent: Model Prediction Only (L5)
nav_order: 319
evidence_level: L5
indication_count: 3
last_modified_at: 2026-10-15
monitor_inn: "NEVIRAPINE"
predicted_indications: ["feline acquired immunodeficiency syndrome", "simian immunodeficiency virus infection", "neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter"]
description: "Nevirapine: predicted new indication Feline acquired immunodeficiency syndrome (evidence level L5). Approved use: Not recorded in the local data"
---

# Nevirapine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **3** 
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

## Nevirapine: Feline acquired immunodeficiency syndrome
{: .text-alpha }

## One-Sentence Summary

Nevirapine is a non-nucleoside reverse transcriptase inhibitor, originally used to treat HIV infection and AIDS.
The TxGNN model predicts it may be effective for **feline acquired immunodeficiency syndrome**, but there are **no clinical trials** and only **1 publication** (a preclinical biochemical and structural study), so the evidence is weak.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Feline acquired immunodeficiency syndrome |
| TxGNN Prediction Score | 0.9985 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Nevirapine is an NNRTI that binds an allosteric pocket of HIV-1 reverse transcriptase (RT). Detailed mechanism of action data are not available in the data compiled for this report, so this description relies on its known drug class.

Feline immunodeficiency virus (FIV) is a lentivirus, like HIV, and it also depends on a reverse transcriptase. This gives a plausible mechanistic rationale. However, NNRTIs are generally weak or inactive against FIV RT because the NNRTI binding pocket differs. The high TxGNN score therefore probably reflects the shared lentiviral disease class and not demonstrated activity.

The only publication retrieved (2023) compares NNRTIs, including nevirapine, efavirenz and rilpivirine, against feline and human immunodeficiency virus RT. It is preclinical, and the available text does not show the direction of the result. FIV infection in cats is a veterinary condition, not a human clinical indication, so this prediction is a research consideration and not a human repurposing opportunity.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [38031646](https://pubmed.ncbi.nlm.nih.gov/38031646/) | 2023 | Biochemical/structural in vitro study | Journal of Veterinary Science | Compares nevirapine, efavirenz and rilpivirine against feline and human immunodeficiency virus RT to assess NNRTI potential for FIV; the direction of the result cannot be confirmed from the available text |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2640023 | Nevirapine Viatris (valid) | Tab 200 mg | Not available |
| 2014947 | Viramune Suspension (valid) | Oral suspension 10 mg per ml | Not available |

## Safety Considerations

- **Conditions listed as contraindications or high-risk in the knowledge graph** (these are graph associations, not package insert text): liver failure, liver disease, drug-induced liver injury, gallbladder disease, chronic hepatitis B and C virus infection, hepatitis C virus infection, exanthem, Stevens-Johnson syndrome, toxic epidermal necrolysis, Lyell syndrome, rhabdomyolysis, nodular fasciitis.
- **Drug Interactions**: 100 interactions were recorded. The first 20 reviewed are all of Moderate severity, including rabeprazole, bupropion, aprepitant, clarithromycin, the corticosteroids (dexamethasone, betamethasone, budesonide, prednisolone, triamcinolone), saxagliptin, linagliptin, eliglustat, naltrexone, naloxegol, naldemedine, cisapride, granisetron, ondansetron, nitisinone and orlistat. Nevirapine is known to induce CYP3A4 and CYP2B6, which likely underlies many of these.

Please refer to the package insert for the full warnings and contraindications, as label-based data were not available.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on a high model score and a single preclinical paper. NNRTIs are generally weak against FIV RT, and FIV infection is not a human indication. Nevirapine is also no longer actively marketed in New Zealand (both licences cancelled), and it carries notable liver and skin toxicity risks.

**To proceed, the following is needed:**
- Full text of PMID 38031646 to confirm whether nevirapine showed activity against FIV RT
- Mechanism of action data and the Medsafe package insert warnings and contraindications
- Veterinary efficacy and safety studies, if a veterinary application is to be considered
- A decision on whether this non-human indication is in scope for this evaluation

---

*Other predictions with lower priority:* **Simian immunodeficiency virus infection** (L4, Hold) is a model-system finding. Native SIV RT is generally resistant to NNRTIs, and NNRTI activity appears only in engineered SHIV chimeras carrying HIV-1 RT. **Neurodevelopmental disorder with ataxic gait, absent speech, and decreased cortical white matter** (L5, Hold) has no mechanistic link, trials or literature, and the safety burden would be significant in a paediatric neurological population.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

