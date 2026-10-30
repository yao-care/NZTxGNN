---
layout: default
title: Omalizumab
parent: Moderate Evidence (L3-L4)
nav_order: 339
evidence_level: L3
indication_count: 10
last_modified_at: 2026-10-30
monitor_inn: "OMALIZUMAB"
predicted_indications: ["bronchitis", "atopic eczema", "obstructive lung disease", "dermatitis", "bronchial neoplasm (disease)", "acne keloid", "acrodermatitis chronica atrophicans", "hydroa vacciniforme, familial", "neonatal dermatomyositis", "secondary interstitial lung disease specific to childhood associated with a connective tissue disease"]
description: "Omalizumab: predicted new indication Bronchitis (evidence level L3). Approved use: Not recorded in the local data"
---

# Omalizumab
{: .fs-9 }

Evidence Level: **L3** | Predicted Indications: **10** 
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

## Omalizumab: Bronchitis
{: .text-alpha }

## One-Sentence Summary

Omalizumab is an anti-IgE monoclonal antibody originally used for asthma (including allergic and intrinsic asthma) and idiopathic urticaria.
The TxGNN model predicts it may be effective for **bronchitis**, but direct support is thin: **2 loosely related clinical trials** and **7 publications**, none of which test omalizumab in bronchitis as a distinct condition.
The data compiled for this report show stronger support for other predicted indications, notably atopic eczema and obstructive lung disease, which are discussed in the closing sections.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 1.0000 |
| Evidence Level | L3 |
| New Zealand Market Status | Listed on the Pharmac Pharmaceutical Schedule (3 of 3 items) – Medsafe licence status is not included in this data |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold (a research question only) |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the compiled data. Omalizumab is known to be a humanized monoclonal antibody that binds free IgE and reduces FcεRI expression on mast cells and basophils, which dampens allergic airway inflammation. Its efficacy in allergic asthma is well established.

The link to bronchitis is indirect. It runs through allergic asthma and asthma-COPD overlap, where chronic bronchitis features coexist. The one trial that comes closest (NCT02049294) enrolled patients with asthma and persistent eosinophilic bronchitis, but only 11 participants. No trial tests omalizumab in bronchitis as a standalone condition. The high TxGNN score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, such as bronchitis and asthma, not biological evidence.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02049294](https://clinicaltrials.gov/study/NCT02049294) | Phase 2/3 | Completed | 11 | Placebo-controlled test of whether omalizumab lets patients with asthma and persistent eosinophilic bronchitis reduce their prednisone dose. Very small; weak indirect support only |
| [NCT02477332](https://clinicaltrials.gov/study/NCT02477332) | Phase 2 | Completed | 382 | Dose-finding study of QGE031 (a different anti-IgE agent) in chronic spontaneous urticaria, with an active comparator. Not about bronchitis; low relevance |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16222080](https://pubmed.ncbi.nlm.nih.gov/16222080/) | 2005 | Review | Clin Rev Allergy Immunol | Omalizumab reduces free IgE and FcεRI expression, and is effective in moderate-to-severe persistent asthma |
| [30196731](https://pubmed.ncbi.nlm.nih.gov/30196731/) | 2018 | Review | Expert Opin Pharmacother | Management of asthma in smokers, including chronic bronchitis and asthma-COPD overlap; treatment is uncertain because these patients are excluded from most trials |
| [21121874](https://pubmed.ncbi.nlm.nih.gov/21121874/) | 2011 | Cohort (pooled analysis) | Curr Med Res Opin | Safety and tolerability of omalizumab in children with allergic asthma |
| [35369622](https://pubmed.ncbi.nlm.nih.gov/35369622/) | 2022 | Cohort | Postepy Dermatol Alergol | Biologics may help middle-aged or older patients with severe allergic asthma-COPD overlap |
| [17663923](https://pubmed.ncbi.nlm.nih.gov/17663923/) | 2007 | Review | Allergol Immunopathol | Overview of monoclonal antibodies in paediatrics, including allergic disease |
| [26466493](https://pubmed.ncbi.nlm.nih.gov/26466493/) | 2015 | Review | Masui | Preoperative management of asthma or chronic bronchitis; mentions omalizumab for severe allergic asthma |
| [31478531](https://pubmed.ncbi.nlm.nih.gov/31478531/) | 2019 | Case report | J Investig Allergol Clin Immunol | Plastic bronchitis after bronchial thermoplasty (no abstract available) |

---

## New Zealand Market Information

All three licences are valid; the approved-indication text is not available.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2243369 | Xolair | Inj 150 mg vial | Not available (valid) |
| 2687925 | Xolair AU | Inj 150 mg prefilled syringe | Not available (valid) |
| 2555042 | Xolair | Inj 150 mg prefilled syringe | Not available (valid) |

---

## Safety Considerations

- **Drug Interactions**: Eight interactions were recorded. Seven come from an interaction database: one Moderate (omacetaxine mepesuccinate) and six Minor (zinc sulfate, zinc acetate, zinc gluconate, zinc chloride, Echinacea, vitamin E). The eighth is a pharmacology entry describing omalizumab's target, IgE (the immunoglobulin heavy constant epsilon gene), rather than a true drug interaction.

Please refer to the package insert for other safety information (warnings and contraindications), as these data were not available.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Bronchitis is supported only by an indirect link through asthma and by one very small trial (n=11) in eosinophilic bronchitis.

**Other predicted indications in the data (for context):**
- **Obstructive lung disease** (Evidence Level L1, Proceed with Guardrails): many completed Phase 3 trials in allergic asthma, and a Phase 2 trial in allergen-sensitized COPD is recruiting (NCT07059091, n=334). Because asthma is already an established use, this largely confirms existing use rather than a new repurposing signal. The guardrail is that the support holds for allergic asthma, not for COPD.
- **Atopic eczema** and **dermatitis** (L2, Research Question): a completed Phase 4 trial in severe paediatric atopic eczema (NCT02300701, n=62) and several small studies, but efficacy is inconsistent. Guidelines favour agents targeting IL-4/IL-13, so any benefit is likely limited to selected patients.
- **Bronchial neoplasm, acne keloid, acrodermatitis chronica atrophicans, familial hydroa vacciniforme, neonatal dermatomyositis, and childhood connective-tissue-disease interstitial lung disease** (L5, Hold): model prediction only, with no mechanistic link or supporting studies.

**To proceed, the following is needed:**
- Mechanism-of-action data from DrugBank
- Package insert warnings and contraindications from Medsafe
- Clarification of the current regulatory status in New Zealand, where all three licences are valid
- A controlled trial in bronchitis or chronic bronchitis phenotypes, if this indication is pursued
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

