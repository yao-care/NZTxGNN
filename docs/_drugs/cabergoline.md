---
layout: default
title: Cabergoline
parent: Model Prediction Only (L5)
nav_order: 77
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Cabergoline: predicted new indication Pituitary cancer (evidence level L5). Approved use: Hyperprolactinaemia and prolactin-producing pituitary tumour"
monitor_inn: "CABERGOLINE"
predicted_indications: ["progeria-short stature-pigmented nevi syndrome", "pituitary cancer", "cerebral palsy, spastic quadriplegic", "glaucoma"]
---

# Cabergoline
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

## Cabergoline: From Hyperprolactinaemia to Pituitary Cancer
{: .text-alpha }

## One-Sentence Summary

Cabergoline is a dopamine agonist originally used to treat hyperprolactinaemia and prolactin-producing pituitary tumours.
The TxGNN model predicts it may be effective for **pituitary cancer**, with **14 clinical trials** and **20 publications** in the compiled data. Most of this evidence concerns pituitary adenomas (nonfunctioning, corticotroph) rather than true pituitary carcinoma.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperprolactinaemia and prolactin-producing pituitary tumour (from the knowledge graph; no approved indication text is available from New Zealand licences) |
| Predicted New Indication | Pituitary cancer (the highest-evidence prediction; the top-ranked prediction by score is pituitary adenocarcinoma, which has only case reports) |
| TxGNN Prediction Score | 99.04% (pituitary cancer); 99.06% for pituitary adenocarcinoma |
| Evidence Level | L1 (for pituitary adenoma subtypes; see caveats below) |
| New Zealand Market Status | Cancelled only (no currently valid licences) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the compiled data. Based on known pharmacology, cabergoline is a D2 dopamine receptor agonist. Prolactin-secreting tumours, and some corticotroph, somatotroph and nonfunctioning adenoma cells, express D2 receptors. D2 agonism can therefore reduce hormone secretion and tumour size.

Prolactinoma is already an original indication, so evidence in prolactinoma does not count as repurposing. The repurposing-relevant evidence is for nonfunctioning pituitary adenoma and corticotroph tumours (Cushing disease). Reviews also describe newer anti-tumour mechanisms of cabergoline, including suppression of tumour cell proliferation and induction of cell death.

An important caveat: the trials and literature address pituitary **adenomas**, not true (metastatic) pituitary carcinoma, which is rare and aggressive. The high TxGNN score likely reflects pharmacological similarity to the approved prolactinoma indication. Evidence applies to adenoma subtypes, not carcinoma.

Other top predictions were weaker:
- **Pituitary adenocarcinoma** (score 99.06%, L4): only case reports (ectopic ACTH secretion, MEN1), no trials.
- **Progeria-short stature-pigmented nevi syndrome** (L5) and **spastic quadriplegic cerebral palsy** (L5): no identifiable mechanistic link and no supporting studies.
- **Glaucoma** (L4): the only cabergoline-specific report is a case of cabergoline-induced bilateral angle-closure glaucoma, which is a safety signal rather than a benefit.

---

## Clinical Trial Evidence

The table lists the most relevant trials for pituitary cancer/adenoma (14 were found; 10 shown).

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00889525](https://clinicaltrials.gov/study/NCT00889525) | Phase 3 | Completed | Not reported | Cabergoline for Cushing disease due to corticotroph pituitary adenoma (D2 receptor rationale) |
| [NCT03271918](https://clinicaltrials.gov/study/NCT03271918) | Phase 3 | Completed | 140 | Randomised open-label trial of cabergoline in residual nonfunctioning pituitary adenoma after transsphenoidal surgery |
| [NCT02288962](https://clinicaltrials.gov/study/NCT02288962) | Phase 3 | Active, not recruiting | 60 | Randomised trial of dopamine agonist in nonfunctioning pituitary adenomas |
| [NCT01620138](https://clinicaltrials.gov/study/NCT01620138) | Phase 2/3 | Completed | 21 | Somatostatin and dopamine receptor expression in nonfunctioning adenomas and resistant prolactinomas; response to pasireotide and cabergoline |
| [NCT07034859](https://clinicaltrials.gov/study/NCT07034859) | Phase 4 | Enrolling by invitation | 30 | Cabergoline vs no treatment for 48 weeks in nonfunctioning pituitary adenoma |
| [NCT00376064](https://clinicaltrials.gov/study/NCT00376064) | Phase 4 | Completed | 20 | Octreotide plus cabergoline in acromegaly partially responsive to somatostatin analogue alone |
| [NCT01915303](https://clinicaltrials.gov/study/NCT01915303) | Phase 2 | Terminated | 68 | Pasireotide alone or with cabergoline in Cushing disease |
| [NCT00460616](https://clinicaltrials.gov/study/NCT00460616) | N/A | Completed | 50 | Observational study of cardiac abnormalities and valvular regurgitation in prolactinoma patients on long-term cabergoline |
| [NCT07463235](https://clinicaltrials.gov/study/NCT07463235) | Phase 3 | Recruiting | 70 | High vs standard cabergoline dose in microprolactinoma (original indication; dosing and safety) |
| [NCT07045935](https://clinicaltrials.gov/study/NCT07045935) | Phase 4 | Recruiting | 60 | Metabolic outcomes under dopamine agonists in prolactinoma (original indication) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [37097352](https://pubmed.ncbi.nlm.nih.gov/37097352/) | 2023 | Review | JAMA | Diagnosis and management of pituitary adenomas |
| [31597135](https://pubmed.ncbi.nlm.nih.gov/31597135/) | 2020 | Review | Neuroendocrinology | New anti-tumour mechanisms of cabergoline and potential broader applications in pituitary and other tumours |
| [38989697](https://pubmed.ncbi.nlm.nih.gov/38989697/) | 2024 | Preclinical | Neuro-oncology | Targeting HTR2B suppresses nonfunctioning pituitary adenoma growth and sensitises it to cabergoline |
| [28025719](https://pubmed.ncbi.nlm.nih.gov/28025719/) | 2017 | Review | Pituitary | Cabergoline as an add-on option in acromegaly |
| [25732645](https://pubmed.ncbi.nlm.nih.gov/25732645/) | 2015 | Review | Endocrinol Metab Clin North Am | Cabergoline in pituitary tumours and valvular disorders; most studies show no significant valvulopathy increase |
| [36013562](https://pubmed.ncbi.nlm.nih.gov/36013562/) | 2022 | Review | Medicina (Kaunas) | Treatment of prolactinoma, including aggressive prolactinomas |
| [40876473](https://pubmed.ncbi.nlm.nih.gov/40876473/) | 2025 | Review | Lancet Diabetes Endocrinol | Prolactin-secreting adenomas: pathogenesis, diagnosis and management |
| [28170483](https://pubmed.ncbi.nlm.nih.gov/28170483/) | 2017 | Review | JAMA | Diagnosis and treatment of pituitary adenomas |
| [20497940](https://pubmed.ncbi.nlm.nih.gov/20497940/) | 2010 | Case report | Endocr Pract | Long-term octreotide or cabergoline in ectopic corticotropin hypersecretion |
| [41781297](https://pubmed.ncbi.nlm.nih.gov/41781297/) | 2026 | Review | Best Pract Res Clin Endocrinol Metab | Surgery for prolactinoma; notes psychiatric adverse reactions to dopamine agonists |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 275824 | Dostinex | Tab 0.5 mg | Not available (licence cancelled) |
| 275840 | Dostinex | Tab 0.5 mg | Not available (licence cancelled) |

Both licences are cancelled, so no cabergoline product is currently valid in New Zealand according to the compiled data.

---

## Safety Considerations

- **Contraindications suggested by the knowledge graph** (these are graph-derived associations, not label text): liver disease, gallbladder disease, heart valve disease, pulmonary fibrosis (including postinflammatory), IgG4-related retroperitoneal fibrosis, and pre-eclampsia (including severe).
- **Drug Interactions**: 60 interactions were identified. The main ones are:
  - **Major**: lorcaserin, clarithromycin.
  - **Moderate**: metoclopramide.
  - Many others have unclassified severity, including proton pump inhibitors (omeprazole, pantoprazole, lansoprazole, rabeprazole), H2 blockers (ranitidine, famotidine), corticosteroids (prednisone, prednisolone, hydrocortisone), metformin, glimepiride, pioglitazone, rosiglitazone, simvastatin and morphine.
- **Other signals from the evidence**:
  - Cardiac valvulopathy with high-dose or long-term use (debated; most studies show no significant increase in pituitary tumour patients).
  - Impulse control disorders and psychiatric adverse reactions.
  - A case report of bilateral angle-closure glaucoma.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (for pituitary adenoma subtypes only; **Hold** for the other predicted indications)

**Rationale:**
- Several completed or ongoing Phase 3 trials (NCT00889525, NCT03271918, NCT02288962) support cabergoline in nonfunctioning and corticotroph pituitary adenomas. However, no trial directly addresses true pituitary carcinoma, so the evidence supports adenoma subtypes only.
- Pituitary adenocarcinoma, progeria-short stature-pigmented nevi syndrome, spastic quadriplegic cerebral palsy and glaucoma each have only case reports, indirect data or no data, so they should be held.

**To proceed, the following is needed:**
- Package insert warnings and contraindications (from Medsafe), which are currently unavailable and block formal safety screening.
- Detailed mechanism of action data from DrugBank.
- Published results from the completed Phase 3 trials in nonfunctioning and corticotroph adenomas, to confirm efficacy.
- A clear regulatory pathway, since both New Zealand licences are cancelled.
- A monitoring plan for cardiac valvulopathy (echocardiography with high-dose or long-term use) and impulse control disorders, and caution in patients with a history of angle-closure glaucoma.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

