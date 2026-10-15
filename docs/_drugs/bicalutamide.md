---
layout: default
title: Bicalutamide
parent: Model Prediction Only (L5)
nav_order: 65
evidence_level: L5
indication_count: 10
---

# Bicalutamide
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Bicalutamide: From Prostate Cancer (Original Use) to Hypertrichosis

Note: The data compiled for this report contains no approved indication text for the New Zealand licence. Bicalutamide is a known androgen receptor antagonist, but the original indication is not stated in the data. The top-ranked prediction by TxGNN score is hypertrichosis, but the only prediction with meaningful clinical evidence is **female breast carcinoma** (rank 9), which is also covered below.

## One-Sentence Summary

Bicalutamide is a non-steroidal androgen receptor (AR) antagonist; its original indication is not recorded in the New Zealand licence data compiled for this report.
The TxGNN model's top-ranked prediction is **hypertrichosis** (score 99.69%), supported by only **0 clinical trials** and **1 comment/letter**, so evidence is very weak.
Among the top 10 predictions, **female breast carcinoma** has the most support, with **1 Phase 2 trial** and **19 publications**, mostly preclinical and review articles.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in the licence data (the single licence has no indication text) |
| Predicted New Indication | Hypertrichosis (rank 1); female breast carcinoma has the strongest evidence (rank 9) |
| TxGNN Prediction Score | 99.69% (hypertrichosis); 99.11% (female breast carcinoma) |
| Evidence Level | L4 (hypertrichosis); L2 (female breast carcinoma, per the evidence assessment) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 1 (cancelled) |
| Recommended Decision | Hold (hypertrichosis); Research Question for female breast carcinoma |

## Why is This Prediction Reasonable?

Bicalutamide is a non-steroidal androgen receptor antagonist. Detailed mechanism of action data is not available in the DrugBank entry compiled for this report, but its AR-blocking action is well established.

**Hypertrichosis.** Androgen signalling drives hair follicle activity, so AR blockade is plausible for androgen-dependent hair growth. The only supporting item is a comment on a retrospective review of bicalutamide for minoxidil-induced hypertrichosis in female pattern hair loss. This is indirect, secondary evidence and concerns drug-induced rather than congenital or generalised hypertrichosis. No trials are registered.

**Female breast carcinoma.** A subset of breast cancers, notably AR-positive triple-negative breast cancer (TNBC) and apocrine or luminal AR-positive tumours, express the androgen receptor. Bicalutamide may reduce proliferation and invasion in these tumours. Preclinical work supports this (TNBC cell lines, bicalutamide plus curcumin, AR/ERK co-targeting that triggers ferroptosis). AR is also implicated in resistance to endocrine therapy. However, a Phase 2 study of bicalutamide plus an aromatase inhibitor in ER+/AR+ advanced breast cancer was terminated early, with a 6-month clinical benefit rate of 16.7% and no objective responses. Patient selection by AR expression is essential.

**Other predictions (ranks 2–8 and 10).** These include a malformation syndrome with dental component, Ambras-type hypertrichosis, Dandy-Walker syndrome, genetic hair shaft abnormality, leprosy, multiple endocrine neoplasia, pulmonary hypertension and nephrogenic syndrome of inappropriate antidiuresis. None has a plausible AR-antagonism mechanism supported by evidence, and most are likely network artefacts of the knowledge graph. They are evidence level L5 (model prediction only) and Hold.

## Clinical Trial Evidence

No trials are registered for the top-ranked prediction (hypertrichosis). Trials for the other predictions with trial data:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT03650894](https://clinicaltrials.gov/study/NCT03650894) | Phase 2 | Active, not recruiting | 30 | Nivolumab + ipilimumab + bicalutamide in metastatic HER2-negative breast cancer; tests AR blockade with checkpoint therapy; results not yet reported, and the combination design cannot isolate bicalutamide's contribution (female breast carcinoma) |
| [NCT03878524](https://clinicaltrials.gov/study/NCT03878524) | Phase 1 | Terminated | 2 | SMMART precision-oncology trial; not specific to multiple endocrine neoplasia; no interpretable efficacy data |
| [NCT02615223](https://clinicaltrials.gov/study/NCT02615223) | Not applicable | Unknown | 120 | Endocrine therapy with or without cryoablation for stage IV prostate cancer (quality of life and tumour control); not specific to multiple endocrine neoplasia |

## Literature Evidence

**Hypertrichosis (rank 1)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35304167](https://pubmed.ncbi.nlm.nih.gov/35304167/) | 2022 | Comment/Letter | J Am Acad Dermatol | Comment on a retrospective review of bicalutamide for minoxidil-induced hypertrichosis in female pattern hair loss; indirect evidence |

**Female breast carcinoma (rank 9; selected from 19 publications)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31434793](https://pubmed.ncbi.nlm.nih.gov/31434793/) | 2020 | Phase 2 single-arm study | Oncologist | Bicalutamide + aromatase inhibitor in ER+/AR+ advanced breast cancer showed no synergy; 6-month clinical benefit rate 16.7%, objective response rate 0%; terminated early |
| [40853613](https://pubmed.ncbi.nlm.nih.gov/40853613/) | 2025 | Review | Curr Med Sci | Anti-androgen therapy (bicalutamide, enzalutamide, enobosarm) for AR-positive TNBC; combinations being explored |
| [29940524](https://pubmed.ncbi.nlm.nih.gov/29940524/) | 2018 | Review | Cancer Treat Rev | AR as a potential target in the luminal androgen receptor (LAR) TNBC subtype |
| [35027319](https://pubmed.ncbi.nlm.nih.gov/35027319/) | 2022 | Review | Clin Breast Cancer | Molecular and clinical features of AR-positive apocrine breast carcinoma |
| [40974527](https://pubmed.ncbi.nlm.nih.gov/40974527/) | 2026 | Preclinical | Sci China Life Sci | Bicalutamide synergises with an ERK inhibitor to trigger ferroptosis via FOXC2 in TNBC |
| [39063165](https://pubmed.ncbi.nlm.nih.gov/39063165/) | 2024 | Preclinical | Int J Mol Sci | Bicalutamide enhances doxorubicin and docetaxel in human and canine inflammatory mammary cancer models |
| [31917699](https://pubmed.ncbi.nlm.nih.gov/31917699/) | 2020 | Preclinical | Anti-Cancer Drugs | Bicalutamide plus curcumin showed a strong effect in AR-positive TNBC models |
| [32332626](https://pubmed.ncbi.nlm.nih.gov/32332626/) | 2020 | Preclinical | Medicine | Bicalutamide inhibited proliferation and invasion of MDA-MB-231 TNBC cells |
| [24740738](https://pubmed.ncbi.nlm.nih.gov/24740738/) | 2014 | Review | J Mol Endocrinol | Reassessment of the role of AR in breast cancer |
| [21633166](https://pubmed.ncbi.nlm.nih.gov/21633166/) | 2011 | Preclinical/Translational | J Clin Invest | Identified six TNBC subtypes including LAR, guiding targeted therapy selection |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2523655 | Binarex | Tab 50 mg | Not stated (licence cancelled) |

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted hormonal therapy (non-steroidal anti-androgen), not a conventional cytotoxic |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert; the knowledge graph links the drug to liver disease and anaemia, so liver function and blood counts are reasonable to review |
| Handling Protection | Please refer to the package insert warnings and precautions |

## Safety Considerations

- **Contraindication signals (knowledge graph, not label text)**: liver disease, gallbladder disease, hyperglycaemia, diabetes mellitus, diabetic ketoacidosis, anaemia, macrocytic anaemia, interstitial lung disease and drug-induced liver injury.
- **Drug Interactions**: 100 interactions were identified; the 20 listed in the compiled data are mostly Moderate. Two are **Major**: cisapride and dolasetron (both associated with QT-prolongation concerns). Moderate examples include clarithromycin, levofloxacin, granisetron, aprepitant, famotidine, loperamide, budesonide and several laxatives (bisacodyl, lactulose, lactitol, magnesium citrate, magnesium hydroxide, castor oil, glycerin, mineral oil). Metronidazole is Minor.

Package insert warnings and contraindications were not available, so please refer to the package insert for full safety information.

## Conclusion and Next Steps

**Decision: Hold** (hypertrichosis and ranks 2–8 and 10). Female breast carcinoma may be pursued as a **Research Question**.

**Rationale:**
- The top-ranked prediction, hypertrichosis, rests on one indirect comment article and no trials. Most other high-scoring predictions have no plausible mechanism and no evidence. Bicalutamide's New Zealand licence is cancelled, with no active market presence.
- Female breast carcinoma has a plausible AR-based mechanism and one ongoing Phase 2 trial, but a completed Phase 2 study of bicalutamide plus an aromatase inhibitor showed no meaningful activity, and the remaining evidence is preclinical or review-based. Benefit is likely limited to AR-positive subsets.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening)
- Mechanism of action data from DrugBank
- Confirmed original indication and regulatory history for the cancelled licence
- Results from NCT03650894 and clear AR-expression selection criteria for breast cancer
- For hypertrichosis, the full retrospective study on minoxidil-induced hypertrichosis, and prospective data in androgen-driven hair disorders
- A review of the major interactions (cisapride, dolasetron) and of the liver and metabolic risk signals before any new use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

