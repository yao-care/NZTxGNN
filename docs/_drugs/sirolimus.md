---
layout: default
title: Sirolimus
parent: High Evidence (L1-L2)
nav_order: 425
evidence_level: L2
indication_count: 7
last_modified_at: 2026-10-15
monitor_inn: "SIROLIMUS"
predicted_indications: ["liposarcoma", "ovarian myxoid liposarcoma", "uterine corpus perivascular epithelioid cell tumor", "benign PEComa", "lymphangiomyoma", "lung PEComa", "vulva sarcoma"]
description: "Sirolimus: predicted new indication Liposarcoma (evidence level L2). Approved use: Not recorded in the local data"
---

# Sirolimus
{: .fs-9 }

Evidence Level: **L2** | Predicted Indications: **7** 
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

## Sirolimus: Liposarcoma
{: .text-alpha }

## One-Sentence Summary

Sirolimus is an mTOR inhibitor, originally listed for renal cell carcinoma and lymphangioleiomyomatosis (LAM).
The TxGNN model predicts it may be effective for **liposarcoma**,
but only **1 clinical trial** (a Phase 2 single-arm combination study) and a small set of mostly indirect publications currently support this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Liposarcoma |
| TxGNN Prediction Score | 0.9989 |
| Evidence Level | L2 |
| New Zealand Market Status | Marketed (valid licences: 3 of 3) |
| Number of Authorizations | 3 (valid: 3) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known pharmacology, sirolimus inhibits mTORC1, a kinase that drives cell growth and proliferation. It is already linked to renal cell carcinoma and LAM, both diseases with mTOR pathway involvement.

Activation of the PI3K/AKT/mTOR pathway has been reported in soft tissue sarcomas, including liposarcoma. This gives a plausible mechanistic bridge from the original indications to the predicted one.

However, the evidence is thin. The single Phase 2 trial tested sirolimus in combination with another agent and has no randomized control, so sirolimus's independent contribution cannot be judged. The supporting literature is mostly reviews of sarcoma targeted therapy and papers on cancer risk in transplant patients, which are only indirect support.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02821507](https://clinicaltrials.gov/study/NCT02821507) | Phase 2 | Completed | 70 | Single-arm study of sirolimus plus cyclophosphamide in metastatic or unresectable myxoid liposarcoma and chondrosarcoma; results and the liposarcoma-specific subset were not available |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16434506](https://pubmed.ncbi.nlm.nih.gov/16434506/) | 2006 | RCT | J Am Soc Nephrol | In renal transplant recipients, sirolimus after early cyclosporine withdrawal reduced cancer risk; indirect relevance to sarcoma |
| [39796641](https://pubmed.ncbi.nlm.nih.gov/39796641/) | 2024 | Review | Cancers | Overview of novel therapeutics and regulatory approvals in soft tissue sarcoma |
| [37222206](https://pubmed.ncbi.nlm.nih.gov/37222206/) | 2023 | Review | Curr Opin Oncol | Rationale and trial results for molecular-targeted agents in advanced sarcomas |
| [20497911](https://pubmed.ncbi.nlm.nih.gov/20497911/) | 2010 | Review | Bull Cancer | Targeted treatments for rare connective tissue tumours and sarcomas |
| [26093731](https://pubmed.ncbi.nlm.nih.gov/26093731/) | 2015 | Review | Transplant Proc | Cancer screening in renal transplant patients on long-term immunosuppression |
| [42753013](https://pubmed.ncbi.nlm.nih.gov/42753013/) | 2026 | Cohort | Fam Cancer | Clinical spectrum of PTEN hamartoma tumor syndrome; indirect |
| [32711543](https://pubmed.ncbi.nlm.nih.gov/32711543/) | 2020 | Case report | Diagn Pathol | Three cases of fibro-adipose vascular anomaly with emphasis on the mTOR pathway; indirect |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2109638 | Rapamune | Tab 1 mg | Not available (valid) |
| 2059339 | Rapamune | Oral liq 1 mg per ml | Not available (valid) |
| 2258218 | Rapamune | Tab 2 mg | Not available (valid) |

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (mTOR inhibitor); not a conventional cytotoxic agent |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert; typically blood counts, lipids, glucose, and liver and renal function |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Disease-level cautions (knowledge graph, not label text)**: The knowledge graph links sirolimus with caution in several conditions, including:
  - Metabolic: hyperglycaemia, diabetes mellitus, diabetic ketoacidosis, hyperlipidaemia, hypertriglyceridaemia
  - Organ disease: kidney disease, liver disease, hypertension, gallbladder disease, interstitial lung disease
  - Infections: infectious disease, pneumocystosis, cytomegalovirus infection, progressive multifocal leukoencephalopathy
  - Vascular: stroke and cerebrovascular disorders, pericardial effusion
  - Other: hyperbilirubinaemia, cancer
- **Drug Interactions**: 100 interactions were found. Main examples:
  - **Major**: amphotericin B (including lipid complex), mesalazine, balsalazide, clarithromycin
  - **Moderate**: antidiabetic agents (acarbose, metformin, pioglitazone, alogliptin, albiglutide, canagliflozin, dapagliflozin, chlorpropamide), corticosteroids (hydrocortisone, dexamethasone, betamethasone, budesonide, triamcinolone), aprepitant, cimetidine

Package insert warnings and contraindications were not available; please refer to the package insert for full safety information.

---

## Other Predicted Indications Worth Noting

| Predicted Indication | TxGNN Score | Evidence Level | Note |
|------|------|------|------|
| Lymphangiomyoma (LAM) | 0.9979 | L5 | Phase 3 MILES trial (NCT00414648, n=89) and Phase 3 early-disease trial (NCT03150914, n=60). LAM is already an original indication, so this is confirmatory rather than true repurposing. Recommendation: Proceed with Guardrails |
| Lung PEComa | 0.9963 | L5 | Evidence is extrapolated from LAM; recommendation: Proceed with Guardrails |
| Benign PEComa | 0.9980 | L5 | Mostly case reports and retrospective series; mTORC1 rationale via TSC1/TSC2 loss |
| Ovarian myxoid liposarcoma, uterine PEComa, renal cell carcinoma variants, vulva sarcoma | 99.6–0.9980 | L5 | Prediction only, no supportive trials or relevant literature |

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
For liposarcoma, the evidence is limited to a single Phase 2 single-arm combination trial and indirect literature, so sirolimus's own effect cannot be established. In addition, the drug is marketed in New Zealand (3 of 3 licences valid), and package insert safety data and mechanism of action data are missing.

**To proceed, the following is needed:**
- Results of NCT02821507, including the liposarcoma-specific subset and the identity of the combination partner, to assess sirolimus's independent contribution
- Randomized or at least controlled evidence of sirolimus in liposarcoma
- Detailed mechanism of action data
- Package insert warnings and contraindications for a formal safety review
- Confirmation of current regulatory status in New Zealand (3 of 3 licences valid)
- A review of the interaction profile (100 interactions, including several major) before any clinical use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

