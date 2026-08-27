---
layout: default
title: Lenalidomide
parent: 僅模型預測 (L5)
nav_order: 199
evidence_level: L5
indication_count: 6
---

# Lenalidomide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
{: .fs-6 .fw-300 }

---

## 目錄
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## 藥師評估報告

</div>

# Lenalidomide: From Multiple Myeloma/MDS to Myeloid Leukemia

## One-Sentence Summary

> Lenalidomide is an oral immunomodulatory drug (IMiD) whose established use — per literature evidence in this pack — is multiple myeloma and del(5q)-associated myelodysplastic syndrome (MDS).
> The TxGNN model predicts it may be effective for **Myeloid Leukemia**,
> with **50 clinical trials** and **20 publications** currently identified, though only a subset are graded as directly relevant.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from `taiwan_regulatory.licenses` (no records — drug not marketed in New Zealand). Literature evidence (PMID 23316859) notes Lenalidomide is approved for transfusion-dependent anemia in del(5q) MDS and for multiple myeloma in combination with dexamethasone. |
| Predicted New Indication | Myeloid Leukemia |
| TxGNN Prediction Score | 99.49% |
| Evidence Level | L2 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack (`original_moa: [Data Gap]`). Based on the literature evidence included in the pack (PMID 23316859), Lenalidomide is an oral immunomodulatory drug derived from thalidomide, with established efficacy in transfusion-dependent anemia due to del(5q) myelodysplastic syndrome and in multiple myeloma (combined with dexamethasone). Its mechanism is understood to act through cereblon (CRBN)-mediated degradation of IKZF1/IKZF3, driving immunomodulatory and anti-angiogenic effects (supported by PMID 39881283, which describes CRBN stabilization as central to Lenalidomide's antileukemic activity).

Myelodysplastic syndrome and acute/myeloid leukemia exist on a biological continuum: MDS is a clonal hematopoietic stem-cell disorder that carries a well-documented risk of transformation into myeloid leukemia. Because Lenalidomide's efficacy is already established at the MDS end of this spectrum, extending its immunomodulatory and anti-angiogenic mechanism to myeloid leukemia — particularly in combination with hypomethylating agents such as azacitidine — has clear biological plausibility. This is consistent with the evidence pack's own rationale: "AML/MDS are both malignant proliferative disorders of hematopoietic stem cells; Lenalidomide's immunomodulatory and anti-angiogenic mechanism has clear benefit on the MDS side and extending to AML (especially combined with azacitidine) is biologically plausible, but AML-specific evidence independent of MDS remains at Phase 1/2."

However, evidence specific to myeloid leukemia (as opposed to MDS broadly) remains concentrated in Phase 1/2 trials, with only one Phase 3 AML-maintenance trial (NCT04490707, status UNKNOWN) identified, supporting the L2 evidence-level classification rather than a higher tier.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01904643](https://clinicaltrials.gov/study/NCT01904643) | Phase 1 | Terminated | 17 | Lenalidomide prior to MEC re-induction chemotherapy in relapsed/refractory AML — direct AML population (Grade A). |
| [NCT01246622](https://clinicaltrials.gov/study/NCT01246622) | Phase 1 | Completed | 32 | Cytarabine + Lenalidomide in relapsed/refractory AML — direct AML population (Grade A). |
| [NCT00839059](https://clinicaltrials.gov/study/NCT00839059) | Phase 1 | Terminated | 14 | Dose-escalation of Lenalidomide monotherapy in newly diagnosed/relapsed/refractory AML (Grade A). |
| [NCT00744536](https://clinicaltrials.gov/study/NCT00744536) | Phase 2 | Completed | 20 | Lenalidomide + metronomic melphalan in higher-risk MDS/CMML, anti-angiogenic rationale (Grade B). |
| [NCT04490707](https://clinicaltrials.gov/study/NCT04490707) | Phase 3 | Unknown | 60 | Azacitidine + Lenalidomide as MRD-guided maintenance in elderly/unfit AML. |
| [NCT01016600](https://clinicaltrials.gov/study/NCT01016600) | Phase 1/2 | Completed | 31 | Azacitidine + Lenalidomide for AML — toxicity and remission rates. |
| [NCT02126553](https://clinicaltrials.gov/study/NCT02126553) | Phase 2 | Completed | 29 | Lenalidomide maintenance in high-risk AML in remission. |
| [NCT02472691](https://clinicaltrials.gov/study/NCT02472691) | Phase 2 | Completed | 50 | Lenalidomide + azacitidine + DLI for MDS/CMML/AML relapse after allogeneic transplant. |
| [NCT00352365](https://clinicaltrials.gov/study/NCT00352365) | Phase 2 | Completed | 41 | Lenalidomide monotherapy in previously untreated del(5q) AML, patients ≥60 declining induction chemo. |
| [NCT00360672](https://clinicaltrials.gov/study/NCT00360672) | Phase 2 | Completed | 27 | Lenalidomide in relapsed/refractory AML or high-risk MDS with chromosome 5 abnormalities. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34955443](https://pubmed.ncbi.nlm.nih.gov/34955443/) | 2022 | Phase 1b trial | Journal of Geriatric Oncology | Lenalidomide as post-remission therapy in older AML adults — safety and geriatric functional assessment. |
| [31221030](https://pubmed.ncbi.nlm.nih.gov/31221030/) | 2019 | Systematic Review/Meta-analysis | Hematology (Amsterdam) | Azacitidine + Lenalidomide efficacy/adverse events across AML, MDS, and CMML. |
| [23644421](https://pubmed.ncbi.nlm.nih.gov/23644421/) | 2013 | Cohort | Leukemia | Review of azacitidine + lenalidomide combination in MDS/AML — rationale and outcomes. |
| [37259567](https://pubmed.ncbi.nlm.nih.gov/37259567/) | 2023 | Cohort (Azalena Trial) | Haematologica | Azacitidine + Lenalidomide + DLI for post-transplant relapse of AML/MDS/CMML. |
| [37435080](https://pubmed.ncbi.nlm.nih.gov/37435080/) | 2023 | Cohort | Frontiers in Immunology | Azacitidine + low-dose Lenalidomide as relapse-prophylaxis after allo-HSCT in AML. |
| [23316859](https://pubmed.ncbi.nlm.nih.gov/23316859/) | 2013 | Review | Expert Opinion on Investigational Drugs | Overview of Lenalidomide as a novel AML treatment; source for original MDS/myeloma indication context. |
| [30653424](https://pubmed.ncbi.nlm.nih.gov/30653424/) | 2019 | Trial | Journal of Clinical Oncology | Lenalidomide + azacitidine as salvage therapy after allo-SCT relapse in AML. |
| [40250191](https://pubmed.ncbi.nlm.nih.gov/40250191/) | 2025 | Phase 1 trial | Leukemia Research | Lenalidomide + bortezomib in AML/MDS relapsing after allogeneic stem cell transplant. |
| [34471239](https://pubmed.ncbi.nlm.nih.gov/34471239/) | 2021 | pending | Bone Marrow Transplantation | Safety/tolerability of Lenalidomide maintenance in post-transplant AML and high-risk MDS. |
| [37288607](https://pubmed.ncbi.nlm.nih.gov/37288607/) | 2023 | Review | American Journal of Hematology | 2023 update on MDS diagnosis, risk-stratification, and management, including progression to AML. |

---

## New Zealand Market Information

Currently not marketed in New Zealand — no authorization records found (`total_licenses: 0`, `licenses: []`).

---

## Cytotoxicity

Lenalidomide is classified here as an antineoplastic agent (hematologic malignancy indications per literature evidence: multiple myeloma, MDS, and the predicted myeloid leukemia indication).

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy — immunomodulatory imide drug (IMiD); non-classical cytotoxic, cereblon (CRBN)-mediated mechanism |
| Myelosuppression Risk | Please refer to the package insert warnings and precautions (neutropenia and thrombocytopenia are frequently reported adverse events across the cited combination trials, but no quantified toxicity data is available in this evidence pack) |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | Please refer to the package insert warnings and precautions |
| Handling Protection | Please refer to the package insert warnings and precautions |

---

## Safety Considerations

Please refer to the package insert for safety information. (`key_warnings`, `contraindications`, and DDI query all returned no data in this evidence pack — DDI query status: `not_found`.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- A **Blocking**-severity data gap (DG001: TFDA/Medsafe package insert — warnings and contraindications) prevents entry into S1 safety evaluation, and the drug is not currently marketed in New Zealand (0 authorizations). While the myeloid leukemia signal is biologically plausible and supported by L2-level evidence (multiple completed Phase 1/2 trials, one Phase 3 AML-maintenance trial of unknown status), safety data is entirely absent, so the candidate cannot advance past the research-question stage.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — resolves the blocking gap (DG001)
- Confirmed original approved indication and mechanism of action (DrugBank API query) — resolves DG002
- Drug-drug interaction (DDI) data, currently `not_found`
- Follow-up on NCT04490707 (Phase 3, AML maintenance) completion status, currently `UNKNOWN`
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

