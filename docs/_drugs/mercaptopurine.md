---
layout: default
title: Mercaptopurine
parent: 僅模型預測 (L5)
nav_order: 217
evidence_level: L5
indication_count: 10
---

# Mercaptopurine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **10** 個
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

# Mercaptopurine: From Acute Lymphoblastic Leukemia to Myeloid Leukemia

## One-Sentence Summary

Mercaptopurine (6-MP) is a purine antimetabolite whose established use — evident throughout the accompanying literature and trial evidence in this pack — is as the backbone of acute lymphoblastic leukemia (ALL) maintenance therapy. The TxGNN model's top-ranked prediction is **Myeloid Leukemia**, with **29 clinical trials** and **20 publications** currently supporting this direction, though closer review shows this largely reflects mercaptopurine's already-documented historical role in acute promyelocytic leukemia (APL) maintenance rather than a wholly novel mechanism.

> **Note on original indication:** The structured regulatory field for original indication was not populated in this evidence pack (data gap). The "Acute Lymphoblastic Leukemia" reference above is derived from repeated context within the supplied clinical trial and literature evidence (e.g., mercaptopurine + methotrexate maintenance regimens), not from an external source.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in structured regulatory data; extensively referenced in evidence as Acute Lymphoblastic Leukemia (ALL) maintenance therapy |
| Predicted New Indication | Myeloid Leukemia |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available (data gap). Based on known information, mercaptopurine is a thiopurine/purine antimetabolite; its efficacy as part of maintenance chemotherapy for acute lymphoblastic leukemia has been well established in the literature included in this pack, and mechanistically the same antiproliferative pathway — conversion to thioguanine nucleotides, incorporation into DNA/RNA, and inhibition of de novo purine synthesis — is not lineage-specific, making applicability to myeloid malignancies biologically plausible.

Importantly, the supporting evidence indicates this is less a "new" hypothesis than a validated extension of existing practice: multiple Phase 3/Phase 4 trials (e.g., AIDA, AIDA2000, PETHEMA LPA2005) already incorporate 6-mercaptopurine, alongside ATRA and methotrexate, as **standard post-remission maintenance therapy for acute promyelocytic leukemia (APL)**, a recognized subtype of acute myeloid leukemia. The repurposing rationale attached to this prediction explicitly frames it as a "standard/historical regimen" for AML (particularly APL) maintenance rather than a novel repurposing hypothesis. This strengthens confidence in the signal but also means the primary value of pursuing this indication is regulatory/label formalization rather than discovery of new biological activity.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00492856](https://clinicaltrials.gov/study/NCT00492856) | Phase 3 | Completed | 105 | S0521 RCT directly comparing 6-MP + methotrexate maintenance vs. observation in low/intermediate-risk APL |
| [NCT00136084](https://clinicaltrials.gov/study/NCT00136084) | Phase 3 | Completed | 238 | Collaborative trial for newly diagnosed AML/MDS comparing two multi-agent chemotherapy regimens |
| [NCT06199557](https://clinicaltrials.gov/study/NCT06199557) | Phase 1/2 | Recruiting | 48 | Hydroxyurea + valproic acid, or 6-MP + valproic acid, in AML/high-risk MDS patients unfit for standard therapy |
| [NCT00003934](https://clinicaltrials.gov/study/NCT00003934) | Phase 3 | Completed | 420 | Tretinoin ± arsenic trioxide consolidation followed by maintenance with intermittent tretinoin plus mercaptopurine/methotrexate in untreated APL |
| [NCT00180128](https://clinicaltrials.gov/study/NCT00180128) | Phase 4 | Unknown | 80 | AIDA2000 risk-adapted APL therapy; maintenance includes 2-year course of 6-MP, methotrexate, and ATRA |
| [NCT00465933](https://clinicaltrials.gov/study/NCT00465933) | Phase 4 | Completed | N/A | AIDA induction with risk-adapted consolidation; ATRA + methotrexate + mercaptopurine salvage for relapse |
| [NCT00408278](https://clinicaltrials.gov/study/NCT00408278) | Phase 4 | Completed | 300 | PETHEMA LPA2005: risk-adapted APL therapy with maintenance of ATRA plus low-dose methotrexate/mercaptopurine |
| [NCT00482833](https://clinicaltrials.gov/study/NCT00482833) | Phase 3 | Completed | 276 | Arsenic trioxide + ATRA vs. standard ATRA/anthracycline (AIDA) in newly diagnosed non-high-risk APL |
| [NCT02688140](https://clinicaltrials.gov/study/NCT02688140) | Phase 3 | Completed | 135 | Arsenic trioxide + ATRA + idarubicin vs. AIDA regimen in newly diagnosed high-risk APL |
| [NCT00962767](https://clinicaltrials.gov/study/NCT00962767) | Phase 3 | Completed | 168 | Gemtuzumab ozogamicin vs. 2-year ATRA + chemotherapy maintenance in intermediate/high-risk APL |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [10497848](https://pubmed.ncbi.nlm.nih.gov/10497848/) | 1999 | RCT | International Journal of Hematology | JALSG-AML92: adding etoposide to daunorubicin/cytarabine/6-mercaptopurine induction showed no additional benefit in adult AML |
| [8174198](https://pubmed.ncbi.nlm.nih.gov/8174198/) | 1994 | RCT | Cancer Chemotherapy and Pharmacology | Nationwide randomized comparison of daunorubicin vs. aclarubicin combined with cytarabine, 6-mercaptopurine, and prednisolone in untreated AML |
| [26425037](https://pubmed.ncbi.nlm.nih.gov/26425037/) | 2015 | Cohort | Journal of Korean Medical Science | Oral maintenance with 6-MP and methotrexate in transplant-ineligible AML patients after first complete remission |
| [9095207](https://pubmed.ncbi.nlm.nih.gov/9095207/) | 1997 | Cohort | Cancer Investigation | High-dose continuous 6-MP followed by intermediate-dose cytarabine as first-remission consolidation in pediatric AML |
| [1793832](https://pubmed.ncbi.nlm.nih.gov/1793832/) | 1991 | — | International Journal of Hematology | Intensive individualized induction with behenoyl cytarabine, daunorubicin, and 6-MP in adult AML (71% CR rate) |
| [1657335](https://pubmed.ncbi.nlm.nih.gov/1657335/) | 1991 | — | Chinese Medical Journal | Cytarabine + daunorubicin + 6-mercaptopurine combination induction/consolidation in adult AML |
| [1059498](https://pubmed.ncbi.nlm.nih.gov/1059498/) | 1975 | — | Cancer | Cytarabine, daunorubicin, prednisolone, and mercaptopurine or thioguanine in childhood AML; 78% remission rate |
| [4518586](https://pubmed.ncbi.nlm.nih.gov/4518586/) | 1973 | — | Cancer | Cytosine arabinoside combined with 6-mercaptopurine in adult AML |
| [5220682](https://pubmed.ncbi.nlm.nih.gov/5220682/) | 1966 | Case Series | Minnesota Medicine | Early case series of AML treated with 6-mercaptopurine and cyclophosphamide |
| [13930127](https://pubmed.ncbi.nlm.nih.gov/13930127/) | 1963 | — | Blood | Comparison of remission patterns in acute myelocytic leukemia with methyl-glyoxal-bis-guanylhydrazone vs. 6-mercaptopurine |

---

## New Zealand Market Information

Mercaptopurine is currently **not marketed** in New Zealand, with no authorization records available in this evidence pack.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (purine antimetabolite / thiopurine class) |
| Myelosuppression Risk | High — the literature in this pack consistently identifies neutropenia/leukopenia as the primary dose-limiting toxicity, strongly modulated by TPMT and NUDT15 genetic polymorphisms |
| Emetogenicity Classification | Please refer to the package insert warnings and precautions |
| Monitoring Items | CBC with differential, liver function tests, and TPMT/NUDT15 genotype or phenotype testing before and during therapy |
| Handling Protection | Standard cytotoxic drug handling precautions required |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Multiple Phase 3/4 trials and consistent literature confirm 6-mercaptopurine's established role in APL/AML maintenance regimens, giving this prediction L1-level evidence. However, the drug is not currently marketed in New Zealand, and this appears to be a formalization of existing off-label/protocol use rather than a genuinely novel mechanism — guardrails should focus on regulatory pathway and safety documentation rather than efficacy uncertainty.

**To proceed, the following is needed:**
- TFDA package insert warnings/contraindications (currently a Blocking data gap — required before any S1 safety assessment)
- Detailed mechanism of action data from DrugBank (currently a High-severity data gap)
- A New Zealand market authorization pathway assessment, since the drug has zero existing licenses in-market
- TPMT/NUDT15 pharmacogenomic testing protocol as part of any monitoring plan
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

