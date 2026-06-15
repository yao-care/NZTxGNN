---
layout: default
title: Anastrozole
parent: 僅模型預測 (L5)
nav_order: 30
evidence_level: L5
indication_count: 6
---

# Anastrozole
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

# Anastrozole: From Third-Generation Aromatase Inhibitor to Female Breast Carcinoma

## One-Sentence Summary

Anastrozole is a potent, selective third-generation non-steroidal aromatase inhibitor that suppresses oestrogen biosynthesis by more than 85% in postmenopausal women, and is globally established as a standard-of-care adjuvant endocrine therapy for hormone receptor-positive (HR+) breast cancer — though it currently holds no Medsafe authorisation in New Zealand.
The TxGNN model predicts it may be highly effective for **Female Breast Carcinoma** with a confidence score of **99.68%**, representing a strong validation of its established clinical profile.
This prediction is supported by **50 registered clinical trials** and **20 published studies**, including multiple landmark Phase 3 RCTs enrolling tens of thousands of patients.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Not approved in New Zealand (globally used for HR+ postmenopausal breast cancer) |
| Predicted New Indication | Female Breast Carcinoma |
| TxGNN Prediction Score | 99.68% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorisations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Anastrozole selectively and competitively inhibits the aromatase enzyme complex (CYP19A1), blocking the conversion of androgens (androstenedione, testosterone) to oestrogens (oestradiol, oestrone) in peripheral tissues. In postmenopausal women, the ovaries no longer produce meaningful oestrogen; instead, peripheral aromatisation in adipose tissue, muscle, and the tumour microenvironment becomes the dominant source. By suppressing circulating oestradiol by more than 85%, anastrozole severs the oestrogen-driven proliferative signalling cascade that sustains ER-positive breast cancer cells — specifically the ERα → cyclin D1 → CDK4/6 → Rb phosphorylation axis that drives cell-cycle progression.

The connection between anastrozole's mechanism and female breast carcinoma is direct: approximately 75% of breast cancers express oestrogen receptor, and oestrogen deprivation through aromatase inhibition is the foundational strategy in their management. The landmark ATAC trial (n = 9,366) demonstrated that anastrozole significantly prolonged disease-free survival compared with tamoxifen as adjuvant therapy, establishing it as a preferred first-line agent in postmenopausal HR+ disease. Large prevention trials (IBIS-II) further confirmed a durable reduction in breast cancer incidence in high-risk postmenopausal women treated with anastrozole over 5 years, with benefit extending well beyond treatment completion.

The TxGNN score of 99.68% (ranked #3,280 globally across all possible drug–disease pairs) reflects the model's recognition of the deep, multi-layered evidence network linking anastrozole to female breast carcinoma in the biomedical knowledge graph. Mechanistically, newer research has also identified that anastrozole — uniquely among the third-generation aromatase inhibitors — acts as a ligand for oestrogen receptor α and regulates fatty acid synthase (FASN), suggesting additional tumour-suppressive mechanisms beyond simple oestrogen deprivation.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT00849030](https://clinicaltrials.gov/study/NCT00849030) | Phase 3 | Completed | 9,358 | ATAC trial: Anastrozole (Arimidex) alone vs tamoxifen alone vs combination as 5-year adjuvant treatment in postmenopausal breast cancer; anastrozole demonstrated superior disease-free survival and recurrence reduction vs tamoxifen |
| [NCT00053898](https://clinicaltrials.gov/study/NCT00053898) | Phase 3 | Completed | 3,104 | Anastrozole vs tamoxifen in postmenopausal women with hormone receptor-positive DCIS undergoing lumpectomy with radiation; evaluated relative efficacy in preventing invasive recurrence |
| [NCT04359420](https://clinicaltrials.gov/study/NCT04359420) | N/A | Completed | 32,298 | BC-Predict risk-stratified NHS Breast Screening Programme; anastrozole evaluated as chemoprevention agent in high-risk women, providing large-scale population-level prevention evidence |
| [NCT00301457](https://clinicaltrials.gov/study/NCT00301457) | Phase 3 | Completed | 1,914 | Randomised comparison of 3 vs 6 years of anastrozole after 2–3 years of tamoxifen as adjuvant therapy in postmenopausal hormone-sensitive breast cancer; assessed optimal treatment duration |
| [NCT04964934](https://clinicaltrials.gov/study/NCT04964934) | Phase 3 | Active, not recruiting | 315 | AZD9833 (next-generation oral SERD) + CDK4/6 inhibitor vs anastrozole/letrozole + CDK4/6 inhibitor in HR+/HER2− MBC with detectable ESR1 mutation; anastrozole serves as the active standard-of-care comparator arm |
| [NCT00635713](https://clinicaltrials.gov/study/NCT00635713) | Phase 3 | Completed | 588 | Fulvestrant 125 mg and 250 mg vs anastrozole 1 mg in postmenopausal women with advanced breast cancer progressing after prior endocrine therapy; assessed time to tumour progression |
| [NCT02441946](https://clinicaltrials.gov/study/NCT02441946) | Phase 2 | Completed | 224 | neoMONARCH: Abemaciclib + anastrozole vs abemaciclib monotherapy vs anastrozole monotherapy in neoadjuvant setting for postmenopausal HR+/HER2− breast cancer; demonstrated synergistic anti-proliferative effect |
| [NCT00784680](https://clinicaltrials.gov/study/NCT00784680) | Phase 3 | Completed | 308 | Quality of life comparison between anastrozole alone, tamoxifen alone, and combination in first 2 years of adjuvant treatment for postmenopausal breast cancer |
| [NCT01723774](https://clinicaltrials.gov/study/NCT01723774) | Phase 2 | Active, not recruiting | 84 | Palbociclib (CDK4/6 inhibitor) + anastrozole in neoadjuvant setting for Stage 2–3 ER+/HER2− breast cancer; evaluated pathological complete response rate compared to historical AI monotherapy controls |
| [NCT01626222](https://clinicaltrials.gov/study/NCT01626222) | Phase 3B | Completed | 301 | 4EVER: Everolimus + exemestane in postmenopausal HR+ breast cancer progressing on prior non-steroidal AI (including anastrozole); provides class-effect evidence for AI-failure sequencing strategies |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [15639680](https://pubmed.ncbi.nlm.nih.gov/15639680/) | 2005 | Phase 3 RCT (landmark) | *Lancet* | ATAC trial 5-year results: Anastrozole significantly prolonged disease-free survival (HR 0.87) vs tamoxifen at 68-month median follow-up in 9,366 postmenopausal ER+ breast cancer patients; established anastrozole as preferred adjuvant therapy |
| [31839281](https://pubmed.ncbi.nlm.nih.gov/31839281/) | 2020 | Phase 3 RCT (prevention) | *Lancet* | IBIS-II long-term follow-up: Anastrozole vs placebo in high-risk postmenopausal women; 49% reduction in breast cancer incidence with durable benefit extending 5+ years beyond treatment completion |
| [26686313](https://pubmed.ncbi.nlm.nih.gov/26686313/) | 2016 | Phase 3 RCT | *Lancet* | IBIS-II DCIS: Anastrozole vs tamoxifen in postmenopausal women with HR+ DCIS; anastrozole demonstrated comparable or superior locoregional and contralateral recurrence prevention with a different toxicity profile |
| [9024711](https://pubmed.ncbi.nlm.nih.gov/9024711/) | 1997 | Phase 3 RCT | *Cancer* | Anastrozole 1 mg and 10 mg vs megestrol acetate in 386 postmenopausal women with advanced breast carcinoma after tamoxifen progression; established anastrozole's non-inferior efficacy with improved tolerability |
| [28415634](https://pubmed.ncbi.nlm.nih.gov/28415634/) | 2017 | Meta-analysis of RCTs | *Oncotarget* | Systematic meta-analysis of anastrozole vs tamoxifen RCTs; anastrozole superior in disease-free survival with distinct toxicity profile (less thromboembolism/endometrial cancer; more bone and musculoskeletal effects) |
| [34667110](https://pubmed.ncbi.nlm.nih.gov/34667110/) | 2022 | Mechanistic study | *Molecular Cancer Therapeutics* | Anastrozole uniquely regulates fatty acid synthase (FASN) in breast cancer, a mechanism not shared by exemestane or letrozole; suggests additional tumour-suppressive pathways beyond oestrogen deprivation |
| [32701512](https://pubmed.ncbi.nlm.nih.gov/32701512/) | 2020 | Pharmacogenomics (GWAS) | *JCI Insight* | GWAS of MA.27 Phase 3 trial: CSMD1 SNP variant associated with improved breast cancer-free interval under anastrozole; mechanistically, CSMD1 regulates complement signalling — a novel secondary mechanism of anastrozole action |
| [34048027](https://pubmed.ncbi.nlm.nih.gov/34048027/) | 2021 | Pharmacogenomics study | *Clinical Pharmacology & Therapeutics* | SNP–treatment interaction analysis in 4,465 patients from MA.27; identified genetic factors differentiating efficacy of anastrozole vs exemestane, supporting personalised AI selection |
| [20923259](https://pubmed.ncbi.nlm.nih.gov/20923259/) | 2010 | Systematic Review | *Expert Opinion on Drug Safety* | Comprehensive review of anastrozole adjuvant trials confirming superior disease-free survival vs tamoxifen; detailed characterisation of musculoskeletal, bone density, and cardiovascular safety signals |
| [19445563](https://pubmed.ncbi.nlm.nih.gov/19445563/) | 2009 | Comparative Review | *Expert Opinion on Pharmacotherapy* | Head-to-head comparison of anastrozole, letrozole, and exemestane in early breast cancer across four adjuvant trial designs; all three third-generation AIs superior to tamoxifen with consistent class efficacy |

---

## New Zealand Market Information

Anastrozole currently holds no Medsafe authorisations in New Zealand. There are no registered product licences to display.

---

## Cytotoxicity

Anastrozole is an antineoplastic agent indicated for hormone receptor-positive breast cancer. As a targeted endocrine therapy, its safety profile differs substantially from conventional cytotoxic chemotherapy.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Targeted endocrine therapy — third-generation non-steroidal aromatase inhibitor (not conventional cytotoxic) |
| Myelosuppression Risk | Low — anastrozole does not suppress haematopoiesis; bone marrow toxicity is not an expected class effect |
| Emetogenicity Classification | Low — oral hormonal agent; nausea is uncommon and not a dose-limiting concern |
| Monitoring Items | Bone mineral density (baseline and annual DEXA scan), lipid profile, liver function tests (periodic), musculoskeletal symptom assessment (arthralgia/myalgia/grip strength) |
| Handling Protection | Standard oral medication precautions; dedicated cytotoxic handling protocols are not required for hormonal aromatase inhibitors |

---

## Safety Considerations

Please refer to the package insert for full safety information, as New Zealand-specific prescribing data is not yet available in this dataset.

Based on published clinical trial evidence, the following safety considerations are clinically relevant:

- **Musculoskeletal effects**: Arthralgia and myalgia are common (reported in a significant proportion of ATAC trial patients); Grade 2–3 symptoms may necessitate switching to an alternative aromatase inhibitor such as letrozole
- **Bone mineral density loss**: Anastrozole reduces BMD due to oestrogen suppression; baseline and periodic DEXA assessment is standard practice; co-prescription of bisphosphonates should be considered in patients with osteopaenia or high fracture risk
- **Cardiovascular considerations**: Long-term AI therapy may affect endothelial function in postmenopausal women; cardiovascular risk factor monitoring is advisable during extended treatment

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Anastrozole has overwhelming, highest-quality global evidence for efficacy and safety in hormone receptor-positive female breast carcinoma, underpinned by multiple completed Phase 3 RCTs (including the landmark ATAC trial with 9,366 patients and the IBIS-II prevention trial) and a well-characterised 25-year post-marketing pharmacovigilance record. The TxGNN confidence score of 99.68% reflects this strong evidence base, and the L1 evidence rating confirms that the threshold for market authorisation is already met on clinical grounds.

**To proceed, the following is needed:**
- Submit a Medsafe New Zealand market authorisation application, referencing existing FDA, EMA, or TGA approval dossiers for the originator product (Arimidex, AstraZeneca) or a validated generic equivalent
- Obtain and file the current approved global prescribing information to populate the New Zealand safety section (warnings, contraindications, complete drug interaction profile)
- Establish a bone health management protocol for local clinical practice, including DEXA monitoring intervals and criteria for co-prescribing bisphosphonates
- Define eligible patient population (postmenopausal status confirmation, ER/PR receptor testing requirements) aligned with New Zealand breast cancer treatment guidelines
- Assess whether a local pharmacovigilance plan is required by Medsafe for post-approval safety monitoring
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

