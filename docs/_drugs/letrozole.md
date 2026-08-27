---
layout: default
title: Letrozole
parent: 僅模型預測 (L5)
nav_order: 201
evidence_level: L5
indication_count: 10
---

# Letrozole
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

# Letrozole: From Postmenopausal ER+ Breast Cancer to Female Breast Carcinoma

## One-Sentence Summary

Letrozole is a third-generation aromatase inhibitor whose established, internationally approved use is hormone receptor-positive (ER+) breast cancer in postmenopausal women. The TxGNN model's top prediction for this drug is **female breast carcinoma** — effectively confirming its own existing indication rather than identifying a genuinely new one — and this is supported by **50 clinical trials** (including several landmark Phase 3 studies) and **20 publications**.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from New Zealand regulatory data (drug is not currently licensed there). Per the drug's internationally established use — referenced throughout the evidence pack's own literature and trial titles — letrozole is approved for postmenopausal, hormone receptor-positive (ER+) breast cancer. |
| Predicted New Indication | Female Breast Carcinoma |
| TxGNN Prediction Score | 99.98% |
| Evidence Level | L1 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed DrugBank-sourced mechanism-of-action data was not available for this evidence pack (flagged as a High-severity data gap). However, the literature captured in the evidence pack itself is consistent and unambiguous: letrozole is a third-generation, non-steroidal aromatase inhibitor that blocks peripheral conversion of androgens to estrogens, thereby depriving estrogen receptor-positive (ER+) tumor cells of the hormonal signal that drives their proliferation (PMID 17912633, "The discovery and mechanism of action of letrozole"; PMID 20095792).

Because this mechanism acts directly on the estrogen-dependence of ER+ breast tumors, the drug's *original* approved use and TxGNN's *predicted* indication are, in this case, the same underlying disease population — this is not really a "repurposing" candidate in the traditional sense. As the evidence pack's own scoring rationale states, this is "letrozole's original approved indication (postmenopausal ER+ breast cancer), not a repurposing but an already-established standard of care."

The practical value of this prediction is therefore not scientific novelty but **regulatory/market relevance**: letrozole is currently unlicensed in New Zealand (0 authorizations, "not marketed" status), so the TxGNN-flagged evidence base functions as a ready-made dossier supporting a first-time market-entry filing for an indication with an exceptionally mature global evidence base (landmark trials date back to 1998).

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00004205](https://clinicaltrials.gov/study/NCT00004205) | Phase 3 | Completed | 8,028 | Landmark BIG 1-98-type trial comparing letrozole vs. tamoxifen as adjuvant endocrine therapy for postmenopausal ER/PgR+ breast cancer. |
| [NCT01626222](https://clinicaltrials.gov/study/NCT01626222) | Phase 3 | Completed | 301 | Everolimus + exemestane in postmenopausal ER+ breast cancer progressing after non-steroidal aromatase inhibitor therapy; graded A relevance. |
| [NCT02296801](https://clinicaltrials.gov/study/NCT02296801) | Phase 2 (randomized) | Completed | 307 | Palbociclib + letrozole as neoadjuvant therapy in ER+/HER2- primary breast cancer; foundational study for the now-standard CDK4/6i + AI combination. |
| [NCT00171340](https://clinicaltrials.gov/study/NCT00171340) | Phase 3 | Completed | 1,065 | Upfront vs. delayed zoledronic acid to prevent bone loss in postmenopausal ER/PgR+ patients on adjuvant letrozole. |
| [NCT00171314](https://clinicaltrials.gov/study/NCT00171314) | Phase 3 | Completed | 527 | Companion trial evaluating zoledronic acid timing to prevent letrozole-associated bone loss. |
| [NCT01064635](https://clinicaltrials.gov/study/NCT01064635) | Phase 3 | Active, not recruiting | 2,056 | LEAD study comparing standard vs. extended-duration adjuvant letrozole in early postmenopausal breast cancer. |
| [NCT00382070](https://clinicaltrials.gov/study/NCT00382070) | Phase 3 | Unknown | 3,966 | Extended (5-year) letrozole vs. placebo after prior AI/tamoxifen sequence, assessing disease-free survival. |
| [NCT04964934](https://clinicaltrials.gov/study/NCT04964934) | Phase 3 | Active, not recruiting | 315 | Switching to next-generation oral SERD + CDK4/6i vs. continuing AI (letrozole/anastrozole) + CDK4/6i in ESR1-mutated HR+/HER2- metastatic disease. |
| [NCT03248427](https://clinicaltrials.gov/study/NCT03248427) | Phase 2 | Completed | 106 | CORALLEEN: neoadjuvant letrozole + ribociclib vs. chemotherapy in postmenopausal Luminal B/HER2- breast cancer. |
| [NCT00097344](https://clinicaltrials.gov/study/NCT00097344) | Phase 3 | Terminated | 842 | Atamestane + toremifene vs. letrozole in advanced breast cancer; large sample despite early termination. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16382061](https://pubmed.ncbi.nlm.nih.gov/16382061/) | 2005 | RCT | New England Journal of Medicine | BIG 1-98 trial: letrozole superior to tamoxifen as adjuvant treatment for postmenopausal hormone-receptor-positive early breast cancer. |
| [15001182](https://pubmed.ncbi.nlm.nih.gov/15001182/) | 2004 | RCT | Women's Health Issues | Clinical implications and remaining questions from the Letrozole Breast Cancer Trial. |
| [36243120](https://pubmed.ncbi.nlm.nih.gov/36243120/) | 2022 | Review | Life Sciences | Comprehensive review of letrozole pharmacology, toxicity, and therapeutic effects across HR+ breast cancer settings. |
| [35378469](https://pubmed.ncbi.nlm.nih.gov/35378469/) | 2022 | Cohort | Current Problems in Cancer | Predictive and prognostic factors of response to palbociclib + letrozole in HR+/HER2- advanced breast cancer. |
| [20095792](https://pubmed.ncbi.nlm.nih.gov/20095792/) | 2010 | Review | Expert Opinion on Drug Metabolism & Toxicology | Pharmacodynamic, pharmacokinetic, efficacy, and safety review of letrozole in breast cancer. |
| [17912633](https://pubmed.ncbi.nlm.nih.gov/17912633/) | 2007 | Review | Breast Cancer Research and Treatment | Discovery and mechanism of action of letrozole as an aromatase inhibitor. |
| [19445563](https://pubmed.ncbi.nlm.nih.gov/19445563/) | 2009 | Review | Expert Opinion on Pharmacotherapy | Comparative review of anastrozole, letrozole, and exemestane in early breast cancer management. |
| [22738819](https://pubmed.ncbi.nlm.nih.gov/22738819/) | 2012 | Systematic Review | Current Medical Research and Opinion | Lapatinib + letrozole vs. other first-line treatments in HR+/HER2+ advanced/metastatic breast cancer. |
| [34645649](https://pubmed.ncbi.nlm.nih.gov/34645649/) | 2022 | Study | Clinical Cancer Research | Biomarkers of response and resistance to palbociclib plus letrozole in ER+/HER2- breast cancer. |
| [41519129](https://pubmed.ncbi.nlm.nih.gov/41519129/) | 2026 | Study | Cell Reports Medicine | NeoPAL trial: molecular and cellular composition changes after neoadjuvant letrozole + palbociclib in early luminal breast cancer. |

---

## New Zealand Market Information

Letrozole currently has **zero registered authorizations** in New Zealand (`market_status: 未上市` / not marketed; `total_licenses: 0`). No license records are available to tabulate.

---

## Cytotoxicity

Letrozole is used to treat a malignant condition (breast carcinoma) and is classified under antineoplastic/endocrine therapy agents, so this section is included per protocol — though pharmacologically it is a targeted hormonal agent, not a conventional cytotoxic chemotherapy drug.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Endocrine (hormonal) therapy — non-cytotoxic aromatase inhibitor, not a conventional cytotoxic chemotherapeutic |
| Myelosuppression Risk | Low — aromatase inhibitors are not characteristically myelosuppressive; no specific hematologic toxicity data was returned in this evidence pack (DDI query: not found). Please refer to the package insert for confirmation. |
| Emetogenicity Classification | Low — consistent with the general emetogenic profile of hormonal/endocrine anticancer agents |
| Monitoring Items | Bone mineral density (long-term use is associated with bone loss — see zoledronic acid co-administration trials above), lipid profile, liver function |
| Handling Protection | Standard oral solid-dose handling; not classified as a hazardous cytotoxic drug requiring special handling precautions under typical chemotherapy-handling protocols — confirm against local institutional hazardous drug list |

---

## Safety Considerations

Please refer to the package insert for safety information. (No key warnings, contraindications, or drug-interaction data were returned for letrozole in this evidence pack — TFDA package insert retrieval is flagged as a Blocking data gap, DG001.)

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The clinical and literature evidence base for letrozole in ER+ breast cancer is exceptionally mature (L1, ≥2 completed Phase 3 RCTs including a landmark >8,000-patient trial), but this "prediction" essentially reconfirms letrozole's own long-established global indication rather than surfacing a novel therapeutic use. The genuine open question is not efficacy but New Zealand market status: the drug holds zero current authorizations there, so any path forward is a market-entry/regulatory exercise rather than a repurposing R&D question.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert (warnings, contraindications) — currently a Blocking data gap (DG001)
- Confirmed mechanism-of-action documentation from DrugBank — currently a High-severity data gap (DG002)
- Formal New Zealand regulatory filing pathway assessment, given the drug's current unlicensed status
- Drug-drug interaction profile (current DDI query returned no results)
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

