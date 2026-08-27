---
layout: default
title: Pegaspargase
parent: 僅模型預測 (L5)
nav_order: 269
evidence_level: L5
indication_count: 10
---

# Pegaspargase
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

# Pegaspargase: From Acute Lymphoblastic Leukemia to Precursor Lymphoblastic Lymphoma/Leukemia

## One-Sentence Summary

Pegaspargase is a pegylated asparaginase enzyme historically used as a core chemotherapy component for acute lymphoblastic leukemia (ALL) and lymphoblastic lymphoma (LBL). The TxGNN model's top prediction — **Precursor Lymphoblastic Lymphoma/Leukemia** — is essentially the same disease entity as its long-established approved use, supported by **50 clinical trials** and **20 publications**, so this is best read as a confirmation of known efficacy rather than a genuine repurposing signal.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Acute lymphoblastic leukemia (ALL) / lymphoblastic lymphoma (LBL) — well-established approved use; this evidence pack's regulatory license data is empty, so the field cannot be sourced from `taiwan_regulatory` (see note below) |
| Predicted New Indication | Precursor Lymphoblastic Lymphoma/Leukemia |
| TxGNN Prediction Score | 99.96% |
| Evidence Level | L1 |
| New Zealand Market Status | Not marketed (per evidence pack — flagged below as a likely data gap rather than a true market absence) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

> **Data quality note:** The evidence pack itself flags an inconsistency — `original_indications` is empty and `market_status` reads "not marketed," yet pegaspargase is a globally approved, guideline-standard ALL/LBL agent (FDA and multiple national approvals). This is almost certainly a source-database gap rather than a true regulatory status, and should be verified against the primary regulatory registry before this candidate is used for any market-entry decision.

---

## Why is This Prediction Reasonable?

Mechanism-of-action data was not returned by DrugBank in this evidence pack, but the underlying pharmacology is well characterized in the accompanying trial and literature evidence: pegaspargase is a PEGylated form of L-asparaginase that depletes circulating asparagine. Lymphoblasts in precursor B- and T-cell ALL/LBL lack sufficient asparagine synthetase to produce their own asparagine, so systemic depletion selectively starves malignant lymphoblasts of an amino acid essential for protein synthesis, driving apoptosis. This mechanism is fully established, not hypothetical.

Because "Precursor Lymphoblastic Lymphoma/Leukemia" and pegaspargase's known original indication describe the same clinical entity, the predicted "new" indication is not a novel repurposing hypothesis — it is the model correctly recovering the drug's existing, guideline-standard use. The large evidence base (including a Phase 3 COG trial enrolling 5,377 patients, NCT00103285) reflects decades of accumulated confirmatory research rather than emerging off-label signal. This should be treated as a validation case for the model's calibration, not a candidate requiring new-indication development.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00866307](https://clinicaltrials.gov/study/NCT00866307) | Phase 1 | Completed | 104 | Intensified PEG-asparaginase pilot study evaluating side effects when combined with standard chemotherapy in newly diagnosed high-risk ALL |
| [NCT00103285](https://clinicaltrials.gov/study/NCT00103285) | Phase 3 | Completed | 5377 | Large COG trial comparing combination chemotherapy regimens with pegaspargase as a backbone agent in standard-risk B-precursor ALL |
| [NCT00022737](https://clinicaltrials.gov/study/NCT00022737) | Phase 3 | Completed | 220 | COG pilot study for very high-risk ALL/adolescents; PEG-asparaginase is a core regimen component |
| [NCT01094392](https://clinicaltrials.gov/study/NCT01094392) | N/A | Completed | 65 | Direct study of pegaspargase's effect on coagulation parameters during prolonged treatment in childhood/adolescent ALL |
| [NCT02396043](https://clinicaltrials.gov/study/NCT02396043) | Phase 2 | Unknown | 50 | Modified BFM-95 regimen (asparaginase-containing) for newly diagnosed adult T-lymphoblastic lymphoma |
| [NCT05292664](https://clinicaltrials.gov/study/NCT05292664) | Phase 1 | Recruiting | 30 | Venetoclax plus chemotherapy including calaspargase pegol (PEG-asparaginase analog) in pediatric/young adult high-risk hematologic malignancies (ALL/LBL) |
| [NCT07072585](https://clinicaltrials.gov/study/NCT07072585) | Phase 2/3 | Not yet recruiting | 1708 | Daratumumab added to a modified augmented BFM backbone (asparaginase-containing) in newly diagnosed T-ALL/T-LL |
| [NCT03643276](https://clinicaltrials.gov/study/NCT03643276) | Phase 3 | Recruiting | 5000 | AIEOP-BFM ALL 2017 international collaborative treatment protocol for children/adolescents with ALL |
| [NCT05602194](https://clinicaltrials.gov/study/NCT05602194) | Phase 3 | Recruiting | 440 | Levocarnitine prophylaxis to prevent asparaginase-associated hepatotoxicity in adolescents/young adults receiving ALL therapy |
| [NCT06195735](https://clinicaltrials.gov/study/NCT06195735) | N/A | Completed | 649 | Forecasting hypersensitivity to PEG-asparaginase to optimize treatment outcomes in ALL |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [35271306](https://pubmed.ncbi.nlm.nih.gov/35271306/) | 2022 | RCT | J Clin Oncol | COG AALL1231 Phase III trial testing bortezomib in newly diagnosed T-ALL/T-LL, within an asparaginase-containing backbone |
| [40109190](https://pubmed.ncbi.nlm.nih.gov/40109190/) | 2025 | Review | Haematologica | Expert panel consensus on recognition, prevention and management of asparaginase/pegaspargase-associated adverse events in adult ALL/LBL |
| [17696798](https://pubmed.ncbi.nlm.nih.gov/17696798/) | 2007 | Review | Expert Opin Pharmacother | Overview of PEG-asparaginase pharmacology, efficacy and hypersensitivity-limited use in acute leukemias |
| [34228505](https://pubmed.ncbi.nlm.nih.gov/34228505/) | 2021 | Cohort | J Clin Oncol | DFCI 11-001: efficacy and toxicity comparison of pegaspargase vs. calaspargase pegol in childhood ALL |
| [37276451](https://pubmed.ncbi.nlm.nih.gov/37276451/) | 2023 | Cohort | Blood Advances | GIMEMA LAL1913: pegaspargase-modified risk-oriented program for adult ALL/LL |
| [39322712](https://pubmed.ncbi.nlm.nih.gov/39322712/) | 2024 | Phase 2 follow-up | Leukemia | Long-term follow-up of venetoclax added to hyper-CVAD, nelarabine and pegylated asparaginase in T-ALL/LBL |
| [31571395](https://pubmed.ncbi.nlm.nih.gov/31571395/) | 2020 | Cohort | Pediatr Blood Cancer | Rapid desensitization protocol allowing continued pegaspargase use in children with hypersensitivity |
| [38613330](https://pubmed.ncbi.nlm.nih.gov/38613330/) | 2025 | Retrospective | J Oncol Pharm Pract | Toxicity and dose-capping strategy review for pegaspargase in ALL/lymphoma and T-cell lymphoma |
| [40163215](https://pubmed.ncbi.nlm.nih.gov/40163215/) | 2025 | Phase 2 | Int J Hematol | Multicenter Japanese study of efficacy, safety and pharmacokinetics of lyophilized pegaspargase in untreated ALL |
| [34528411](https://pubmed.ncbi.nlm.nih.gov/34528411/) | 2021 | Cohort | Cancer Medicine | Levocarnitine as a strategy for pegaspargase-induced hepatotoxicity in older children/young adults with ALL |

---

## New Zealand Market Information

No authorizations are on record in this evidence pack (`total_licenses = 0`, `licenses = []`). Given the extensive global clinical use documented above, this most likely reflects incomplete data collection rather than genuine non-availability — recommend re-querying the regulatory source before treating this as a market-access gap.

---

## Cytotoxicity

Pegaspargase is a cytotoxic antineoplastic agent (core chemotherapy component of ALL/LBL regimens; DrugBank-classified antineoplastic enzyme therapy), so this section applies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic — enzyme-based agent (asparagine-depleting biologic), used as a core component of multi-agent ALL/LBL chemotherapy backbones |
| Myelosuppression Risk | Low-to-moderate as a single agent (asparaginase itself is not primarily marrow-suppressive); however it is almost always given within multi-agent regimens (vincristine, anthracyclines, corticosteroids) where combined myelosuppression is clinically significant |
| Emetogenicity Classification | Low as monotherapy; combination-regimen emetogenicity depends on co-administered agents |
| Monitoring Items | Hepatic function (transaminases, bilirubin — hepatotoxicity reported in multiple cohorts), pancreatic enzymes/lipase (pancreatitis risk), coagulation parameters (fibrinogen, antithrombin — thrombosis/bleeding risk), triglycerides and glucose (hypertriglyceridemia and hyperglycemia reported), and hypersensitivity monitoring (anti-drug antibodies, infusion reactions) |
| Handling Protection | Requires standard hazardous/cytotoxic parenteral antineoplastic drug handling precautions per institutional protocol |

---

## Safety Considerations

Please refer to the package insert for safety information. This evidence pack's structured safety fields (key warnings, contraindications, drug-drug interactions) returned no data — this corresponds to data gap **DG001** (TFDA/regulatory package insert not yet retrieved), which is flagged as **Blocking** for safety pre-assessment and must be resolved before this candidate proceeds further.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
The scored evidence level (L1, based on a completed Phase 3 RCT with >5,000 patients plus extensive supporting literature) is strong, but it supports an already-established indication rather than a novel repurposing opportunity — the guardrails here are about correcting the data pipeline, not de-risking a new clinical hypothesis.

**To proceed, the following is needed:**
- Resolve DG001 (Blocking): retrieve the actual TFDA/regulatory package insert for safety warnings and contraindications
- Resolve DG002 (High): obtain confirmed mechanism-of-action data from DrugBank rather than relying on inferred literature context
- Reconcile the `original_indications = []` / `market_status = "未上市"` inconsistency against known regulatory approvals before using this record for market-status decisions
- If the goal is genuine repurposing discovery, deprioritize rank-1 (a known indication) and instead investigate lower-ranked, evidence-backed signals in this same evidence pack — e.g., rank 8 ("Hodgkins lymphoma"), where the underlying trials/literature actually concentrate on extranodal NK/T-cell lymphoma (NKTCL), suggesting a possible disease-label mismatch worth verifying against the knowledge graph's disease ontology
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

