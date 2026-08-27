---
layout: default
title: Ribociclib
parent: 僅模型預測 (L5)
nav_order: 302
evidence_level: L5
indication_count: 4
---

# Ribociclib
{: .fs-9 }

證據等級: **L5** | 預測適應症: **4** 個
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

# Ribociclib: From HR+/HER2- Breast Cancer to Myeloid Leukemia

## One-Sentence Summary

Ribociclib is a CDK4/6 inhibitor used internationally (as Kisqali) for HR+/HER2- advanced or metastatic breast cancer, though it is not currently registered in New Zealand.
The TxGNN model predicts it may be effective for **Myeloid Leukemia**, but this direction is currently supported only by **0 clinical trials** and **3 publications**, one of which reports the opposite association (AML arising after CDK4/6 inhibitor exposure).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | HR+/HER2- advanced/metastatic breast cancer (per literature in this evidence pack; not NZ-registered) |
| Predicted New Indication | Myeloid Leukemia |
| TxGNN Prediction Score | 99.35% |
| Evidence Level | L4 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Official mechanism-of-action documentation for ribociclib is currently a data gap (DrugBank query pending, item DG002). Based on the literature within this evidence pack, ribociclib is consistently described as an oral, highly selective CDK4/6 (cyclin-dependent kinase 4/6) inhibitor that blocks cell-cycle progression via the cyclin D–CDK4/6–Rb pathway, and it is used clinically for HR+/HER2- breast cancer.

The rationale for extending this mechanism to myeloid leukemia is that the cyclin D–CDK4/6–Rb axis is also over-activated in a subset of AML blasts; one in vitro study (PMID 32560251) reports that CDK4/6 inhibitors can overcome pharmacokinetic drug resistance in AML cells, suggesting a theoretical basis for antileukemic activity.

However, this mechanistic hypothesis is directly contradicted by a second source in the same evidence set: a case report (PMID 30575100) describing a patient who **developed** AML with eosinophilia after CDK4/6 inhibitor treatment, consistent with the known myelosuppressive/marrow-toxicity profile of this drug class rather than a therapeutic effect. A third publication (PMID 41641105) is a case report of vulvar/breast adenocarcinoma that is largely unrelated to leukemia treatment. With no clinical trials testing ribociclib for myeloid leukemia, the evidence direction is currently ambiguous rather than supportive.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [32560251](https://pubmed.ncbi.nlm.nih.gov/32560251/) | 2020 | Preclinical/In vitro | Cancers | CDK4/6 inhibitors may overcome pharmacokinetic drug resistance (ABCB1/ABCG2-mediated) in AML cells in vitro |
| [30575100](https://pubmed.ncbi.nlm.nih.gov/30575100/) | 2019 | Case Report (adverse event) | American Journal of Hematology | AML with eosinophilia arose after CDK4/6 inhibitor treatment in a patient with underlying clonal hematopoiesis — signal of marrow toxicity, not treatment benefit |
| [41641105](https://pubmed.ncbi.nlm.nih.gov/41641105/) | 2026 | Case Report (largely unrelated) | Frontiers in Oncology | Case of vulvar adenocarcinoma with concomitant breast cancer; not directly relevant to myeloid leukemia treatment |

---

## New Zealand Market Information

Ribociclib is not currently marketed in New Zealand — no product license or authorization records are available (0 licenses on file).

---

## Cytotoxicity

Ribociclib is an antineoplastic agent (CDK4/6 inhibitor used in breast cancer treatment), so this section applies.

| Item | Content |
|------|------|
| Cytotoxicity Classification | Targeted therapy (CDK4/6 inhibitor — not a conventional cytotoxic chemotherapeutic) |
| Myelosuppression Risk | High — multiple pharmacovigilance and meta-analysis sources in this evidence set (e.g., hematological toxicity network meta-analysis, FAERS-based comparative studies, systematic review of CDK4/6 inhibitor hematological adverse events) consistently identify neutropenia, thrombocytopenia, and leukopenia as common, often dose-limiting toxicities of this drug class |
| Emetogenicity Classification | Not established from current evidence pack — refer to official package insert once obtained |
| Monitoring Items | CBC with differential (baseline and periodic, particularly neutrophils and platelets); liver function tests; ECG/QT monitoring should be confirmed against the label, as QT prolongation is a recognized class concern for CDK4/6 inhibitors |
| Handling Protection | Oral small-molecule targeted agent; institutional hazardous-oral-oncolytic handling precautions are recommended pending confirmation from the official package insert (currently a Blocking data gap, see below) |

---

## Safety Considerations

Official TFDA/NZ package insert data (key warnings, contraindications) and drug-drug interaction data are not yet available (Blocking data gap DG001) — please refer to the package insert for safety information once obtained.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for the myeloid leukemia hypothesis is preclinical-only (L4), with no clinical trials and a directly conflicting adverse-event signal (CDK4/6 inhibitor–associated AML). Combined with the drug's unregistered status in New Zealand and a Blocking safety data gap (no TFDA/NZ package insert yet obtained), the candidate cannot proceed past initial screening. Note also that other TxGNN-ranked candidates for this drug (thrombocytopenia, marcothrombocytopenia, hereditary thrombocytopenia) appear to reflect ribociclib's known myelosuppressive adverse-effect profile rather than genuine treatment opportunities — this raises a general caution about interpreting raw TxGNN scores for this drug without evidence triage.

**To proceed, the following is needed:**
- Official TFDA/NZ package insert (warnings, contraindications) — Blocking gap DG001
- Verified mechanism-of-action documentation from DrugBank — High-priority gap DG002
- A dedicated preclinical/translational study to resolve the conflicting AML signal (antileukemic activity vs. drug-induced AML risk) before any clinical trial is considered
- Re-triage of the thrombocytopenia-related candidates to confirm they represent ADR signals rather than repurposing opportunities, before further evaluation resources are committed
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

