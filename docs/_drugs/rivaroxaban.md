---
layout: default
title: Rivaroxaban
parent: 僅模型預測 (L5)
nav_order: 310
evidence_level: L5
indication_count: 4
---

# Rivaroxaban
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

# Rivaroxaban: From Anticoagulation (VTE/Atrial Fibrillation) to Rheumatoid Arthritis

## One-Sentence Summary

> Rivaroxaban is a direct oral Factor Xa inhibitor used clinically in the treatment/prevention of venous thromboembolism and stroke prevention in non-valvular atrial fibrillation (based on evidence-pack trial/literature context; no official New Zealand label is available).
> The TxGNN model predicts it may be effective for **Rheumatoid Arthritis**,
> with **0 clinical trials** and **4 tangential publications** currently identified — none of which demonstrate a disease-modifying effect in RA.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available from NZ licensing data (drug not marketed); trial/literature context indicates anticoagulant use for VTE and non-valvular atrial fibrillation |
| Predicted New Indication | Rheumatoid Arthritis |
| TxGNN Prediction Score | 99.57% |
| Evidence Level | L5 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Formal mechanism-of-action data for rivaroxaban is flagged as a data gap in this evidence pack (DG002, High severity). Based on the available context, rivaroxaban is a direct oral Factor Xa inhibitor (anticoagulant), used in the settings referenced by the supporting trials and literature — venous thromboembolism (DVT/PE) and stroke prevention in non-valvular atrial fibrillation.

The proposed link to rheumatoid arthritis (RA) is indirect: chronic inflammation in RA activates the coagulation cascade and raises VTE risk, so anticoagulation could theoretically reduce thrombotic complications in RA patients. However, there is no evidence that Factor Xa inhibition modifies RA's underlying inflammatory or joint-destructive pathways.

The high TxGNN score most likely reflects network proximity between coagulation and inflammation pathways in the knowledge graph, rather than a demonstrated causal treatment mechanism — a caveat explicitly noted in the evidence pack's own rationale annotation for this prediction.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34175144](https://pubmed.ncbi.nlm.nih.gov/34175144/) | 2021 | Cohort/Lab study (thrombin generation assay) | La Revue de médecine interne | Thrombin generation assay used to evaluate hypercoagulability in autoimmune disease (e.g., antiphospholipid syndrome); not specific to rivaroxaban or RA treatment |
| [29621248](https://pubmed.ncbi.nlm.nih.gov/29621248/) | 2018 | Cohort (adherence study) | PLoS ONE | Real-world adherence comparison of rivaroxaban vs. apixaban in non-valvular atrial fibrillation; unrelated to RA |
| [33141212](https://pubmed.ncbi.nlm.nih.gov/33141212/) | 2020 | Review | JAMA | General review of DVT/PE diagnosis and treatment; background VTE epidemiology, not RA-specific |
| [41918541](https://pubmed.ncbi.nlm.nih.gov/41918541/) | 2026 | Case Report | Cureus | Case of thromboembolic cerebral infarction in an 88-year-old woman with RA (on oral steroids) despite anticoagulation for atrial fibrillation — illustrates comorbidity, not therapeutic evidence |

*Note: relevance grading for these publications is marked "pending" in the evidence pack; none directly assess rivaroxaban's efficacy in RA.*

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Official NZ warnings and contraindications data are marked as a Blocking data gap — DG001 — pending retrieval from the regulatory source.)*

---

## Other Candidate Indications (Not Yet Prioritized)

This evidence pack also flagged three additional low-confidence predictions for rivaroxaban:

| Disease | TxGNN Score | Evidence Level | Recommendation | Note |
|---|---|---|---|---|
| Gout | 99.51% | L5 | Hold | Only literature hit concerns CYP450 drug interactions of benzbromarone, an unrelated uricosuric — likely a search mismatch, no mechanistic link to Factor Xa inhibition |
| HIV infectious disease | 99.17% | L4 | Hold | All trials/literature address anticoagulation safety and drug-drug interactions (ritonavir/cobicistat CYP3A4/P-gp) in HIV patients already on anticoagulants — a safety topic, not antiviral efficacy evidence |
| Brachydactyly-syndactyly syndrome | 99.10% | L5 | Hold | Rare genetic limb malformation syndrome; no biological plausibility, no trials or literature |

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The TxGNN score is high, but no clinical trials and no disease-specific literature support rivaroxaban's efficacy in rheumatoid arthritis; existing literature reflects coagulation–inflammation network proximity and comorbidity, not a causal treatment mechanism. The drug is also not currently marketed in New Zealand and lacks official label safety data (Blocking gap DG001), which prevents progression to the S1 safety review stage.

**To proceed, the following is needed:**
- Official NZ Medsafe-equivalent package insert covering warnings and contraindications (DG001, Blocking)
- Detailed mechanism-of-action data from DrugBank (DG002, High)
- Preclinical or mechanistic studies testing Factor Xa inhibition specifically against RA-associated synovial inflammation/coagulation pathways
- Re-evaluation of the other three candidate indications (gout, HIV, brachydactyly-syndactyly) if new trial or literature evidence emerges — none currently support a treatment link
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

