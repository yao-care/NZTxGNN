---
layout: default
title: Clofazimine
parent: 僅模型預測 (L5)
nav_order: 80
evidence_level: L5
indication_count: 3
---

# Clofazimine
{: .fs-9 }

證據等級: **L5** | 預測適應症: **3** 個
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

The `txgnn-pipeline` skill is focused on pipeline management and training — it doesn't apply to this report-writing task. The system prompt already provides the full report format. Proceeding directly with the report.

---

# Clofazimine: From Leprosy to Pneumocystosis

## One-Sentence Summary

Clofazimine is an antimycobacterial riminophenazine dye, used primarily as part of WHO multidrug therapy (MDT) for leprosy (Hansen's disease) and in regimens for multidrug-resistant tuberculosis (MDR-TB).
The TxGNN model predicts it may be effective for **Pneumocystosis** (Pneumocystis jirovecii pneumonia, PCP), with **1 associated clinical trial** and **4 publications** retrieved — though the evidence links are largely indirect, arising from shared HIV/AIDS comorbidity context rather than direct pharmacological activity against *P. jirovecii*.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Leprosy (Hansen's disease); component of MDR-TB multidrug regimens |
| Predicted New Indication | Pneumocystosis (Pneumocystis jirovecii pneumonia) |
| TxGNN Prediction Score | 99.90% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in this evidence pack. Based on known published information, clofazimine is a riminophenazine compound included in the WHO multidrug therapy regimen for leprosy (alongside dapsone and rifampicin) and in all-oral shortened regimens for rifampicin-resistant/multidrug-resistant tuberculosis (MDR/RR-TB). Its antimicrobial activity is generally attributed to reactive oxygen species (ROS) generation and disruption of bacterial cell membranes — mechanisms that account for its efficacy against mycobacteria.

*Pneumocystis jirovecii*, the causative agent of PCP, is a fungal pathogen. Clofazimine has no established antifungal mechanism and no published in vitro activity against *P. jirovecii*. The apparent connection in the TxGNN model most likely reflects knowledge-graph node proximity: both MAC infection and PCP are AIDS-defining opportunistic infections that co-occur in severely immunocompromised patients, generating strong co-occurrence signals in the underlying graph — rather than a genuine pharmacological link. The repurposing rationale in the evidence pack explicitly flags this as a probable graph-topology artifact.

In short, the 99.90% TxGNN score is epidemiologically understandable but mechanistically unsupported. No study to date demonstrates that clofazimine has direct activity against *P. jirovecii*, either in vitro or in a clinical setting.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00002058](https://clinicaltrials.gov/study/NCT00002058) | Not Applicable | Completed | N/A | Randomized controlled prophylaxis study of clofazimine for **MAC infection** in HIV patients (CD4 ≤100/mm³ or prior PCP episode). Primary target was Mycobacterium avium complex prevention — PCP history served only as an eligibility criterion, not a treatment outcome. |

> **Important:** The sole identified trial targets MAC prophylaxis in HIV disease. Pneumocystosis appears only as a patient selection criterion for high-risk classification; this trial provides no evidence of clofazimine activity against PCP.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [8501340](https://pubmed.ncbi.nlm.nih.gov/8501340/) | 1993 | RCT (MAC prophylaxis) | The Journal of Infectious Diseases | Randomized, prospective open-label trial of clofazimine 50 mg/day vs. no treatment for MAC prophylaxis in 110 HIV patients with prior PCP episode or CD4 ≤100/mm³. Primary endpoint was MAC prevention — PCP history was an inclusion criterion, not a study outcome. |
| [2714863](https://pubmed.ncbi.nlm.nih.gov/2714863/) | 1989 | Case Report | Infection | AIDS patient with *Mycobacterium kansasii* lung disease complicated by concurrent PCP. Treatment included isoniazid, ethambutol, clofazimine, and ciprofloxacin targeting mycobacteria; TMP-SMX was added for PCP. Rapid recovery achieved — clofazimine's role was directed at *M. kansasii*, not *P. jirovecii*. |
| [6299154](https://pubmed.ncbi.nlm.nih.gov/6299154/) | 1983 | Case Report | Annals of Internal Medicine | AIDS patient with hemophilia presenting with PCP, later developing disseminated *M. avium-intracellulare* bacteremia. Documents co-occurrence of PCP and MAC in an early AIDS case; clofazimine is not cited as treatment in the abstract. |
| [11363899](https://pubmed.ncbi.nlm.nih.gov/11363899/) | 1996 | Review | PI Perspective | Opportunistic infections update (no abstract available). Review covering the management landscape for AIDS-associated opportunistic infections during the pre-HAART era. |

> **Critical assessment:** None of these four publications provide direct evidence that clofazimine treats or prevents PCP. The association is contextual — patients receiving clofazimine for mycobacterial disease simultaneously had PCP as a concurrent or qualifying diagnosis. The evidence does not support a direct therapeutic claim for pneumocystosis.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The high TxGNN prediction score for pneumocystosis reflects knowledge-graph proximity between AIDS-defining opportunistic infections (MAC and PCP sharing the same high-risk immunocompromised host), not a direct pharmacological effect. Clofazimine has no established antifungal mechanism against *Pneumocystis jirovecii*, and none of the retrieved clinical trials or publications demonstrate direct activity in PCP. With zero New Zealand authorizations and no mechanistic basis, this candidate does not meet the threshold for further clinical development without foundational preclinical data.

**To proceed, the following would be needed:**

- **In vitro susceptibility data:** Test clofazimine (and active metabolites) against *Pneumocystis jirovecii* to establish whether any fungicidal or fungistatic activity exists at clinically achievable concentrations
- **Mechanistic hypothesis:** Determine whether ROS-generating activity or membrane-disruption properties extend to fungal cell wall/membrane targets in *P. jirovecii*
- **MOA clarification:** Retrieve full DrugBank MOA profile (DG002 data gap) to identify any cross-class receptor or enzyme targets that may be relevant
- **Immunomodulatory angle:** Evaluate whether clofazimine's immunomodulatory properties (separate from direct antimicrobial activity) could offer an adjunctive benefit in PCP management in immunocompromised patients — this represents the more scientifically plausible hypothesis
- **Safety profile:** Obtain TFDA/full package insert warnings and contraindications (DG001 data gap) before any protocol design
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

