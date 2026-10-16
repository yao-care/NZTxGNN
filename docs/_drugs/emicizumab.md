---
layout: default
title: Emicizumab
parent: Model Prediction Only (L5)
nav_order: 174
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Emicizumab: predicted new indication Acquired coagulation factor deficiency (acquired hemophilia A). This… (evidence level L5). Approved use: Hemophilia"
monitor_inn: "EMICIZUMAB"
predicted_indications: ["pseudo-von Willebrand disease", "primary release disorder of platelets", "Glanzmann thrombasthenia", "Scott syndrome", "acquired coagulation factor deficiency", "bleeding diathesis due to a collagen receptor defect", "hemorrhagic disorder due to a constitutional thrombocytopenia", "thrombotic thrombocytopenic purpura", "fetal and neonatal alloimmune thrombocytopenia", "flood factor deficiency"]
---

# Emicizumab
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

# Emicizumab: From Hemophilia to Acquired Coagulation Factor Deficiency

## One-Sentence Summary

Emicizumab is a bispecific antibody originally used to prevent bleeding in hemophilia (hemophilia A).
The TxGNN model's top-ranked predictions are mostly platelet disorders with no supporting evidence, but the fifth-ranked prediction, **acquired coagulation factor deficiency** (in practice, acquired hemophilia A), is well supported by **19 publications**, including 3 prospective phase 2/3 studies. No clinical trial records were compiled for this report.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hemophilia; symptomatic hemophilia in female carriers (from the knowledge graph; no approved indication text available from the New Zealand licences) |
| Predicted New Indication | Acquired coagulation factor deficiency (acquired hemophilia A). This is the best-supported candidate; the top-ranked prediction, pseudo-von Willebrand disease, has no evidence |
| TxGNN Prediction Score | 99.90% (rank 5 prediction; the rank 1 prediction, pseudo-von Willebrand disease, scored 99.99%) |
| Evidence Level | L2 (prospective, non-randomized phase 2/3 studies; no randomized trials) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Proceed with Guardrails (for acquired hemophilia A only) |

## Why is This Prediction Reasonable?

Emicizumab is a bispecific antibody that bridges activated factor IX and factor X, mimicking the function of activated factor VIII (FVIIIa). Detailed mechanism of action data was not available in the compiled data, but this bridging function is described in the literature and the pharmacology data.

Acquired hemophilia A is caused by autoantibodies that neutralize the patient's own factor VIII. Emicizumab does not depend on factor VIII and is not neutralized by these antibodies. This is the same reason it works in congenital hemophilia A patients who have developed inhibitors. The mechanistic link between the original and new indication is therefore strong.

The top-ranked TxGNN predictions are a different matter. Pseudo-von Willebrand disease, primary release disorder of platelets, Glanzmann thrombasthenia, Scott syndrome and collagen receptor defects are platelet-function disorders. Emicizumab acts on the coagulation cascade and does not correct these defects. These high scores are model predictions only and have no clinical support. Thrombotic thrombocytopenic purpura is a particular concern, because promoting coagulation could be harmful. "Flood factor deficiency" appears to be a non-standard disease name and needs verification.

## Clinical Trial Evidence

Currently no related clinical trials registered in the data compiled for this report. The literature below describes prospective studies, but their registry numbers need to be confirmed separately.

## Literature Evidence

The 19 publications retrieved for acquired hemophilia A are listed below, with the key studies first. Some study types are inferred from titles and abstracts.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36696195](https://pubmed.ncbi.nlm.nih.gov/36696195/) | 2023 | Phase III open-label single-arm trial | J Thromb Haemost | First prospective multicenter study of emicizumab prophylaxis in acquired hemophilia A (Japan) |
| [37858328](https://pubmed.ncbi.nlm.nih.gov/37858328/) | 2023 | Phase 2 open-label single-arm trial (GTH-AHA-EMI) | Lancet Haematol | Emicizumab protected against bleeding and allowed deferral of immunosuppression during the first 12 weeks |
| [39134043](https://pubmed.ncbi.nlm.nih.gov/39134043/) | 2025 | Prospective study, final analysis (AGEHA) | Thromb Haemost | Final results including immunosuppression-ineligible patients and long-term prophylaxis |
| [40795229](https://pubmed.ncbi.nlm.nih.gov/40795229/) | 2025 | Cohort study | Blood Adv | Two-year follow-up of GTH-AHA-EMI patients: sustained survival benefit with postponed immunosuppression |
| [38936699](https://pubmed.ncbi.nlm.nih.gov/38936699/) | 2024 | Comparative/cohort study | J Thromb Haemost | Emicizumab compared with immunosuppressive therapy for managing acquired hemophilia A |
| [39361769](https://pubmed.ncbi.nlm.nih.gov/39361769/) | 2024 | Retrospective cohort | Blood Adv | Real-world data on 62 US patients treated off-label with emicizumab |
| [38049124](https://pubmed.ncbi.nlm.nih.gov/38049124/) | 2024 | Consensus recommendations | Hamostaseologie | GTH-AHA working group recommendations for emicizumab use |
| [39536818](https://pubmed.ncbi.nlm.nih.gov/39536818/) | 2025 | Review | J Thromb Haemost | Narrative review and management approach in the emicizumab era |
| [37276345](https://pubmed.ncbi.nlm.nih.gov/37276345/) | 2023 | Case series | Haemophilia | Emicizumab in acquired hemophilia A; no randomized trials exist |
| [36795341](https://pubmed.ncbi.nlm.nih.gov/36795341/) | 2023 | Review | Blood Transfus | Pros and cons of emicizumab for preventing and treating bleeding in acquired hemophilia A |

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2553538 | Hemlibra | Inj 150 mg in 1 ml vial | Not available (cancelled) |
| 2553554 | Hemlibra | Inj 105 mg in 0.7 ml vial | Not available (cancelled) |
| 2553511 | Hemlibra | Inj 30 mg in 1 ml vial | Not available (cancelled) |
| 2553546 | Hemlibra | Inj 60 mg in 0.4 ml vial | Not available (cancelled) |

## Safety Considerations

- **Drug Interactions** (5 recorded):
  - **Major**: anti-inhibitor coagulant complex (activated prothrombin complex concentrate). Concomitant use is associated with a risk of thrombotic microangiopathy and thrombosis.
  - **Moderate**: recombinant antihemophilic factor (factor VIII) and recombinant factor VIIa.
  - Pharmacological targets: coagulation factors IX and X (the antibody binds both).
- **Thrombotic risk**: the literature highlights this particularly in older patients and with concomitant bypassing agents. Thrombotic thrombocytopenic purpura is a setting where emicizumab could be harmful.

Package insert warnings and contraindications were not available. Please refer to the package insert for further safety information.

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (acquired hemophilia A only). **Hold** for all other predicted indications.

**Rationale:**
For acquired hemophilia A, there is a strong mechanistic basis and several prospective phase 2/3 studies plus real-world cohorts, but no randomized trials. The platelet-disorder predictions have no evidence and no plausible mechanism, and the high model scores should not be read as therapeutic benefit. Also note that all four New Zealand licences are cancelled.

**To proceed, the following is needed:**
- Confirm clinical trial registrations (NCT numbers) for the prospective studies
- Obtain the package insert warnings and contraindications (a blocking gap for safety screening)
- Obtain detailed mechanism of action data
- Clarify the New Zealand regulatory status, since all licences are cancelled and the approved indication text is missing
- Define a thrombosis risk plan: avoid concomitant activated prothrombin complex concentrate, and set the immunosuppression strategy and monitoring for coagulation assay limitations
- Verify the disease name "flood factor deficiency" before any further evaluation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

