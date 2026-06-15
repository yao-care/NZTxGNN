---
layout: default
title: Clobazam
parent: 僅模型預測 (L5)
nav_order: 78
evidence_level: L5
indication_count: 10
---

# Clobazam
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

# Clobazam: From Lennox-Gastaut Syndrome to Febrile Infection-Related Epilepsy Syndrome

## One-Sentence Summary

Clobazam is a 1,5-benzodiazepine antiepileptic and anxiolytic drug, established in international guidelines as adjunctive therapy for seizures associated with Lennox-Gastaut syndrome (LGS) and one of only eight ASMs with specific FDA approval for that indication.
The TxGNN model predicts it may be effective for **Febrile Infection-Related Epilepsy Syndrome (FIRES)**, a catastrophic super-refractory status epilepticus occurring in previously healthy individuals,
with **0 clinical trials** and **2 publications** (indirect, class-level benzodiazepine evidence) currently supporting this direction.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Adjunctive therapy for seizures in Lennox-Gastaut syndrome (FDA-approved internationally; not registered in New Zealand) |
| Predicted New Indication | Febrile Infection-Related Epilepsy Syndrome (FIRES) |
| TxGNN Prediction Score | 99.82% |
| Evidence Level | L4 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the current evidence pack. Based on published literature, clobazam is a 1,5-benzodiazepine that acts as a positive allosteric modulator (PAM) of GABA-A receptors — enhancing chloride ion influx, augmenting inhibitory neurotransmission, and raising the seizure threshold across multiple seizure types. Unlike classical 1,4-benzodiazepines (diazepam, clonazepam), its 1,5-diazepine ring confers a longer effective half-life (active metabolite N-desmethylclobazam t½ ~71 h), oral bioavailability, and a relatively favourable sedation and tolerance profile, which makes it pharmacokinetically suitable for sub-acute and chronic seizure management.

FIRES is a form of new-onset refractory status epilepticus (NORSE) triggered by a febrile illness, characterised by catastrophic, drug-resistant seizures requiring prolonged pharmacological coma with midazolam, pentobarbital or propofol. The critical clinical challenge is the sub-acute weaning phase: clinicians need a longer-acting oral agent that can substitute for intravenous anaesthetics while maintaining seizure control. The existing literature demonstrates proof-of-concept for oral benzodiazepines in this role — PMID 35770765 (2022) reports successful use of enteral lorazepam as a midazolam-weaning strategy in midazolam-dependent FIRES patients. Clobazam, sharing the same GABA-A PAM mechanism but offering superior duration of action and oral formulation, is a logical next candidate for this weaning role.

Clobazam's strongest established evidence sits within the broader epileptic encephalopathy spectrum. It is included in AAN 2018 practice guidelines (Level A recommendation; PMID 29898971), two Cochrane reviews support its use in focal and generalised seizures (PMID 29995989, PMID 25280512), and a prospective study confirms add-on efficacy in temporal lobe epilepsy with hippocampal sclerosis (PMID 15825553). The TxGNN knowledge graph captures this mechanistic and nosological proximity between LGS-type encephalopathies and FIRES, explaining the high prediction score. However, no current study directly evaluates clobazam in FIRES, leaving the extrapolation at the preclinical / mechanistic reasoning level.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|-------------|
| [35770765](https://pubmed.ncbi.nlm.nih.gov/35770765/) | 2022 | Case Series | Epileptic Disorders | Enteral lorazepam served as an effective oral benzodiazepine weaning substitute for midazolam in FIRES; provides class-level proof-of-concept that oral BZDs can bridge the sub-acute FIRES phase — the role clobazam could theoretically fill |
| [39958143](https://pubmed.ncbi.nlm.nih.gov/39958143/) | 2025 | Case Report | Cureus | Perampanel reduced barbiturate dependency in a 13-year-old FIRES patient; underscores the unmet clinical need for oral weaning agents in FIRES and the absence of established options, which this repurposing inquiry aims to address |

> **Note:** Neither publication directly studies clobazam in FIRES. Both provide indirect support through demonstration of oral/enteral neuroactive agents as weaning strategies in the same condition.

---

## New Zealand Market Information

Clobazam is not currently authorised or marketed in New Zealand (Medsafe). No product licences were found in the regulatory database query conducted on 2026-03-29.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for clobazam specifically in FIRES is absent — the two supporting publications concern other benzodiazepines and unrelated agents, with no clinical trials and no direct clobazam data in this indication. The mechanistic extrapolation (GABA-A PAM → super-refractory status epilepticus weaning) is pharmacologically coherent but remains unvalidated in the FIRES population.

**To proceed, the following is needed:**

- **Fill data gaps first**: Obtain formal MOA data from DrugBank and retrieve the package insert (including warnings and contraindications) before any safety assessment can be initiated
- **Targeted literature search**: Conduct a dedicated search for clobazam specifically in FIRES sub-acute phase or NORSE management — current search used broad FIRES terms and may have missed case-level reports
- **Design a research question**: Formulate a prospective case series or international FIRES registry sub-study examining clobazam as an oral midazolam-weaning strategy, with pre-specified endpoints (days to wean, seizure recurrence rate, discharge ASM regimen)
- **Tolerance risk assessment**: Evaluate benzodiazepine tolerance and withdrawal risk in the FIRES clinical context before escalating evidence stage; this is a known class-effect concern for long-term BZD use
- **Consider prioritising rank-6 indication**: Among all ten TxGNN-predicted indications, childhood-onset epileptic encephalopathy (rank 6; primarily Lennox-Gastaut syndrome; Evidence Level **L1**, AAN Level A) represents a far more immediately actionable repurposing target — clobazam already holds FDA approval for LGS and a New Zealand regulatory submission pathway (Medsafe section 23 application) deserves higher priority in the near term
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

