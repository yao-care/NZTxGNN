---
layout: default
title: Omalizumab
parent: 僅模型預測 (L5)
nav_order: 257
evidence_level: L5
indication_count: 10
---

# Omalizumab
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

# Omalizumab: From Severe Allergic Asthma to Bronchitis

## One-Sentence Summary

Omalizumab is a recombinant humanized anti-IgE monoclonal antibody, clinically established for moderate-to-severe persistent allergic asthma and chronic spontaneous urticaria (CSU). The TxGNN model predicts it may also be effective for **Bronchitis**, but current support is limited to **2 clinical trials** (neither enrolling a bronchitis-diagnosed population directly) and **8 publications**, most of which focus on asthma rather than bronchitis itself.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Severe allergic asthma / Chronic spontaneous urticaria (known approved use — not present in this evidence pack's regulatory license data, which is empty) |
| Predicted New Indication | Bronchitis |
| TxGNN Prediction Score | 99.9992% |
| Evidence Level | L3 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack. Based on known pharmacology, omalizumab binds free serum IgE, blocking its interaction with the high-affinity FcεRI receptor on mast cells and basophils, which reduces IgE-mediated inflammatory signaling. This mechanism is well established in severe allergic asthma and CSU.

Bronchitis and allergic asthma share partial overlap in airway inflammatory mechanism — both can involve IgE-mediated eosinophil and mast cell infiltration of the airway. However, the two clinical trials cited as supporting evidence are essentially asthma studies, not populations with a primary bronchitis diagnosis, and the literature likewise concentrates on asthma rather than acute or chronic bronchitis as such.

One cited case report (PMID 31478531) describes "plastic bronchitis" occurring after bronchial thermoplasty — a mechanically triggered condition unrelated to omalizumab's pharmacologic effect — which further weakens the direct evidentiary link. Overall, the mechanistic rationale for bronchitis is plausible but remains indirect inference rather than disease-specific proof.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02477332](https://clinicaltrials.gov/study/NCT02477332) | Phase 2 | Completed | 382 | Dose-finding study of QGE031 (ligelizumab, an anti-IgE antibody) as add-on therapy in chronic spontaneous urticaria; population is not bronchitis-specific — indirect evidence only. |
| [NCT02049294](https://clinicaltrials.gov/study/NCT02049294) | Phase 2/3 | Completed | 11 | Steroid-sparing effect of omalizumab in patients with asthma and persistent eosinophilic bronchitis; very small sample (n=11), primary endpoint was corticosteroid dose reduction, not bronchitis resolution. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16222080](https://pubmed.ncbi.nlm.nih.gov/16222080/) | 2005 | Review (postmarketing) | Clinical reviews in allergy & immunology | Postapproval review: omalizumab reduces free IgE and FcεRI expression, improving airway inflammation in moderate-to-severe asthma. |
| [21121874](https://pubmed.ncbi.nlm.nih.gov/21121874/) | 2011 | Safety/Cohort | Current medical research and opinion | Pooled safety analysis of omalizumab in children with IgE-mediated allergic asthma. |
| [30196731](https://pubmed.ncbi.nlm.nih.gov/30196731/) | 2018 | Review | Expert opinion on pharmacotherapy | Discusses smoking-induced airway disease (chronic bronchitis, COPD, asthma-COPD overlap); notes these populations are largely excluded from trials, limiting drug-treatment evidence. |
| [17663923](https://pubmed.ncbi.nlm.nih.gov/17663923/) | 2007 | Review | Allergologia et immunopathologia | General review of monoclonal antibodies in pediatric allergic disease; not bronchitis-specific. |
| [35369622](https://pubmed.ncbi.nlm.nih.gov/35369622/) | 2022 | Cohort | Postepy dermatologii i alergologii | Omalizumab in older patients with severe allergic asthma–COPD overlap. |
| [21163396](https://pubmed.ncbi.nlm.nih.gov/21163396/) | 2010 | Review | Revue des maladies respiratoires | French expert review on definitions/management of adult asthma exacerbations. |
| [31478531](https://pubmed.ncbi.nlm.nih.gov/31478531/) | 2019 | Case report (unrelated mechanism) | Journal of investigational allergology & clinical immunology | Case of "plastic bronchitis" following bronchial thermoplasty — a procedural complication, not related to omalizumab's pharmacologic effect. |
| [26466493](https://pubmed.ncbi.nlm.nih.gov/26466493/) | 2015 | Review | Masui (Japanese journal of anesthesiology) | Preoperative management review of bronchial asthma/chronic bronchitis patients; mentions omalizumab as an option in severe allergic asthma. |

---

## New Zealand Market Information

Omalizumab is currently **not marketed** in New Zealand, with **0 authorizations** on file in this evidence pack. No product license or approved indication text is available.

---

## Safety Considerations

Please refer to the package insert for safety information. (No structured safety data — key warnings, contraindications, or DDI — is available in this evidence pack; retrieval of TFDA/local package insert data is flagged as a blocking data gap.)

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The top-ranked predicted indication (bronchitis) is supported only by indirect evidence — its two cited trials primarily enrolled asthma or CSU patients rather than bronchitis-diagnosed populations, and one supporting publication is an unrelated case report. Combined with a blocking data gap on regulatory safety information (package insert warnings/contraindications unavailable), this candidate cannot yet clear an initial safety screen (S1).

**Note on stronger candidates in this evidence pack:** Rank 3 ("obstructive lung disease") carries a substantially stronger evidence base (L1, multiple completed Phase 3 RCTs, "Proceed with Guardrails") — but its own rationale flags that this largely restates omalizumab's *already-approved* severe allergic asthma indication rather than representing a genuinely novel repurposing hypothesis.

**To proceed, the following is needed:**
- TFDA/NZ package insert data (warnings, contraindications) to clear the blocking safety data gap (DG001)
- Confirmed mechanism of action detail from DrugBank (DG002)
- Clinical trials or literature enrolling bronchitis-diagnosed patients specifically (rather than asthma/CSU populations)
- Confirmation of original approved indication text from a regulatory source, since `original_indications` and `taiwan_regulatory.licenses` are both empty in this evidence pack
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

