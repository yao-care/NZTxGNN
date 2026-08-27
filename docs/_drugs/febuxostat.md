---
layout: default
title: Febuxostat
parent: 僅模型預測 (L5)
nav_order: 147
evidence_level: L5
indication_count: 3
---

# Febuxostat
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

# Febuxostat: From Hyperuricemia to Renal Hypouricemia

## One-Sentence Summary

Febuxostat is a non-purine selective xanthine oxidase inhibitor originally used to control hyperuricemia in patients with gout. The TxGNN model predicts a possible role in **Renal Hypouricemia (RHUC)** — specifically in preventing the exercise-induced kidney injury that can complicate this condition — with **1 clinical trial** and **2 publications** currently touching on this direction.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hyperuricemia associated with gout (based on established drug information; no original-indication text is recorded in the local regulatory dataset because the product is not locally registered) |
| Predicted New Indication | Renal Hypouricemia |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action documentation for this record is currently a data gap. Based on well-established pharmacology, febuxostat is a potent, non-purine selective inhibitor of xanthine oxidase, blocking the conversion of hypoxanthine → xanthine → uric acid. This mechanism is the basis of its approved use for lowering serum urate in hyperuricemia/gout.

At first glance, "hyperuricemia treatment" and "renal hypouricemia" (a disorder of chronically **low** serum urate caused by urate-transporter defects such as URAT1/GLUT9 mutations) appear to be opposite ends of the uric-acid spectrum. However, the supporting literature clarifies the actual clinical rationale: patients with renal hypouricemia are paradoxically prone to **exercise-induced acute kidney injury (EIAKI)**, because intense anaerobic exercise causes a sudden surge in renal urate excretion and intratubular urate crystallization. Since febuxostat reduces uric acid **production** (not just reabsorption), it lowers the filtered/excreted urate load during exercise, which may reduce the risk of crystallization-related AKI — even though it does not correct the underlying transporter defect itself. This is consistent with the case-level evidence identified (PMID 36754409), where febuxostat was used specifically to prevent recurrent EIAKI in a patient with genetically confirmed RHUC.

It is also worth noting that the same evidence pack contains two lower-ranked but mechanistically clearer predictions — HPRT partial deficiency and Lesch-Nyhan syndrome — both of which are purine-salvage-pathway disorders causing uric acid **overproduction**, a setting where xanthine oxidase inhibitors (allopurinol classically, febuxostat as an alternative in renal impairment or allopurinol intolerance) are already used off-label. This pattern across ranks 1–3 suggests the model is correctly anchoring on febuxostat's core xanthine-oxidase-inhibition pharmacology, applied here to a related but distinct clinical scenario (complication prevention rather than direct correction of hypouricemia).

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT04398251](https://clinicaltrials.gov/study/NCT04398251) | Phase 4 | Unknown | 100 | Prospective controlled study exploring whether uric acid control affects stone recurrence and renal function in patients with hyperuricemia-associated calculi (indirect relevance — focused on hyperuricemic stone disease, not renal hypouricemia directly) |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [36754409](https://pubmed.ncbi.nlm.nih.gov/36754409/) | 2023 | Case Report | Internal Medicine (Tokyo, Japan) | Describes a 16-year-old with familial renal hypouricemia (compound heterozygous URAT1 mutations) and recurrent exercise-induced AKI; febuxostat proposed as prophylaxis when exercise-limiting/hydration measures are insufficient |
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clinical Rheumatology | Narrative review of hypouricemia etiology and clinical management relevant to rheumatologists; background context rather than direct treatment evidence |

---

## New Zealand Market Information

Febuxostat is currently not marketed in New Zealand — no product authorizations are recorded, and no approved-indication text is available from the local regulatory dataset.

---

## Safety Considerations

Please refer to the package insert for safety information. No key warnings, contraindications, or drug interaction data are currently available in this evidence pack.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The predicted indication is currently supported only by a single case report and a mechanistically indirect Phase 4 trial with unknown completion status — no completed RCT or systematic evidence base exists yet. In addition, a **Blocking**-severity data gap (missing TFDA/NZ package-insert warnings and contraindications) currently prevents even the initial safety pre-assessment (S1) required before further evaluation.

**To proceed, the following is needed:**
- Local package insert / label data (warnings, contraindications) to resolve the Blocking data gap (DG001)
- Confirmed mechanism-of-action documentation from DrugBank (DG002)
- Additional case series or cohort-level data on febuxostat use for EIAKI prevention in renal hypouricemia, beyond the single published case
- Clarification of the drug's original-indication documentation, since no local license record currently exists
- An assessment of local market-entry feasibility given the current "not marketed" status in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

