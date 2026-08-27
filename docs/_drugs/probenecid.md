---
layout: default
title: Probenecid
parent: 僅模型預測 (L5)
nav_order: 290
evidence_level: L5
indication_count: 3
---

# Probenecid
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

Using **superpowers:using-superpowers** — checked for applicable skills; this is a template-driven document-generation task with the full formatting spec already supplied in the prompt, so no additional skill applies. Proceeding directly.

Note before the report: the evidence pack contains **three** ranked TxGNN predictions, and the top-ranked one (`hypouricemia, renal`) has a mechanistic rationale in the data itself explaining that probenecid's pharmacology runs in the **opposite direction** of what the disease needs. I'm reporting this honestly rather than presenting it as a clean positive signal, and I've added a compact section covering candidates #2 and #3 since the template's single-indication structure would otherwise silently drop them.

---

# Probenecid: From Gout/Hyperuricemia to Renal Hypouricemia

## One-Sentence Summary

> Probenecid is a classic uricosuric agent, historically used to treat gout/hyperuricemia (and as an adjunct to prolong penicillin levels); no original-indication or MOA data is present in this evidence pack. The TxGNN model's top prediction is **Renal Hypouricemia**, but the mechanism is directionally backwards — probenecid *lowers* uric acid, whereas renal hypouricemia is a disease of *already-excessive* urate loss — so this is very likely a model artifact rather than a genuine repurposing opportunity. **0 clinical trials** and **20 publications** exist for this candidate, none of which support therapeutic use.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in evidence pack (`original_indications` empty); known clinical use is gout/hyperuricemia (uricosuric agent) |
| Predicted New Indication | Renal Hypouricemia (hypouricemia, renal) |
| TxGNN Prediction Score | 99.73% |
| Evidence Level | L5 (model prediction only) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in this evidence pack (DG002). Based on known pharmacology, probenecid is a uricosuric agent that inhibits URAT1/OAT1/OAT3-mediated proximal tubular reabsorption of urate, thereby **lowering** serum uric acid and **increasing** urinary urate excretion.

This is the problem: renal hypouricemia is caused by **loss-of-function mutations in SLC22A12 (URAT1)** — the same transporter probenecid inhibits — which already causes excessive urate loss. Giving probenecid to these patients would not treat the condition; it would push urate excretion further in the same pathological direction, worsening hypouricemia and increasing the risk of exercise-induced acute renal failure, a well-documented complication in this population. Two of the cited case reports (PMID 854144, PMID 8341392) actually tested probenecid directly in renal hypouricemia patients and found a **blunted or absent** uricosuric response — empirical confirmation that the drug offers no benefit here, consistent with the receptor already being maximally engaged by the underlying mutation.

The most plausible explanation is that TxGNN scored this pair highly because probenecid, URAT1, and "urate metabolism" are tightly co-located in the knowledge graph, without the model distinguishing the *direction* of the pharmacological effect relative to the disease. This is an inverse-direction artifact, not a genuine therapeutic hypothesis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16678460](https://pubmed.ncbi.nlm.nih.gov/16678460/) | 2006 | Review | Mol Genet Metab | Hereditary renal hypouricemia caused by loss-of-function mutations in SLC22A12 (URAT1), impairing tubular urate reabsorption |
| [7771493](https://pubmed.ncbi.nlm.nih.gov/7771493/) | 1995 | Review | Am J Kidney Dis | Renal hypouricemia can trigger exercise-induced acute renal failure; reviews prevention strategies |
| [31650389](https://pubmed.ncbi.nlm.nih.gov/31650389/) | 2020 | Review | Clin Rheumatol | Narrative update on hypouricemia etiology and classification for rheumatologists |
| [14694169](https://pubmed.ncbi.nlm.nih.gov/14694169/) | 2004 | Cohort | J Am Soc Nephrol | 32 Japanese renal hypouricemia patients; urate clearance linked to SLC22A12/URAT1 mutations |
| [3813739](https://pubmed.ncbi.nlm.nih.gov/3813739/) | 1987 | Case Report | Arch Intern Med | Diabetic patients with renal hypouricemia show increased pyrazinamide-suppressible urate clearance |
| [1656732](https://pubmed.ncbi.nlm.nih.gov/1656732/) | 1991 | Case Report | Am J Kidney Dis | Cholangiocarcinoma case with severe renal hypouricemia and markedly elevated urate clearance |
| [14655203](https://pubmed.ncbi.nlm.nih.gov/14655203/) | 2003 | Case Report | Am J Kidney Dis | Two brothers with hereditary renal hypouricemia and exercise-induced acute renal failure |
| [1944743](https://pubmed.ncbi.nlm.nih.gov/1944743/) | 1991 | Case Report | Nephron | Type 1 diabetics show elevated urate clearance/fractional excretion consistent with renal hypouricemia |
| [854144](https://pubmed.ncbi.nlm.nih.gov/854144/) | 1977 | Case Report | Nephron | Familial hypouricemia showed **attenuated response to probenecid and pyrazinamide** — direct evidence the drug doesn't help this condition |
| [8341392](https://pubmed.ncbi.nlm.nih.gov/8341392/) | 1993 | Case Report | Nephron | Novel renal hypouricemia subtype **unresponsive to probenecid or pyrazinamide**, combining secretion and reabsorption defects |

## New Zealand Market Information

Probenecid is currently **not marketed** in New Zealand — 0 authorizations on file in this evidence pack.

## Safety Considerations

Please refer to the package insert for safety information. (Key warnings, contraindications, and DDI data are all marked as gaps in this evidence pack — DG001, "TFDA package insert warnings/contraindications," is flagged as a **Blocking** gap that prevents a full S1 safety review.)

---

## Additional TxGNN-Predicted Candidates

The evidence pack ranked two further candidates for probenecid; both are also scored **Hold**.

### #2 — Lesch-Nyhan Syndrome (score 99.39%, rank 5143, Evidence Level L4)

Unlike renal hypouricemia, this condition (HGPRT deficiency causing severe hyperuricemia plus neuropsychiatric symptoms) matches probenecid's urate-lowering direction. However, uricosuric agents are relatively contraindicated in this population because increasing urinary urate excretion raises the risk of uric acid stones and urate nephropathy; standard therapy is a xanthine oxidase inhibitor (allopurinol), not a uricosuric. Literature is old (1968–1976) and consists of case discussions/reviews, not controlled studies of probenecid in this indication.

Currently no related clinical trials registered.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [24078686](https://pubmed.ncbi.nlm.nih.gov/24078686/) | 2013 | Review | Bioinformatics | Computational method for identifying enzyme targets in human uric acid metabolism disorders |
| [1070064](https://pubmed.ncbi.nlm.nih.gov/1070064/) | 1976 | Case Report | Res Publ Assoc Res Nerv Ment Dis | Probenecid-induced CSF HVA accumulation used as a dopamine-turnover marker in Tourette's syndrome (unrelated indication) |
| [4232133](https://pubmed.ncbi.nlm.nih.gov/4232133/) | 1968 | Review | Fed Proc | Seminar discussion of unanswered questions in Lesch-Nyhan syndrome |
| [5093939](https://pubmed.ncbi.nlm.nih.gov/5093939/) | 1971 | Review | Monatsschr Kinderheilkd | Treatment of congenital hyperuricemia |

### #3 — HGPRT Partial Deficiency / Kelley-Seegmiller Syndrome (score 99.37%, rank 5305, Evidence Level L5)

Directionally plausible (hyperuricemia/gout without neurological involvement), but there is **zero clinical trial or literature support** in this evidence pack — the prediction rests solely on the TxGNN score. Uricosuric therapy would still need screening for prior urolithiasis before consideration.

Currently no related clinical trials registered. Currently no related literature available.

---

## Conclusion and Next Steps

**Decision: Hold** (all three candidates)

**Rationale:**
- The top candidate (renal hypouricemia) is mechanistically backwards — probenecid would worsen, not treat, the condition, and two case reports directly tested probenecid in these patients with a blunted/absent response.
- Candidate #2 (Lesch-Nyhan) is directionally plausible but carries a known contraindication-risk profile (stone/nephropathy) and only decades-old, uncontrolled literature.
- Candidate #3 has no supporting evidence beyond the model score (L5).
- Two blocking/high-severity data gaps remain unresolved: TFDA/regulatory package-insert warnings and contraindications (DG001, **Blocking**), and drug MOA detail (DG002, **High**).

**To proceed, the following is needed:**
- Resolve DG001 (package insert warnings/contraindications) before any S1 safety review can proceed
- Resolve DG002 (formal MOA sourcing from DrugBank)
- If candidate #2 is pursued despite the contraindication concern, a nephrology/genetics expert review of urolithiasis risk in Lesch-Nyhan patients
- No further investment recommended for candidate #1 (renal hypouricemia) absent a re-examination of the TxGNN prediction for directionality errors
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

