---
layout: default
title: Nitrofurantoin
parent: 僅模型預測 (L5)
nav_order: 246
evidence_level: L5
indication_count: 10
---

# Nitrofurantoin
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

# Nitrofurantoin: From Urinary Tract Infection to Rheumatoid Arthritis

## One-Sentence Summary

> Nitrofurantoin is a nitrofuran-class antibacterial whose established pharmacological role — referenced throughout this evidence pack — is treatment of urinary tract infection (UTI); no formal New Zealand licensing record for this indication exists in the current dataset.
> The TxGNN model ranks **Rheumatoid Arthritis** as its top predicted new indication (score **99.89%**), supported by **12 literature records**, but on review this literature describes nitrofurantoin as a **cause of adverse events in RA patients** (pulmonary fibrosis, drug interaction with methotrexate) rather than as a treatment for RA.
> **This is a low-confidence, mechanistically unsupported prediction; the evidence pack's own analysis recommends Hold for all 10 ranked predictions, several of which appear to reflect adverse-event co-occurrence rather than therapeutic signal.**

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not formally recorded in this dataset (no New Zealand license data). Literature cited within this evidence pack (e.g. PMID 899886, PMID 39238303) refers to nitrofurantoin's established use for urinary tract infection (UTI). |
| Predicted New Indication | Rheumatoid Arthritis |
| TxGNN Prediction Score | 99.89% |
| Evidence Level | L5 (model prediction; existing literature does not constitute supportive clinical evidence) |
| New Zealand Market Status | ✗ Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | **Hold** |

---

## Why is This Prediction Reasonable?

Detailed formal mechanism-of-action data was not retrievable for this candidate (TFDA/Medsafe package insert data gap, DG001/DG002 — Blocking/High severity). Based on the pharmacological context referenced across the evidence pack's literature, nitrofurantoin is a broad-spectrum antibacterial: bacterial flavoprotein reductases convert it into reactive intermediates that damage bacterial ribosomal proteins and DNA. This is a bactericidal mechanism specific to microbial metabolism, with no established role in modulating autoimmune or synovial inflammatory pathways.

**This prediction does not have a plausible mechanistic basis.** Rheumatoid arthritis is driven by autoimmune-mediated synovial inflammation, and there is no known pharmacological pathway connecting nitrofurantoin's antibacterial mode of action to RA disease modification. On closer review, the literature underlying this prediction largely documents nitrofurantoin as a **risk factor for adverse pulmonary and hepatic events in RA patients**, not as a therapeutic agent: a documented case of irreversible pulmonary fibrosis from a methotrexate–nitrofurantoin interaction in an RA patient (PMID 35145797), reviews listing nitrofurantoin among drugs causing pulmonary fibrosis/interstitial lung disease (PMID 15195196, PMID 25362778), and a case report where nitrofurantoin was a differential cause of drug-induced liver injury in a patient with autoimmune/rheumatoid disease features (PMID 41635325).

Taken together, the high TxGNN score most likely reflects **co-occurrence of "nitrofurantoin" and "rheumatoid arthritis" in adverse-event and case-report literature**, rather than a genuine treatment signal. This should be interpreted as a **safety flag for RA patients who may be prescribed nitrofurantoin for UTI (particularly those on methotrexate)**, not as support for repurposing nitrofurantoin to treat RA.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [31222078](https://pubmed.ncbi.nlm.nih.gov/31222078/) | 2019 | Cohort (self-controlled case series) | Scientific Reports | Self-controlled case series (n=31,992 newly diagnosed RA patients, UK CPRD GOLD) examining antibiotic exposure and timing relative to RA flares — infection/antibiotic exposure was associated with flare risk, not therapeutic benefit. |
| [3335140](https://pubmed.ncbi.nlm.nih.gov/3335140/) | 1988 | Cohort | Chest | Cohort of 57 RA patients hospitalized for interstitial lung fibrosis; poor prognosis reported. Describes RA-associated lung disease outcomes, unrelated to nitrofurantoin treatment. |
| [899886](https://pubmed.ncbi.nlm.nih.gov/899886/) | 1977 | Cohort | Acta Medica Scandinavica | Short-term nitrofurantoin therapy for bacteriuria in a middle-aged female population with 1-year follow-up — a standard UTI treatment study, unrelated to RA. |
| [15195196](https://pubmed.ncbi.nlm.nih.gov/15195196/) | 2004 | Review | Saudi Medical Journal | Review of drug-induced pulmonary fibrosis; lists nitrofurantoin among causative agents and notes RA itself predisposes to pulmonary fibrosis — a toxicity profile, not an RA treatment mechanism. |
| [25362778](https://pubmed.ncbi.nlm.nih.gov/25362778/) | 2014 | Review | La Revue du praticien | Review of drug-induced interstitial lung disease; nitrofurantoin listed among antibiotics implicated in this toxicity. |
| [4608019](https://pubmed.ncbi.nlm.nih.gov/4608019/) | 1974 | Review | Der Internist | General synopsis of alveolitis and pulmonary fibrosis etiologies; background review only. |
| [35145797](https://pubmed.ncbi.nlm.nih.gov/35145797/) | 2022 | Case report | Cureus | 94-year-old woman on long-term methotrexate for RA developed irreversible pulmonary fibrosis after receiving nitrofurantoin for a UTI — a documented drug–drug interaction hazard, not a treatment signal. |
| [41635325](https://pubmed.ncbi.nlm.nih.gov/41635325/) | 2026 | Case report | Cureus | Autoimmune hepatitis work-up in which nitrofurantoin (alongside other drugs) is listed as a differential cause of drug-induced liver injury — a hepatotoxicity signal. |
| [11937933](https://pubmed.ncbi.nlm.nih.gov/11937933/) | 2002 | Case report | Annales de dermatologie et de venereologie | Case of phenylbutazone-induced sialadenitis; nitrofurantoin mentioned only incidentally as another drug reported to cause sialadenitis. |
| [8104358](https://pubmed.ncbi.nlm.nih.gov/8104358/) | 1993 | Case report | Revue de pneumologie clinique | Case of gold-salt-induced pneumonitis with CD4 alveolitis in an RA-type context; nitrofurantoin not directly implicated, cited as comparator drug-induced lung disease. |

**None of the above literature demonstrates a therapeutic benefit of nitrofurantoin in rheumatoid arthritis.** The majority document adverse pulmonary, hepatic, or interaction risks, particularly in patients concurrently treated with methotrexate.

---

## New Zealand Market Information

Nitrofurantoin is **not currently marketed in New Zealand** — no Medsafe product authorization records were found (0 licenses).

---

## Safety Considerations

Formal package-insert safety data (key warnings, contraindications, drug–drug interactions) could not be retrieved for this candidate — this is logged as a **Blocking data gap (DG001)** requiring TFDA/Medsafe package insert review before any Stage 1 safety assessment can proceed.

**Safety signals identified during this evidence review** (from literature associated with the predicted-indications analysis, not from a formal safety database — flagged here because of their direct clinical relevance):

- **Methotrexate interaction / pulmonary fibrosis**: A case of irreversible pulmonary fibrosis following nitrofurantoin use in a patient on long-term methotrexate (PMID 35145797). Relevant given RA patients are commonly treated with methotrexate.
- **Methemoglobinemia**: Nitrofurantoin (and its photoactivated metabolite 5-nitrofurfural) is a documented cause of methemoglobinemia, including in neonates with concurrent hemolytic anemia (PMID 3176031, PMID 930081, PMID 5359411). This is an established adverse effect, not a treatable indication.
- **Renal impairment**: Repurposing rationale for the diabetic nephropathy candidate notes nitrofurantoin is relatively contraindicated in reduced renal function (eGFR decline) due to drug accumulation and insufficient urinary concentration for efficacy — relevant to any population with renal comorbidity.
- **Hepatotoxicity**: Listed as a differential cause of drug-induced liver injury/autoimmune hepatitis in a 2026 case report (PMID 41635325).

---

## Additional Note: Other Model-Predicted Indications Reviewed

This evidence pack ranks 10 candidate indications for nitrofurantoin (TxGNN scores 99.89%–99.38%). Beyond rheumatoid arthritis, the remaining 9 were also reviewed and **all carry a "Hold" recommendation**:

- Several (autosomal dominant familial hematuria-retinal arteriolar tortuosity-contractures syndrome, brain small vessel disease 1, brachydactyly-syndactyly syndrome, colobomatous microphthalmia-rhizomelic dysplasia syndrome) are rare genetic/structural syndromes with no plausible mechanistic link and no supporting literature or trials.
- Two (**methemoglobinemia, alpha type** and **methemoglobinemia**) are particularly notable: nitrofurantoin is a well-documented **cause** of methemoglobinemia, not a treatment for it. These predictions likely reflect the model learning an adverse-event co-occurrence rather than a therapeutic relationship, and should not be advanced.
- The remainder (diabetic nephropathy, sclerosing cholangitis, gout) are supported only by incidental or comorbidity-related literature (e.g., UTI epidemiology in diabetic patients), not by mechanistic or therapeutic evidence.

This pattern suggests the top-ranked predictions for this candidate are more likely driven by co-occurrence in adverse-event/case-report literature than by genuine drug-repurposing signal, and this candidate should be deprioritized relative to others with cleaner mechanistic or trial-based support.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
No predicted indication for nitrofurantoin in this evidence pack has a plausible mechanistic basis or supportive clinical trial evidence. The literature underlying the top-ranked prediction (rheumatoid arthritis) documents safety risks (pulmonary fibrosis with methotrexate co-administration, methemoglobinemia, hepatotoxicity) rather than therapeutic benefit, and the drug is not currently marketed in New Zealand.

**To proceed, the following is needed:**
- TFDA/Medsafe package insert data (warnings, contraindications) — currently a Blocking data gap (DG001)
- Formal mechanism-of-action documentation from DrugBank or equivalent source (DG002)
- If any of the 10 predicted indications is pursued further, a targeted literature re-review to distinguish true therapeutic signal from adverse-event co-occurrence artifact, since several current "predictions" (notably methemoglobinemia) appear to be known toxicities rather than treatable indications
- Explicit safety review for methotrexate co-administration and renal-function-based dosing restrictions before any RA-related evaluation proceeds
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

