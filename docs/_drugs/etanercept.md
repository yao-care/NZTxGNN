---
layout: default
title: Etanercept
parent: 僅模型預測 (L5)
nav_order: 139
evidence_level: L5
indication_count: 6
---

# Etanercept
{: .fs-9 }

證據等級: **L5** | 預測適應症: **6** 個
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

以下是根據 Evidence Pack 生成的藥師評估報告：

---

# Etanercept: From Rheumatoid Arthritis to Rheumatoid Vasculitis

## One-Sentence Summary

Etanercept (Enbrel) is a soluble p75 TNF receptor fusion protein approved globally for rheumatoid arthritis, ankylosing spondylitis, psoriatic arthritis, juvenile idiopathic arthritis, and plaque psoriasis, though it currently holds no regulatory approval in New Zealand.
The TxGNN model predicts it may be effective for **Rheumatoid Vasculitis** with a score of **99.71%**.
However, this prediction is substantially challenged by **6 clinical trials** and **20 publications** — the most directly relevant trial (WGET) demonstrated etanercept was **ineffective** in ANCA-associated vasculitis, and multiple publications document it as a paradoxical **inducer** of vasculitis as an adverse event.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Rheumatoid arthritis (global approval; no New Zealand regulatory record) |
| Predicted New Indication | Rheumatoid Vasculitis |
| TxGNN Prediction Score | 99.71% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Etanercept is a dimeric fusion protein consisting of two extracellular ligand-binding domains of the human p75 TNF receptor (TNFR2), linked to the Fc portion of human IgG1. Unlike monoclonal anti-TNF antibodies, etanercept simultaneously neutralizes both TNF-α and TNF-β (lymphotoxin-α), blocking their binding to cell surface TNF receptors and suppressing downstream NF-κB activation and pro-inflammatory cytokine cascades.

Rheumatoid vasculitis (RV) is among the most severe extra-articular manifestations of rheumatoid arthritis, characterized by necrotizing inflammation of small-to-medium blood vessels. TNF-α is prominently elevated in the vessel walls of affected patients, and RA-associated vasculitis shares many immunopathological features with the underlying joint disease — supporting the hypothesis that TNF-α blockade could suppress vascular inflammation in the same way it controls synovitis.

However, a critical paradox limits this rationale. As a soluble receptor fusion protein, etanercept can form stable immune complexes with TNF, which may deposit in vessel walls and trigger complement-mediated vasculitic injury — a mechanism distinct from monoclonal antibodies. This **paradoxical vasculitis** has been documented across multiple case reports and pharmacovigilance cohorts. Furthermore, the landmark Wegener's Granulomatosis Etanercept Trial (WGET, NCT00001901), the only completed Phase I/II trial directly testing etanercept in ANCA-associated vasculitis, demonstrated etanercept provided **no benefit** for maintaining remission. Together, the negative trial result and the drug-induced vasculitis safety signal substantially undermine the TxGNN prediction for this indication.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00001901](https://clinicaltrials.gov/study/NCT00001901) | Phase 1/2 | Completed | 60 | Direct test of etanercept (TNFR:Fc) in Wegener's granulomatosis, an ANCA-associated vasculitis. This is the WGET trial — **the pivotal negative result**: etanercept failed to maintain remission in Wegener's and did not reduce relapse rate, constituting the strongest direct evidence **against** repurposing in vasculitis. |
| [NCT05696106](https://clinicaltrials.gov/study/NCT05696106) | N/A | Unknown | 750,000 | Large pharmacovigilance database study on incident IMID risk in patients treated with biologics or immunosuppressants. Up to 25% of IMID patients develop a second IMID; supports safety monitoring rationale but highlights biologic-related autoimmune risks. |
| [NCT01557322](https://clinicaltrials.gov/study/NCT01557322) | N/A | Completed | 1,754 | Real-world characterization study of moderate RA patients newly starting etanercept vs. non-biologic therapy (BSRBR). Provides epidemiological context for RA-associated complications including vasculitis; no direct vasculitis efficacy data. |
| [NCT01579006](https://clinicaltrials.gov/study/NCT01579006) | N/A | Completed | 184 | Observational study of tocilizumab in RA patients with inadequate response to DMARDs or prior biologics. Relevant as background context for biologic treatment paradigms in RA; indirect vasculitis relevance only. |
| [NCT02590562](https://clinicaltrials.gov/study/NCT02590562) | N/A | Completed | 808 | Cross-sectional survey of bDMARD treatment patterns in Chinese RA patients (single visit, no follow-up). Provides treatment landscape data; no vasculitis efficacy outcomes. |
| [NCT07138898](https://clinicaltrials.gov/study/NCT07138898) | Phase 2 | Not yet recruiting | 80 | Perioperative immunosuppressant management in rheumatology patients undergoing elective shoulder arthroplasty. Compares different drug-holding durations for flare prevention; no direct relevance to vasculitis treatment. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|---------|
| [33058033](https://pubmed.ncbi.nlm.nih.gov/33058033/) | 2021 | Systematic Review | Clinical Rheumatology | PRISMA-based systematic review of biological drugs in rheumatoid vasculitis treatment. Evaluates the evidence base for biologics (including TNF inhibitors) in RV; key reference for the current state of evidence. |
| [28391344](https://pubmed.ncbi.nlm.nih.gov/28391344/) | 2017 | Narrative Review | Nephrology, Dialysis, Transplantation | Reviews the role of TNFα blockade in ANCA-associated vasculitis and glomerulonephritis. Despite biological rationale, clinical evidence for TNFα blockade in AAV is inconclusive; etanercept-specific data limited. |
| [28123776](https://pubmed.ncbi.nlm.nih.gov/28123776/) | 2017 | Pharmacovigilance Cohort | RMD Open | BSRBR-RA cohort comparing drug-specific risk of vasculitis-like events (VLEs) in TNFi-treated vs. non-biologic DMARD-treated RA patients. Quantifies real-world vasculitis risk associated with anti-TNF agents including etanercept. |
| [15468348](https://pubmed.ncbi.nlm.nih.gov/15468348/) | 2004 | Review | Journal of Rheumatology | Early review of TNF-alpha blockade and the risk of vasculitis. Documents the emerging paradox of vasculitis induction during anti-TNF therapy; foundational reference for this safety concern. |
| [15853915](https://pubmed.ncbi.nlm.nih.gov/15853915/) | 2005 | Case Series / Mechanistic | Scandinavian Journal of Immunology | Immunological analysis of cutaneous vasculitis associated with both etanercept and infliximab. Explores autoimmunity mechanisms underlying TNF inhibitor-induced vasculitis, including immune complex formation. |
| [15801034](https://pubmed.ncbi.nlm.nih.gov/15801034/) | 2005 | Case Report | Journal of Rheumatology | Proliferative lupus nephritis and leukocytoclastic vasculitis developing during etanercept treatment. Demonstrates that anti-TNF therapy can trigger systemic autoimmunity including renal vasculitis. |
| [12209493](https://pubmed.ncbi.nlm.nih.gov/12209493/) | 2002 | Case Report/Series | Arthritis and Rheumatism | Accelerated nodulosis and vasculitis following etanercept therapy for RA. One of the earliest reports documenting paradoxical vasculitis as a drug-induced adverse event. |
| [11792895](https://pubmed.ncbi.nlm.nih.gov/11792895/) | 2002 | Case Report | Rheumatology (Oxford) | Etanercept and infliximab associated with cutaneous vasculitis in RA patients. Provides early pharmacovigilance evidence for class-level vasculitis risk with TNF inhibitors. |
| [41327089](https://pubmed.ncbi.nlm.nih.gov/41327089/) | 2025 | Case Report | BMC Nephrology | RA patient developing membranous nephropathy and ANCA-associated vasculitis successively; linked to biological agent use. Highlights increasing reports of RA-related nephropathy in the era of biologics. |
| [31668853](https://pubmed.ncbi.nlm.nih.gov/31668853/) | 2019 | Comparative Study | Biologicals | Real-world national cohort comparing biosimilar etanercept (SB4) to originator ETN in RA. Provides reference efficacy and safety benchmarks for etanercept class applicable to potential RV use. |

---

## Safety Considerations

Please refer to the package insert for safety information.

**Critical safety signal specific to this indication:** Multiple pharmacovigilance studies and case series document that etanercept therapy is associated with **paradoxical induction of vasculitis** — including leukocytoclastic vasculitis, cutaneous vasculitis, and ANCA-associated vasculitis — as an adverse drug reaction. This signal is directly contradictory to the intended therapeutic goal of treating rheumatoid vasculitis, and constitutes a special concern requiring prospective monitoring protocol design if further investigation is pursued.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The most directly relevant clinical evidence — the Wegener's Granulomatosis Etanercept Trial (WGET, NCT00001901, Phase I/II, n=60, completed) — demonstrated that etanercept **failed to maintain remission** in ANCA-associated vasculitis, providing strong negative evidence against this repurposing direction. Compounding this, etanercept has been documented as a paradoxical **inducer** of vasculitis in multiple pharmacovigilance cohorts and case series, creating a contradictory therapeutic signal when targeting rheumatoid vasculitis. The overall evidence level is L3, with no supportive RCT data.

**To proceed, the following is needed:**
- Mechanistic clarification of whether the p75-TNFR fusion protein structure confers different vasculitis risk compared to anti-TNF monoclonal antibodies (infliximab, adalimumab), which may show more favorable RV evidence
- Subgroup stratification: investigate whether specific RV subtypes (e.g., isolated cutaneous RV vs. systemic RV with organ involvement) might respond differently to ETN
- Systematic comparison of all TNF inhibitor classes in RV outcomes — a class-effect difference may exist that repositions monoclonal anti-TNF agents as preferred candidates
- Formal pharmacovigilance review quantifying the absolute risk of ETN-induced vasculitis vs. the expected therapeutic benefit in RV patients
- Mechanism of action documentation (DrugBank API query) to support or refute the biological plausibility
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

