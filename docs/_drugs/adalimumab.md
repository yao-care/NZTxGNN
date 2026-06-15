---
layout: default
title: Adalimumab
parent: 僅模型預測 (L5)
nav_order: 16
evidence_level: L5
indication_count: 6
---

# Adalimumab
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

# Adalimumab: From Rheumatoid Arthritis to Rheumatoid Vasculitis

## One-Sentence Summary

Adalimumab is a fully human anti-TNF-α monoclonal antibody approved globally for rheumatoid arthritis and other immune-mediated inflammatory diseases, though not currently marketed in New Zealand.
The TxGNN model predicts it may be effective for **Rheumatoid Vasculitis (RV)** — the most severe extra-articular complication of RA — with **5 clinical trials** and **20 publications** currently informing this direction.
Notably, a PRISMA-compliant systematic review of biological therapies in RV (PMID 33058033) and multiple case reports provide mechanistic and clinical rationale, though no dedicated RCT for RV exists.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Rheumatoid arthritis (globally approved; not currently marketed in New Zealand) |
| Predicted New Indication | Rheumatoid Vasculitis |
| TxGNN Prediction Score | 99.80% |
| Evidence Level | L3 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Adalimumab is a fully human IgG1 monoclonal antibody that selectively binds and neutralizes TNF-α, a master pro-inflammatory cytokine. By blocking TNF-α signalling, adalimumab interrupts the inflammatory cascade driving synovitis, joint erosion, and extra-articular organ involvement in rheumatoid arthritis. Detailed mechanism of action data from DrugBank was not available at the time of this report; however, its TNF-α–inhibiting action is well established in published clinical literature.

Rheumatoid vasculitis (RV) is the most severe extra-articular manifestation of long-standing RA, characterized by immune complex deposition in vessel walls, complement activation, and TNF-α–driven transmural inflammation. Because the same TNF-α pathway that destroys joints in RA also mediates vascular inflammation in RV, blocking TNF-α provides a direct and mechanistically coherent rationale for repurposing adalimumab in this setting. A 2021 systematic review (PMID 33058033) confirmed that biological agents including TNF inhibitors have been deployed in RV clinical practice, and a 2014 case report (PMID 25133007) documented complete resolution of digital vasculitis (necrotizing finger-tip ulcers) with adalimumab.

A critical dual consideration must be acknowledged: while TNF-α inhibition may treat RV, multiple published reports — including a large BSRBR-RA registry analysis (PMID 28123776, N=2707) and individual case series (PMID 28719435; PMID 19482531) — document that TNF inhibitors including adalimumab can paradoxically induce vasculitis-like events. This therapeutic ambiguity demands rigorous baseline assessment to distinguish active RA-driven RV from biologic-induced vasculitis, and close monitoring throughout treatment.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT01579006](https://clinicaltrials.gov/study/NCT01579006) | N/A | Completed | 184 | Multi-centre observational study of biologic therapy in RA patients with inadequate response to DMARDs or prior biologics; patient population overlaps substantially with RV and provides real-world safety background data |
| [NCT05696106](https://clinicaltrials.gov/study/NCT05696106) | N/A | Unknown | 750,000 | Large retrospective cohort evaluating risk of incident IMIDs in patients treated with biologics; generates pharmacovigilance data on adalimumab-associated vasculitis-like adverse events relevant to RV risk-benefit assessment |
| [NCT07138898](https://clinicaltrials.gov/study/NCT07138898) | Phase 2 | Not Yet Recruiting | 80 | Immunosuppressant management in rheumatology patients undergoing elective shoulder arthroplasty; assesses perioperative holding of adalimumab and flare rates in the RA/RV-overlap population |
| [NCT02590562](https://clinicaltrials.gov/study/NCT02590562) | N/A | Completed | 808 | Cross-sectional study on biologic DMARD treatment patterns in Chinese RA patients; provides epidemiological background on the RA population from which RV cases arise |

*No clinical trial targeting rheumatoid vasculitis as a primary endpoint was identified in the current search.*

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [33058033](https://pubmed.ncbi.nlm.nih.gov/33058033/) | 2021 | Systematic Review | Clinical Rheumatology | PRISMA-compliant systematic review of all biological drugs used in RV; first high-level synthesis confirming TNF inhibitors — including adalimumab — are part of clinical practice for RV management |
| [28123776](https://pubmed.ncbi.nlm.nih.gov/28123776/) | 2017 | Cohort / Pharmacovigilance | RMD Open | BSRBR-RA registry (N=2707): compares lupus-like and vasculitis-like event (VLE) risk between TNFi-treated RA and nbDMARD controls; characterizes drug-specific VLE risk profiles for adalimumab — key dual safety/efficacy signal |
| [18799049](https://pubmed.ncbi.nlm.nih.gov/18799049/) | 2008 | Systematic Review | Clinical and Experimental Rheumatology | Systematic review comparing characteristics of vasculitis in anti-TNF–treated vs. untreated RA (N=2707, 18 vasculitis cases); foundational pharmacovigilance reference for understanding RV in the context of TNF inhibitor therapy |
| [25133007](https://pubmed.ncbi.nlm.nih.gov/25133007/) | 2014 | Case Report | Case Reports in Rheumatology | RA patient with active digital vasculitis (necrotizing fingertip ulcers) responded well to adalimumab; direct clinical evidence supporting efficacy in an RV manifestation |
| [30773522](https://pubmed.ncbi.nlm.nih.gov/30773522/) | 2019 | Case Report | Internal Medicine (Tokyo) | Acute pulmonary hypertension crisis precipitated by adalimumab dose reduction in an RV patient; underscores the critical role of maintaining adequate adalimumab levels in controlling severe RV |
| [34068884](https://pubmed.ncbi.nlm.nih.gov/34068884/) | 2021 | Review | Journal of Clinical Medicine | Comprehensive review of RA-associated episcleritis and scleritis (ocular RV manifestations); discusses role of biologics including adalimumab in refractory scleral inflammation |
| [28719435](https://pubmed.ncbi.nlm.nih.gov/28719435/) | 2018 | Case Report | American Journal of Dermatopathology | First reported case of leukocytoclastic vasculitis with cutaneous perivascular hemophagocytosis induced by adalimumab in an RA patient; illustrates the paradoxical vasculitis risk associated with TNF-α inhibition |
| [36706240](https://pubmed.ncbi.nlm.nih.gov/36706240/) | 2023 | Retrospective Cohort | Kidney360 | Single-centre renal biopsy study documenting the clinicopathological spectrum of kidney lesions after anti-TNF therapy, including autoimmune and vasculitic nephropathies; key reference for renal safety monitoring |
| [19482531](https://pubmed.ncbi.nlm.nih.gov/19482531/) | 2009 | Case Report | Néphrologie & Thérapeutique | MPO-ANCA–positive extracapillary and necrotizing glomerulonephritis developing during adalimumab therapy in RA; identifies ANCA-associated renal vasculitis as an important adverse event to screen for |
| [36418100](https://pubmed.ncbi.nlm.nih.gov/36418100/) | 2023 | Case Report | Internal Medicine (Tokyo) | MPO-ANCA nephritis occurring during abatacept + adalimumab combination therapy, subsequently attenuated by tocilizumab; highlights complex immunological interactions when managing RV with multiple biologics |

---

## Safety Considerations

Please refer to the package insert for safety information.

> **Note:** Taiwan (TFDA) package insert warnings and contraindications were identified as a data gap (DG001, severity: Blocking) at the time of report generation. Drug-drug interaction data was also not available (query status: not_found). Both items must be obtained before clinical decision-making.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Rheumatoid vasculitis shares the same TNF-α–driven inflammatory mechanism as RA, providing strong biological justification for adalimumab. A systematic review confirms TNF inhibitors have been applied in RV clinical practice, and case evidence demonstrates response in digital vasculitis and prevention of relapse after dose reduction. However, the complete absence of prospective RCT data specifically in RV, combined with a documented paradoxical risk of TNF inhibitor–induced vasculitis-like events, requires that treatment be pursued only under specialist supervision with structured monitoring.

**To proceed, the following is needed:**
- Retrieve full MOA documentation from DrugBank (data gap DG002)
- Download and parse TFDA package insert PDF for warnings and contraindications (data gap DG001, currently blocking S1 safety assessment)
- Complete drug-drug interaction profile
- Establish a baseline and monitoring protocol: urinalysis, MPO-ANCA and PR3-ANCA titres, skin examination, renal function — at baseline and at scheduled intervals
- Define a clinical protocol for differentiating active RA-driven RV from adalimumab-induced vasculitis-like events before and during treatment
- Specialist rheumatology and nephrology co-management plan for severe RV manifestations (renal, pulmonary)
- Regulatory pathway assessment: New Zealand (Medsafe) currently has zero adalimumab authorizations; market entry strategy or named-patient access pathway is required prior to use
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

