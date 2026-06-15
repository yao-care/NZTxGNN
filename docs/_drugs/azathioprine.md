---
layout: default
title: Azathioprine
parent: 僅模型預測 (L5)
nav_order: 41
evidence_level: L5
indication_count: 10
---

# Azathioprine
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

# AZATHIOPRINE: From Transplant Rejection to Inflammatory Bowel Disease

## One-Sentence Summary

Azathioprine is a thiopurine immunosuppressant historically used for organ transplant rejection prevention and autoimmune disease management, but currently not registered in New Zealand.
The TxGNN model predicts it may be effective for **Inflammatory Bowel Disease (IBD)** — encompassing both Crohn's disease and ulcerative colitis —
with **50+ clinical trials** and **20+ publications** currently supporting this direction, earning the highest **L1** evidence rating in this multi-indication analysis.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | No registered indication in New Zealand (product not marketed) |
| Predicted New Indication | Inflammatory Bowel Disease (IBD) |
| TxGNN Prediction Score | 99.52% |
| Evidence Level | L1 |
| New Zealand Market Status | Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available from the local regulatory database. Based on well-established clinical pharmacology, azathioprine is a prodrug converted in vivo first to 6-mercaptopurine (6-MP) and further to 6-thioguanine nucleotides (6-TGN). These active metabolites block de novo purine synthesis by inhibiting HPRT and PRPP amidotransferase, selectively suppressing T and B lymphocyte proliferation and inducing T cell apoptosis through the Rac1-mediated mitochondrial pathway. This cascade also reduces pro-inflammatory cytokine secretion — particularly IL-2 and TNF-α — in the intestinal mucosa.

IBD is driven by aberrant, T cell-mediated mucosal immune activation. Azathioprine's selective lymphocyte-suppressing and cytokine-reducing mechanism directly targets this underlying pathophysiology, making the mechanistic rationale highly compelling. Notably, the TxGNN model independently ranked ulcerative colitis as a second high-confidence indication (Rank 9, TxGNN score 99.33%, L1 evidence, "Proceed with Guardrails"), reinforcing a coherent IBD-class signal across two related predictions.

Azathioprine carries over 45 years of clinical experience in IBD globally. Four successive Cochrane systematic reviews (2007, 2012, 2016, and a 2025 update) alongside a pooled meta-analysis have confirmed its efficacy in maintaining corticosteroid-free remission in both Crohn's disease and ulcerative colitis. It is recommended as a first-line immunomodulator in major international IBD guidelines (ECCO, AGA, BSG). Its non-registration in New Zealand most likely reflects a market access gap rather than any deficit in clinical evidence.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|-------------|-------|--------|------------|--------------|
| [NCT00094458](https://clinicaltrials.gov/study/NCT00094458) | Phase 3 | Completed | 508 | Infliximab vs. Infliximab+AZA vs. AZA monotherapy in biologic/immunomodulator-naïve Crohn's disease; landmark trial directly evaluating AZA as both monotherapy and combination strategy |
| [NCT03101800](https://clinicaltrials.gov/study/NCT03101800) | Phase 3 | Unknown | 84 | Low-dose AZA + Allopurinol vs. standard AZA monotherapy in ulcerative colitis; evaluates whether co-administration improves response and reduces adverse events in up to 50% treatment failures |
| [NCT03464136](https://clinicaltrials.gov/study/NCT03464136) | Phase 3b | Completed | 386 | Ustekinumab vs. adalimumab in biologic-naïve Crohn's disease after failure of conventional therapy including AZA; benchmarks outcomes and positions AZA in the treatment sequence |
| [NCT02177071](https://clinicaltrials.gov/study/NCT02177071) | Phase 4 | Completed | 211 | SPARE trial: IFX+antimetabolites vs. IFX monotherapy vs. antimetabolite (AZA) monotherapy in Crohn's disease in sustained steroid-free remission; informs de-escalation decisions |
| [NCT01015391](https://clinicaltrials.gov/study/NCT01015391) | N/A | Unknown | 100 | T2 preparation vs. AZA for maintenance of clinical and endoscopic remission in Crohn's disease post-surgical resection; direct head-to-head RCT comparing AZA to an alternative agent |
| [NCT05584228](https://clinicaltrials.gov/study/NCT05584228) | N/A | Not Yet Recruiting | 150 | SMART trial: AZA + subcutaneous infliximab vs. ileocecal resection in symptomatic small bowel Crohn's disease; positions AZA-containing medical therapy as the active comparator arm |
| [NCT00984568](https://clinicaltrials.gov/study/NCT00984568) | Phase 3 | Terminated | 28 | Conventional step-up strategy (AZA as core agent) vs. early infliximab monotherapy in moderate-to-severe active UC; AZA-containing approach as active control |
| [NCT03185611](https://clinicaltrials.gov/study/NCT03185611) | Phase 3 | Unknown | 120 | Rifaximin + thiopurine vs. thiopurine alone for postoperative endoscopic recurrence prevention in Crohn's disease; evaluates AZA combination optimisation |
| [NCT04304950](https://clinicaltrials.gov/study/NCT04304950) | Phase 4 | Completed | 28 | Chronotherapy in IBD: morning vs. evening AZA/6-MP dosing; investigates whether dosing time affects disease activity and treatment outcomes |
| [NCT00577538](https://clinicaltrials.gov/study/NCT00577538) | N/A | Completed | 7 | Prevalence and risk factors for lymphoproliferative disease in IBD patients receiving AZA/6-MP; provides critical long-term safety surveillance data |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|------|------|---------|--------------|
| [40013523](https://pubmed.ncbi.nlm.nih.gov/40013523/) | 2025 | Cochrane Review | Cochrane Database Syst Rev | 2025 updated Cochrane review confirms AZA/6-MP efficacy for maintenance of remission in UC; most current evidence synthesis with updated trial inclusion |
| [39586616](https://pubmed.ncbi.nlm.nih.gov/39586616/) | 2025 | RCT | Gut | ACTIVE trial: top-down infliximab+AZA superior to AZA monotherapy for maintenance in acute severe UC patients responding to intravenous steroids |
| [27192092](https://pubmed.ncbi.nlm.nih.gov/27192092/) | 2016 | Cochrane Review | Cochrane Database Syst Rev | Cochrane systematic review: AZA/6-MP vs. placebo for maintenance of remission in ulcerative colitis; evidence base reviewed at Level 1 |
| [22972046](https://pubmed.ncbi.nlm.nih.gov/22972046/) | 2012 | Cochrane Review | Cochrane Database Syst Rev | Earlier Cochrane review establishing foundational evidence for AZA/6-MP maintenance efficacy in UC; included in the 2016 and 2025 updates |
| [19392869](https://pubmed.ncbi.nlm.nih.gov/19392869/) | 2009 | Meta-analysis | Aliment Pharmacol Ther | Meta-analysis of 7 RCTs: AZA and 6-MP significantly superior to placebo for UC maintenance; confirms therapeutic equivalence of the two thiopurines |
| [29293971](https://pubmed.ncbi.nlm.nih.gov/29293971/) | 2018 | Clinical Review | J Crohn's Colitis | State-of-the-art thiopurine review: indications, efficacy, safety monitoring, drug optimisation with allopurinol, and emerging pharmacogenomic insights |
| [19072367](https://pubmed.ncbi.nlm.nih.gov/19072367/) | 2008 | Mechanistic Review | Expert Rev Gastroenterol Hepatol | Molecular mechanism of AZA in IBD: 6-TGN-induced T cell apoptosis via Rac1/mitochondrial pathway; summarises clinical trial evidence confirming mechanistic rationale |
| [10499471](https://pubmed.ncbi.nlm.nih.gov/10499471/) | 1999 | Clinical Review | Scand J Gastroenterol Suppl | Review supporting long-term AZA therapy in Crohn's disease; documents clinical approval basis and early efficacy/safety data |
| [30954317](https://pubmed.ncbi.nlm.nih.gov/30954317/) | 2019 | Clinical Review | Gastroenterol Hepatol | Evidence on thiopurine discontinuation in IBD; informs clinical guidance on AZA therapy duration and withdrawal strategies |
| [37586320](https://pubmed.ncbi.nlm.nih.gov/37586320/) | 2023 | Mechanistic Study | Cell Reports Medicine | Gut microbiota (Blautia wexlerae) promotes AZA therapy failure in IBD via decreased 6-MP bioavailability; identifies microbiome as a novel modifier of treatment response |

---

## New Zealand Market Information

Azathioprine is currently not registered in New Zealand. No product authorizations are on file in the Medsafe database. This status reflects a market access gap rather than an absence of therapeutic evidence — azathioprine is a guideline-recommended, standard-of-care therapy for IBD in numerous countries including the United States, United Kingdom, the European Union, Australia, and Japan.

---

## Cytotoxicity

Azathioprine belongs to the thiopurine (purine antimetabolite) class, originally developed as anticancer therapy before its immunosuppressant properties were recognized for transplantation and autoimmune disease. It is classified under cytotoxic drug handling protocols in most jurisdictions.

| Item | Content |
|------|---------|
| Cytotoxicity Classification | Conventional cytotoxic — Thiopurine/Purine antimetabolite class |
| Myelosuppression Risk | High — Dose-dependent leukopenia and thrombocytopenia are the most serious adverse effects; NUDT15 and TPMT genetic variants significantly increase risk of severe myelosuppression, particularly in East Asian populations with higher NUDT15 risk-allele frequency |
| Emetogenicity Classification | Low |
| Monitoring Items | CBC with differential and platelets (weekly for first month, biweekly for second and third month, then every 3 months thereafter); liver and renal function tests at similar intervals; 6-TGN and 6-MMP metabolite levels to guide dose optimisation; TPMT and NUDT15 genotyping strongly recommended prior to initiation |
| Handling Protection | Must follow cytotoxic drug handling regulations — tablets must not be crushed or split without appropriate protective equipment; disposal according to cytotoxic waste protocols |

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Azathioprine holds the strongest evidence base among all TxGNN-predicted indications in this multi-indication pack: four Cochrane systematic reviews, a pooled meta-analysis, and multiple Phase 3 randomized controlled trials provide Level 1 evidence confirming its efficacy for maintaining remission in inflammatory bowel disease. Its non-registration in New Zealand reflects a market access gap, not a clinical evidence deficit. An independent second prediction (ulcerative colitis, Rank 9, L1) further corroborates the IBD therapeutic signal.

**To proceed, the following is needed:**
- Obtain and review complete local package insert data — warnings, contraindications, and drug interactions are currently classified as data gaps and must be resolved before clinical deployment
- Confirm detailed mechanism of action data via DrugBank API query (currently flagged as High-severity data gap)
- Establish pre-treatment TPMT and NUDT15 pharmacogenomic testing as mandatory practice — particularly critical for East Asian patients, who carry a substantially higher frequency of NUDT15 risk variants associated with severe myelosuppression
- Define therapeutic drug monitoring protocol: CBC and liver/renal function at defined intervals; 6-TGN target range (235–450 pmol/8×10⁸ RBC) and 6-MMP upper limit (<5,700 pmol/8×10⁸ RBC) for dose guidance
- Evaluate the low-dose AZA + allopurinol co-administration strategy (NCT03101800) as an optimisation option to redirect metabolism toward 6-TGN, improve therapeutic response, and reduce 6-MMP-related hepatotoxicity
- Conduct a formal drug-drug interaction assessment, with priority focus on: allopurinol (requires 75% AZA dose reduction due to xanthine oxidase inhibition), 5-ASA agents (may inhibit TPMT and increase myelosuppression risk), angiotensin-converting enzyme inhibitors (increased leukopenia risk), and co-prescribed biologics
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

