---
layout: default
title: Iloprost
parent: 僅模型預測 (L5)
nav_order: 171
evidence_level: L5
indication_count: 9
---

# Iloprost
{: .fs-9 }

證據等級: **L5** | 預測適應症: **9** 個
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

Using the report template you supplied (no additional skill applies — this is a direct content-generation task from a fully specified evidence pack).

Note on scope: this evidence pack (`TW-DB01088-multi`) screened **9 candidate indications**, not one. The TxGNN top-score candidate (rank 1, hypotrichosis) has zero supporting evidence and is not a credible lead. I've built the report around the strongest evidence-backed candidate (rank 3, **PAH associated with congenital heart disease** — L2, Proceed with Guardrails), and added a summary table of the other 8 candidates for completeness.

---

# Iloprost: From Pulmonary Arterial Hypertension to Pulmonary Arterial Hypertension Associated with Congenital Heart Disease

## One-Sentence Summary

> Iloprost is a synthetic prostacyclin (PGI2) analogue and IP-receptor agonist, internationally approved for WHO Group 1 pulmonary arterial hypertension (inhaled and intravenous formulations); it is not currently marketed in New Zealand.
> The TxGNN model, together with real-world evidence, supports its use in **Pulmonary Arterial Hypertension Associated with Congenital Heart Disease** — a recognized WHO Group 1 subtype — with **1 clinical trial** and **20 publications** currently identified.
> This same evidence pack also flagged two other PAH subtypes (connective tissue disease–associated and HIV-associated PAH) at the same evidence tier, while several non-PAH predictions (e.g., hair-loss disorders) had no supporting evidence at all.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Pulmonary Arterial Hypertension (WHO Group 1) — established international indication (inhaled/IV); no New Zealand–specific approved indication text on file |
| Predicted New Indication | Pulmonary Arterial Hypertension Associated with Congenital Heart Disease |
| TxGNN Prediction Score | 99.32% (rank 5603) |
| Evidence Level | L2 |
| New Zealand Market Status | Not marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Detailed, structured mechanism-of-action data was not retrieved from DrugBank for this candidate (data gap DG002). Based on information embedded in the evidence pack itself, iloprost is a synthetic prostacyclin (PGI2) analogue that acts as an IP-receptor agonist, producing pulmonary and systemic vasodilation and inhibiting platelet aggregation. It is already approved and used clinically for WHO Group 1 pulmonary arterial hypertension via inhaled (e.g., Ventavis) and intravenous (e.g., Ilomedin) formulations.

Pulmonary arterial hypertension associated with congenital heart disease (including Eisenmenger physiology) is classified within the same WHO Group 1 PAH umbrella as idiopathic PAH. The underlying pathophysiology — pulmonary vascular remodeling, elevated pulmonary vascular resistance, and endothelial dysfunction — is shared across Group 1 subtypes, which is why prostacyclin-pathway therapies are used as a class across this entire group.

Because iloprost's vasodilatory and antiplatelet mechanism targets the pathophysiology common to all Group 1 PAH subtypes rather than being specific to idiopathic disease, extending its use to the congenital-heart-disease subtype is a mechanistically direct, low-novelty extension rather than a speculative new mechanism — consistent with the L2/"Proceed with Guardrails" scoring assigned in this evidence pack.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT01383083](https://clinicaltrials.gov/study/NCT01383083) | N/A | Unknown | 42 | Safety, tolerability, and hemodynamic effects of iloprost in adult PAH related to congenital heart disease (Eisenmenger physiology); relevance graded A (directly targets this population), but phase is unlabeled and trial status is unverified |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [28608969](https://pubmed.ncbi.nlm.nih.gov/28608969/) | 2017 | Cohort | Clin Exp Pharmacol Physiol | Iloprost's effect on endothelial biomarkers (NO, ET-1, ADMA, Gal-3, BNP, UA) in CHD-PAH patients |
| [29426959](https://pubmed.ncbi.nlm.nih.gov/29426959/) | 2018 | Cohort | Pediatr Cardiol | Acute hemodynamic effects and safety of inhaled iloprost in children with simple CHD-associated PAH |
| [24729548](https://pubmed.ncbi.nlm.nih.gov/24729548/) | 2015 | Cohort | Pediatr Pulmonol | Long-term effects of inhaled iloprost in children with pulmonary hypertension |
| [30719004](https://pubmed.ncbi.nlm.nih.gov/30719004/) | 2018 | Cohort | Front Pharmacol | Cardiac MRI study showing acute iloprost inhalation improves right ventricular function in PAH |
| [19436672](https://pubmed.ncbi.nlm.nih.gov/19436672/) | 2009 | Review | Vasc Health Risk Manag | Review of inhaled iloprost for pulmonary hypertension control in children |
| [36010107](https://pubmed.ncbi.nlm.nih.gov/36010107/) | 2022 | Case Series | Children (Basel) | Long-term add-on sildenafil + bosentan + iloprost strategy in Eisenmenger syndrome (n=5) |
| [25316472](https://pubmed.ncbi.nlm.nih.gov/25316472/) | 2014 | Case Report | Saudi Med J | Intensive inhaled iloprost + sildenafil resolved pericardial effusion in unrepaired VSD with severe PAH |
| [27053694](https://pubmed.ncbi.nlm.nih.gov/27053694/) | 2016 | Consensus Statement | Heart | European expert consensus on hemodynamic assessment and vasoreactivity testing in pediatric pulmonary vascular disease |
| [16919006](https://pubmed.ncbi.nlm.nih.gov/16919006/) | 2006 | Review | Eur J Clin Invest | Treatment options, including prostacyclin analogues, in children with PAH |
| [17990138](https://pubmed.ncbi.nlm.nih.gov/17990138/) | 2007 | Registry | Swiss Med Wkly | Swiss national PAH registry — paediatric experience |

---

## New Zealand Market Information

Iloprost is not currently marketed in New Zealand — no Medsafe authorization records were found (0 licenses on file).

---

## Other Candidate Indications Screened (This Evidence Pack)

| Rank | Predicted Indication | TxGNN Score | Evidence Level | Decision |
|------|----------------------|-------------|-----------------|----------|
| 1 | Hypotrichosis simplex of the scalp | 99.45% | L5 | Hold |
| 2 | Congenital hypotrichosis milia | 99.33% | L5 | Hold |
| 4 | Pulmonary arteriovenous malformation | 99.31% | L4 | Hold |
| 5 | PAH associated with connective tissue disease | 99.21% | L2 | Proceed with Guardrails |
| 6 | PAH associated with HIV infection | 99.21% | L2 | Proceed with Guardrails |
| 7 | PAH associated with chronic hemolytic anemia | 99.21% | L4 | Research Question |
| 8 | PAH associated with schistosomiasis | 99.21% | L4 | Research Question |
| 9 | Diffuse alopecia areata | 99.10% | L5 | Hold |

Ranks 5 and 6 have comparable evidence strength to the featured indication and warrant the same guardrails; ranks 1, 2, 4, and 9 lack a plausible mechanistic or evidentiary basis and should not be pursued without new data.

---

## Safety Considerations

Please refer to the package insert for safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Iloprost's vasodilatory/antiplatelet mechanism is already validated across WHO Group 1 PAH, and CHD-associated PAH is supported by 20 publications plus one directly relevant (though status-unverified) clinical trial. However, the primary trial identified (NCT01383083) has unknown status and no confirmed completion outcome, and no MOA or formal safety-label data are on file for this drug.

**To proceed, the following is needed:**
- Resolve data gap DG001 (TFDA/international package insert warnings and contraindications) — currently Blocking, and must be cleared before any S1 safety evaluation
- Resolve data gap DG002 (formal DrugBank MOA record) to support the mechanistic rationale with structured data
- Verify current status and outcome of NCT01383083
- Since iloprost is not marketed in New Zealand, confirm regulatory pathway (e.g., new registration vs. named-patient/compassionate use) before any local development
- Consider parallel evaluation of the two other L2 candidates (CTD-associated and HIV-associated PAH), which share the same class-level rationale
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

