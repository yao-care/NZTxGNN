---
layout: default
title: Bumetanide
parent: 僅模型預測 (L5)
nav_order: 55
evidence_level: L5
indication_count: 1
---

# Bumetanide
{: .fs-9 }

證據等級: **L5** | 預測適應症: **1** 個
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

# Bumetanide: From Loop Diuresis to Acute Pulmonary Heart Disease

## One-Sentence Summary

Bumetanide is a potent loop diuretic classically used for fluid overload conditions including oedema associated with congestive heart failure, hepatic, and renal disease. The TxGNN model predicts it may be effective for **Acute Pulmonary Heart Disease (Cor Pulmonale)**, with **3 clinical trials** and **5 publications** currently supporting this direction. The mechanistic rationale is strong, though the direct clinical evidence base remains at the observational/review level.

---

## Quick Overview

| Item | Content |
|------|---------|
| Original Indication | Oedema associated with congestive heart failure, hepatic and renal disease (loop diuretic) |
| Predicted New Indication | Acute Pulmonary Heart Disease (Cor Pulmonale) |
| TxGNN Prediction Score | 99.58% |
| Evidence Level | L3 |
| Taiwan Market Status | Not Marketed (未上市) |
| Number of Authorizations | 0 |
| Recommended Decision | Proceed with Guardrails |

---

## Why Is This Prediction Reasonable?

Bumetanide is a high-ceiling loop diuretic that acts primarily by inhibiting NKCC2 (Na⁺-K⁺-2Cl⁻ co-transporter) in the thick ascending limb of the Loop of Henle, producing a rapid and potent natriuresis and diuresis. Its molar potency is approximately 40 times that of furosemide, meaning therapeutically equivalent effects are achieved at much lower doses. By reducing circulating blood volume, bumetanide directly lowers cardiac preload and pulmonary venous pressure — both central pathophysiological drivers of acute fluid overload in cor pulmonale.

In acute pulmonary heart disease, right ventricular strain secondary to elevated pulmonary artery pressures leads to systemic venous congestion and progressive fluid retention. Bumetanide's capacity to rapidly offload this volume burden makes it mechanistically applicable. Additionally, there is emerging evidence that bumetanide's inhibition of NKCC1 (the peripheral isoform) may induce pulmonary vascular smooth muscle relaxation, potentially attenuating pulmonary vascular resistance (PVR) — though this effect in humans with cor pulmonale has not yet been conclusively established.

One important caution: in acute right heart failure (particularly massive pulmonary embolism-associated cor pulmonale), the right ventricle is often preload-dependent to maintain cardiac output. Aggressive diuresis risks reducing output further. This means bumetanide's application in this setting requires careful haemodynamic monitoring and strict dose titration, distinguishing it from its more straightforward use in left-sided heart failure oedema.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT07375212](https://clinicaltrials.gov/study/NCT07375212) | Phase 4 | Withdrawn | 0 | Planned to test whether a single 4 mg intranasal dose of bumetanide acutely reduces pulmonary artery pressure and blood volume in outpatient heart failure patients with implanted haemodynamic monitoring devices (CardioMEMS / Cordella). Withdrawn before enrolment — no data available. |
| [NCT06885164](https://clinicaltrials.gov/study/NCT06885164) | N/A | Recruiting | 200 | Observational study evaluating seismocardiography as a remote monitoring technology in heart failure patients. Not a bumetanide intervention trial; provides disease-context background only. |
| [NCT05580510](https://clinicaltrials.gov/study/NCT05580510) | Phase 2/3 | Unknown | 160 | Investigates empagliflozin and sacubitril/valsartan in adults with CHD-associated heart failure with reduced ejection fraction. Bumetanide is not the study drug; provides background on HF treatment in complex populations. |

> **Note:** No completed interventional trials directly testing bumetanide in acute pulmonary heart disease were identified. The most directly relevant trial (NCT07375212) was withdrawn before enrolment.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3304383](https://pubmed.ncbi.nlm.nih.gov/3304383/) | 1987 | Clinical Haemodynamic Study | *British Journal of Clinical Pharmacology* | IV bumetanide (25 µg/kg) in 24 patients with acute or chronic heart failure: reduced cardiac index and pulmonary artery occlusion pressure (PAOP) at rest; increased systemic arterial resistance. Provides direct mechanistic evidence of bumetanide's haemodynamic effects in acute HF. |
| [6391889](https://pubmed.ncbi.nlm.nih.gov/6391889/) | 1984 | Pharmacological Review | *Drugs* | Comprehensive review of bumetanide pharmacodynamics and pharmacokinetics. Confirms efficacy in oedema associated with congestive heart failure, acute pulmonary congestion, and renal/hepatic disease. Establishes foundational evidence for use in fluid overload states relevant to cor pulmonale. |
| [19142155](https://pubmed.ncbi.nlm.nih.gov/19142155/) | 2009 | Narrative Review | *American Journal of Therapeutics* | Reviews therapeutic options for acute decompensated heart failure. Highlights loop diuretics (including bumetanide) as first-line agents for managing acute fluid overload, directly relevant to the acute pulmonary heart disease setting. |
| [19843838](https://pubmed.ncbi.nlm.nih.gov/19843838/) | 2009 | Comparative Review | *Annals of Pharmacotherapy* | Compares loop diuretics (furosemide, bumetanide, torsemide) for safety, efficacy, pharmacokinetics, and cost. Supports bumetanide's clinical utility and notes superior oral bioavailability over furosemide in some settings. |
| [39366035](https://pubmed.ncbi.nlm.nih.gov/39366035/) | 2024 | Epidemiological Study | *American Journal of Emergency Medicine* | Large-scale analysis of heart failure ED presentations in the US (2016–2023). Characterises morbidity, admission rates, and treatment patterns. Contextualises the clinical burden of acute HF, supporting the unmet need this repurposing candidate targets. |

---

## Taiwan Market Information

Bumetanide is currently **not registered or marketed in Taiwan**. No regulatory authorizations have been identified in the TFDA database.

> For jurisdictions where bumetanide is approved (e.g., US, EU), prescribing information should be referenced directly for dosing, warnings, and contraindications.

---

## Safety Considerations

Detailed local package insert data (TFDA warnings and contraindications) was not available at the time of this report. Based on established pharmacological knowledge of loop diuretics, the following are the primary safety considerations relevant to the acute pulmonary heart disease indication:

- **Risk of Excessive Preload Reduction**: In right ventricular failure (e.g., massive PE-associated cor pulmonale), the RV depends on adequate preload to maintain output. Over-diuresis may precipitate haemodynamic collapse — strict titration and invasive or non-invasive haemodynamic monitoring are essential.
- **Electrolyte Imbalances**: Hypokalaemia, hyponatraemia, and hypomagnesaemia are class effects of loop diuretics and may exacerbate cardiac arrhythmias in critically ill patients.
- **Ototoxicity**: A dose-related risk at high intravenous doses, particularly with concomitant aminoglycoside use.

> Please refer to the originator package insert for the complete list of warnings, contraindications, and drug interactions. Local TFDA package insert data was not retrieved at the time of analysis.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Bumetanide's mechanism of action (NKCC2-mediated loop diuresis with preload reduction) is directly and plausibly linked to the haemodynamic pathophysiology of acute pulmonary heart disease. While no completed interventional trials specifically in cor pulmonale were identified, the drug's established clinical use in acute decompensated heart failure provides indirect but substantial supportive evidence (L3), and the TxGNN prediction score of 99.58% reflects strong model confidence based on biological network relationships.

**To proceed, the following is needed:**

- **Retrieve TFDA package insert** (DG001): Complete local safety data (contraindications, warnings) is required before any clinical application or safety dossier can be finalised
- **Confirm MOA data from DrugBank** (DG002): Formal mechanistic annotation will strengthen the repurposing justification and regulatory narrative
- **Conduct a focused systematic review** on bumetanide in right heart failure and pulmonary hypertension to identify any unpublished or grey-literature evidence not captured by the current PubMed search
- **Design a prospective pilot study**: Given the withdrawn NCT07375212, a small prospective haemodynamic study in cor pulmonale patients with pulmonary artery pressure monitoring would constitute the highest-priority evidence gap
- **Define patient selection criteria**: Distinguish preload-dependent (RV failure from massive PE) vs. preload-independent (chronic cor pulmonale) subtypes to guide safe dosing protocols
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

