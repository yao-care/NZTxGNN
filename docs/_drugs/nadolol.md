---
layout: default
title: Nadolol
parent: Model Prediction Only (L5)
nav_order: 315
evidence_level: L5
indication_count: 5
last_modified_at: 2026-10-15
description: "Nadolol: predicted new indication Malignant hypertensive renal disease (evidence level L5). Approved use: Hypertension (hypertensive disorder). The New Zealand licence records…"
monitor_inn: "NADOLOL"
predicted_indications: ["malignant hypertensive renal disease"]
---

# Nadolol
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **5** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist">

## Pharmacist Assessment Report

</div>

# Nadolol: From Hypertension to Malignant Hypertensive Renal Disease

## One-Sentence Summary

Nadolol is a non-selective beta-blocker originally used to treat hypertension.
The TxGNN model predicts it may be effective for **malignant hypertensive renal disease**,
but **no clinical trials** and **no directly relevant publications** currently support this prediction, so it rests on model prediction alone.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Hypertension (hypertensive disorder). The New Zealand licence records contain no approved indication text, so this comes from the knowledge-graph indication data. |
| Predicted New Indication | Malignant hypertensive renal disease |
| TxGNN Prediction Score | 99.59% |
| Evidence Level | L5 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 4 (all cancelled) |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available in the data compiled for this report. Based on known information, nadolol is a non-selective beta-blocker. Its efficacy in hypertension is established, and it may plausibly apply to malignant hypertensive renal disease by lowering blood pressure and reducing sympathetic and renin-angiotensin activity.

Malignant hypertensive renal disease is a severe kidney complication of very high blood pressure. It is essentially the original indication in a more severe form, which explains the high prediction score. This is a weak form of support, however. Beta-blockers are not first-line treatment for hypertensive emergencies, and the prediction has no clinical data behind it.

Other predictions with the same model score level, in ranked order, are:

- **Malignant renovascular hypertension** (score 99.59%): renin release driven by renovascular disease can be reduced by beta-1 blockade, which gives a plausible rationale. However, this severe subtype usually needs revascularization or therapy that targets the renin-angiotensin system.
- **Pulmonary hypertension with unclear multifactorial mechanism** (99.53%): beta-blockers are generally cautioned against in pulmonary arterial hypertension because they can reduce cardiac output and right ventricular function. Any rationale is speculative, and the safety concern is substantial.
- **Pulmonary hypertension owing to lung disease and/or hypoxia** (99.53%): the retrieved literature is preclinical (see below), and nadolol's non-selective action carries a bronchospasm risk.
- **Braddock syndrome** (99.43%): a very rare condition for which no mechanistic rationale could be verified from the available data.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

No literature was retrieved for the top-ranked prediction. Twelve records were retrieved for the fourth-ranked prediction, pulmonary hypertension owing to lung disease and/or hypoxia. Ten are shown below. All are preclinical, animal, or mechanistic studies, and none is a human study of nadolol in pulmonary hypertension.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [6488695](https://pubmed.ncbi.nlm.nih.gov/6488695/) | 1984 | Preclinical (dog) | Clin Physiol Biochem | Chronic propranolol lowered pulmonary artery pressure and vascular resistance in dogs. Nadolol, a structurally dissimilar beta-blocker, produced a similar decrease in total pulmonary resistance. |
| [25350545](https://pubmed.ncbi.nlm.nih.gov/25350545/) | 2014 | Preclinical (rat) | PLoS One | β2-adrenergic receptor-dependent attenuation of hypoxic pulmonary vasoconstriction prevented progression of pulmonary arterial hypertension in intermittent hypoxic rats. |
| [24795691](https://pubmed.ncbi.nlm.nih.gov/24795691/) | 2014 | Preclinical (rat) | Front Neurol | β1-blockers lowered norepinephrine release by inhibiting presynaptic β1-adrenoceptors in normotensive and hypertensive rats. |
| [39252618](https://pubmed.ncbi.nlm.nih.gov/39252618/) | 2025 | Preclinical (rat) | CNS Neurol Disord Drug Targets | Nadolol attenuated brain cell ferroptosis in a rat ischemic stroke model via the HOIL-1/IRP2 pathway. |
| [2566504](https://pubmed.ncbi.nlm.nih.gov/2566504/) | 1989 | Preclinical/Review | Eur J Pharmacol | Ancillary properties of beta-blockers contributed to cardioprotection against anoxia in isolated guinea pig hearts. |
| [8729060](https://pubmed.ncbi.nlm.nih.gov/8729060/) | 1996 | Preclinical (rabbit) | J Mol Cell Cardiol | Studied early action potential shortening in hypoxic hearts, including the effect of nadolol, and the role of catecholamine-mediated chloride currents. |
| [8935706](https://pubmed.ncbi.nlm.nih.gov/8935706/) | 1996 | Preclinical/Review | Naunyn Schmiedebergs Arch Pharmacol | Examined the role of substance P in myocardial stunning after brief ischemia in guinea pig hearts. |
| [1670627](https://pubmed.ncbi.nlm.nih.gov/1670627/) | 1991 | Preclinical (canine myocytes) | Circulation | Studied how hypoxia influences adrenergic modulation of triggered activity in isolated canine myocytes. |
| [1913717](https://pubmed.ncbi.nlm.nih.gov/1913717/) | 1991 | Preclinical | Cardiologia | Examined variations in arrhythmogenic response to catecholamines in acute myocardial ischemia. |
| [31527180](https://pubmed.ncbi.nlm.nih.gov/31527180/) | 2019 | Preclinical (fish) | J Exp Biol | Explored a possible protective role of the β3-adrenoceptor in the goldfish heart under hypoxia. |

## New Zealand Market Information

All four authorizations are cancelled. The records contain no approved indication text.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2647540 | Nadolol BNM | Tab 40 mg | Not recorded (cancelled) |
| 2647559 | Nadolol BNM | Tab 80 mg | Not recorded (cancelled) |
| 2621118 | Nadolol BNM | Tab 40 mg | Not recorded (cancelled) |
| 2621126 | Nadolol BNM | Tab 80 mg | Not recorded (cancelled) |

## Safety Considerations

The package insert warnings and contraindications were not available, so please refer to the package insert for authoritative safety information. The following comes from the drug-disease knowledge graph, not from label text.

- **Contraindication signals (knowledge graph):** the graph links nadolol to these conditions, grouped here by theme:
  - Cardiac and respiratory: atrioventricular block, pulmonary emphysema, bronchial disease
  - Neuromuscular: myasthenia gravis (including adult-onset), neuromuscular junction disease
  - Metabolic and endocrine: diabetes mellitus, diabetic ketoacidosis, hyperuricemia, gout, hyperparathyroidism, osteitis fibrosa, hypercalcemia, nephrocalcinosis, phosphorus metabolism disease, potassium deficiency
  - Kidney, liver, and digestive: kidney disease, anuria, pyoureter, liver disease, hepatic coma, hepatic encephalopathy, pancreatitis, gallbladder disease
  - Immune: systemic lupus erythematosus (including pediatric)
  - Psychiatric: anxiety disorder, neurotic disorder, dysthymic disorder
  - Neonatal: neonatal jaundice
- **Drug Interactions:** the interaction query returned 100 records, and 20 are listed in the data compiled for this report.
  - **Major:** epinephrine.
  - **Moderate:** hydrocortisone, dexamethasone, betamethasone, triamcinolone, budesonide, methscopolamine, hyoscyamine, atropine, bupropion, morphine, calcium phosphate, calcium acetate, canagliflozin, dapagliflozin, chlorpropamide, cimetidine.
  - **Minor:** acetylsalicylic acid, clarithromycin, magnesium oxide.
- **Indication-specific cautions:** non-selective beta-blockade carries bronchospasm and hemodynamic risks in hypoxic lung disease. Beta-blockers are also generally cautioned against in pulmonary arterial hypertension.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
All five predictions rest on model scores alone (evidence level L5). The only literature retrieved is preclinical and does not address nadolol's clinical efficacy. The top two predictions largely restate the original hypertension indication in a severe form. Nadolol currently has no valid New Zealand authorization, and package insert safety data are unavailable.

**To proceed, the following is needed:**
- Package insert warnings and contraindications from Medsafe, which is a blocking gap for safety screening
- Mechanism of action data, for example from DrugBank
- A targeted literature search for human studies of nadolol or beta-blockers in malignant hypertensive renal disease and renovascular hypertension
- Clarification of the regulatory pathway, since all New Zealand authorizations are cancelled
- A safety assessment for the pulmonary hypertension predictions, given the cautions against beta-blockers in that setting
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

