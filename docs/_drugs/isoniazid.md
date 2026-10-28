---
layout: default
title: Isoniazid
parent: Model Prediction Only (L5)
nav_order: 245
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-15
monitor_inn: "ISONIAZID"
predicted_indications: ["conjunctivitis"]
description: "Isoniazid: predicted new indication Conjunctivitis (evidence level L5). Approved use: Not recorded in the local data"
---

# Isoniazid
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **1** 
{: .fs-6 .fw-300 }

---

## Table of Contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

<div id="pharmacist" markdown="1">

## Pharmacist Assessment Report

</div>

## Isoniazid: Conjunctivitis
{: .text-alpha }

## One-Sentence Summary

Isoniazid is an antitubercular drug, originally used to treat pulmonary tuberculosis.
The TxGNN model predicts it may be effective for **conjunctivitis**, but there are **no registered clinical trials** and the **12 publications** found are mostly case reports, adverse-effect reviews and old field studies, so the evidence is weak and mostly indirect.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Conjunctivitis |
| TxGNN Prediction Score | 0.9936 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 2 of 2) |
| Number of Authorizations | 2 (valid: 2) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism of action data is not available for this report. From general pharmacology, isoniazid inhibits mycolic acid synthesis in Mycobacterium species. Its effect is therefore specific to mycobacteria.

The link to conjunctivitis is indirect. It applies only to forms associated with mycobacterial infection or tubercular hypersensitivity, such as primary conjunctival tuberculosis and phlyctenular keratoconjunctivitis. It may also apply to conjunctivitis occurring with reactive arthritis after intravesical BCG therapy. There is no evidence of activity against common bacterial, viral or allergic conjunctivitis.

The high TxGNN score comes from the TxGNN model (deep learning) and may reflect similarities the model learned between related diseases, such as tuberculosis-related conditions, not biological evidence or clinical data for conjunctivitis in general. Isoniazid also appears in ocular side-effect literature, so it may cause as well as treat eye findings. The prediction is therefore plausible only for a narrow, tuberculosis-related subset of conjunctivitis.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

Of the 12 publications retrieved, the following are most relevant. None is an RCT.

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [14253168](https://pubmed.ncbi.nlm.nih.gov/14253168/) | 1965 | Cohort / prophylaxis field study | Am Rev Respir Dis | Isoniazid prophylaxis for phlyctenular keratoconjunctivitis among Eskimos in southwestern Alaska (no abstract available) |
| [5103251](https://pubmed.ncbi.nlm.nih.gov/5103251/) | 1971 | Not classified | Ann Oculist | Use of isoniazid in local treatment of ocular tuberculosis (no abstract available) |
| [10641112](https://pubmed.ncbi.nlm.nih.gov/10641112/) | 1999 | Not classified | Oftalmologia | 28 cases of tuberculous keratoconjunctivitis, 13 in children with primary tuberculosis; all had a positive tuberculin test |
| [33607832](https://pubmed.ncbi.nlm.nih.gov/33607832/) | 2021 | Case report | Medicine | Paediatric phlyctenular keratoconjunctivitis associated with primary sinonasal tuberculosis |
| [14089390](https://pubmed.ncbi.nlm.nih.gov/14089390/) | 1964 | Case report | Arch Ophthalmol | Primary tuberculosis of the conjunctiva |
| [10084171](https://pubmed.ncbi.nlm.nih.gov/10084171/) | 1999 | Case report | Rev Rhum (Engl Ed) | Isoniazid used for refractory arthropathy after intravesical BCG therapy |
| [12226788](https://pubmed.ncbi.nlm.nih.gov/12226788/) | 2002 | Case report | Dtsch Med Wochenschr | Chronic reactive arthritis with conjunctivitis after intravesical BCG |
| [1363080](https://pubmed.ncbi.nlm.nih.gov/1363080/) | 1992 | Review (adverse effects) | Optom Clin | Ocular side effects of systemic drugs, including conjunctival reactions |

The remaining retrieved items (BCG-related polyarthritis reports, toxic epidermal necrolysis and an unrelated Munchausen-by-proxy case) offer little support.

---

## New Zealand Market Information

Both New Zealand authorisations are cancelled, and no approved indication text is recorded.

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2689294 | Isoniazid Teva | Tab 100 mg | Not recorded (valid) |
| 2695901 | Noumed Isoniazid | Tab 100 mg | Not recorded (valid) |

---

## Safety Considerations

- **Disease associations flagged in the knowledge graph (not label text)**: liver failure and acute liver failure, hepatitis, liver disease, gallbladder disease, porphyria (including inherited porphyria and porphyrin metabolism disease), peripheral neuropathy and peripheral nervous system disease, diabetes mellitus, diabetic ketoacidosis, gout, substance abuse/dependence, and slow acetylator status.
- **Drug Interactions**: The interaction screen returned 100 interacting drugs; the first 20 reviewed are mostly Moderate or Minor.
  - Moderate: antidiabetic agents (acarbose, albiglutide, alogliptin, metformin, pioglitazone, canagliflozin, dapagliflozin, chlorpropamide), vitamin D forms (cholecalciferol, calcifediol, calcitriol), metronidazole, alosetron, budesonide (including nasal) and picosulfuric acid.
  - Minor: corticosteroids (hydrocortisone, triamcinolone, dexamethasone, betamethasone).
- Ocular side-effect literature lists isoniazid among systemic drugs that can affect the eye.

The package insert was not available, so its warnings and contraindications could not be reviewed. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The high TxGNN score is not backed by clinical data for general conjunctivitis. The literature is limited to old or small reports on tuberculosis-related eye disease, and there are no clinical trials.

**To proceed, the following is needed:**
- Narrow the indication to tuberculosis-associated conjunctivitis or phlyctenular keratoconjunctivitis, rather than conjunctivitis in general
- Mechanism of action data and a review of the package insert warnings and contraindications
- Controlled clinical evidence in the narrowed population
- Clarification of the regulatory position, since both New Zealand licences are cancelled
- A safety plan covering liver function, peripheral neuropathy and acetylator status
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

