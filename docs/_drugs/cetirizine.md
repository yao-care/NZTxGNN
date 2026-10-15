---
layout: default
title: Cetirizine
parent: High Evidence (L1-L2)
nav_order: 92
evidence_level: L2
indication_count: 6
---

# Cetirizine
{: .fs-9 }

Evidence Level: **L2** | Predicted Indications: **6** 
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

# Cetirizine: From Allergic Rhinitis to Allergic Urticaria

## One-Sentence Summary

Cetirizine is an H1 antihistamine originally used for allergic rhinitis, allergic conjunctivitis and idiopathic urticaria.
The TxGNN model predicts it may be effective for **allergic urticaria**,
with **1 clinical trial** (a small Phase 3 pilot) and **18 publications** currently supporting this direction, mostly reviews.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Allergic rhinitis (other listed indications include vasomotor rhinitis, atopic conjunctivitis and idiopathic urticaria) |
| Predicted New Indication | Allergic urticaria |
| TxGNN Prediction Score | 99.99% |
| Evidence Level | L2 |
| New Zealand Market Status | Cancelled only (no currently valid authorizations) |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails |

---

## Why is This Prediction Reasonable?

Cetirizine is a selective peripheral H1-receptor antagonist. Detailed mechanism of action data was not available in the data compiled for this report, but pharmacology data confirm it acts on the human H1 receptor (HRH1). Studies in rodents show negligible brain penetration, consistent with its low sedation profile.

In allergic urticaria, histamine released from mast cells drives the wheal-and-flare reaction, so blocking H1 receptors directly targets the underlying pathophysiology. Cetirizine is already listed for idiopathic urticaria, so this is a close extension within the same disease family rather than a distant repurposing.

The same logic supports a second prediction in the data, **cold urticaria** (L3 evidence), where small clinical studies show cetirizine reduces cold-induced wheals. Other predictions (nasal cavity disease, acute laryngopharyngitis, rosacea conjunctivitis, recalcitrant atopic dermatitis) have weak or no supporting evidence and are not recommended for progression at this stage.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02023164](https://clinicaltrials.gov/study/NCT02023164) | Phase 3 | Completed | 36 | Randomized, double-blind pilot comparing IV cetirizine 10 mg with IV diphenhydramine 50 mg in acute urticaria in emergency, urgent care and allergy settings. It is a feasibility study, not a definitive efficacy trial, and uses an IV route. |

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [16278258](https://pubmed.ncbi.nlm.nih.gov/16278258/) | 2005 | Review | Ann Pharmacother | Efficacy and safety of first- and newer-generation oral antihistamines in allergic rhinitis and chronic idiopathic urticaria |
| [17017922](https://pubmed.ncbi.nlm.nih.gov/17017922/) | 2006 | Review | Curr Med Chem | Levocetirizine (the R-enantiomer of cetirizine) pharmacology and use in allergic disease |
| [12113226](https://pubmed.ncbi.nlm.nih.gov/12113226/) | 2002 | Review | Clin Allergy Immunol | H1-antihistamines in children; strong efficacy evidence in allergic rhinoconjunctivitis |
| [7510611](https://pubmed.ncbi.nlm.nih.gov/7510611/) | 1993 | Review | Drugs | Cetirizine is effective and well tolerated in seasonal/perennial allergic rhinitis and chronic idiopathic urticaria in adults |
| [1981354](https://pubmed.ncbi.nlm.nih.gov/1981354/) | 1990 | Review | Drugs | Cetirizine pharmacology and clinical potential in allergic rhinitis, pollen-induced asthma and chronic urticaria |
| [7530629](https://pubmed.ncbi.nlm.nih.gov/7530629/) | 1994 | Review | Drugs | Non-sedating antihistamines are the mainstay of treatment for chronic idiopathic urticaria |
| [18339040](https://pubmed.ncbi.nlm.nih.gov/18339040/) | 2008 | Review | Allergy | Role of H1 antihistamines (rupatadine) in allergic rhinitis and chronic urticaria |
| [11034010](https://pubmed.ncbi.nlm.nih.gov/11034010/) | 2000 | Case report | J Clin Gastroenterol | Cetirizine-induced cholestasis in a man with no prior liver disease (safety signal) |

Most of this literature concerns related antihistamines or chronic urticaria rather than allergic urticaria specifically, so it supports the class rationale rather than providing direct proof.

---

## Safety Considerations

- **Contraindications (knowledge-graph associations, not label text)**: The TxGNN knowledge graph links cetirizine to caution or contraindication in hypertension, kidney disease (including chronic and end-stage renal failure), liver disease, gallbladder disease, diabetes mellitus and diabetic ketoacidosis, benign prostatic hyperplasia, coronary artery disease and atherosclerosis, angle-closure glaucoma, and hyperthyroidism (Graves disease, thyrotoxicosis). These should be verified against the official product label.
- **Hepatic signal**: One published case report describes cetirizine-induced cholestasis, and transient transaminase elevations were seen in under 2% of patients in US trials.
- **Drug Interactions**: The interaction query returned only the pharmacological target (H1 receptor); no specific drug-drug interactions were identified.

Official package insert warnings and contraindications were not available in the data compiled for this report.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails**

**Rationale:**
Allergic urticaria is a close extension of cetirizine's existing urticaria and allergy indications, with a clear histamine-mediated mechanism. Supporting evidence is a completed but small Phase 3 pilot (n=36, IV route) plus mainly review literature, so the evidence level is L2 rather than L1. Cold urticaria is a reasonable secondary candidate (L3).

**To proceed, the following is needed:**
- Retrieve and review the Medsafe package insert for official warnings and contraindications (currently a blocking gap for safety screening)
- Obtain detailed mechanism of action data from DrugBank
- Confirm New Zealand regulatory status: both licenses are cancelled, so no valid authorization currently exists, and the approved indication text is not available
- Seek adequately powered randomized trials of oral cetirizine in allergic urticaria, as the existing pilot used an IV formulation
- Define a safety monitoring plan for patients with renal, hepatic, cardiovascular, prostatic or glaucoma-related conditions
- Narrow the broad "nasal cavity disease" prediction to specific entities before any further evaluation
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

