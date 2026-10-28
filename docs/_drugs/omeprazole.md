---
layout: default
title: Omeprazole
parent: Model Prediction Only (L5)
nav_order: 340
evidence_level: L5
indication_count: 1
last_modified_at: 2026-10-28
monitor_inn: "OMEPRAZOLE"
predicted_indications: ["duodenal obstruction"]
description: "Omeprazole: predicted new indication Duodenal obstruction (evidence level L5). Approved use: Not recorded in the local data"
---

# Omeprazole
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

## Omeprazole: Duodenal obstruction
{: .text-alpha }

## One-Sentence Summary

Omeprazole is a proton pump inhibitor originally used for acid-related upper gastrointestinal conditions such as gastric and duodenal ulcer, gastroesophageal reflux disease, esophagitis and Zollinger-Ellison syndrome.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not recorded in the local data |
| Predicted New Indication | Duodenal obstruction |
| TxGNN Prediction Score | 0.9964 |
| Evidence Level | L5 |
| New Zealand Market Status | Marketed (valid licences: 9 of 9) |
| Number of Authorizations | 9 (valid: 9) |
| Recommended Decision | Hold (treat as a research question only) |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available. Based on known information, omeprazole is a proton pump inhibitor. Its efficacy in acid-related upper gastrointestinal disease is established, and its known action is to suppress gastric acid secretion by inhibiting the H+/K+ ATPase.

Duodenogastric reflux is closely related to the conditions omeprazole already treats, because it affects the same upper gastrointestinal tract and can cause overlapping symptoms and mucosal injury. This overlap probably drives the high TxGNN score. It is not independent clinical confirmation.

Mechanistically the fit is only partial. Omeprazole reduces the acid component of mucosal injury, but it does not stop bile and duodenal contents from refluxing into the stomach. Several animal studies (PMIDs 10389684, 8943968, 33027361) suggest that acid blockade combined with duodenogastric reflux may promote foregut mucosal growth and gastric carcinogenesis in rats. Small human studies in Barrett's oesophagus and a paediatric study of oesophageal bile reflux have looked at omeprazole's effect on bile reflux, but they are limited in size and quality.

A second prediction, **duodenal obstruction** (score 0.9964), was also reviewed. It is a mainly mechanical condition that needs dilation or surgery. Omeprazole can only treat an underlying ulcer or H. pylori-related inflammation, so the link is indirect. The retrieved literature consists of case reports with no omeprazole-specific efficacy data, and the assessment for that indication is Hold.

---

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02685150](https://clinicaltrials.gov/study/NCT02685150) | N/A | Completed | 157 | Endoscopic tri-modal imaging to distinguish functional dyspepsia from reflux disease. A diagnostic study that does not test omeprazole as a treatment, so it gives only indirect context. |

For the second prediction (duodenal obstruction), one trial was found, [NCT00557921](https://clinicaltrials.gov/study/NCT00557921). It is a terminated Phase 3 trial (5,000 participants) of a clopidogrel plus omeprazole combination for preventing upper gastrointestinal bleeding and ulcer events. It does not address duodenal obstruction, and its early termination limits interpretation, so it should not be counted as L1 evidence.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9824338](https://pubmed.ncbi.nlm.nih.gov/9824338/) | 1998 | Clinical study | Gut | Effect of omeprazole 20 mg twice daily on duodenogastric and gastro-oesophageal bile reflux in Barrett's oesophagus |
| [10994616](https://pubmed.ncbi.nlm.nih.gov/10994616/) | 2000 | Clinical study | Scand J Gastroenterol | Omeprazole may reduce antral duodenogastric reflux in Barrett's oesophagus |
| [16641575](https://pubmed.ncbi.nlm.nih.gov/16641575/) | 2006 | Prospective study | J Pediatr Gastroenterol Nutr | Omeprazole treatment of oesophageal bile reflux in children |
| [18679668](https://pubmed.ncbi.nlm.nih.gov/18679668/) | 2008 | Review | Eur J Clin Pharmacol | Update on clinical use and pharmacokinetics of proton pump inhibitors |
| [10389684](https://pubmed.ncbi.nlm.nih.gov/10389684/) | 1999 | Animal study (rat) | Dig Dis Sci | Acid blockade with omeprazole promoted gastric carcinogenesis induced by duodenogastric reflux |
| [8943968](https://pubmed.ncbi.nlm.nih.gov/8943968/) | 1996 | Animal study (rat) | Dig Dis Sci | Duodenogastric reflux stimulated foregut mucosal growth, which was potentiated by gastric acid blockade |
| [33027361](https://pubmed.ncbi.nlm.nih.gov/33027361/) | 2020 | Animal study (rat) | Acta Cir Bras | Examined whether omeprazole protects against gastric adenocarcinoma in rats with duodenogastric reflux |
| [8076761](https://pubmed.ncbi.nlm.nih.gov/8076761/) | 1994 | Clinical physiology study | Gastroenterology | Relationship of pH and duodenogastro-oesophageal reflux to Barrett's oesophagus |
| [12836018](https://pubmed.ncbi.nlm.nih.gov/12836018/) | 2003 | Clinical description | Eur J Pediatr | Primary duodenogastric reflux in six children and adolescents unresponsive to antacids |

None of these is a randomised controlled trial. The human studies are small, and the animal studies raise a potential safety concern rather than showing benefit.

---

## New Zealand Market Information


| Authorization Number | Product Name | Dosage Form | Status |
|---------|------|------|-----------|
| 2692163 | Omeprazole Teva | Capsule 20 mg | Valid |
| 2604949 | Omeprazole actavis 20 | Capsule 20 mg | Valid |
| 2586908 | Ocicure | Injection 40 mg ampoule with diluent | Valid |
| 2604957 | Omeprazole actavis 40 | Capsule 40 mg | Valid |
| 2604930 | Omeprazole actavis 10 | Capsule 10 mg | Valid |

Four further cancelled licences exist (Midwest powder, Omeprazole Teva 10 mg and 40 mg capsules, and Dr Reddy's Omeprazole injection).

---

## Safety Considerations

- **Contraindications flagged in the knowledge graph** (these are model-derived associations, not label text): liver failure, liver disease, gallbladder disease, congestive heart failure, osteoporosis, postmenopausal osteoporosis, bone fracture and chronic atrophic gastritis.
- **Drug Interactions**: 100 interactions were recorded. Examples include:
  - **Major**: acalabrutinib, atazanavir.
  - **Moderate**: abametapir (topical), hydrochlorothiazide, alprazolam, amphotericin B, amikacin, amphetamine, anagrelide, fenofibrate, apalutamide, armodafinil, atorvastatin.
  - **Minor**: lidocaine, nifedipine, ambrisentan, acetylsalicylic acid.
- **Animal data signal**: in rat models, omeprazole combined with duodenogastric reflux promoted foregut mucosal growth and carcinogenesis. The relevance to humans is unknown, but it warrants caution for long-term use in this setting.

Please refer to the package insert for the full warnings and precautions.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The prediction rests on model similarity to approved acid-related indications rather than direct evidence. There is no relevant treatment trial, the human data are small, and animal studies raise a possible carcinogenesis concern. Omeprazole acts on acid, not on bile reflux.

**To proceed, the following is needed:**
- Detailed mechanism of action data (MOA)
- Package insert warnings and contraindications (this is a blocking gap for safety screening)
- Well-designed controlled trials of omeprazole in duodenogastric reflux, with bile reflux and symptoms as endpoints
- Long-term safety assessment of acid suppression in patients with bile reflux, including cancer risk
- Confirmation of the current regulatory status and available formulations in New Zealand
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

