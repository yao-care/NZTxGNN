---
layout: default
title: Capecitabine
parent: Model Prediction Only (L5)
nav_order: 82
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Capecitabine: predicted new indication Gastric adenocarcinoma and proximal polyposis of the stomach . Most-s… (evidence level L5). Approved use: Gastric cancer"
monitor_inn: "CAPECITABINE"
predicted_indications: ["gastric adenocarcinoma and proximal polyposis of the stomach", "microinvasive gastric cancer", "signet ring cell gastric adenocarcinoma", "gastric tubular adenocarcinoma", "gastric cardia adenocarcinoma", "carcinoma of stomach, salivary gland type", "gastric pylorus carcinoma", "gastric body carcinoma", "Epstein-Barr virus-associated gastric carcinoma", "malignant gastric granular cell tumor"]
---

# Capecitabine
{: .fs-9 }

Evidence Level: **L5** | Predicted Indications: **10** 
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

# Capecitabine: From Gastric Cancer to Gastric Tubular Adenocarcinoma

## One-Sentence Summary

Capecitabine is an oral fluoropyrimidine chemotherapy originally indicated for gastric cancer and metastatic colon cancer.
The TxGNN model predicts it may be effective for **gastric tubular adenocarcinoma**, the highest-evidence prediction among the ten candidates reviewed, which is supported by **20 publications** (including several Phase 3 RCTs of capecitabine-containing regimens) but **no registered clinical trials**.
This is largely a sub-type of an existing indication, so it is better viewed as confirmation than as true repurposing.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Gastric cancer (the licence records contain no approved indication text; the original indications come from the TxGNN knowledge graph: metastatic colon cancer and gastric cancer subtypes) |
| Predicted New Indication | Gastric adenocarcinoma and proximal polyposis of the stomach (rank 1, TxGNN score 99.94%; evidence level L5, no trials or literature). Most-supported candidate, discussed below: gastric tubular adenocarcinoma (score 99.94%) |
| TxGNN Prediction Score | 99.94% |
| Evidence Level | L1 (for gastric tubular adenocarcinoma); L5 for the top-ranked prediction |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 2 (both cancelled) |
| Recommended Decision | Proceed with Guardrails (gastric tubular adenocarcinoma) |

**Note on the candidates:** The top-ranked prediction (gastric adenocarcinoma and proximal polyposis of the stomach, a rare hereditary syndrome) has no trials or literature and is rated Hold. The report therefore focuses on gastric tubular adenocarcinoma, which has the strongest evidence. Other candidates are summarised in the table below.

| Rank | Predicted Disease | Score | Evidence Level | Recommendation |
|------|------|------|------|------|
| 1 | Gastric adenocarcinoma and proximal polyposis of the stomach | 99.94% | L5 | Hold |
| 2 | Microinvasive gastric cancer | 99.94% | L5 | Hold |
| 3 | Signet ring cell gastric adenocarcinoma | 99.94% | L4 | Research Question |
| 4 | **Gastric tubular adenocarcinoma** | 99.94% | L1 | Proceed with Guardrails |
| 5 | Gastric cardia adenocarcinoma | 99.91% | L2 | Proceed with Guardrails |
| 6 | Carcinoma of stomach, salivary gland type | 99.91% | L3 | Research Question |
| 7 | Gastric pylorus carcinoma | 99.91% | L4 | Hold |
| 8 | Gastric body carcinoma | 99.90% | L4 | Research Question |
| 9 | Epstein-Barr virus-associated gastric carcinoma | 99.90% | L3 | Research Question |
| 10 | Malignant gastric granular cell tumor | 99.89% | L5 | Hold |

---

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available in the data compiled for this report. Based on known information, capecitabine is a fluoropyrimidine that is converted in the body to 5-fluorouracil (5-FU), which inhibits thymidylate synthase and disrupts DNA synthesis. Its efficacy in gastric cancer is established, and mechanistically it may be applicable to the new indication.

Tubular adenocarcinoma is the most common histology within gastric adenocarcinoma, so the predicted indication overlaps heavily with the original one. The retrieved literature includes Phase 3 RCTs of capecitabine-containing regimens, such as CLASSIC (adjuvant capecitabine plus oxaliplatin after D2 gastrectomy) and several first-line trials of immunotherapy or targeted agents added to a fluoropyrimidine/platinum backbone. However, these papers cover gastric and gastro-oesophageal junction adenocarcinoma in general, not tubular histology specifically.

Several other candidates are weaker. For example, signet ring cell carcinoma often has lower chemosensitivity, microinvasive gastric cancer is usually treated by resection, and the benefit seen in EBV-associated carcinoma is likely driven by the PD-1 inhibitor rather than capecitabine.

---

## Clinical Trial Evidence

Currently no related clinical trials are registered for gastric tubular adenocarcinoma.

For reference, the following trials were found for neighbouring candidates:

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT00040859](https://clinicaltrials.gov/study/NCT00040859) | Phase 2 | Completed | 48 | Oxaliplatin plus capecitabine in metastatic oesophagogastric adenocarcinoma, including gastric cardia (directly tests capecitabine) |
| [NCT00938470](https://clinicaltrials.gov/study/NCT00938470) | Phase 2 | Completed | 73 | Extended neoadjuvant therapy including capecitabine in locally advanced oesophagus/GEJ/cardia adenocarcinoma; capecitabine's contribution not isolated |
| [NCT01295086](https://clinicaltrials.gov/study/NCT01295086) | N/A | Completed | 27 | Dose-finding of docetaxel, oxaliplatin and capecitabine plus trastuzumab in HER2-positive advanced disease |
| [NCT02494583](https://clinicaltrials.gov/study/NCT02494583) | Phase 3 | Completed | 763 | Pembrolizumab with or without cisplatin plus 5-FU or capecitabine in advanced gastric/GEJ adenocarcinoma; capecitabine effect not isolated |
| [NCT07091227](https://clinicaltrials.gov/study/NCT07091227) | Phase 2 | Not yet recruiting | 66 | AK112 plus chemotherapy as neoadjuvant therapy in signet ring cell-containing gastric/GEJ cancer |
| [NCT00021047](https://clinicaltrials.gov/study/NCT00021047) | Phase 1/2 | Completed | Not reported | Epirubicin, carboplatin and capecitabine in advanced solid tumours |
| [NCT00010023](https://clinicaltrials.gov/study/NCT00010023) | Phase 1 | Completed | Not reported | Capecitabine plus cisplatin in advanced solid tumours (safety and dosing) |
| [NCT05007106](https://clinicaltrials.gov/study/NCT05007106) | Phase 2 | Completed | 611 | Vibostolimab/pembrolizumab basket study; capecitabine not the focus |
| [NCT06349044](https://clinicaltrials.gov/study/NCT06349044) | N/A | Unknown | 120 | Radiotherapy plus immunotherapy basket study in digestive tumours; capecitabine role unclear |

---

## Literature Evidence

Selected publications for gastric tubular adenocarcinoma (the most relevant RCTs are listed first):

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [22226517](https://pubmed.ncbi.nlm.nih.gov/22226517/) | 2012 | RCT (Phase 3) | Lancet | CLASSIC: adjuvant capecitabine plus oxaliplatin after D2 gastrectomy vs surgery alone in stage II-IIIB gastric cancer |
| [34252374](https://pubmed.ncbi.nlm.nih.gov/34252374/) | 2021 | RCT (Phase 3) | Lancet Oncol | RESOLVE: perioperative/postoperative SOX vs adjuvant CapeOX in locally advanced gastric/GEJ cancer |
| [39952264](https://pubmed.ncbi.nlm.nih.gov/39952264/) | 2025 | Trial (final report) | Lancet Oncol | RESOLVE final report with updated overall survival analysis |
| [30982686](https://pubmed.ncbi.nlm.nih.gov/30982686/) | 2019 | RCT (Phase 2/3) | Lancet | FLOT4: FLOT vs fluorouracil or capecitabine plus cisplatin and epirubicin as perioperative therapy |
| [34102137](https://pubmed.ncbi.nlm.nih.gov/34102137/) | 2021 | RCT (Phase 3) | Lancet | CheckMate 649: first-line nivolumab plus chemotherapy vs chemotherapy alone |
| [37524953](https://pubmed.ncbi.nlm.nih.gov/37524953/) | 2023 | RCT (Phase 3) | Nat Med | GLOW: zolbetuximab plus CAPOX in CLDN18.2-positive gastric/GEJ adenocarcinoma |
| [37875143](https://pubmed.ncbi.nlm.nih.gov/37875143/) | 2023 | RCT (Phase 3) | Lancet Oncol | KEYNOTE-859: pembrolizumab plus chemotherapy in HER2-negative advanced gastric cancer |
| [38806195](https://pubmed.ncbi.nlm.nih.gov/38806195/) | 2024 | RCT (Phase 3) | BMJ | RATIONALE-305: tislelizumab plus chemotherapy as first-line treatment |
| [38051328](https://pubmed.ncbi.nlm.nih.gov/38051328/) | 2023 | RCT | JAMA | ORIENT-16: sintilimab plus chemotherapy in unresectable gastric/GEJ cancer |
| [20728210](https://pubmed.ncbi.nlm.nih.gov/20728210/) | 2010 | RCT (Phase 3) | Lancet | ToGA: trastuzumab plus chemotherapy in HER2-positive advanced gastric/GEJ cancer |

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2634937 | Capecitabine Viatris | Tab 500 mg | Not recorded (status: cancelled) |
| 2634929 | Capecitabine Viatris | Tab 150 mg | Not recorded (status: cancelled) |

Both authorizations are cancelled, so there is currently no valid marketed product in New Zealand.

---

## Cytotoxicity

| Item | Content |
|------|------|
| Cytotoxicity Classification | Conventional cytotoxic (fluoropyrimidine class, oral prodrug of 5-FU) |
| Myelosuppression Risk | Moderate. The knowledge graph lists neutropenia, thrombocytopenia, anaemia and agranulocytosis as contraindicated conditions |
| Emetogenicity Classification | Low to moderate |
| Monitoring Items | Complete blood count with differential, liver function (including bilirubin), renal function |
| Handling Protection | Follow cytotoxic drug handling regulations. Please refer to the package insert warnings and precautions |

---

## Safety Considerations

- **Contraindications / conditions of concern (from the knowledge graph, not label text):** liver disease, gallbladder disease, hyperbilirubinaemia (including hereditary forms and Gilbert syndrome), impaired renal function, thrombocytopenia, neutropenia, agranulocytosis, anaemia (including macrocytic anaemia), infectious disease, coronary artery disease, sudden cardiac arrest, heart conduction disease, necrotizing enterocolitis, stomatitis, and **dihydropyrimidine dehydrogenase (DPD) deficiency**.
- **Drug Interactions:** The interaction screen returned 100 interactions in total. Those listed include moderate interactions with metronidazole and tinidazole, and minor interactions with antacids (aluminium hydroxide, magaldrate, magnesium carbonate, magnesium hydroxide, magnesium oxide) and levofloxacin. Others, such as proton pump inhibitors, metformin, morphine and simvastatin, have unclassified severity. The literature-based assessment also flags a warfarin interaction, which should be checked.

Package insert warnings were not available for this report, so please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Proceed with Guardrails** (for gastric tubular adenocarcinoma and gastric cardia adenocarcinoma; Hold for the L5 candidates, namely the hereditary gastric adenocarcinoma/polyposis syndrome, microinvasive gastric cancer and malignant gastric granular cell tumor)

**Rationale:**
- Phase 3 RCT evidence supports capecitabine-containing regimens in gastric adenocarcinoma, which is already an original indication of the drug. The result mainly confirms an existing use and is not a new repurposing finding. Evidence specific to tubular histology is lacking.
- Gastric cardia adenocarcinoma has two completed Phase 2 trials (L2), but no direct Phase 3 evidence for that site.

**To proceed, the following is needed:**
- Mechanism of action data and the package insert warnings and contraindications (both currently missing)
- Confirmation of the regimen and biomarker selection (HER2, CLDN18.2, PD-L1)
- DPD deficiency screening and a warfarin interaction management plan
- Verification of local availability, since both New Zealand authorizations are cancelled
- Histology-specific data to separate capecitabine's contribution from that of combination partners, particularly immunotherapy
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

