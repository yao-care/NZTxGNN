---
layout: default
title: Omeprazole
parent: 僅模型預測 (L5)
nav_order: 258
evidence_level: L5
indication_count: 2
---

# Omeprazole
{: .fs-9 }

證據等級: **L5** | 預測適應症: **2** 個
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

# Omeprazole: From Acid-Related Gastric Disorders to Duodenogastric Reflux

## One-Sentence Summary

Omeprazole is a proton-pump inhibitor established for acid-related gastrointestinal disorders; the specific approved indication text is not present in this evidence pack (data gap).
The TxGNN model predicts it may be relevant to **Duodenogastric Reflux**, with **1 clinical trial** and **20 publications** currently associated with this direction — though, as detailed below, several of these papers report a cautionary rather than purely supportive signal.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available in this evidence pack (data gap — no `taiwan_regulatory.licenses` entries); omeprazole is generally established as a proton-pump inhibitor for acid-related GI disorders |
| Predicted New Indication | Duodenogastric Reflux |
| TxGNN Prediction Score | 99.64% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available for this candidate (data gap). Based on known pharmacology, omeprazole belongs to the proton-pump inhibitor (PPI) class, which suppresses gastric acid secretion by irreversibly inhibiting the H+/K+-ATPase in gastric parietal cells; its efficacy in acid-related disorders is well established.

Duodenogastric reflux (DGR) involves backflow of bile and duodenal contents into the stomach, which can co-occur with acid reflux and contribute to gastric/esophageal mucosal injury. Because PPIs are widely used in patients where acid and bile reflux coexist (e.g., Barrett's esophagus), it is mechanistically plausible that TxGNN identified a strong association between omeprazole and DGR from the literature graph.

However, the association is mechanistically ambiguous rather than clearly therapeutic. Several human studies in the evidence (Manifold 2000, Marshall 1998) suggest omeprazole can reduce the *acid* component of reflux and may modestly reduce antral DGR, but does not eliminate the bile component. More importantly, multiple preclinical (rat) studies (Wetscher 1999, Wetscher 1996, Monteiro 2020) report that gastric acid blockade with omeprazole **potentiates** DGR-induced mucosal growth stimulation and gastric carcinogenesis in animal models — a safety signal, not an efficacy signal. This means the TxGNN link likely reflects heavy co-study of omeprazole and DGR in the literature (both benefit and risk contexts) rather than a validated therapeutic use.

## Clinical Trial Evidence

| Trial Number | Phase | Status | Enrollment | Key Findings |
|---------|------|------|------|---------|
| [NCT02685150](https://clinicaltrials.gov/study/NCT02685150) | Phase NA | Completed | 157 | Diagnostic imaging study (endoscopic tri-modal imaging: NBI/AFI/WLI) to differentiate acid vs. bile reflux disease from functional dyspepsia; not a treatment-efficacy trial for omeprazole. |

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [9824338](https://pubmed.ncbi.nlm.nih.gov/9824338/) | 1998 | Clinical study | Gut | Omeprazole 20 mg twice daily reduced acid reflux but its effect on duodenogastric and duodenogastro-oesophageal bile reflux in Barrett's oesophagus was limited/unclear. |
| [10994616](https://pubmed.ncbi.nlm.nih.gov/10994616/) | 2000 | Clinical study | Scand J Gastroenterol | Omeprazole may reduce antral duodenogastric reflux in Barrett oesophagus, though acid suppression increases refluxate cytotoxicity concerns. |
| [19491829](https://pubmed.ncbi.nlm.nih.gov/19491829/) | 2009 | Clinical study | Am J Gastroenterol | Compared duodenogastroesophageal and acid reflux in GERD patients who did vs. did not respond to once-daily PPI therapy. |
| [11232672](https://pubmed.ncbi.nlm.nih.gov/11232672/) | 2001 | Clinical study | Am J Gastroenterol | Barrett's esophagus patients show increased acid and bile reflux vs. reflux esophagitis; PPI therapy's effect on the bile component evaluated. |
| [16641575](https://pubmed.ncbi.nlm.nih.gov/16641575/) | 2006 | Prospective study | J Pediatr Gastroenterol Nutr | Prospective study of omeprazole for oesophageal bile reflux in children; assessed proton pump inhibitor therapy effect. |
| [9841990](https://pubmed.ncbi.nlm.nih.gov/9841990/) | 1998 | Clinical study | J Gastrointest Surg | Bile reflux in Barrett's esophagus evaluated with combined pH/bilirubin monitoring; medical acid suppression and fundoplication compared. |
| [10389684](https://pubmed.ncbi.nlm.nih.gov/10389684/) | 1999 | Preclinical (rat) | Dig Dis Sci | **Safety signal**: gastric acid blockade with omeprazole promoted gastric carcinogenesis induced by duodenogastric reflux in rats. |
| [8943968](https://pubmed.ncbi.nlm.nih.gov/8943968/) | 1996 | Preclinical (rat) | Dig Dis Sci | DGR caused foregut mucosal growth stimulation that was potentiated by omeprazole-induced acid blockade. |
| [33027361](https://pubmed.ncbi.nlm.nih.gov/33027361/) | 2020 | Preclinical (rat) | Acta Cir Bras | Investigated whether omeprazole and nitrites had a protective or promoting effect on gastric adenocarcinoma in rats with induced DGR. |
| [15052437](https://pubmed.ncbi.nlm.nih.gov/15052437/) | 2004 | Preclinical (rat) | Gastric Cancer | Related PPI class (lansoprazole) promoted gastric carcinogenesis in rats with DGR — supports a class-level mechanistic caution. |

## New Zealand Market Information

Currently not marketed in New Zealand; no authorization records are present in this evidence pack (`total_licenses: 0`).

## Safety Considerations

Please refer to the package insert for safety information. (Note: several preclinical studies above indicate a potential carcinogenesis-promoting interaction between acid blockade and duodenogastric reflux — this should be factored into any safety review even though it falls outside the structured `safety` data fields.)

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The only registered trial is a diagnostic (non-interventional) study, not a treatment trial, and the literature base is dominated by observational/mechanistic studies rather than confirmed RCTs (Evidence Level L3).
- Multiple preclinical studies suggest acid blockade by omeprazole may *potentiate* DGR-related carcinogenesis, which is a safety concern rather than a supportive efficacy signal — this warrants caution before any further development.
- TFDA/package-insert safety data is marked as Blocking (DG001), meaning the candidate cannot yet enter formal safety pre-screening (S1).

**To proceed, the following is needed:**
- TFDA/regulatory package insert (warnings, contraindications) to resolve DG001 (Blocking)
- Confirmed mechanism of action data to resolve DG002 (High)
- A dedicated interventional/RCT evaluating omeprazole specifically for duodenogastric reflux, since existing trials are diagnostic rather than therapeutic
- Expert toxicology review of the carcinogenesis signal seen in preclinical DGR + acid-blockade models before considering this indication further
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

