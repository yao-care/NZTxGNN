---
layout: default
title: Paromomycin
parent: 僅模型預測 (L5)
nav_order: 266
evidence_level: L5
indication_count: 8
---

# Paromomycin
{: .fs-9 }

證據等級: **L5** | 預測適應症: **8** 個
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

# Paromomycin: From Intestinal Amebiasis to Peritonitis (Amebic Complication)

## One-Sentence Summary

Paromomycin is a non-absorbable aminoglycoside historically used against intestinal amebiasis and (via other routes) leishmaniasis; formal original-indication data is missing from this evidence pack. Among 8 TxGNN-predicted indications for this drug, only one — **Peritonitis** — is backed by actual literature, while the other 7 (all scoring higher, e.g. hepatoportal sclerosis, portal hypertension, cirrhosis subtypes) have **zero clinical or literature evidence** and are flagged by the pack itself as likely embedding-cluster artifacts rather than real signals. The peritonitis link is narrow and indirect: it traces back to amebic colitis perforating into amebic peritonitis, not general bacterial peritonitis, supported by **0 clinical trials** and **20 PubMed records** (most only tangentially related).

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — drug is not marketed in Taiwan/NZ (no license records); per pack context, paromomycin is known as an anti-amebic/anti-leishmanial aminoglycoside |
| Predicted New Indication | Peritonitis (specifically, as a complication of amebic colitis/liver abscess — not general bacterial peritonitis) |
| TxGNN Prediction Score | 99.03% (rank 7348) |
| Evidence Level | L4 (mechanism/indirect infectious-disease link; no clinical trials) |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

**Note on ranking:** The pack's #1–#7 ranked predictions (hepatoportal sclerosis, early-onset familial noncirrhotic portal hypertension, hepatopulmonary syndrome, idiopathic copper-associated cirrhosis, primitive portal vein thrombosis, hepatic porphyria, acute urate nephropathy) all score higher (99.79–99.90%) but have **no supporting trials or literature at all**, and the pack's own mechanistic-link notes describe them as probable "liver-disease node clustering" artifacts in the knowledge graph rather than genuine pharmacological signals. Peritonitis (rank 8) is the only candidate reaching decision stage S1 with actual evidence, so it is the focus of this report.

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data for paromomycin is not available in this evidence pack (`[Data Gap]`, DG002, High severity). Based on the pack's contextual notes, paromomycin is a non-absorbable aminoglycoside antibiotic that acts primarily within the gut lumen, historically used against intestinal amebiasis (*Entamoeba histolytica*) and, via parenteral/topical routes, against visceral and cutaneous leishmaniasis.

The connection to peritonitis is indirect and infection-specific rather than a broad anti-inflammatory or antibacterial mechanism: amebic colitis can occasionally progress to bowel perforation and amebic peritonitis, a recognized complication of untreated or severe intestinal amebiasis. One case report in the evidence set (PMID 19242252) explicitly documents peritoneal, pleural, and pericardial extension of amebic liver abscess, and another (PMID 34840954) describes paromomycin used successfully alongside metronidazole for fulminant amoebic colitis. This supports a plausible but narrow role — treating the underlying amebic infection to prevent or manage amebic peritonitis — not a general therapeutic effect against peritonitis of other etiologies (bacterial, post-surgical, dialysis-related, etc.).

Most of the remaining literature attached to this prediction (in vitro Leishmania drug-interaction and resistance studies, formulation/nanoparticle papers) reflects paromomycin's established antiparasitic pharmacology rather than direct evidence for peritonitis, and one record (PMID 15619205, ovarian carcinoma E-cadherin expression) appears unrelated and is likely a co-occurrence artifact from the literature search.

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [19242252](https://pubmed.ncbi.nlm.nih.gov/19242252/) | 2009 | Case Report | Current Opinion in Pediatrics | Amebic liver abscess with peritoneal, pleural, and pericardial extension; underscores under-recognized complications |
| [34840954](https://pubmed.ncbi.nlm.nih.gov/34840954/) | 2021 | Case Report | IDCases | Metronidazole + paromomycin successfully treated fulminant amoebic colitis (with intussusception) during cytotoxic chemotherapy |
| [11096579](https://pubmed.ncbi.nlm.nih.gov/11096579/) | 1999 | Review | Current Treatment Options in Gastroenterology | Amebiasis management guidance; metronidazole first-line, invasive treatment only if medical therapy fails |
| [23833177](https://pubmed.ncbi.nlm.nih.gov/23833177/) | 2013 | Review | J Antimicrob Chemother | Immunomodulatory effects of antileishmanial drugs including paromomycin (mechanistic background, not peritonitis-specific) |
| [4293095](https://pubmed.ncbi.nlm.nih.gov/4293095/) | 1967 | Review (toxicity) | JAMA | Nephrotoxicity of antibiotics — safety-relevant background, not efficacy evidence |

**Note:** The evidence.literature array for this indication contains 20 records total; the remaining 15 are predominantly in vitro *Leishmania* susceptibility/resistance and drug-formulation studies with no direct relevance to peritonitis, plus one apparently unrelated oncology paper (PMID 15619205). These are excluded from the table above as non-relevant.

---

## New Zealand Market Information

No authorization records — Paromomycin is not currently marketed in New Zealand (`market_status`: 未上市, 0 licenses).

---

## Safety Considerations

Please refer to the package insert for safety information.

*(Note: `key_warnings`, `contraindications`, and DDI data are all marked as data gaps in this pack. DG001 — TFDA/package-insert warnings and contraindications — is flagged as a Blocking gap, meaning a formal safety (S1) evaluation cannot be completed until this is resolved.)*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Evidence for peritonitis is indirect (limited to amebic colitis/liver abscess progressing to amebic peritonitis) and consists only of case reports and reviews — no clinical trials or systematic studies test paromomycin against peritonitis directly. The other 7 higher-scoring TxGNN predictions for this drug have no evidence at all and are likely knowledge-graph artifacts, reinforcing that this candidate needs targeted validation rather than immediate advancement.

**To proceed, the following is needed:**
- TFDA/manufacturer package insert (warnings, contraindications) — currently a Blocking data gap
- DrugBank-sourced mechanism of action detail
- A literature search specifically scoped to "paromomycin AND amebic/parasitic peritonitis" to separate genuinely relevant records from the broader antileishmanial literature currently attached
- Clarification of the intended peritonitis subtype (amebic vs. bacterial vs. dialysis-associated) before any further evidence grading, since the current mechanistic link supports only the amebic subtype
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

