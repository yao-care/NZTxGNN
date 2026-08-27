---
layout: default
title: Nystatin
parent: 僅模型預測 (L5)
nav_order: 249
evidence_level: L5
indication_count: 10
---

# Nystatin
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

# Nystatin: From Candidiasis to Vulvovaginitis

## One-Sentence Summary

Nystatin is a polyene antifungal antibiotic long used to treat mucocutaneous and mucosal *Candida* infections. The TxGNN model predicts it may be effective for **Vulvovaginitis**, a diagnosis for which *Candida* species are already the leading infectious cause, with **0 registered clinical trials** but **20 supporting publications** currently identified. No New Zealand safety data (Medsafe warnings/contraindications) are on file, which blocks a formal safety assessment at this stage.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available on file (drug not marketed in NZ; general pharmacology indicates antifungal/candidiasis use — see note below) |
| Predicted New Indication | Vulvovaginitis |
| TxGNN Prediction Score | 99.92% |
| Evidence Level | L3 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data is not available in the evidence pack (data gap DG002). Based on well-established general pharmacology, nystatin is a polyene antifungal that binds ergosterol in the fungal cell membrane, forming pores that cause leakage of intracellular contents and fungal cell death. It has long been used as a first-line topical or oral treatment for mucocutaneous and mucosal *Candida* infections, including oral thrush and cutaneous candidiasis.

Vulvovaginitis and vulvovaginal candidiasis are closely linked conditions: per the literature in this evidence pack, *Candida albicans* accounts for 85–90% of vulvovaginal candidiasis cases, and vulvovaginal candidiasis is the second most common cause of vaginitis overall after bacterial vaginosis (PMIDs 25775428, 21718579, 19454049). Because nystatin directly targets *Candida* species, the mechanistic rationale for its effect on vulvovaginitis is strong — this is less a novel repurposing signal and more a recognition of nystatin's already-documented antifungal role in a large subset of vulvovaginitis cases. Supporting this, an observational study of 283 patients with complicated vulvovaginal candidosis correlated in vitro nystatin susceptibility with clinical outcome (PMID 20406393), and an animal model demonstrated nystatin's protective and immunomodulatory effects on vaginal epithelium during *Candida* infection (PMID 30359236). Notably, several reviews indicate nystatin has largely been superseded by azole antifungals as first-line therapy (PMID 1436934), retaining relevance mainly for azole-resistant or recurrent cases (PMIDs 39771534, 21774671).

---

## Clinical Trial Evidence

Currently no related clinical trials registered.

---

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [39771534](https://pubmed.ncbi.nlm.nih.gov/39771534/) | 2024 | Review | Pharmaceutics | Reviews management of fluconazole-resistant vulvovaginal candidosis; nystatin discussed alongside boric acid, oteseconazole, and ibrexafungerp as alternative therapy |
| [20406393](https://pubmed.ncbi.nlm.nih.gov/20406393/) | 2011 | Observational | Mycoses | In 283 patients with complicated vulvovaginal candidosis, correlated in vitro fluconazole/nystatin susceptibility with clinical treatment outcome |
| [30359236](https://pubmed.ncbi.nlm.nih.gov/30359236/) | 2018 | Preclinical (animal model) | BMC Microbiology | Nystatin enhanced mucosal immune response and protected vaginal epithelial ultrastructure in a rat model of vulvovaginal candidiasis |
| [21774671](https://pubmed.ncbi.nlm.nih.gov/21774671/) | 2011 | Review | J Women's Health | Reviews boric acid for recurrent vulvovaginal candidiasis in the context of rising resistance to conventional antifungals including nystatin |
| [25775428](https://pubmed.ncbi.nlm.nih.gov/25775428/) | 2015 | Review | BMJ Clinical Evidence | Evidence review confirming *Candida albicans* causes 85–90% of vulvovaginal candidiasis, the second most common cause of vaginitis |
| [21718579](https://pubmed.ncbi.nlm.nih.gov/21718579/) | 2010 | Review | BMJ Clinical Evidence | Evidence review of vulvovaginal candidiasis diagnosis and treatment options |
| [19454049](https://pubmed.ncbi.nlm.nih.gov/19454049/) | 2007 | Review | BMJ Clinical Evidence | Evidence review of vulvovaginal candidiasis; reaffirms *Candida* as dominant causative organism |
| [16620487](https://pubmed.ncbi.nlm.nih.gov/16620487/) | 2005 | Review | Clinical Evidence | Earlier iteration of the BMJ vulvovaginal candidiasis evidence review series |
| [16047929](https://pubmed.ncbi.nlm.nih.gov/16047929/) | 2005 | Clinical study | Ceska gynekologie | Evaluated diagnostics and therapy of combined vaginal nystatin/nifuratel products in mixed vulvovaginitis |
| [1436934](https://pubmed.ncbi.nlm.nih.gov/1436934/) | 1992 | Review | Obstet Gynecol Clin North Am | Reviews topical antifungal agents; notes nystatin was surpassed by imidazoles/triazoles as first-line vulvovaginal candidiasis therapy |

---

## New Zealand Market Information

Nystatin currently holds no marketing authorization in New Zealand (0 licenses on file; market status: not marketed), so no product/dosage-form information is available to summarize.

---

## Safety Considerations

Please refer to the package insert for safety information.

*Note: Medsafe/TFDA warnings and contraindications for nystatin are marked as a blocking data gap (DG001) — this must be resolved before a formal S1 safety assessment can proceed.*

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
The mechanistic link between nystatin and vulvovaginitis is plausible and partly already established (via vulvovaginal candidiasis), and it is supported by observational and preclinical literature — but no clinical trials exist for this specific indication, a blocking safety data gap (Medsafe warnings/contraindications) prevents a formal safety review, and nystatin has zero marketing authorizations in New Zealand. Separately, 9 lower-ranked predicted indications (ranks 2–10, e.g. orbital disease, cystic teratoma, biotin metabolic disease) were also generated by TxGNN but show no mechanistic plausibility or supporting evidence and have been screened out as likely model noise.

**To proceed, the following is needed:**
- Medsafe-approved package insert (warnings, contraindications) — resolves blocking gap DG001
- Detailed mechanism-of-action documentation from DrugBank — resolves gap DG002
- Confirmation of whether "vulvovaginitis" as a TxGNN-predicted indication meaningfully extends beyond nystatin's already-documented use in vulvovaginal candidiasis
- A New Zealand marketing authorization pathway assessment if commercial launch is being considered
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

