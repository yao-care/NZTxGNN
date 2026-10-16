---
layout: default
title: Clavulanic Acid
parent: Model Prediction Only (L5)
nav_order: 104
evidence_level: L5
indication_count: 10
last_modified_at: 2026-10-15
description: "Clavulanic Acid: predicted new indication Ureaplasma urethritis (evidence level L5). Approved use: Urinary tract infection, bacterial pneumonia…"
---

# Clavulanic Acid
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

# Clavulanic Acid: From Bacterial Infections to Gonococcal Urethritis

## One-Sentence Summary

Clavulanic acid is a beta-lactamase inhibitor used together with amoxicillin to treat bacterial infections such as urinary tract infection and bacterial pneumonia.
The TxGNN model's top-ranked prediction is **Ureaplasma urethritis**, but this has **no clinical trials** and only indirect literature. A better-supported alternative among the predictions is **gonococcal urethritis**, with **9 publications**, several of them 1980s clinical studies.

---

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Urinary tract infection, bacterial pneumonia, Staphylococcus aureus infection, Escherichia coli infection, Klebsiella pneumonia (approved indication text is not available from the New Zealand licence records) |
| Predicted New Indication | Ureaplasma urethritis (rank 1); gonococcal urethritis is the best-supported prediction (rank 2) |
| TxGNN Prediction Score | 99.93% (Ureaplasma urethritis); 99.93% (gonococcal urethritis) |
| Evidence Level | L5 (Ureaplasma urethritis); L3 (gonococcal urethritis) |
| New Zealand Market Status | Cancelled only |
| Number of Authorizations | 3 (all cancelled) |
| Recommended Decision | Hold |

---

## Why is This Prediction Reasonable?

Detailed mechanism-of-action data are not available in the data compiled for this report. Based on known pharmacology, clavulanic acid is a beta-lactamase inhibitor with no meaningful antibacterial activity of its own. It is combined with amoxicillin to restore amoxicillin's activity against beta-lactamase-producing bacteria.

**Ureaplasma urethritis (rank 1):** The mechanistic case is weak. Ureaplasma has no cell wall, so beta-lactam antibiotics are not expected to work, and a beta-lactamase inhibitor would not change that. The high score most likely reflects a general association with urogenital infections rather than a true biological link. The retrieved literature covers urethritis in general (Pasteurella bettyae, empirical treatment, anaerobes in prostatitis) and contains nothing specific to Ureaplasma and clavulanic acid.

**Gonococcal urethritis (rank 2):** Here the mechanism is plausible. Clavulanic acid inhibits the penicillinase produced by *Neisseria gonorrhoeae*, which restores amoxicillin activity against penicillinase-producing strains. This is a historical use, however. Current gonorrhoea management is driven by widespread resistance and ceftriaxone-based guideline regimens, so this is not a novel repurposing opportunity.

**Other predictions:** Uterine inflammatory disease (rank 3) has a theoretical rationale, since amoxicillin/clavulanate covers polymicrobial genital tract infection. Xanthogranulomatous pyelonephritis (rank 4) and urogenital tuberculosis (rank 5) have only weak rationale. Ranks 6-10 (celiac trunk compression syndrome, abdominal ectopic pregnancy, abdominal cystic lymphangioma, lymph node palisaded myofibroblastoma, disease of uterine broad ligament) are largely non-infectious and likely model artifacts.

---

## Clinical Trial Evidence

Currently no related clinical trials are registered for Ureaplasma urethritis or any other predicted indication in the data compiled for this report.

---

## Literature Evidence

Literature for the top-ranked prediction (Ureaplasma urethritis) is indirect and low-tier. The table below therefore also lists the most relevant publications for gonococcal urethritis, the best-supported prediction.

**Ureaplasma urethritis (rank 1)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [34366085](https://pubmed.ncbi.nlm.nih.gov/34366085/) | 2021 | Case report/Series | Infectious Diseases Now | Urethritis and balanitis caused by *Pasteurella bettyae* in men who have sex with men; no clavulanic acid evaluation |
| [19544099](https://pubmed.ncbi.nlm.nih.gov/19544099/) | 2009 | Review | Rev Esp Quimioter | Prevalence of organisms causing urethritis in men in Madrid |
| [21947096](https://pubmed.ncbi.nlm.nih.gov/21947096/) | 2011 | Review | Rev Esp Quimioter | Best empirical treatment for urethritis in Madrid |
| [9669200](https://pubmed.ncbi.nlm.nih.gov/9669200/) | 1998 | Review | Int J Androl | Possible role of anaerobic bacteria in chronic prostatitis |

**Gonococcal urethritis (rank 2)**

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [3533755](https://pubmed.ncbi.nlm.nih.gov/3533755/) | 1986 | Randomised comparative trial | Genitourin Med | Single-dose cefuroxime axetil vs amoxicillin 3 g + clavulanic acid 0.25 g (with probenecid) in 500 patients with uncomplicated gonorrhoea |
| [6365235](https://pubmed.ncbi.nlm.nih.gov/6365235/) | 1984 | Comparative clinical trial | Br J Vener Dis | In 121 men, amoxicillin + clavulanate had a 9.4% culture-positive rate at 7 days vs 26.3% with intramuscular procaine penicillin |
| [6428699](https://pubmed.ncbi.nlm.nih.gov/6428699/) | 1984 | Clinical study | Br J Vener Dis | Two-dose Augmentin cured 95.9% of 192 men's group vs 87.4% with kanamycin |
| [4007860](https://pubmed.ncbi.nlm.nih.gov/4007860/) | 1985 | Clinical study | Genitourin Med | Single-dose amoxicillin 3 g + clavulanic acid 125 mg plus probenecid cured 97 of 100 assessable patients, including 11 of 13 with penicillinase-producing strains |
| [3004176](https://pubmed.ncbi.nlm.nih.gov/3004176/) | 1985 | Clinical study | Afr J Med Med Sci | Single-dose Augmentin for gonococcal urethritis in Ibadan, where about 80% of strains produce penicillinase |
| [6757686](https://pubmed.ncbi.nlm.nih.gov/6757686/) | 1982 | Clinical study | Med J Malaysia | Single-dose oral amoxycillin + clavulanic acid in males (no abstract available) |
| [3147528](https://pubmed.ncbi.nlm.nih.gov/3147528/) | 1988 | Clinical study/Review | Sex Transm Dis | Therapy options for antibiotic-resistant gonorrhoea, including clavulanic acid added to amoxicillin or penicillin G |
| [3132029](https://pubmed.ncbi.nlm.nih.gov/3132029/) | 1988 | Laboratory study | Hinyokika Kiyo | Penicillinase production and susceptibility of gonococcal isolates, including amoxicillin/clavulanate |
| [21947096](https://pubmed.ncbi.nlm.nih.gov/21947096/) | 2011 | Review | Rev Esp Quimioter | Best empirical treatment for urethritis |

Evidence for the other predictions is minimal. Uterine inflammatory disease has a postpartum endometritis guideline, veterinary studies, a murine Chlamydia model and an IUD-associated actinomycosis case. Urogenital tuberculosis has a single case report, and xanthogranulomatous pyelonephritis and ranks 6-10 have no literature.

---

## New Zealand Market Information

| Authorization Number | Product Name | Dosage Form | Approved Indication |
|---------|------|------|-----------|
| 2602121 | Curam Duo 500/125 | Tablet 500 mg (amoxicillin) with clavulanic acid 125 mg | Not available (cancelled) |
| 2684748 | Amoxiclav Devatis Forte | Granules for oral liquid, amoxicillin 50 mg with clavulanic acid 12.5 mg per ml | Not available (cancelled) |
| 319473 | Augmentin | Granules for oral liquid, amoxicillin 25 mg with clavulanic acid 6.25 mg per ml | Not available (cancelled) |

All three authorizations are cancelled, so no valid product is currently listed.

---

## Safety Considerations

- **Disease-level contraindication signals (from the knowledge graph, not label text):** kidney disease, pyoureter, liver disease, gallbladder disease, cholestasis (including benign recurrent and familial intrahepatic forms), *Clostridium difficile* colitis, perinatal necrotizing enterocolitis, infectious mononucleosis, and chronic Epstein-Barr virus infection syndrome.
- **Drug Interactions:** 16 interactions were identified.
  - **Major:** leflunomide and teriflunomide.
  - **Moderate:** methotrexate, naltrexone, balsalazide, asparaginase (*E. coli*), pegaspargase, brentuximab vedotin, clofarabine, epirubicin, idelalisib, interferon beta-1a, interferon beta-1b, peginterferon beta-1a, tioguanine, and trabectedin.

Package insert warnings and contraindications have not been retrieved. Please refer to the package insert for full safety information.

---

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
- The top-ranked prediction, Ureaplasma urethritis, has no trials and a mechanistic argument against it, because Ureaplasma lacks a cell wall.
- Gonococcal urethritis is the only prediction with clinical studies and a sound mechanism, but the studies date from the 1980s and the use is historical rather than new. All New Zealand licences are cancelled, and current practice favours ceftriaxone-based regimens.
- The remaining predictions are supported only by computation or indirect evidence.

**To proceed, the following is needed:**
- Medsafe package insert warnings and contraindications (a blocking gap for safety screening).
- Mechanism-of-action data from DrugBank.
- Current gonococcal resistance and guideline data to judge whether the historical use has any contemporary relevance.
- If uterine or pelvic infection is pursued, human clinical studies of amoxicillin/clavulanate in that setting.
- Clarification of whether any active New Zealand amoxicillin/clavulanate product is available.
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

