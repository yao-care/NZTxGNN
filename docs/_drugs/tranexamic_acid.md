---
layout: default
title: Tranexamic Acid
parent: 僅模型預測 (L5)
nav_order: 347
evidence_level: L5
indication_count: 1
---

# Tranexamic Acid
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

# Tranexamic Acid: From Antifibrinolytic Bleeding Control to Amenorrhea

## One-Sentence Summary

Tranexamic acid (DB00302) is a well-known antifibrinolytic agent whose established pharmacology is used to *reduce* bleeding (e.g., heavy menstrual bleeding, surgical hemorrhage) — no structured original-indication record was returned in this evidence pack. The TxGNN model predicts a **99.19% score** association with **amenorrhea**, but this prediction is supported by **0 clinical trials** and only **2 review-level publications**, neither of which actually discusses amenorrhea — both are about controlling uterine bleeding. This mismatch strongly suggests a knowledge-graph labeling artifact rather than a genuine repurposing signal.

## Quick Overview

| Item | Content |
|------|------|
| Original Indication | Not available — no New Zealand license record; known pharmacologically as an antifibrinolytic used to control bleeding (e.g., heavy menstrual bleeding, surgical hemorrhage) |
| Predicted New Indication | Amenorrhea (disease) |
| TxGNN Prediction Score | 99.19% |
| Evidence Level | L4 |
| New Zealand Market Status | ✗ Not Marketed |
| Number of Authorizations | 0 |
| Recommended Decision | Hold |

## Why is This Prediction Reasonable?

Currently, detailed mechanism of action data is not available as a structured field. Based on known pharmacology, tranexamic acid is a competitive, reversible inhibitor that binds the lysine-binding site of plasminogen, blocking its conversion to plasmin. This antifibrinolytic action **reduces** clot breakdown and is clinically used to **decrease** bleeding — most notably in heavy menstrual bleeding (menorrhagia) and abnormal uterine bleeding (AUB).

This is directionally opposite to the predicted indication, amenorrhea (the absence of menstruation). A drug that reduces menstrual blood loss is not mechanistically positioned to induce or treat an *absence* of menses; if anything, the pharmacological logic points toward menorrhagia/AUB, not amenorrhea.

Consistent with this, both supporting publications are about controlling uterine bleeding — not amenorrhea: one reviews pharmacological therapy for abnormal uterine bleeding, and the other reviews menstrual suppression/prophylaxis in hematologic cancer patients (i.e., using agents like tranexamic acid to manage bleeding risk during cytopenia, not to induce amenorrhea per se). This strongly suggests the TxGNN prediction reflects an **ontology/label confusion** between adjacent menstrual-disorder nodes (amenorrhea vs. menorrhagia/AUB) in the knowledge graph, rather than a genuine, literature-grounded repurposing hypothesis.

## Clinical Trial Evidence

Currently no related clinical trials registered.

## Literature Evidence

| PMID | Year | Type | Journal | Key Findings |
|------|-----|------|------|---------|
| [21701432](https://pubmed.ncbi.nlm.nih.gov/21701432/) | 2011 | Review | Menopause (New York, N.Y.) | Reviews pharmacological therapy for abnormal uterine bleeding; nonhormonal agents (including antifibrinolytics) reduce bleeding by 25–35%. Concerns bleeding *control*, not amenorrhea. |
| [39043214](https://pubmed.ncbi.nlm.nih.gov/39043214/) | 2024 | Review | Journal of Oncology Pharmacy Practice | Systematic approach to menses prophylaxis and suppression in pre-menopausal hematologic cancer patients with treatment-related cytopenias; discusses menstrual suppression agents, not amenorrhea treatment specifically. |

## Safety Considerations

Please refer to the package insert for safety information.

## Conclusion and Next Steps

**Decision: Hold**

**Rationale:**
Despite a high TxGNN score, the predicted indication (amenorrhea) contradicts the known direction of tranexamic acid's antifibrinolytic mechanism, and neither supporting publication actually addresses amenorrhea — both concern bleeding control. This pattern is more consistent with a disease-node labeling error in the knowledge graph than a valid repurposing signal, and there are zero clinical trials to corroborate the direction.

**To proceed, the following is needed:**
- Confirm with the TxGNN modeling team whether the "amenorrhea (disease)" node was correctly mapped, or whether this signal actually corresponds to menorrhagia/abnormal uterine bleeding (AUB) — a mislabeled node would invalidate this candidate as stated
- TFDA/regulatory package insert with warnings and contraindications (currently a **Blocking** data gap — required before any S1 safety review)
- DrugBank-sourced mechanism of action record (currently a data gap; general pharmacology was used above as a substitute)
- If re-scoped to menorrhagia/AUB, re-run literature and clinical trial evidence search under the corrected indication before re-evaluating
## Disclaimer

This content is for research purposes only and does not constitute medical advice.
Clinical validation is required before any clinical application.

---

