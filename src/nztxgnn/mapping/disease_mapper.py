"""疾病映射模組 - 葡萄牙語適應症/治療類別映射至 TxGNN 疾病本體"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


# 葡萄牙語-英語疾病/症狀對照表
DISEASE_DICT = {
    # === 心血管系統 (Cardiovascular) ===
    "htn": "hypertension",
    "high blood pressure": "hypertension",
    "elevated blood pressure": "hypertension",
    "low blood pressure": "hypotension",
    "heart attack": "myocardial infarction",
    "mi": "myocardial infarction",
    "ami": "acute myocardial infarction",
    "chest pain": "angina",
    "angina pectoris": "angina",
    "irregular heartbeat": "arrhythmia",
    "cardiac arrhythmia": "arrhythmia",
    "afib": "atrial fibrillation",
    "a-fib": "atrial fibrillation",
    "chf": "heart failure",
    "congestive heart failure": "heart failure",
    "cad": "coronary artery disease",
    "coronary heart disease": "coronary artery disease",
    "ihd": "ischemic heart disease",
    "dvt": "deep vein thrombosis",
    "blood clot": "thrombosis",
    "pe": "pulmonary embolism",
    "high cholesterol": "hypercholesterolemia",
    "hyperlipidemia": "dyslipidemia",

    # === 呼吸系統 (Respiratory) ===
    "copd": "chronic obstructive pulmonary disease",
    "emphysema": "chronic obstructive pulmonary disease",
    "chronic bronchitis": "chronic obstructive pulmonary disease",
    "shortness of breath": "dyspnea",
    "difficulty breathing": "dyspnea",
    "wheezing": "asthma",
    "lung infection": "pneumonia",
    "flu": "influenza",
    "common cold": "upper respiratory infection",
    "uri": "upper respiratory infection",
    "ards": "acute respiratory distress syndrome",
    "sleep apnea": "obstructive sleep apnea",
    "osa": "obstructive sleep apnea",
    "cf": "cystic fibrosis",
    "tb": "tuberculosis",

    # === 消化系統 (Gastrointestinal) ===
    "gerd": "gastroesophageal reflux disease",
    "acid reflux": "gastroesophageal reflux disease",
    "heartburn": "gastroesophageal reflux disease",
    "stomach ulcer": "peptic ulcer",
    "pud": "peptic ulcer disease",
    "ibs": "irritable bowel syndrome",
    "ibd": "inflammatory bowel disease",
    "crohn's": "crohn disease",
    "crohns": "crohn disease",
    "uc": "ulcerative colitis",
    "constipation": "constipation",
    "diarrhea": "diarrhea",
    "n/v": "nausea and vomiting",
    "nausea": "nausea",
    "fatty liver": "hepatic steatosis",
    "nafld": "non-alcoholic fatty liver disease",
    "nash": "non-alcoholic steatohepatitis",
    "cirrhosis": "liver cirrhosis",
    "hep c": "hepatitis c",
    "hep b": "hepatitis b",
    "gallstones": "cholelithiasis",
    "pancreatitis": "pancreatitis",

    # === 神經系統 (Neurological) ===
    "migraine headache": "migraine",
    "tension headache": "tension-type headache",
    "cluster headache": "cluster headache",
    "seizure": "epilepsy",
    "convulsion": "epilepsy",
    "epileptic": "epilepsy",
    "stroke": "cerebrovascular accident",
    "cva": "cerebrovascular accident",
    "brain attack": "cerebrovascular accident",
    "tia": "transient ischemic attack",
    "mini-stroke": "transient ischemic attack",
    "ms": "multiple sclerosis",
    "parkinson's": "parkinson disease",
    "parkinsons": "parkinson disease",
    "pd": "parkinson disease",
    "alzheimer's": "alzheimer disease",
    "alzheimers": "alzheimer disease",
    "ad": "alzheimer disease",
    "dementia": "dementia",
    "memory loss": "dementia",
    "neuropathy": "peripheral neuropathy",
    "nerve pain": "neuropathic pain",
    "als": "amyotrophic lateral sclerosis",
    "lou gehrig's disease": "amyotrophic lateral sclerosis",

    # === 精神疾病 (Psychiatric) ===
    "mdd": "major depressive disorder",
    "clinical depression": "major depressive disorder",
    "gad": "generalized anxiety disorder",
    "anxiety": "anxiety disorder",
    "panic attack": "panic disorder",
    "ocd": "obsessive-compulsive disorder",
    "ptsd": "post-traumatic stress disorder",
    "bipolar": "bipolar disorder",
    "manic depression": "bipolar disorder",
    "schizophrenia": "schizophrenia",
    "adhd": "attention deficit hyperactivity disorder",
    "add": "attention deficit disorder",
    "insomnia": "insomnia",
    "sleep disorder": "sleep disorder",
    "eating disorder": "eating disorder",
    "anorexia": "anorexia nervosa",
    "bulimia": "bulimia nervosa",
    "substance abuse": "substance use disorder",
    "addiction": "substance use disorder",
    "alcoholism": "alcohol use disorder",

    # === 內分泌系統 (Endocrine) ===
    "dm": "diabetes mellitus",
    "t2dm": "type 2 diabetes mellitus",
    "t1dm": "type 1 diabetes mellitus",
    "type 2 diabetes": "type 2 diabetes mellitus",
    "type 1 diabetes": "type 1 diabetes mellitus",
    "sugar diabetes": "diabetes mellitus",
    "high blood sugar": "hyperglycemia",
    "low blood sugar": "hypoglycemia",
    "thyroid": "thyroid disease",
    "overactive thyroid": "hyperthyroidism",
    "underactive thyroid": "hypothyroidism",
    "graves disease": "hyperthyroidism",
    "hashimoto's": "hashimoto thyroiditis",
    "goiter": "thyroid goiter",
    "pcos": "polycystic ovary syndrome",
    "cushing's": "cushing syndrome",
    "addison's": "addison disease",
    "acromegaly": "acromegaly",
    "obesity": "obesity",
    "overweight": "obesity",
    "metabolic syndrome": "metabolic syndrome",

    # === 肌肉骨骼系統 (Musculoskeletal) ===
    "ra": "rheumatoid arthritis",
    "oa": "osteoarthritis",
    "degenerative joint disease": "osteoarthritis",
    "djd": "osteoarthritis",
    "joint pain": "arthralgia",
    "back pain": "low back pain",
    "lbp": "low back pain",
    "neck pain": "cervical pain",
    "osteopenia": "osteopenia",
    "bone loss": "osteoporosis",
    "gout": "gout",
    "gouty arthritis": "gout",
    "fibro": "fibromyalgia",
    "fibromyalgia": "fibromyalgia",
    "lupus": "systemic lupus erythematosus",
    "sle": "systemic lupus erythematosus",
    "ankylosing spondylitis": "ankylosing spondylitis",
    "as": "ankylosing spondylitis",
    "psa": "psoriatic arthritis",
    "tendinitis": "tendinitis",
    "bursitis": "bursitis",
    "carpal tunnel": "carpal tunnel syndrome",

    # === 皮膚疾病 (Dermatological) ===
    "eczema": "atopic dermatitis",
    "atopic dermatitis": "atopic dermatitis",
    "psoriasis": "psoriasis",
    "acne": "acne vulgaris",
    "pimples": "acne vulgaris",
    "hives": "urticaria",
    "rash": "dermatitis",
    "skin rash": "dermatitis",
    "ringworm": "tinea",
    "athlete's foot": "tinea pedis",
    "jock itch": "tinea cruris",
    "nail fungus": "onychomycosis",
    "warts": "verruca",
    "cold sore": "herpes labialis",
    "shingles": "herpes zoster",
    "rosacea": "rosacea",
    "vitiligo": "vitiligo",
    "alopecia": "alopecia",
    "hair loss": "alopecia",
    "melanoma": "melanoma",
    "skin cancer": "skin neoplasm",

    # === 泌尿系統 (Urological) ===
    "uti": "urinary tract infection",
    "bladder infection": "cystitis",
    "kidney infection": "pyelonephritis",
    "kidney stones": "nephrolithiasis",
    "renal calculi": "nephrolithiasis",
    "ckd": "chronic kidney disease",
    "kidney failure": "renal failure",
    "esrd": "end-stage renal disease",
    "bph": "benign prostatic hyperplasia",
    "enlarged prostate": "benign prostatic hyperplasia",
    "prostatitis": "prostatitis",
    "overactive bladder": "overactive bladder",
    "oab": "overactive bladder",
    "incontinence": "urinary incontinence",
    "ed": "erectile dysfunction",
    "impotence": "erectile dysfunction",

    # === 眼科 (Ophthalmological) ===
    "pink eye": "conjunctivitis",
    "red eye": "conjunctivitis",
    "glaucoma": "glaucoma",
    "cataracts": "cataract",
    "macular degeneration": "age-related macular degeneration",
    "amd": "age-related macular degeneration",
    "dry eyes": "dry eye syndrome",
    "diabetic retinopathy": "diabetic retinopathy",
    "retinal detachment": "retinal detachment",

    # === 耳鼻喉 (ENT) ===
    "ear infection": "otitis media",
    "swimmers ear": "otitis externa",
    "sore throat": "pharyngitis",
    "strep throat": "streptococcal pharyngitis",
    "tonsillitis": "tonsillitis",
    "sinusitis": "sinusitis",
    "sinus infection": "sinusitis",
    "rhinitis": "rhinitis",
    "runny nose": "rhinorrhea",
    "hay fever": "allergic rhinitis",
    "vertigo": "vertigo",
    "dizziness": "vertigo",
    "tinnitus": "tinnitus",
    "ringing in ears": "tinnitus",
    "hearing loss": "hearing loss",

    # === 感染症 (Infectious) ===
    "covid": "covid-19",
    "covid-19": "covid-19",
    "coronavirus": "covid-19",
    "hiv": "hiv infection",
    "aids": "acquired immunodeficiency syndrome",
    "herpes": "herpes simplex",
    "hsv": "herpes simplex",
    "mono": "infectious mononucleosis",
    "ebv": "epstein-barr virus infection",
    "mrsa": "methicillin-resistant staphylococcus aureus infection",
    "staph infection": "staphylococcal infection",
    "strep infection": "streptococcal infection",
    "c diff": "clostridioides difficile infection",
    "cdiff": "clostridioides difficile infection",
    "sepsis": "sepsis",
    "blood poisoning": "sepsis",
    "meningitis": "meningitis",
    "encephalitis": "encephalitis",
    "malaria": "malaria",
    "lyme disease": "lyme disease",
    "std": "sexually transmitted infection",
    "sti": "sexually transmitted infection",
    "chlamydia": "chlamydia infection",
    "gonorrhea": "gonorrhea",
    "syphilis": "syphilis",

    # === 過敏 (Allergic) ===
    "food allergy": "food allergy",
    "drug allergy": "drug hypersensitivity",
    "seasonal allergies": "seasonal allergic rhinitis",
    "allergic reaction": "allergic reaction",
    "anaphylaxis": "anaphylaxis",
    "latex allergy": "latex allergy",

    # === 婦科 (Gynecological) ===
    "period pain": "dysmenorrhea",
    "menstrual cramps": "dysmenorrhea",
    "irregular periods": "menstrual irregularity",
    "heavy periods": "menorrhagia",
    "pms": "premenstrual syndrome",
    "pmdd": "premenstrual dysphoric disorder",
    "menopause": "menopause",
    "hot flashes": "menopausal symptoms",
    "endometriosis": "endometriosis",
    "fibroids": "uterine fibroids",
    "uterine fibroids": "uterine leiomyoma",
    "ovarian cyst": "ovarian cyst",
    "ectopic pregnancy": "ectopic pregnancy",
    "miscarriage": "spontaneous abortion",
    "infertility": "infertility",
    "vaginal infection": "vaginitis",
    "yeast infection": "vulvovaginal candidiasis",
    "bv": "bacterial vaginosis",

    # === 腫瘤/癌症 (Oncology) ===
    "breast ca": "breast cancer",
    "breast cancer": "breast neoplasm",
    "lung ca": "lung cancer",
    "lung cancer": "lung neoplasm",
    "colon ca": "colon cancer",
    "colon cancer": "colorectal cancer",
    "colorectal ca": "colorectal cancer",
    "crc": "colorectal cancer",
    "prostate ca": "prostate cancer",
    "prostate cancer": "prostate neoplasm",
    "skin ca": "skin cancer",
    "lymphoma": "lymphoma",
    "hodgkin's": "hodgkin lymphoma",
    "nhl": "non-hodgkin lymphoma",
    "leukemia": "leukemia",
    "aml": "acute myeloid leukemia",
    "all": "acute lymphoblastic leukemia",
    "cml": "chronic myeloid leukemia",
    "cll": "chronic lymphocytic leukemia",
    "multiple myeloma": "multiple myeloma",
    "mm": "multiple myeloma",
    "pancreatic ca": "pancreatic cancer",
    "ovarian ca": "ovarian cancer",
    "cervical ca": "cervical cancer",
    "bladder ca": "bladder cancer",
    "kidney ca": "kidney cancer",
    "rcc": "renal cell carcinoma",
    "brain tumor": "brain neoplasm",
    "glioblastoma": "glioblastoma",
    "gbm": "glioblastoma",
    "hcc": "hepatocellular carcinoma",
    "liver cancer": "hepatocellular carcinoma",
    "thyroid ca": "thyroid cancer",
    "esophageal ca": "esophageal cancer",
    "stomach ca": "gastric cancer",
    "gastric cancer": "gastric cancer",
    "tumor": "neoplasm",
    "malignancy": "malignant neoplasm",
    "metastasis": "metastatic disease",
    "mets": "metastatic disease",

    # === 一般症狀 (General Symptoms) ===
    "fever": "fever",
    "high temperature": "fever",
    "pain": "pain",
    "chronic pain": "chronic pain",
    "inflammation": "inflammation",
    "swelling": "edema",
    "edema": "edema",
    "fatigue": "fatigue",
    "tiredness": "fatigue",
    "weakness": "asthenia",
    "weight loss": "weight loss",
    "weight gain": "weight gain",
    "loss of appetite": "anorexia",
    "dehydration": "dehydration",
    "anemia": "anemia",
    "low iron": "iron deficiency anemia",
    "bruising": "ecchymosis",
    "bleeding": "hemorrhage",
    "clotting disorder": "coagulation disorder",
}


def load_disease_vocab(filepath: Optional[Path] = None) -> pd.DataFrame:
    """載入 TxGNN 疾病詞彙表"""
    if filepath is None:
        filepath = Path(__file__).parent.parent.parent.parent / "data" / "external" / "disease_vocab.csv"
    return pd.read_csv(filepath)


def build_disease_index(disease_df: pd.DataFrame) -> Dict[str, Tuple[str, str]]:
    """建立疾病名稱索引（關鍵詞 -> (disease_id, disease_name)）"""
    index = {}

    for _, row in disease_df.iterrows():
        disease_id = row["disease_id"]
        disease_name = row["disease_name"]
        name_upper = row["disease_name_upper"]

        # 完整名稱
        index[name_upper] = (disease_id, disease_name)

        # 提取關鍵詞（按空格和逗號分割）
        keywords = re.split(r"[,\s\-]+", name_upper)
        for kw in keywords:
            kw = kw.strip()
            if len(kw) > 3 and kw not in index:  # 只保留較長的關鍵詞
                index[kw] = (disease_id, disease_name)

    return index


def extract_indications(indication_str: str) -> List[str]:
    """從適應症/治療類別文字提取個別適應症

    使用葡萄牙語常見分隔符分割
    """
    if not indication_str:
        return []

    # 正規化
    text = indication_str.strip().lower()

    # 使用分隔符分割
    parts = re.split(r"[.;]", text)

    indications = []
    for part in parts:
        # 再用逗號細分
        sub_parts = re.split(r"[,/]", part)
        for sub in sub_parts:
            sub = sub.strip()
            # 移除常見前綴
            sub = re.sub(r"^(para |tratamento de |indicado para |usado para )", "", sub)
            # 移除常見後綴
            sub = re.sub(r"( e outros| etc\.?)$", "", sub)
            sub = sub.strip()
            if sub and len(sub) >= 2:
                indications.append(sub)

    return indications


def translate_indication(indication: str) -> List[str]:
    """將葡萄牙語適應症翻譯為英文關鍵詞"""
    keywords = []
    indication_lower = indication.lower()

    for pt, en in DISEASE_DICT.items():
        if pt in indication_lower:
            keywords.append(en.upper())

    return keywords


def map_indication_to_disease(
    indication: str,
    disease_index: Dict[str, Tuple[str, str]],
) -> List[Tuple[str, str, float]]:
    """將單一適應症映射到 TxGNN 疾病

    Returns:
        [(disease_id, disease_name, confidence), ...]
    """
    results = []

    # 翻譯為英文關鍵詞
    keywords = translate_indication(indication)

    for kw in keywords:
        # 完全匹配
        if kw in disease_index:
            disease_id, disease_name = disease_index[kw]
            results.append((disease_id, disease_name, 1.0))
            continue

        # 部分匹配
        for index_kw, (disease_id, disease_name) in disease_index.items():
            if kw in index_kw or index_kw in kw:
                results.append((disease_id, disease_name, 0.8))

    # 去重並按信心度排序
    seen = set()
    unique_results = []
    for disease_id, disease_name, conf in sorted(results, key=lambda x: -x[2]):
        if disease_id not in seen:
            seen.add(disease_id)
            unique_results.append((disease_id, disease_name, conf))

    return unique_results[:5]  # 最多返回 5 個匹配


def map_fda_indications_to_diseases(
    fda_df: pd.DataFrame,
    disease_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """將巴西 ANVISA 藥品治療類別映射到 TxGNN 疾病"""
    if disease_df is None:
        disease_df = load_disease_vocab()

    disease_index = build_disease_index(disease_df)

    results = []

    for _, row in fda_df.iterrows():
        # ANVISA 使用 CLASSE_TERAPEUTICA 而非適應症
        indication_str = row.get("CLASSE_TERAPEUTICA", "")
        if not indication_str:
            continue

        # 提取個別適應症
        indications = extract_indications(indication_str)

        for ind in indications:
            # 翻譯並映射
            matches = map_indication_to_disease(ind, disease_index)

            if matches:
                for disease_id, disease_name, confidence in matches:
                    results.append({
                        "NUMERO_REGISTRO_PRODUTO": row.get("NUMERO_REGISTRO_PRODUTO", ""),
                        "NOME_PRODUTO": row.get("NOME_PRODUTO", ""),
                        "CLASSE_TERAPEUTICA": indication_str[:100],
                        "extracted_indication": ind,
                        "disease_id": disease_id,
                        "disease_name": disease_name,
                        "confidence": confidence,
                    })
            else:
                results.append({
                    "NUMERO_REGISTRO_PRODUTO": row.get("NUMERO_REGISTRO_PRODUTO", ""),
                    "NOME_PRODUTO": row.get("NOME_PRODUTO", ""),
                    "CLASSE_TERAPEUTICA": indication_str[:100],
                    "extracted_indication": ind,
                    "disease_id": None,
                    "disease_name": None,
                    "confidence": 0,
                })

    return pd.DataFrame(results)


def get_indication_mapping_stats(mapping_df: pd.DataFrame) -> dict:
    """計算適應症映射統計"""
    total = len(mapping_df)
    mapped = mapping_df["disease_id"].notna().sum()
    unique_indications = mapping_df["extracted_indication"].nunique()
    unique_diseases = mapping_df[mapping_df["disease_id"].notna()]["disease_id"].nunique()

    return {
        "total_indications": total,
        "mapped_indications": int(mapped),
        "mapping_rate": mapped / total if total > 0 else 0,
        "unique_indications": unique_indications,
        "unique_diseases": unique_diseases,
    }
