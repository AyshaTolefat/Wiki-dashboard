from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Tuple, Dict

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

INP = DATA_DIR / "gender_occupation_with_sectors.csv"
OUT = DATA_DIR / "gender_occupation_with_isco.csv"

# -----------------------------
# ISCO Major Groups (0–9)
# -----------------------------
ISCO_MAJOR_TITLES: Dict[int, str] = {
    0: "Armed forces occupations",
    1: "Managers",
    2: "Professionals",
    3: "Technicians and associate professionals",
    4: "Clerical support workers",
    5: "Service and sales workers",
    6: "Skilled agricultural, forestry and fishery workers",
    7: "Craft and related trades workers",
    8: "Plant and machine operators, and assemblers",
    9: "Elementary occupations",
}

# -----------------------------
# Default mapping: your sectors -> ISCO Major Group
# (Conservative defaults; refined later by label rules)
# -----------------------------
SECTOR_TO_MAJOR: Dict[str, int] = {
    "Politics & Government": 1,          # often legislators/senior officials/managers
    "Business & Finance": 1,             # managers by default; clerical refined via label rules
    "Science & Research": 2,
    "Medicine & Health": 2,
    "Education": 2,
    "Law & Justice": 2,
    "Arts & Entertainment": 2,
    "Media & Journalism": 2,
    "Religion": 2,
    "Environment & Design": 2,
    "Sports": 2,                         # sports workers exist as sub-major 34/342; keep under 2 and refine
    "Public Service & Activism": 2,      # many map to 26 or 34; refine via label rules
    "Military & Security": 5,            # many are protective services (54); armed forces refined via label rules
    "Agriculture": 6,
    "Manufacturing & Trades": 7,
    "Transport & Logistics": 8,
    "Labor Workers": 9,
    "Other / Unclassified": -1,          # unknown
}

# -----------------------------
# Sub-major group refinement rules (two-digit codes)
# These are applied using occupationLabel, regardless of sector, because labels often
# identify clerical vs manager vs professional, etc.
#
# NOTE: Two-digit ISCO sub-major groups (examples):
# Managers (1): 11, 12, 13, 14
# Professionals (2): 21, 22, 23, 24, 25, 26
# Technicians/assoc (3): 31, 32, 33, 34, 35
# Clerical (4): 41, 42, 43, 44
# Service/sales (5): 51, 52, 53, 54
# Agri (6): 61, 62, 63
# Craft (7): 71, 72, 73, 74, 75
# Plant/machine (8): 81, 82, 83
# Elementary (9): 91, 92, 93, 94, 95, 96
# Armed forces (0): treated as major-only here (0) unless label is explicit.
# -----------------------------

SUB_MAJOR_RULES: list[tuple[str, int, str]] = [
    # --- Armed forces vs protective services ---
    (r"\b(armed forces|soldier|military|navy|air force|army)\b", 0, "armed forces keyword"),
    (r"\b(police|police officer|detective|security guard|bodyguard)\b", 54, "protective services"),
    (r"\b(firefighter)\b", 54, "protective services"),

    # --- Teaching professionals / education ---
    (r"\b(teacher|professor|lecturer|university teacher|educator)\b", 23, "teaching professionals"),
    (r"\b(librarian|archivist)\b", 26, "cultural professionals (library/archive)"),

    # --- Health professionals ---
    (r"\b(doctor|physician|surgeon|psychiatrist|psychologist|dentist|veterinarian|pharmacist)\b", 22, "health professionals"),
    (r"\b(nurse|midwife)\b", 22, "health professionals"),

    # --- Legal professionals ---
    (r"\b(judge|lawyer|attorney|jurist|solicitor|notary|prosecutor)\b", 26, "legal, social and cultural professionals"),

    # --- Business & admin professionals vs clerical ---
    (r"\b(economist|accountant|auditor|financial analyst|analyst)\b", 24, "business and administration professionals"),
    (r"\b(consultant|business executive|entrepreneur|manager|director|chairperson|chief|executive|ceo)\b", 12, "administrative and commercial managers"),
    (r"\b(secretary|administrative assistant|office worker|clerk)\b", 41, "general and keyboard clerks/secretaries"),

    # --- Science & engineering professionals ---
    (r"\b(physicist|mathematician|scientist|researcher|engineer|inventor|statistician)\b", 21, "science and engineering professionals"),
    (r"\b(political scientist|sociologist|historian|linguist|logician)\b", 26, "social/cultural professionals"),

    # --- Media / journalism / arts / culture ---
    (r"\b(journalist|editor|news presenter|presenter|broadcaster|documentarian|filmmaker)\b", 26, "cultural professionals (media/journalism)"),
    (r"\b(actor|film actor|television actor|singer|rapper|musician|composer|screenwriter|playwright|producer|film producer|director|film director|cinematographer|photographer)\b", 26, "cultural professionals (performing/creative)"),
    (r"\b(painter|sculptor|cartoonist|illustrator|artist|performance artist|video artist)\b", 26, "cultural professionals (visual/creative)"),
    (r"\b(beauty pageant contestant|model|fashion model)\b", 52, "sales/service-related public-facing work (heuristic)"),

    # --- Sports ---
    (r"\b(athlete|footballer|tennis player|boxer|cyclist|runner|volleyball player|triathlete|taekwondo)\b", 34, "sports and fitness workers"),

    # --- Agriculture / forestry ---
    (r"\b(farmer|farmworker|agricultural worker)\b", 61, "market-oriented skilled agricultural workers"),
    (r"\b(forester|forestry worker)\b", 62, "forestry and related workers"),

    # --- Transport (drivers etc.) ---
    (r"\b(driver|pilot|sailor|railway worker)\b", 83, "drivers and mobile-plant operators"),

    # --- Craft / trades / printing ---
    (r"\b(typographer|printer|printmaker|lithographer)\b", 73, "handicraft and printing workers"),
    (r"\b(mechanic|machinist|technician)\b", 72, "metal, machinery and related trades (heuristic)"),
    (r"\b(manufacturer)\b", 75, "other craft and related trades (heuristic)"),

    # --- Elementary / labour ---
    (r"\b(labou?rer|miner|gold miner|construction worker|factory worker|cleaner)\b", 93, "labourers in mining/construction/manufacturing/transport"),
]

def normalize_text(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip().lower())

def infer_major_from_sector(sector: str) -> Optional[int]:
    if sector in SECTOR_TO_MAJOR:
        m = SECTOR_TO_MAJOR[sector]
        return None if m == -1 else m
    return None

def infer_sub_major_from_label(label: str) -> Tuple[Optional[int], Optional[str]]:
    s = normalize_text(label)
    for pat, code, note in SUB_MAJOR_RULES:
        if re.search(pat, s):
            return code, note
    return None, None

def sub_major_title(code: int) -> str:
    # Minimal titles for interpretability (not exhaustive).
    titles = {
        # Managers
        11: "Chief executives, senior officials and legislators",
        12: "Administrative and commercial managers",
        13: "Production and specialized services managers",
        14: "Hospitality, retail and other services managers",
        # Professionals
        21: "Science and engineering professionals",
        22: "Health professionals",
        23: "Teaching professionals",
        24: "Business and administration professionals",
        25: "Information and communications technology professionals",
        26: "Legal, social and cultural professionals",
        # Technicians & associate
        34: "Legal, social, cultural and related associate professionals / Sports & fitness workers (incl. 342)",
        35: "Information and communications technicians",
        # Clerical
        41: "General and keyboard clerks",
        42: "Customer services clerks",
        43: "Numerical and material recording clerks",
        44: "Other clerical support workers",
        # Service & sales
        51: "Personal service workers",
        52: "Sales workers",
        53: "Personal care workers",
        54: "Protective services workers",
        # Agriculture
        61: "Market-oriented skilled agricultural workers",
        62: "Market-oriented skilled forestry, fishery and hunting workers",
        63: "Subsistence farmers, fishers, hunters and gatherers",
        # Craft
        71: "Building and related trades workers",
        72: "Metal, machinery and related trades workers",
        73: "Handicraft and printing workers",
        74: "Electrical and electronic trades workers",
        75: "Food processing, woodworking, garment and other craft and related trades workers",
        # Plant/machine
        81: "Stationary plant and machine operators",
        82: "Assemblers",
        83: "Drivers and mobile plant operators",
        # Elementary
        91: "Cleaners and helpers",
        92: "Agricultural, forestry and fishery labourers",
        93: "Labourers in mining, construction, manufacturing and transport",
        94: "Food preparation assistants",
        95: "Street and related sales and service workers",
        96: "Refuse workers and other elementary workers",
    }
    return titles.get(code, "Sub-major group (title not set in script)")

def main():
    if not INP.exists():
        raise SystemExit(f"Missing input: {INP}")

    df = pd.read_csv(INP)

    if "occupationLabel" not in df.columns or "sector" not in df.columns:
        raise SystemExit("Expected columns: occupationLabel, sector")

    major_codes = []
    major_titles = []
    sub_codes = []
    sub_titles = []
    methods = []
    notes = []

    for _, row in df.iterrows():
        sector = str(row.get("sector", "")).strip()
        label = str(row.get("occupationLabel", "")).strip()

        # Start with sector -> major
        major = infer_major_from_sector(sector)
        method = "sector_to_major"
        note_parts = []

        # Try to infer sub-major directly from label
        sub, sub_note = infer_sub_major_from_label(label)
        if sub is not None:
            # If label suggests armed forces (0) or any two-digit code, reconcile major
            if sub == 0:
                major = 0
            else:
                major = int(str(sub)[0])  # 23 -> 2, 54 -> 5, etc.
            method = "label_rule"
            if sub_note:
                note_parts.append(sub_note)

        # If still no major, mark unknown
        if major is None:
            major_codes.append("")
            major_titles.append("")
            sub_codes.append("")
            sub_titles.append("")
            methods.append("unmapped")
            notes.append("No sector/label rule matched; consider manual review")
            continue

        major_codes.append(int(major))
        major_titles.append(ISCO_MAJOR_TITLES[int(major)])

        if sub is None or sub == 0:
            sub_codes.append("")
            sub_titles.append("")
        else:
            sub_codes.append(int(sub))
            sub_titles.append(sub_major_title(int(sub)))

        methods.append(method)
        notes.append("; ".join(note_parts) if note_parts else "")

    df["isco_major_code"] = major_codes
    df["isco_major_title"] = major_titles
    df["isco_sub_major_code"] = sub_codes
    df["isco_sub_major_title"] = sub_titles
    df["isco_mapping_method"] = methods
    df["isco_mapping_notes"] = notes

    df.to_csv(OUT, index=False)
    print(f"Wrote: {OUT}")

    # Quick summary for sanity-checking
    print("\nISCO major distribution:")
    print(df["isco_major_code"].value_counts(dropna=False))

    print("\nTop unmapped occupationLabels:")
    unmapped = df[df["isco_mapping_method"] == "unmapped"]
    if not unmapped.empty:
        print(unmapped["occupationLabel"].value_counts().head(25))

if __name__ == "__main__":
    main()
