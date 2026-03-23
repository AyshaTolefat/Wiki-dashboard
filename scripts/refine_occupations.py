from __future__ import annotations

import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

INP = DATA_DIR / "gender_occupation_with_isco_refined.csv"
OUT = DATA_DIR / "gender_occupation_with_isco_refined.csv"

ISCO_MAJOR_TITLES = {
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

# Optional: map ISCO major groups to your dashboard "sector" labels
SECTOR_FROM_ISCO_MAJOR = {
    0: "Armed forces",
    1: "Management / Business",
    2: "Professional / Academic",
    3: "Technical / Associate professional",
    4: "Clerical / Administrative",
    5: "Service / Sales",
    6: "Agriculture / Forestry / Fishery",
    7: "Craft / Trades",
    8: "Operators / Drivers",
    9: "Elementary / Labour",
}


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def set_isco(
    df: pd.DataFrame,
    idx,
    major: int | str,
    sub: int | str = "",
    method: str = "",
    note: str = "",
):
    """
    Set ISCO major/sub-major info and also fill sector columns so the dashboard doesn't show NaN.
    """
    if major == "":
        df.loc[idx, "isco_major_code"] = ""
        df.loc[idx, "isco_major_title"] = ""
        df.loc[idx, "isco_sub_major_code"] = ""
        df.loc[idx, "isco_sub_major_title"] = ""
    else:
        major = int(major)
        df.loc[idx, "isco_major_code"] = major
        df.loc[idx, "isco_major_title"] = ISCO_MAJOR_TITLES.get(major, "")

        if str(sub).strip() != "":
            df.loc[idx, "isco_sub_major_code"] = int(sub)
            df.loc[idx, "isco_sub_major_title"] = ""
        else:
            df.loc[idx, "isco_sub_major_code"] = ""
            df.loc[idx, "isco_sub_major_title"] = ""

        # ✅ also set sector from ISCO (so it stops being "Other / Unclassified")
        df.loc[idx, "sector"] = SECTOR_FROM_ISCO_MAJOR.get(major, "Other / Unclassified")
        df.loc[idx, "sector_source"] = "isco_major_rule"

    if method:
        df.loc[idx, "isco_mapping_method"] = method

    if note:
        existing = str(df.loc[idx, "isco_mapping_notes"] or "").strip()
        df.loc[idx, "isco_mapping_notes"] = (existing + "; " + note).strip("; ").strip()


def mark_non_occupation(df: pd.DataFrame, idx, note: str):
    """
    Standard way to mark a row as NOT an occupation (and keep UI tidy).
    """
    df.loc[idx, "isco_mapping_method"] = "non_occupation"

    existing = str(df.loc[idx, "isco_mapping_notes"] or "").strip()
    df.loc[idx, "isco_mapping_notes"] = (existing + "; " + note).strip("; ").strip()

    # clear ISCO
    df.loc[idx, "isco_major_code"] = ""
    df.loc[idx, "isco_major_title"] = ""
    df.loc[idx, "isco_sub_major_code"] = ""
    df.loc[idx, "isco_sub_major_title"] = ""

    # clean sector
    df.loc[idx, "sector"] = "Not an occupation"
    df.loc[idx, "sector_source"] = "non_occupation_rule"


def main():
    if not INP.exists():
        raise SystemExit(f"Missing input: {INP}")

    df = pd.read_csv(INP)

    # ---- Ensure columns exist ----
    needed_cols = [
        "occupationLabel",
        "sector",
        "sector_source",
        "isco_major_code",
        "isco_major_title",
        "isco_sub_major_code",
        "isco_sub_major_title",
        "isco_mapping_method",
        "isco_mapping_notes",
    ]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = ""

    # ---- Fix NaNs so dashboard never shows "nan" ----
    df["occupationLabel"] = df["occupationLabel"].fillna("")
    df["sector"] = df["sector"].fillna("Other / Unclassified")
    df["sector_source"] = df["sector_source"].fillna("")
    df["isco_mapping_method"] = df["isco_mapping_method"].fillna("")
    df["isco_mapping_notes"] = df["isco_mapping_notes"].fillna("")
    for c in ["isco_major_title", "isco_sub_major_title"]:
        df[c] = df[c].fillna("")

    # Only target rows currently unmapped (or your special-territory defaults)
    unmapped_mask = df["isco_mapping_method"].isin(["", "unmapped", "special_territory_default"])

    # 1) Clearly NOT occupations under ISCO: crime/status/rank/relationship roles
    # NOTE: labels are normalized to lowercase, so patterns should be lowercase.
    non_occ_patterns = [
        r"\b(criminal|serial killer|murderer|drug trafficker|warlord)\b",
        r"\b(political prisoner|prisoner|pensioner|housewife|homemaker|retired|student)\b",
        r"\b(aristocrat|royalty|noble|princess|prince|duke|count|baron)\b",
        r"\b(first lady)\b",
        r"\b(volunteer|feminist|bibliophile|refugee|polyglot|contemporary witness|informant)\b",
        r"\b(warrior|mercenary|combatant|archer|gangster|robber|rapist|torturer)\b",
        r"\b(consort|adventurer|leader|employee|rescuer)\b",
        r"\b(philatelist|spree killer|thief|ruler|continuity|seer|spiritual medium|procurer)\b",
        r"\b(activism|business undergraduate|lifestyle guru|coaching|self-employed person)\b",
        r"\b(professions libérales et assimilés|thinker|mujahid|conscientious objector)\b",
        r"\b(documentary participant|unemployed|chernobyl liquidator|hiker)\b",
        r"\b(animal protectionist)\b",
        r"\b(crime boss|assassin|courtier|hermit|wartime collaborator)\b",
        r"\b(openstreetmap contributor|engelandvaarder|diploma of business administration)\b",
        r"\b(intern|apprentice)\b",
        r"^q\d+$",  # wikidata artifact labels after norm()
        r"\b(theatrical occupation|speaker)\b",
        r"\b(culture personality|public administration|city council|anciens cadres)\b",
        r"\bmilitary personnel\b",
        r"\b(literary criticism|history|teaching|entrepreneurship|restoration|legal profession)\b",
        r"\b(smuggler|convict|habitual offender|fugitive|brigand|victim)\b",
        r"\b(alien abduction claimant|autodidact|religious person)\b",
        r"\b(sports fan|fan|member)\b",
        r"\b(independence fighter)\b",
        r"\b(gestapo agent)\b",
        r"\b(bdsm practitioner)\b",
        r"\b(art department)\b",
        r"\b(beauty pageant contestant|citizen|professional|chair|social media|bass guitar|athletics|business administration|civil service)\b",
        r"\b(activist|delinquent|bandit|mobster|contract killer|counterfeiter|unofficial collaborator)\b",
        r"\b(holder of the procuration)\b",
    ]

    # 2) Rules: High-frequency “real” occupations (your current big list)
    rules = [
        # Politics / law / public admin (Professionals)
        (r"\bpolitician\b", 2, "", "label_rule_v2", "politician -> professionals"),
        (r"\b(lawyer|jurist|judge)\b", 2, "", "label_rule_v2", "law -> professionals"),
        (r"\b(diplomat)\b", 2, "", "label_rule_v2", "diplomat -> professionals"),
        (r"\b(civil servant)\b", 4, "", "label_rule_v2", "civil servant -> clerical/admin"),

        # ---- FINAL ROUND 5 ----
        # Academic / language / culture
        (r"\bterminologist\b", 2, "", "label_rule_v8", "language professional"),
        (r"\bvexillologist\b", 2, "", "label_rule_v8", "academic specialist"),
        (r"\bmuseographer\b", 2, "", "label_rule_v8", "museum professional"),

        # Media / arts
        (r"\banimator\b", 2, "", "label_rule_v8", "creative professional"),
        (r"\bsongwriter\b", 2, "", "label_rule_v8", "creative professional"),
        (r"\bmaestro\b", 2, "", "label_rule_v8", "music professional"),
        (r"\bprojectionist\b", 3, "", "label_rule_v8", "technical associate professional"),
        (r"\bauthor\b", 2, "", "label_rule_v8", "writer professional"),

        # Technology
        (r"\btechnology specialist\b", 2, "", "label_rule_v8", "technical professional"),

        # Business
        (r"\bhotel owner\b", 1, "", "label_rule_v8", "business manager"),
        (r"\blandlord\b", 1, "", "label_rule_v8", "property manager"),

        # Health
        (r"\bradiation therapist\b", 2, "", "label_rule_v8", "health professional"),

        # Public safety
        (r"\bpolice officer\b", 3, "", "label_rule_v8", "associate professional"),

        # Logistics
        (r"\bdispatcher\b", 3, "", "label_rule_v8", "associate professional"),

        # Sports
        (r"\bboxer\b", 3, "", "label_rule_v8", "sports competitor"),

        # Education / academia (Professionals)
        (r"\b(professor|university teacher|academic|researcher|scientist|historian|philologist|literary scholar)\b", 2, "", "label_rule_v2", "education/academia -> professionals"),
        (r"\b(teacher|school teacher|primary school teacher|educator|music educator|drama teacher)\b", 2, "", "label_rule_v2", "teacher -> professionals"),

        # Health (Professionals)
        (r"\b(physician|doctor|pediatrician|pathologist|pharmacist|surgeon|cardiac surgeon)\b", 2, "", "label_rule_v2", "health -> professionals"),

        # Media / arts / writing (Professionals)
        (r"\b(actor|film actor|stage actor|voice actor|television actor)\b", 2, "", "label_rule_v2", "actor -> professionals"),
        (r"\b(writer|prose writer|poet|playwright|screenwriter|essayist|journalist|opinion journalist|translator|lyricist)\b", 2, "", "label_rule_v2", "writing/media -> professionals"),
        (r"\b(artist|painter|printmaker|illustrator|sculptor|poster artist|cinematographer|film director|theatrical director|curator)\b", 2, "", "label_rule_v2", "arts -> professionals"),
        (r"\b(musician|singer|opera singer|composer|conductor|guitarist|jazz guitarist|singer-songwriter)\b", 2, "", "label_rule_v2", "music -> professionals"),

        # Business / management
        (r"\b(entrepreneur|businessperson|manager|director|founder)\b", 1, "", "label_rule_v2", "business/managerial -> managers"),

        # Sports
        (r"\b(association football player|football player|athlete|athletics competitor|high jumper|volleyball player|wrestler)\b", 3, "", "label_rule_v2", "sports competitor -> associate professionals"),
        (r"\b(association football coach|volleyball coach|coach)\b", 3, "", "label_rule_v2", "coach -> associate professionals"),
        (r"\b(referee)\b", 3, "", "label_rule_v2", "referee -> associate professionals"),
        (r"\b(sports executive)\b", 1, "", "label_rule_v2", "sports exec -> managers"),

        # Engineering / architecture
        (r"\b(engineer|civil engineer|chemical engineer|electrical engineer|architect)\b", 2, "", "label_rule_v2", "engineering/architecture -> professionals"),

        # Agriculture
        (r"\b(farmer)\b", 6, "", "label_rule_v2", "farmer -> skilled agri"),
        (r"\b(fisher|fisherman|fisherwoman)\b", 6, 62, "label_rule_v2", "fishery worker"),

        # Religion
        (r"\b(priest|monk|hegumen|archimandrite|presbyter|deacon|theologian|religious sister)\b", 2, "", "label_rule_v2", "religious occupation -> professionals"),

        # ---- HIGH IMPACT REMAINING UNMAPPED ----
        (r"\bscholar\b", 2, "", "label_rule_v3", "scholar -> professionals"),
        (r"\b(functionary|commissioner)\b", 2, "", "label_rule_v3", "public administration -> professionals"),
        (r"\b(preservationist|museum professional)\b", 2, "", "label_rule_v3", "heritage/culture -> professionals"),
        (r"\b(zootechnician)\b", 3, "", "label_rule_v3", "animal production technician"),
        (r"\b(winemaker)\b", 6, "", "label_rule_v3", "agricultural producer"),
        (r"\b(bookbinder)\b", 7, "", "label_rule_v3", "craft trade"),
        (r"\b(bookkeeper)\b", 4, "", "label_rule_v3", "clerical support"),
        (r"\b(actuary)\b", 2, "", "label_rule_v3", "finance professional"),
        (r"\b(copywriter)\b", 2, "", "label_rule_v3", "communication professional"),
        (r"\b(radio operator)\b", 3, "", "label_rule_v3", "technical operator"),
        (r"\b(tonmeister)\b", 3, "", "label_rule_v3", "sound technician"),
        (r"\b(sommelier)\b", 5, "", "label_rule_v3", "service worker"),
        (r"\b(evangelist)\b", 2, "", "label_rule_v3", "religious professional"),

        # ---- FINAL CLEANUP RULES ----
        (r"\bprostitute\b", 5, "", "label_rule_v4", "sex worker -> service worker"),
        (r"\bproprietor\b", 1, "", "label_rule_v4", "business owner -> manager"),
        (r"\b(coordinator|interviewer|recording supervisor|radio employee)\b", 3, "", "label_rule_v4", "associate professional"),
        (r"\bcadres de la fonction publique\b", 2, "", "label_rule_v4", "public administration professional"),
        (r"\byogi\b", 2, "", "label_rule_v4", "religious professional"),
        (r"\boperator\b", 8, "", "label_rule_v4", "machine/operator heuristic"),

        # ---- FINAL CLEANUP ROUND 2 ----
        (r"\binnkeeper\b", 5, "", "label_rule_v5", "innkeeper -> service worker"),
        (r"\bstevedore\b", 9, "", "label_rule_v5", "dock labourer -> elementary occupation"),
        (r"\bair traffic controller\b", 3, "", "label_rule_v5", "transport technician"),
        (r"\bswimmer\b", 3, "", "label_rule_v5", "sports competitor"),
        (r"\bbalkanologist\b", 2, "", "label_rule_v5", "academic specialist"),
        (r"\bmedia professional\b", 2, "", "label_rule_v5", "media professional"),
        (r"\bnarrator\b", 2, "", "label_rule_v5", "performing arts professional"),
        (r"\bphotographer\b", 2, "", "label_rule_v5", "creative professional"),
        (r"\bcultural agent\b", 3, "", "label_rule_v5", "cultural associate professional"),
        (r"\btelegraphist\b", 3, "", "label_rule_v5", "communications technician"),
        (r"\bempresario\b", 1, "", "label_rule_v5", "manager/promoter"),
        (r"\bgastronomist\b", 2, "", "label_rule_v5", "food professional"),
        (r"\bkhaṭīb\b", 2, "", "label_rule_v5", "religious preacher"),
        (r"\bdawah\b", 2, "", "label_rule_v5", "religious professional"),
        (r"\bcall girl\b", 5, "", "label_rule_v5", "sex worker -> service worker"),

        # ---- FINAL ROUND 3 ----
        (r"\bturner\b", 7, "", "label_rule_v6", "metal/wood trade"),
        (r"\bwarehouseman\b", 9, "", "label_rule_v6", "warehouse labour"),
        (r"\bcourier\b", 9, "", "label_rule_v6", "delivery worker"),
        (r"\bbasketball player\b", 3, "", "label_rule_v6", "sports competitor"),
        (r"\baudiologist\b", 2, "", "label_rule_v6", "health professional"),
        (r"\bunlicensed assistive personnel\b", 3, "", "label_rule_v6", "health associate professional"),
        (r"\bcourt bailiff\b", 3, "", "label_rule_v6", "legal associate professional"),
        (r"\bparalegal\b", 3, "", "label_rule_v6", "legal associate professional"),
        (r"\bpollster\b", 3, "", "label_rule_v6", "survey associate professional"),
        (r"\bcensor\b", 2, "", "label_rule_v6", "regulatory professional"),
        (r"\bfilm distributor\b", 1, "", "label_rule_v6", "business manager"),
        (r"\bdriving instructor\b", 2, "", "label_rule_v6", "instructor -> professional"),
        (r"\bmodel\b", 2, "", "label_rule_v6", "performing arts professional"),
        (r"\bdiscussion moderator\b", 3, "", "label_rule_v6", "communication associate professional"),

        # ---- FINAL ROUND 4 ----
        (r"\bgraphologist\b", 2, "", "label_rule_v7", "specialist professional"),
        (r"\bhistorical geographer\b", 2, "", "label_rule_v7", "academic professional"),
        (r"\bergonomist\b", 2, "", "label_rule_v7", "health/safety professional"),
        (r"\bexpeditor\b", 4, "", "label_rule_v7", "logistics clerk"),
        (r"\breceptionist\b", 4, "", "label_rule_v7", "clerical support"),
        (r"\bsport cyclist\b", 3, "", "label_rule_v7", "sports competitor"),
        (r"\bspecial effects supervisor\b", 2, "", "label_rule_v7", "media professional"),
        (r"\btelevision presenter\b", 2, "", "label_rule_v7", "media professional"),
        (r"\bfilm producer\b", 1, "", "label_rule_v7", "managerial role"),
        (r"\bsystem administrator\b", 2, "", "label_rule_v7", "IT professional"),
        (r"\binsurer\b", 2, "", "label_rule_v7", "financial professional"),
        (r"\bfundraiser\b", 3, "", "label_rule_v7", "associate professional"),
        (r"\brepresentative\b", 3, "", "label_rule_v7", "associate professional"),
        (r"\bguide\b", 5, "", "label_rule_v7", "service worker"),
        (r"\babogado\b", 2, "", "label_rule_v7", "lawyer (Spanish)"),
        (r"\bpublic relations\b", 2, "", "label_rule_v7", "PR professional"),
        (r"\bfacilitator\b", 3, "", "label_rule_v7", "associate professional"),
    ]

    for i in df[unmapped_mask].index:
        label = norm(df.at[i, "occupationLabel"])

        # --- First: immediately non-occupations (fast & strict) ---
        if any(re.search(p, label) for p in non_occ_patterns):
            mark_non_occupation(df, i, "Not an occupation under ISCO (status/role/crime/rank/activity/identity)")
            continue

        # --- label_rule_v9 (kept as explicit extra layer) ---
        if re.search(r"^q\d+$", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Raw Wikidata QID without resolved label"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        if re.search(r"\b(it professional)\b", label):
            set_isco(df, i, 2, method="label_rule_v9", note="IT professional -> Professionals")
            continue

        if re.search(r"\b(information professional)\b", label):
            set_isco(df, i, 2, method="label_rule_v9", note="Information professional -> Professionals")
            continue

        if re.search(r"\b(technology specialist)\b", label):
            set_isco(df, i, 2, method="label_rule_v9", note="Technology specialist -> Professionals")
            continue

        if re.search(r"\b(health personnel)\b", label):
            set_isco(df, i, 2, method="label_rule_v9", note="Health personnel -> Professionals")
            continue

        if re.search(r"\b(financial planner)\b", label):
            set_isco(df, i, 2, method="label_rule_v9", note="Financial planner -> Professionals")
            continue

        if re.search(r"\b(cricketer|badminton player|paratriathlete)\b", label):
            set_isco(df, i, 3, method="label_rule_v9", note="Sport competitor")
            continue

        if re.search(r"\b(prophet)\b", label):
            set_isco(df, i, 2, method="label_rule_v9", note="Religious figure")
            continue

        if re.search(r"\b(vice president)\b", label):
            set_isco(df, i, 1, method="label_rule_v9", note="Executive management role")
            continue

        if re.search(r"\b(seaman|sailor)\b", label):
            set_isco(df, i, 8, method="label_rule_v9", note="Seaman / Sailor")
            continue

        if re.search(r"\b(pyrotechnician)\b", label):
            set_isco(df, i, 7, method="label_rule_v9", note="Pyrotechnician -> Craft/trades")
            continue

        if re.search(r"\b(gambler)\b", label) and "professional gambler" not in label:
            mark_non_occupation(df, i, "Lifestyle term, not formal occupation")
            continue

        # --- label_rule_v10 (your latest block, now correctly placed) ---
        if re.search(r"\bvaccinologist\b", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Medical specialist")
            continue

        if re.search(r"\bcorrector\b", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Proofreader/editor")
            continue

        if re.search(r"\binterpreter\b", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Language professional")
            continue

        if re.search(r"\bfashion designer\b", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Creative professional")
            continue

        if re.search(r"\bsociologist\b", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Academic professional")
            continue

        if re.search(r"\bdeltiologist\b", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Stamp specialist")
            continue

        if re.search(r"\bchildcare provider\b", label):
            set_isco(df, i, 5, method="label_rule_v10", note="Service occupation")
            continue

        if re.search(r"\bbuyer\b", label):
            set_isco(df, i, 3, method="label_rule_v10", note="Commercial associate professional")
            continue

        if re.search(r"\bsprinter\b", label):
            set_isco(df, i, 3, method="label_rule_v10", note="Athlete")
            continue

        if re.search(r"ingénieurs et cadres techniques", label):
            set_isco(df, i, 2, method="label_rule_v10", note="Engineers / technical executives")
            continue

        # --- label_rule_v11 ---

        # Real occupations that should be mapped
        if re.search(r"\bstage management\b", label):
            set_isco(df, i, 3, method="label_rule_v11", note="Stage management -> associate professional")
            continue

        if re.search(r"\bmethodologist\b", label):
            set_isco(df, i, 2, method="label_rule_v11", note="Methodologist -> professional")
            continue

        if re.search(r"\bdelegate\b", label):
            set_isco(df, i, 3, method="label_rule_v11", note="Delegate -> associate professional (generic)")
            continue

        if re.search(r"\bcycle sport\b", label):
            set_isco(df, i, 3, method="label_rule_v11", note="Cycle sport -> sports competitor")
            continue

        # These are occupations but might be missing from your rules list due to ordering
        if re.search(r"\barchaeologist\b", label):
            set_isco(df, i, 2, method="label_rule_v11", note="Archaeologist -> professional")
            continue

        if re.search(r"\bpublisher\b", label):
            set_isco(df, i, 1, method="label_rule_v11", note="Publisher -> manager/business role (generic)")
            continue

        if re.search(r"\baltar server\b", label):
            # This is more of a role than a labour-market occupation; treat as non-occupation for consistency
            mark_non_occupation(df, i, "Religious service role, not a labour-market occupation")
            continue

        # --- label_rule_v12 ---

        # Real occupations -> map
        if re.search(r"\blinguist\b", label):
            set_isco(df, i, 2, method="label_rule_v12", note="Linguist -> professional")
            continue

        if re.search(r"\belectromechanic\b", label):
            set_isco(df, i, 7, method="label_rule_v12", note="Electromechanic -> craft/trades")
            continue

        if re.search(r"\bwasherwoman\b", label):
            set_isco(df, i, 5, method="label_rule_v12", note="Washerwoman -> service occupation (cleaning/laundry)")
            continue

        if re.search(r"\brecord producer\b", label):
            set_isco(df, i, 2, method="label_rule_v12", note="Record producer -> media professional")
            continue

        if re.search(r"\bscoutmaster\b", label):
            set_isco(df, i, 2, method="label_rule_v12", note="Scoutmaster -> education/youth professional (generic)")
            continue

        if re.search(r"\bticket controller\b", label):
            set_isco(df, i, 5, method="label_rule_v12", note="Ticket controller -> service occupation")
            continue

        if re.search(r"\bdental person\b", label):
            set_isco(df, i, 2, method="label_rule_v12", note="Dental health worker -> professional (generic)")
            continue

        if re.search(r"\bfemale impersonator\b", label):
            set_isco(df, i, 2, method="label_rule_v12", note="Performer -> professional")
            continue

        if re.search(r"\bsponsor\b", label):
            # "Sponsor" is often a role, but when used as occupation it usually implies patron/business sponsor.
            # Map conservatively to management/business.
            set_isco(df, i, 1, method="label_rule_v12", note="Sponsor -> business role (generic)")
            continue

        # Ambiguous / cultural / non-English term: duojár
        # If you prefer not to guess: mark ambiguous_label rather than force a wrong ISCO.
        if re.search(r"\bduojár\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Non-English/unclear label; left ambiguous"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        # Numerologist / prognostician: treat as non-occupation in ISCO framing (occult claim roles)
        if re.search(r"\b(numerologist|prognostician)\b", label):
            mark_non_occupation(df, i, "Occult/claim role; not mapped to labour-market ISCO groups")
            continue

        # referent: too context-dependent (job title in some orgs, but not stable)
        if re.search(r"\breferent\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Too generic/context-dependent to map reliably"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

                # --- label_rule_v13 (batch for latest top unmapped) ---

        # ✅ Sports competitors
        if re.search(r"\b(handball player)\b", label):
            set_isco(df, i, 3, method="label_rule_v13", note="Handball player -> associate professionals (sports)")
            continue

        # ✅ Clerical / admin
        if re.search(r"\b(office administrator)\b", label):
            set_isco(df, i, 4, method="label_rule_v13", note="Office administrator -> clerical/admin")
            continue

        # ✅ Health / pharmacy
        if re.search(r"\b(druggist)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Druggist -> health professional (generic)")
            continue

        # ✅ Transport / drivers
        if re.search(r"\b(coachman)\b", label):
            set_isco(df, i, 8, method="label_rule_v13", note="Coachman -> drivers/operators")
            continue

        # ✅ Film / media
        if re.search(r"\b(film colorist)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Film colorist -> media/creative professional")
            continue

        # ✅ Arts / writing
        if re.search(r"\b(comedian)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Comedian -> performing arts professional")
            continue

        if re.search(r"\b(novelist)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Novelist -> writer (professional)")
            continue

        # ✅ Religion (work role)
        if re.search(r"\b(sacristan)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Sacristan -> religious occupation (mapped for dashboard)")
            continue

        if re.search(r"\b(spiritualist)\b", label):
            # "spiritualist" can be identity; treat as ambiguous rather than force wrong mapping
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Spiritualist is context-dependent (identity vs occupation); left ambiguous"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        # ✅ Rights / activism (work role)
        if re.search(r"\b(human rights defender)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Human rights defender -> professional (generic)")
            continue

        # ✅ Agriculture
        if re.search(r"\b(cattle rearer)\b", label):
            set_isco(df, i, 6, method="label_rule_v13", note="Cattle rearer -> skilled agricultural worker")
            continue

        if re.search(r"\b(cotton planter)\b", label):
            set_isco(df, i, 6, method="label_rule_v13", note="Cotton planter -> skilled agricultural worker")
            continue

        # ✅ Rail / industry worker
        if re.search(r"\b(railwayman)\b", label):
            set_isco(df, i, 8, method="label_rule_v13", note="Railway worker -> operators/transport worker (generic)")
            continue

        # ✅ Delivery / service
        if re.search(r"\b(milk deliverer)\b", label):
            set_isco(df, i, 5, method="label_rule_v13", note="Milk deliverer -> service worker")
            continue

        # ✅ Care work
        if re.search(r"\b(caregiver)\b", label):
            set_isco(df, i, 5, method="label_rule_v13", note="Caregiver -> service/care worker")
            continue

        # ✅ Law (German label)
        if re.search(r"\b(anwalt)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Anwalt -> lawyer -> professional")
            continue

        # ✅ Magic / performance
        if re.search(r"\b(magician)\b", label):
            set_isco(df, i, 2, method="label_rule_v13", note="Magician -> performing arts professional")
            continue

        # ✅ Library / information field terms (keep cautious)
        if re.search(r"\b(library science)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Library science is a field of study; not a specific occupation"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        # ✅ General “field” terms you probably want as non-occupation
        if re.search(r"\b(management|social work|commercial|visual arts|jurisprudence|ordination|coin collecting)\b", label):
            mark_non_occupation(df, i, "Field/area/qualification/collection activity rather than occupation label")
            continue

        # ✅ Wealth/status terms
        if re.search(r"\b(millionaire|privatier)\b", label):
            mark_non_occupation(df, i, "Wealth/status term, not an occupation")
            continue

        # ✅ Political/historical fighter labels (usually identity/role)
        if re.search(r"\b(armenian freedom fighter|armenian freedom fighter)\b", label):
            mark_non_occupation(df, i, "Political/militant role label; treated as non-occupation for ISCO mapping")
            continue

                # --- label_rule_v14 ---

        # ✅ Clear professionals (major group 2)
        if re.search(r"\b(chemist)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Chemist -> professional")
            continue

        if re.search(r"\b(psychologist)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Psychologist -> professional")
            continue

        if re.search(r"\b(pedagogue)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Pedagogue -> education professional")
            continue

        if re.search(r"\b(graphic designer)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Graphic designer -> creative professional")
            continue

        if re.search(r"\b(addictologist)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Medical specialist -> professional")
            continue

        if re.search(r"\b(proviseur)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="School head (French) -> education professional")
            continue

        if re.search(r"\b(marketologue)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Marketing specialist -> professional")
            continue

        # ✅ Performing arts / sports
        if re.search(r"\b(pianist)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Pianist -> performing arts professional")
            continue

        if re.search(r"\b(radio personality)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Media personality -> professional")
            continue

        if re.search(r"\b(rower|rugby union player)\b", label):
            set_isco(df, i, 3, method="label_rule_v14", note="Sport competitor")
            continue

        if re.search(r"\b(panelist)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Media panelist -> professional")
            continue

        # ✅ Maritime / transport
        if re.search(r"\b(master mariner)\b", label):
            set_isco(df, i, 8, method="label_rule_v14", note="Ship captain -> transport/operator")
            continue

        # ✅ Skilled trades
        if re.search(r"\b(handyman)\b", label):
            set_isco(df, i, 7, method="label_rule_v14", note="Handyman -> craft/trade worker")
            continue

        if re.search(r"\b(weaving master)\b", label):
            set_isco(df, i, 7, method="label_rule_v14", note="Textile craft specialist")
            continue

        # ✅ Military roles
        if re.search(r"\b(sapper)\b", label):
            set_isco(df, i, 0, method="label_rule_v14", note="Military occupation")
            continue

        # ✅ Religious roles
        if re.search(r"\b(muezzin)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Religious occupation")
            continue

        if re.search(r"\b(deaconess)\b", label):
            set_isco(df, i, 2, method="label_rule_v14", note="Religious occupation")
            continue

        # 🚫 Status / non-occupation
        if re.search(r"\b(mother|knight|trustee|deputy)\b", label):
            mark_non_occupation(df, i, "Status/title rather than occupation")
            continue

        if re.search(r"\b(organized crime)\b", label):
            mark_non_occupation(df, i, "Criminal category, not occupation")
            continue

        if re.search(r"\b(public office)\b", label):
            mark_non_occupation(df, i, "Institutional status, not occupation")
            continue

        if re.search(r"\b(fashion|publishing house|theatrical make-up)\b", label):
            mark_non_occupation(df, i, "Field/institution rather than occupation")
            continue

        if re.search(r"\b(bird watcher)\b", label):
            mark_non_occupation(df, i, "Hobby, not occupation")
            continue

        # 🌍 Very obscure / historical term
        if re.search(r"\b(harii)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Ancient/unclear term; left ambiguous"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

                # --- label_rule_v15 ---

        # ✅ Clear professionals
        if re.search(r"\b(medical anthropologist)\b", label):
            set_isco(df, i, 2, method="label_rule_v15", note="Medical anthropologist -> professional")
            continue

        if re.search(r"\b(logistician)\b", label):
            set_isco(df, i, 2, method="label_rule_v15", note="Logistician -> professional")
            continue

        if re.search(r"\b(stenotypist)\b", label):
            set_isco(df, i, 4, method="label_rule_v15", note="Stenotypist -> clerical support")
            continue

        if re.search(r"\b(doorkeeper)\b", label):
            set_isco(df, i, 5, method="label_rule_v15", note="Doorkeeper -> service occupation")
            continue

        if re.search(r"\b(food stylist)\b", label):
            set_isco(df, i, 2, method="label_rule_v15", note="Food stylist -> creative professional")
            continue

        if re.search(r"\b(newspaperperson)\b", label):
            set_isco(df, i, 2, method="label_rule_v15", note="Journalism profession")
            continue

        if re.search(r"\b(reader)\b", label):
            # ambiguous but usually professional (editor/academic context)
            set_isco(df, i, 2, method="label_rule_v15", note="Reader -> academic/editorial role")
            continue

        # ✅ Sports
        if re.search(r"\b(sport shooter|judoka)\b", label):
            set_isco(df, i, 3, method="label_rule_v15", note="Sport competitor")
            continue

        # ✅ Government / law enforcement
        if re.search(r"\b(special agent)\b", label):
            set_isco(df, i, 3, method="label_rule_v15", note="Law enforcement associate professional")
            continue

        # 🚫 Status / wealth
        if re.search(r"\b(billionaire|supercentenarian|candidate)\b", label):
            mark_non_occupation(df, i, "Status/wealth/life-stage term, not occupation")
            continue

        # 🚫 Fields of study / degree labels
        if re.search(r"\b(literary studies|nursing|costume design|hospitality occupation)\b", label):
            mark_non_occupation(df, i, "Field of study rather than occupation label")
            continue

        if re.search(r"\b(master of science in engineering)\b", label):
            mark_non_occupation(df, i, "Academic degree, not occupation")
            continue

        # 🚫 Institutional roles
        if re.search(r"\b(municipal council)\b", label):
            mark_non_occupation(df, i, "Institutional body, not occupation")
            continue

        if re.search(r"\b(occupation)\b", label):
            mark_non_occupation(df, i, "Generic term, not occupation label")
            continue

        # 🚫 Hobby / identity
        if re.search(r"\b(cinephile|globetrotter|boater)\b", label):
            mark_non_occupation(df, i, "Hobby/identity term")
            continue

        # 🌍 Foreign / obscure — safer as ambiguous
        if re.search(r"\b(heimatpfleger|idist|mashgiach ruchani|fakir)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Foreign/unclear occupation term; left ambiguous"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        # 🏗 Very long construction label
        if re.search(r"state-certified engineers specializing in building construction", label):
            set_isco(df, i, 2, method="label_rule_v15", note="Engineer (construction) -> professional")
            continue

                # --- label_rule_v16 ---

        # ✅ Performing arts / media
        if re.search(r"\b(voice-over|hype man)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Performing arts professional")
            continue

        if re.search(r"\b(dancer|drummer)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Performing arts professional")
            continue

        if re.search(r"\b(animated film maker)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Film professional")
            continue

        if re.search(r"\b(prompter)\b", label):
            set_isco(df, i, 3, method="label_rule_v16", note="Theatre technical associate professional")
            continue

        # ✅ Journalism / communication
        if re.search(r"\b(reporter)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Journalism professional")
            continue

        if re.search(r"\b(internet forum moderator)\b", label):
            set_isco(df, i, 3, method="label_rule_v16", note="Communication associate professional")
            continue

        if re.search(r"\b(negotiator)\b", label):
            set_isco(df, i, 3, method="label_rule_v16", note="Associate professional (generic negotiation role)")
            continue

        # ✅ Academic / science
        if re.search(r"\b(mathematician)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Mathematician -> professional")
            continue

        if re.search(r"\b(phoniatrician)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Medical specialist")
            continue

        if re.search(r"\b(ceramicist)\b", label):
            set_isco(df, i, 2, method="label_rule_v16", note="Craft/arts professional")
            continue

        # ✅ Sports
        if re.search(r"\b(baseball player|weightlifting)\b", label):
            set_isco(df, i, 3, method="label_rule_v16", note="Sport competitor")
            continue

        # ✅ Health
        if re.search(r"\b(health visitor)\b", label):
            set_isco(df, i, 3, method="label_rule_v16", note="Health associate professional")
            continue

        # 🚫 Status / awards / crime
        if re.search(r"\b(nobel prize winner|dignitary|jury)\b", label):
            mark_non_occupation(df, i, "Award/status role, not occupation")
            continue

        if re.search(r"\b(pedophile|holocaust perpetrator|holocaust survivor)\b", label):
            mark_non_occupation(df, i, "Crime/historical status, not occupation")
            continue

        # 🚫 Fields of study / generic fields
        if re.search(r"\b(medicine|chemistry|art of sculpture)\b", label):
            mark_non_occupation(df, i, "Field of study, not occupation label")
            continue

        # 🚫 Institutional groups
        if re.search(r"\b(theatre troupe|jury)\b", label):
            mark_non_occupation(df, i, "Institutional/group entity, not occupation")
            continue

        # 🚫 Generic / classification terms
        if re.search(r"\b(liberal profession)\b", label):
            mark_non_occupation(df, i, "Broad socio-economic category, not occupation label")
            continue

        # 🚫 MLM / distributor status
        if re.search(r"\b(amway distributor)\b", label):
            set_isco(df, i, 5, method="label_rule_v16", note="Sales/service worker")
            continue

        # 🇫🇷 French occupational category
        if re.search(r"\b(anciens artisans, commerçants, chefs d'entreprise)\b", label):
            mark_non_occupation(df, i, "Socio-economic census category, not specific occupation")
            continue

                # --- label_rule_v17 ---

        # ✅ Finance / economics
        if re.search(r"\b(banker)\b", label):
            set_isco(df, i, 2, method="label_rule_v17", note="Banker -> professional")
            continue

        if re.search(r"\b(economist)\b", label):
            set_isco(df, i, 2, method="label_rule_v17", note="Economist -> professional")
            continue

        # ✅ Media / film
        if re.search(r"\b(film editor|television producer)\b", label):
            set_isco(df, i, 2, method="label_rule_v17", note="Media professional")
            continue

        # ✅ Technical / drafting
        if re.search(r"\b(draftsperson)\b", label):
            set_isco(df, i, 3, method="label_rule_v17", note="Technical associate professional")
            continue

        # ✅ Trade / craft
        if re.search(r"\b(mask maker)\b", label):
            set_isco(df, i, 7, method="label_rule_v17", note="Craft/trade worker")
            continue

        # ✅ Service roles
        if re.search(r"\b(concierge)\b", label):
            set_isco(df, i, 5, method="label_rule_v17", note="Service worker")
            continue

        if re.search(r"\b(merchant)\b", label):
            set_isco(df, i, 1, method="label_rule_v17", note="Business/management role (generic merchant)")
            continue

        # ✅ Sports
        if re.search(r"\b(sports official)\b", label):
            set_isco(df, i, 3, method="label_rule_v17", note="Sports official -> associate professional")
            continue

        if re.search(r"\b(bowls player)\b", label):
            set_isco(df, i, 3, method="label_rule_v17", note="Sport competitor")
            continue

        # ✅ Port / transport
        if re.search(r"\b(harbormaster)\b", label):
            set_isco(df, i, 1, method="label_rule_v17", note="Harbormaster -> managerial transport role")
            continue

        # ✅ Government role
        if re.search(r"\b(chef de cabinet)\b", label):
            set_isco(df, i, 2, method="label_rule_v17", note="Chief of staff -> professional")
            continue

        # 🚫 Field of study / discipline
        if re.search(r"\b(civil engineering|economics|sport)\b", label):
            mark_non_occupation(df, i, "Field of study/discipline, not occupation label")
            continue

        if re.search(r"\b(literary critic)\b", label):
            set_isco(df, i, 2, method="label_rule_v17", note="Literary critic -> professional")
            continue

        # 🚫 Crime / vigilante / outlaw
        if re.search(r"\b(collaborator with nazi germany|vigilante|outlaw|mole)\b", label):
            mark_non_occupation(df, i, "Crime/political status, not occupation")
            continue

        # 🚫 Generic / unclear terms
        if re.search(r"\b(authority|community organization|educational personnel|carrier)\b", label):
            mark_non_occupation(df, i, "Generic/institutional label, not specific occupation")
            continue

        if re.search(r"\b(layperson)\b", label):
            mark_non_occupation(df, i, "Religious/social status, not occupation")
            continue

        # 🌍 Religious/foreign niche terms — safer as ambiguous
        if re.search(r"\b(shaliach|ealam din)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Foreign/religious term; context-dependent"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

                # --- label_rule_v18 ---

        # ✅ Sports
        if re.search(r"\b(badminton umpire)\b", label):
            set_isco(df, i, 3, method="label_rule_v18", note="Sports official -> associate professional")
            continue

        if re.search(r"\b(caddie|equestrian)\b", label):
            set_isco(df, i, 3, method="label_rule_v18", note="Sport competitor/support role")
            continue

        # ✅ Performing arts / music
        if re.search(r"\b(countertenor)\b", label):
            set_isco(df, i, 2, method="label_rule_v18", note="Singer (performing arts professional)")
            continue

        if re.search(r"\b(cameraman)\b", label):
            set_isco(df, i, 3, method="label_rule_v18", note="Media associate professional")
            continue

        # ✅ Religious occupations
        if re.search(r"\b(mohel)\b", label):
            set_isco(df, i, 2, method="label_rule_v18", note="Religious occupation")
            continue

        if re.search(r"\b(reverend)\b", label):
            set_isco(df, i, 2, method="label_rule_v18", note="Religious occupation")
            continue

        if re.search(r"\b(postulator)\b", label):
            set_isco(df, i, 2, method="label_rule_v18", note="Religious administrative role")
            continue

        # ✅ Service / safety
        if re.search(r"\b(lifeguard)\b", label):
            set_isco(df, i, 5, method="label_rule_v18", note="Protective service worker")
            continue

        if re.search(r"\b(orderly)\b", label):
            set_isco(df, i, 5, method="label_rule_v18", note="Health/service support worker")
            continue

        # ✅ Craft / trade
        if re.search(r"\b(wigmaker)\b", label):
            set_isco(df, i, 7, method="label_rule_v18", note="Craft/trade worker")
            continue

        if re.search(r"\b(ice cream maker)\b", label):
            set_isco(df, i, 7, method="label_rule_v18", note="Food production trade")
            continue

        # ✅ Management / administration
        if re.search(r"\b(business administrator)\b", label):
            set_isco(df, i, 1, method="label_rule_v18", note="Management role")
            continue

        if re.search(r"\b(conseiller)\b", label):
            set_isco(df, i, 2, method="label_rule_v18", note="Advisor/counsellor (professional)")
            continue

        if re.search(r"\b(village head)\b", label):
            set_isco(df, i, 1, method="label_rule_v18", note="Local administrative leader")
            continue

        if re.search(r"\b(tour promoter)\b", label):
            set_isco(df, i, 1, method="label_rule_v18", note="Entertainment management role")
            continue

        # 🚫 Crime / illegal
        if re.search(r"\b(hostage taker|charlatan|privateer)\b", label):
            mark_non_occupation(df, i, "Criminal/historical status, not occupation")
            continue

        # 🚫 Hobby / fringe
        if re.search(r"\b(ghost hunter)\b", label):
            mark_non_occupation(df, i, "Fringe/hobby label, not formal occupation")
            continue

        # 🚫 Field of study / artistic field
        if re.search(r"\b(art of painting|graphic design|sculpture|public speaking)\b", label):
            mark_non_occupation(df, i, "Field/activity rather than occupation label")
            continue

        # 🚫 Generic / vague
        if re.search(r"\b(specialist|controller)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Too generic to map reliably"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        # 🌍 Obscure niche terms
        if re.search(r"\b(draughts problemist|prospectivist|gatherer)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Rare/unclear occupation term"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

                # --- label_rule_v19 ---

        # ✅ Clear professions (major group 2 unless specified)

        if re.search(r"\b(librarian)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Librarian -> professional")
            continue

        if re.search(r"\b(editor)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Editor -> professional")
            continue

        if re.search(r"\b(designer)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Designer -> professional")
            continue

        if re.search(r"\b(research administrator)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Research administrator -> professional")
            continue

        if re.search(r"\b(judicial scrivener)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Legal professional")
            continue

        if re.search(r"\b(balneotherapist)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Health professional")
            continue

        if re.search(r"\b(preparator)\b", label):
            set_isco(df, i, 3, method="label_rule_v19", note="Technical associate professional")
            continue

        if re.search(r"\b(croupier)\b", label):
            set_isco(df, i, 5, method="label_rule_v19", note="Service worker")
            continue

        if re.search(r"\b(newspaper delivery person)\b", label):
            set_isco(df, i, 5, method="label_rule_v19", note="Service/delivery worker")
            continue

        # 🇩🇪 German civil service title
        if re.search(r"\b(diplom-verwaltungswirt)\b", label):
            set_isco(df, i, 2, method="label_rule_v19", note="Public administration professional")
            continue

        # 🇫🇷 French elite corps category (not occupation)
        if re.search(r"\b(grands corps de l'etat)\b", label):
            mark_non_occupation(df, i, "Elite administrative corps category, not occupation")
            continue

        # 🚫 Crime / criminal roles
        if re.search(r"\b(burglar|poisoner|perpetrator|nazi concentration camp commandant)\b", label):
            mark_non_occupation(df, i, "Criminal/historical role, not occupation")
            continue

        # 🚫 Hobby / supporter / fan
        if re.search(r"\b(football supporter|trekkie|scuba diving|boxing|chess organiser)\b", label):
            mark_non_occupation(df, i, "Hobby/supporter label, not occupation")
            continue

        # 🚫 Field of study / discipline
        if re.search(r"\b(public health|mechanical engineering|pedagogy)\b", label):
            mark_non_occupation(df, i, "Field of study/discipline, not occupation label")
            continue

        # 🚫 Generic / vague / object
        if re.search(r"\b(voice|internet meme|caller)\b", label):
            df.loc[i, "isco_mapping_method"] = "ambiguous_label"
            df.loc[i, "isco_mapping_notes"] = "Too vague/non-occupational context"
            df.loc[i, "sector"] = "Other / Unclassified"
            df.loc[i, "sector_source"] = "ambiguous_label_rule"
            continue

        # 🚫 Generic family/religious status
        if re.search(r"\b(father)\b", label):
            mark_non_occupation(df, i, "Family/religious status, not occupation")
            continue

        # 🚫 Overly broad census category
        if re.search(r"\b(personnel of direct services to individuals)\b", label):
            mark_non_occupation(df, i, "Broad census category, not specific occupation")
            continue

        # --- Otherwise: apply your big rules list ---
        matched = False
        for pat, major, sub, method, note in rules:
            if re.search(pat, label):
                set_isco(df, i, major, sub, method=method, note=note)
                matched = True
                break

        if matched:
            continue

        # If still not matched, keep as unmapped but make it look nice in dashboard
        if df.loc[i, "sector"] in ["", "nan"] or pd.isna(df.loc[i, "sector"]):
            df.loc[i, "sector"] = "Other / Unclassified"
        if df.loc[i, "sector_source"] in ["", "nan"] or pd.isna(df.loc[i, "sector_source"]):
            df.loc[i, "sector_source"] = "unmapped"

        if df.loc[i, "isco_mapping_method"] in ["", "special_territory_default"]:
            df.loc[i, "isco_mapping_method"] = "unmapped"
        if not str(df.loc[i, "isco_mapping_notes"]).strip():
            df.loc[i, "isco_mapping_notes"] = "No rule match yet"

    df.to_csv(OUT, index=False, encoding="utf-8")
    print(f"✅ Wrote: {OUT}")

    print("\nMapping method distribution:")
    print(df["isco_mapping_method"].value_counts(dropna=False).head(20))

    print("\nTop remaining unmapped occupation labels:")
    rem = df[df["isco_mapping_method"] == "unmapped"]
    if not rem.empty:
        print(rem["occupationLabel"].value_counts().head(30))


if __name__ == "__main__":
    main()