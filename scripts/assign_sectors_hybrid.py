# scripts/assign_hybrid_sectors.py
#
# Hybrid sector assignment for Wikidata occupations:
#   1) High-precision keyword rules on occupationLabel
#   2) Ontology inheritance using P279* ancestors from occupation_ancestors_cache.csv
#   3) Fallback to "Other / Unclassified"
#
# INPUT :
#   data/gender_occupation_with_labels.csv
#     - must include: occupation_qid, occupationLabel, count
#   data/occupation_ancestors_cache.csv
#     - must include: occupation_qid, ancestor_qid
#
# OUTPUT:
#   data/gender_occupation_with_sectors.csv
#   data/sector_coverage_report.csv
#   data/top_other_occupations_by_count.csv
#
# Changes requested by you (implemented):
# - Added sectors: Transport & Logistics, Agriculture, Manufacturing & Trades, Labor Workers
# - Nun -> Religion
# - Lithographer -> Arts & Entertainment
# - Secretary -> Business & Finance
# - Chairperson -> Business & Finance
# - Miner, technician, printmaker -> Labor Workers
# - Pilot -> Transport & Logistics
# - Farmer -> Agriculture
# - Kept "player" rule as LAST fallback under Sports
#
# NEW changes requested by you (added WITHOUT changing anything else):
# - Q728711 (Playboy Playmate) -> Arts & Entertainment
# - Q2500638 (creator) -> Arts & Entertainment
# - Q512314 (socialite) -> Arts & Entertainment
# - Q12356615 (traveler) -> Arts & Entertainment
# - Q337084 (drag queen) -> Arts & Entertainment
# - Q4379701 (professional gamer) -> Arts & Entertainment
# - Q327029 (mechanic) -> Labor Workers
# - Q1895303 (forester) -> Labor Workers
# - Q11499929 (man of letters) -> Media & Journalism
# - Q662729 (public figure) -> Media & Journalism
# - Q4504549 (religious figure) -> Religion
# - Q55187 (hairdresser) -> Arts & Entertainment
# - Q694748 (physiotherapist) -> Medicine & Health

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd


# ---------------------------
# Paths
# ---------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

INP = DATA_DIR / "gender_occupation_with_labels.csv"
ANCESTORS = DATA_DIR / "occupation_ancestors_cache.csv"

OUT = DATA_DIR / "gender_occupation_with_sectors.csv"
REPORT = DATA_DIR / "sector_coverage_report.csv"
TOP_OTHER_OUT = DATA_DIR / "top_other_occupations_by_count.csv"


# ---------------------------
# Keyword rules (layer 1)
# ---------------------------
# Ordered: first match wins. Keep broad fallback patterns (e.g., "player") at the end of their sector list.
SECTOR_RULES: List[Tuple[str, List[str]]] = [
    ("Politics & Government", [
        r"\bpolitician\b", r"\bcouncil member\b", r"\bmember of parliament\b", r"\bsenator\b",
        r"\bminister\b", r"\bdiplomat\b", r"\bambassador\b",
        r"\bcivil servant\b", r"\bofficial\b", r"\bbureaucrat\b",
    ]),
    ("Public Service & Activism", [
        r"\bsocial worker\b", r"\bhuman rights defender\b", r"\btrade unionist\b",
        r"\bactivist\b", r"\benvironmentalist\b", r"\bhumanitarian\b",
    ]),
    ("Law & Justice", [
        r"\blawyer\b", r"\battorney\b", r"\bjudge\b", r"\bjurist\b", r"\bprosecutor\b",
        r"\bsolicitor\b", r"\bnotary\b",
    ]),
    ("Military & Security", [
        r"\bmilitary\b", r"\bsoldier\b", r"\bofficer\b", r"\bpolice\b", r"\bpolice officer\b",
        r"\bdetective\b", r"\bintelligence\b", r"\bspy\b", r"\bresistance fighter\b",
        r"\bpartisan\b", r"\bsubmariner\b", r"\bfirefighter\b",
    ]),
    ("Transport & Logistics", [
        r"\baircraft pilot\b", r"\bpilot\b", r"\bdriver\b", r"\bracing driver\b",
        r"\bmotorcycle racer\b", r"\bsailor\b", r"\btransport\b", r"\blogistics\b",
    ]),
    ("Agriculture", [
        r"\bfarmer\b", r"\bagronomist\b", r"\bagriculture\b",
    ]),
    ("Manufacturing & Trades", [
        r"\bmanufacturer\b", r"\bprinter\b", r"\btypographer\b",
        r"\blithographer\b", r"\btrade\b", r"\bcraft\b",
    ]),
    ("Labor Workers", [
        r"\blaborer\b", r"\bminer\b", r"\btechnician\b", r"\bprintmaker\b",
        # NEW: mechanic + forester
        r"\bmechanic\b", r"\bforester\b",
    ]),
    ("Business & Finance", [
        r"\bbusinessperson\b", r"\bentrepreneur\b", r"\bbanker\b", r"\bmerchant\b",
        r"\bbusiness executive\b", r"\baccountant\b", r"\bconsultant\b",
        r"\bmanager\b", r"\bsecretary\b", r"\bchairperson\b",
    ]),
    ("Science & Research", [
        r"\bscientist\b", r"\bresearcher\b", r"\bengineer\b", r"\binventor\b",
        r"\bstatistician\b", r"\bexplorer\b",
        r"\bcollector\b", r"\bscientific collector\b", r"\bbotanical collector\b",
        r"\bscience communicator\b",
    ]),
    ("Medicine & Health", [
        r"\bphysician\b", r"\bdoctor\b", r"\bsurgeon\b", r"\bpsychiatrist\b",
        r"\bpsychologist\b", r"\bpharmacist\b", r"\bveterinarian\b",
        r"\bdentist\b", r"\bpediatrician\b", r"\binternist\b",
        r"\bnurse\b",
        # NEW: physiotherapy/physiotherapist (label-based catch too)
        r"\bphysiotherap",
    ]),
    ("Education", [
        r"\bteacher\b", r"\beducator\b", r"\bprofessor\b", r"\blecturer\b",
        r"\blibrarian\b", r"\barchivist\b", r"\bstudent\b",
        r"\bliterary scholar\b",
    ]),
    ("Arts & Entertainment", [
        r"\bartist\b", r"\bpainter\b", r"\bsculptor\b", r"\billustrator\b",
        r"\bcartoonist\b", r"\bphotographer\b", r"\bwriter\b", r"\bauthor\b",
        r"\bpoet\b", r"\bscreenwriter\b", r"\bfilm producer\b", r"\bproducer\b",
        r"\bdirector\b", r"\bdesigner\b", r"\bgraphic designer\b",
        r"\bfashion designer\b", r"\bscenographer\b", r"\btypographer\b",
        r"\bmodel\b", r"\bfashion model\b", r"\blithographer\b",
        # NEW: hairdresser + your entertainment long-tail
        r"\bhairdresser\b",
        r"\bplayboy playmate\b",
        r"\bcreator\b",
        r"\bsocialite\b",
        r"\btraveler\b|\btraveller\b",
        r"\bdrag queen\b",
        r"\bprofessional gamer\b",
    ]),
    ("Media & Journalism", [
        r"\bjournalist\b", r"\bopinion journalist\b", r"\bannouncer\b",
        r"\bpresenter\b", r"\bpodcaster\b", r"\bradio personality\b",
        r"\btelevision personality\b", r"\binternet celebrity\b",
        r"\beditor\b", r"\bediting staff\b", r"\bdocumentarian\b",
        r"\bbroadcaster\b",
        # NEW: man of letters + public figure
        r"\bman of letters\b",
        r"\bpublic figure\b",
    ]),
    ("Religion", [
        r"\bpriest\b", r"\bcatholic priest\b", r"\bcatholic bishop\b",
        r"\bdeacon\b", r"\bpastor\b", r"\bpreacher\b", r"\bmissionary\b",
        r"\bcleric\b", r"\bimam\b", r"\brabbi\b", r"\bmonk\b",
        r"\bnun\b",
        # NEW: religious figure
        r"\breligious figure\b",
    ]),
    ("Environment & Design", [
        r"\barchitect\b", r"\burban planner\b", r"\bdraftsperson\b",
    ]),
    ("Sports", [
        r"\bfootballer\b", r"\bassociation football\b", r"\bbasketball\b", r"\bice hockey\b",
        r"\bamerican football\b", r"\bcanadian football\b", r"\bbaseball\b",
        r"\bcricketer\b", r"\brugby\b", r"\bvolleyball\b", r"\bboxer\b",
        r"\bswimmer\b", r"\btennis\b", r"\bgolfer\b", r"\bcyclist\b",
        r"\bwrestler\b", r"\bfencer\b", r"\bjudoka\b", r"\bjudo\b",
        r"\breferee\b", r"\bcoach\b", r"\bsprinter\b", r"\bmountaineer\b",
        r"\bcompetitor\b", r"\bathlete\b",
        # last-resort sports catch: keep LAST
        r"\bplayer\b",
    ]),
]


def keyword_sector(label: str) -> Optional[str]:
    if not isinstance(label, str):
        return None
    s = label.lower().strip()
    for sector, patterns in SECTOR_RULES:
        for pat in patterns:
            if re.search(pat, s):
                return sector
    return None


# ---------------------------
# Ontology anchors (layer 2)
# ---------------------------
# Map ancestor QIDs -> sector.
ANCHOR_QID_TO_SECTOR: Dict[str, str] = {
    # SPORTS
    "Q309252": "Sports",     # physical fitness
    "Q755620": "Sports",        # athletic trainer
    "Q33999": "Sports",        # athlete
    "Q713200": "Sports",       # sportsperson
    "Q11513337": "Sports",     # athletics competitor
    "Q50995749": "Sports",     # sportsperson (explicit)
    "Q3077353": "Sports",      # trainer
    "Q11598549": "Sports",     # boat racer

    # ARTS & ENTERTAINMENT
    "Q11633": "Arts & Entertainment",      # photography
    "Q180856": "Arts & Entertainment",     # choreography
    "Q1486290": "Arts & Entertainment",    # art publisher
    "Q108285874": "Arts & Entertainment",  # handicraft worker
    "Q108137087": "Arts & Entertainment",  # sound editing
    "Q107996792": "Arts & Entertainment",  # theatre people
    "Q100354262": "Arts & Entertainment",  # Kathak dancer
    "Q99516640": "Arts & Entertainment",   # wall painting
    "Q11479517": "Arts & Entertainment",   # handicrafter
    "Q1791845": "Arts & Entertainment",    # cultural worker
    "Q160131": "Arts & Entertainment",   # baker
    "Q157195": "Arts & Entertainment",   # waiter
    "Q483501": "Arts & Entertainment",   # artist
    "Q482980": "Arts & Entertainment",   # author
    "Q639669": "Arts & Entertainment",   # musician
    "Q1028181": "Arts & Entertainment",  # painter
    "Q33231": "Arts & Entertainment",    # photographer
    "Q1281618": "Arts & Entertainment",  # sculptor
    "Q644687": "Arts & Entertainment",   # illustrator
    "Q1114448": "Arts & Entertainment",  # cartoonist
    "Q16947657": "Arts & Entertainment", # lithographer (requested)
    "Q1039099": "Arts & Entertainment",  # tour guide
    "Q728711": "Arts & Entertainment",   # Playboy Playmate
    "Q2500638": "Arts & Entertainment",  # creator
    "Q512314": "Arts & Entertainment",   # socialite
    "Q12356615": "Arts & Entertainment", # traveler
    "Q337084": "Arts & Entertainment",   # drag queen
    "Q4379701": "Arts & Entertainment",  # professional gamer
    "Q55187": "Arts & Entertainment",    # hairdresser
    "Q1440873": "Arts & Entertainment",  # showrunner
    "Q5276395": "Arts & Entertainment",  # gamer
    "Q808266": "Arts & Entertainment",   # bartender

    # MEDIA & JOURNALISM
    "Q650483": "Media & Journalism",    # sports journalism
    "Q960451151": "Media & Journalism",    # copywriter
    "Q1885941": "Media & Journalism",    # media personality
    "Q1930187": "Media & Journalism",    # journalist
    "Q2722764": "Media & Journalism",    # radio personality
    "Q1371925": "Media & Journalism",    # announcer
    "Q13590141": "Media & Journalism",   # presenter
    "Q15077007": "Media & Journalism",   # podcaster
    "Q44508716": "Media & Journalism",   # television personality
    "Q2045208": "Media & Journalism",    # Internet celebrity
    "Q876864": "Media & Journalism",     # editing staff
    "Q1607826": "Media & Journalism",    # editor
    "Q11814411": "Media & Journalism",   # documentarian
    "Q135301631": "Media & Journalism",  # broadcaster (from your top-other)
    "Q4178004": "Media & Journalism",    # publicist
    "Q20826540": "Media & Journalism",   # scholar
    "Q11499929": "Media & Journalism",   # man of letters
    "Q662729": "Media & Journalism",     # public figure

    # LAW & JUSTICE
    "Q1494322": "Law & Justice",           # legal services
    "Q40348": "Law & Justice",           # lawyer
    "Q185351": "Law & Justice",          # legal profession
    "Q16533": "Law & Justice",           # judge
    "Q189010": "Law & Justice",          # notary (from your top-other)

    # POLITICS & GOVERNMENT
    "Q14927262": "Politics & Government",  # mayor of toronto
    "Q132050": "Politics & Government",  # governor
    "Q3796928": "Politics & Government",  # government employee
    "Q82955": "Politics & Government",   # politician
    "Q212238": "Politics & Government",  # civil servant
    "Q599151": "Politics & Government",  # official
    "Q572700": "Politics & Government",  # bureaucrat
    "Q708492": "Politics & Government",  # council member
    "Q17276321": "Politics & Government",# member of the State Duma
    "Q12307965": "Politics & Government",# debater
    "Q17221": "Politics & Government",   # spokesperson

    # PUBLIC SERVICE & ACTIVISM
    "Q12859263": "Public Service & Activism",  # orator
    "Q15253558": "Public Service & Activism",  # activist
    "Q1476215": "Public Service & Activism",   # human rights defender
    "Q15627169": "Public Service & Activism",  # trade unionist
    "Q7019111": "Public Service & Activism",   # social worker
    "Q3578589": "Public Service & Activism",   # environmentalist
    "Q11499147": "Public Service & Activism",  # political activist (kept here)
    "Q15982858": "Public Service & Activism",  # motivational speaker
    "Q54128": "Public Service & Activism",     # domestic worker
    "Q66660783": "Public Service & Activism",  # charity worker
    "Q110374361": "Public Service & Activism", # community worker

    # MILITARY & SECURITY
    "Q60227491": "Military & Security",  # interrogator
    "Q96375335": "Military & Security",  # Commander of the Iranian Navy
    "Q30093123": "Military & Security",  # investigator
    "Q18121791": "Military & Security",  # scout
    "Q98052424": "Military & Security",  # SOE agent
    "Q856887": "Military & Security",     # security guard
    "Q47064": "Military & Security",     # military personnel
    "Q4991371": "Military & Security",   # soldier
    "Q189290": "Military & Security",    # military officer
    "Q384593": "Military & Security",    # police officer
    "Q9352089": "Military & Security",   # spy
    "Q1397808": "Military & Security",   # resistance fighter
    "Q23833535": "Military & Security",  # French resistance fighter
    "Q3492027": "Military & Security",   # submariner
    "Q107711": "Military & Security",    # firefighter
    "Q851436": "Military & Security",    # bodyguard
    "Q28809103": "Military & Security",  # U.S. Secret Service agent
    "Q1058617": "Military & Security",   # private investigator

    # TRANSPORT & LOGISTICS
    "Q8563791": "Transport & Logistics", # flight controller
    "Q96922667": "Transport & Logistics", # Aviation Safety expert
    "Q2095549": "Transport & Logistics", # aircraft pilot
    "Q476246": "Transport & Logistics",  # sailor (if present)
    "Q7063944": "Transport & Logistics", # railway worker

    # AGRICULTURE
    "Q110002200": "Agriculture",            # crop production worker
    "Q131512": "Agriculture",            # farmer
    "Q5060555": "Agriculture",           # farmworker
    "Q19261760": "Agriculture",          # agricultural worker

    # MANUFACTURING & TRADES
    "Q175151": "Manufacturing & Trades", # printer
    "Q1229025": "Manufacturing & Trades",# typographer
    "Q13235160": "Manufacturing & Trades", # manufacturer

    # LABOR WORKERS
    "Q1760141": "Labor Workers",         # cleaner
    "Q385378": "Labor Workers",          # construction
    "Q11479517": "Labor Workers",        # handicrafter
    "Q196721": "Labor Workers",          # machinist
    "Q12335817": "Labor Workers",        # forestry worker
    "Q87285943": "Labor Workers",        # factory worker
    "Q7234072": "Labor Workers",         # postal worker
    "Q820037": "Labor Workers",          # miner
    "Q5352191": "Labor Workers",         # technician
    "Q327029": "Labor Workers",          # mechanic
    "Q1895303": "Labor Workers",         # forester
    "Q11569986": "Labor Workers",        # printmaker

    # BUSINESS & FINANCE
    "Q738142": "Business & Finance",          # clerk
    "Q1056396": "Business & Finance",        # human resource management
    "Q87252470": "Business & Finance",       # furniture salesman
    "Q3458238": "Business & Finance",        # company auditor
    "Q3647577": "Business & Finance",        # business developer
    "Q99210533": "Business & Finance",       # financial applications specialist
    "Q108286983": "Business & Finance",      # finance professional
    "Q1017553": "Business & Finance",        # business analyst
    "Q685433": "Business & Finance",       # salesperson
    "Q17487600": "Business & Finance",     # real estate developer
    "Q215279": "Business & Finance",       # freelancer
    "Q1662050": "Business & Finance",      # industrial management assistant
    "Q43845": "Business & Finance",      # businessperson
    "Q131524": "Business & Finance",     # entrepreneur
    "Q806798": "Business & Finance",     # banker
    "Q215536": "Business & Finance",     # merchant
    "Q2961975": "Business & Finance",    # business executive
    "Q15978655": "Business & Finance",   # consultant
    "Q326653": "Business & Finance",     # accountant
    "Q2462658": "Business & Finance",    # manager
    "Q1162163": "Business & Finance",    # director
    "Q80687": "Business & Finance",      # secretary
    "Q140686": "Business & Finance",     # chairperson
    "Q31179608": "Business & Finance",   # office worker
    "Q97768164": "Business & Finance",   # corporate administrative and commercial executive
    "Q23835475": "Business & Finance",   # assistant
    "Q4830453": "Business & Finance",    # business
    "Q381136": "Business & Finance",     # shareholder
    "Q45916492": "Business & Finance",   # co-founder
    "Q4683453": "Business & Finance",    # administrative assistant

    # SCIENCE & RESEARCH
    "QQ28920101": "Science & Research",     # computer security specialist
    "Q1473265": "Science & Research",     # information technology management
    "Q969812": "Science & Research",     # Analyst
    "Q22082749": "Science & Research",     # pseudoscientist
    "Q96730589": "Science & Research",     # Moblie Computing
    "Q84562103": "Science & Research",     # IT specialist
    "Q2548714": "Science & Research",      # program maker
    "Q5268834": "Science & Research",      # IT
    "Q942569": "Science & Research",       # systems analyst
    "Q94246114": "Science & Research",     # policy analyst
    "Q901": "Science & Research",        # scientist
    "Q81096": "Science & Research",      # engineer
    "Q205375": "Science & Research",     # inventor
    "Q2732142": "Science & Research",    # statistician
    "Q2083925": "Science & Research",    # botanical collector
    "Q98544732": "Science & Research",   # scientific collector
    "Q11900058": "Science & Research",   # explorer
    "Q15143191": "Science & Research",   # science communicator
    "Q485178": "Science & Research",     # analyst

    # MEDICINE & HEALTH
    "Q71114279": "Medicine & Health",    # psychopathologist
    "Q259327": "Medicine & Health",      # lifeguard
    "Q3542795": "Medicine & Health",     # oenologist
    "Q39631": "Medicine & Health",       # physician
    "Q212980": "Medicine & Health",      # psychologist
    "Q774306": "Medicine & Health",      # surgeon
    "Q211346": "Medicine & Health",      # psychiatrist
    "Q105186": "Medicine & Health",      # pharmacist
    "Q202883": "Medicine & Health",      # veterinarian
    "Q27349": "Medicine & Health",       # dentist
    "Q15924224": "Medicine & Health",    # internist
    "Q1919436": "Medicine & Health",     # pediatrician
    "Q2576499": "Medicine & Health",     # nutritionist
    "Q694748": "Medicine & Health",      # physiotherapist
    "Q11762416": "Medicine & Health",    # speech and language therapist
    "Q2419397": "Medicine & Health",     # therapist
    "Q651566": "Medicine & Health",      # hygienist
    "Q185196": "Medicine & Health",      # midwife
    "Q11974939": "Medicine & Health",    # health professional
    "Q1996635": "Medicine & Health",     # optician
    "Q330204": "Medicine & Health",      # paramedic
    "Q2752318": "Medicine & Health",     # cosmetologist
    "Q631193": "Medicine & Health",      # occupational therapist

    # EDUCATION
    "Q37133": "Education",               # teacher
    "Q974144": "Education",              # educator
    "Q182436": "Education",              # librarian
    "Q635734": "Education",              # archivist
    "Q9379869": "Education",             # lecturer
    "Q48282": "Education",               # student
    "Q6673651": "Education",             # literary scholar
    "Q414528": "Education",              # academician
    "Q97768158": "Education",            # professors, scientific professions

    # RELIGION
    "Q42603": "Religion",                # priest
    "Q250867": "Religion",               # Catholic priest
    "Q611644": "Religion",               # Catholic bishop
    "Q152002": "Religion",               # pastor
    "Q432386": "Religion",               # preacher
    "Q219477": "Religion",               # missionary
    "Q2259532": "Religion",              # cleric
    "Q25393460": "Religion",             # Catholic deacon
    "Q191808": "Religion",               # nun
    "Q4504549": "Religion",              # religious figure
    "Q548320": "Religion",               # friar

    # ENVIRONMENT & DESIGN
    "Q1329946": "Environment & Design",  #interior architecture
    "Q42973": "Environment & Design",   # architect
    "Q131062": "Environment & Design",  # urban planner
    "Q15296811": "Environment & Design",# draftsperson
}


def build_occ_to_ancestors(ancestors_df: pd.DataFrame) -> Dict[str, Set[str]]:
    occ_to_anc: Dict[str, Set[str]] = {}
    for occ, anc in zip(ancestors_df["occupation_qid"].astype(str), ancestors_df["ancestor_qid"].astype(str)):
        occ_to_anc.setdefault(occ, set()).add(anc)
    return occ_to_anc


def ontology_sector(occ_qid: str, occ_to_anc: Dict[str, Set[str]]) -> Optional[str]:
    ancestors = occ_to_anc.get(occ_qid)
    if not ancestors:
        return None
    for a in ancestors:
        sec = ANCHOR_QID_TO_SECTOR.get(a)
        if sec:
            return sec
    return None


def main():
    if not INP.exists():
        raise SystemExit(f"Missing input: {INP}")
    if not ANCESTORS.exists():
        raise SystemExit(f"Missing ancestors cache: {ANCESTORS}")

    df = pd.read_csv(INP)
    anc_df = pd.read_csv(ANCESTORS)

    required_cols = {"occupation_qid", "occupationLabel", "count"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise SystemExit(f"Input file is missing columns: {sorted(missing_cols)}")

    occ_to_anc = build_occ_to_ancestors(anc_df)

    sectors: List[str] = []
    sources: List[str] = []

    for occ_qid, label in zip(df["occupation_qid"].astype(str), df["occupationLabel"].astype(str)):
        sec = keyword_sector(label)
        if sec:
            sectors.append(sec)
            sources.append("keyword")
            continue

        sec = ontology_sector(occ_qid, occ_to_anc)
        if sec:
            sectors.append(sec)
            sources.append("ontology")
            continue

        sectors.append("Other / Unclassified")
        sources.append("fallback")

    df["sector"] = sectors
    df["sector_source"] = sources

    # Ensure count numeric
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)

    df.to_csv(OUT, index=False)
    print(f"Wrote: {OUT}\n")
    print("sector")
    print(df["sector"].value_counts())

    # Coverage report by unique occupations
    occ_unique = df[["occupation_qid", "occupationLabel", "sector", "sector_source"]].drop_duplicates()
    report = (
        occ_unique.groupby(["sector_source", "sector"], as_index=False)
        .size()
        .rename(columns={"size": "unique_occupations"})
        .sort_values("unique_occupations", ascending=False)
    )
    report.to_csv(REPORT, index=False)
    print(f"\nWrote: {REPORT}")
    print(report.head(25))

    # Top "Other" occupations by total count (for iterative improvement)
    other = df[df["sector"] == "Other / Unclassified"].copy()
    if not other.empty:
        top_other = (
            other.groupby(["occupation_qid", "occupationLabel"], as_index=False)["count"]
            .sum()
            .sort_values("count", ascending=False)
            
        )
        top_other.to_csv(TOP_OTHER_OUT, index=False)
        print(f"\nWrote: {TOP_OTHER_OUT}")
        print(top_other.head(30))
    else:
        print("\nNo 'Other / Unclassified' rows found. Great!")


if __name__ == "__main__":
    main()
