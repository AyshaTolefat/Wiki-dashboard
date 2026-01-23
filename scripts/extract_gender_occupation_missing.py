import bz2
import json
import re
from collections import defaultdict, Counter
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

DUMP_PATH = DATA_DIR / "dumps" / "latest-all.json.bz2"
MISSING_LIST = DATA_DIR / "missing_countries_qids.txt"

OUT_DIR = DATA_DIR / "dump_outputs"
OUT_DIR.mkdir(exist_ok=True)

MIN_YEAR = 1900

MALE_QIDS = {"Q6581097", "Q2449503"}
FEMALE_QIDS = {"Q6581072", "Q1052281"}

FLUSH_EVERY_MATCHED = 200_000  # write incremental results to disk


def gender_category(qid):
    if not qid:
        return "Unknown / not stated"
    if qid in MALE_QIDS:
        return "Male"
    if qid in FEMALE_QIDS:
        return "Female"
    return "Non-binary or other"


def extract_qids(claims, pid):
    out = []
    for c in claims.get(pid, []) or []:
        v = (c.get("mainsnak", {}).get("datavalue", {}) or {}).get("value")
        if isinstance(v, dict) and "id" in v and isinstance(v["id"], str) and v["id"].startswith("Q"):
            out.append(v["id"])
    return out


def extract_first_qid(claims, pid):
    xs = extract_qids(claims, pid)
    return xs[0] if xs else None


def extract_year(claims, pid):
    for c in claims.get(pid, []) or []:
        v = (c.get("mainsnak", {}).get("datavalue", {}) or {}).get("value")
        if isinstance(v, dict) and isinstance(v.get("time"), str):
            m = re.match(r"^[+-](\d{4})-", v["time"])
            if m:
                return int(m.group(1))
    return None


def stream_entities_bz2(path: Path):
    with bz2.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line in ("[", "]"):
                continue
            if line.endswith(","):
                line = line[:-1]
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def append_country_counts(country_qid: str, rows: list[tuple[str, str, int]]):
    """
    Append rows to per-country output file:
    country_qid,country,occupation_qid,occupation,genderCategory,count
    """
    out = OUT_DIR / f"gender_occ_{country_qid}.csv"
    header_needed = not out.exists()

    country_uri = f"http://www.wikidata.org/entity/{country_qid}"

    with out.open("a", encoding="utf-8") as f:
        if header_needed:
            f.write("country_qid,country,occupation_qid,occupation,genderCategory,count\n")

        for occ, gcat, cnt in rows:
            occ_uri = f"http://www.wikidata.org/entity/{occ}"
            f.write(f"{country_qid},{country_uri},{occ},{occ_uri},{gcat},{cnt}\n")


def flush_buffer(buffer: dict[str, Counter]):
    """
    Write buffered counts to disk and clear buffer.
    """
    for ctry, c in buffer.items():
        rows = [(occ, gcat, cnt) for (occ, gcat), cnt in c.items()]
        append_country_counts(ctry, rows)
    buffer.clear()


def main():
    if not DUMP_PATH.exists():
        raise SystemExit(f"Missing dump: {DUMP_PATH}")
    if not MISSING_LIST.exists():
        raise SystemExit(f"Missing list: {MISSING_LIST}")

    target_countries = {x.strip() for x in MISSING_LIST.read_text(encoding="utf-8").splitlines() if x.strip()}
    print(f"Target missing countries: {len(target_countries)}")

    # PASS 1: Build place -> country mapping (P17) for target countries only
    place_to_country = {}
    scanned = 0
    print("PASS 1: building place->country mapping (P17) ...")

    for ent in stream_entities_bz2(DUMP_PATH):
        scanned += 1
        qid = ent.get("id")
        if not isinstance(qid, str) or not qid.startswith("Q"):
            continue
        claims = ent.get("claims") or {}
        ctry = extract_first_qid(claims, "P17")
        if ctry and ctry in target_countries:
            place_to_country[qid] = ctry

        if scanned % 2_000_000 == 0:
            print(f"  scanned {scanned:,} entities; mapped places={len(place_to_country):,}")

    print(f"PASS 1 done. mapped places={len(place_to_country):,}")

    # PASS 2: Scan humans and aggregate, flushing periodically
    print("PASS 2: scanning humans and aggregating ...")
    scanned = 0
    matched = 0

    # buffer[country] -> Counter((occupation, genderCategory) -> count)
    buffer: dict[str, Counter] = defaultdict(Counter)

    for ent in stream_entities_bz2(DUMP_PATH):
        scanned += 1
        qid = ent.get("id")
        if not isinstance(qid, str) or not qid.startswith("Q"):
            continue
        claims = ent.get("claims") or {}

        # human?
        if "Q5" not in set(extract_qids(claims, "P31")):
            continue

        year = extract_year(claims, "P569")
        if year is None or year < MIN_YEAR:
            continue

        birthplaces = extract_qids(claims, "P19")
        if not birthplaces:
            continue

        countries = set()
        for place_qid in birthplaces:
            ctry = place_to_country.get(place_qid)
            if ctry:
                countries.add(ctry)

        if not countries:
            continue

        occs = set(extract_qids(claims, "P106"))
        if not occs:
            continue

        g = extract_first_qid(claims, "P21")
        gcat = gender_category(g)

        matched += 1
        for ctry in countries:
            if ctry not in target_countries:
                continue
            c = buffer[ctry]
            for occ in occs:
                c[(occ, gcat)] += 1

        if matched % FLUSH_EVERY_MATCHED == 0:
            print(f"  matched humans={matched:,} scanned={scanned:,} (flushing to disk)")
            flush_buffer(buffer)

    # final flush
    print("Final flush...")
    flush_buffer(buffer)

    print("Done. Outputs in:", OUT_DIR)


if __name__ == "__main__":
    main()
