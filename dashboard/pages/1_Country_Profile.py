import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from components.age_histogram import make_age_histogram, make_age_table, filter_country_age
from components.ethnicity_bubble import make_gender_ethnicity_donut, make_gender_ethnicity_table
from components.language_bar_chart import (
    make_language_bar_top_n,
    make_language_details_table,
    compute_language_kpis,
)
from components.occupation_treemap import (
    filter_country_occ,
    make_occupation_treemap,
    make_occupation_details_table,
)
from components.gender_breakdown_country_total import render_gender_breakdown_country_total
from components.gender_over_decades import render_gender_over_decades


# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/
DATA_DIR = BASE_DIR.parent / "data"

DECADES_PATH = DATA_DIR / "gender_decades_by_country.csv"
GENDER_TOTAL_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"
ETHNIC_PATH = DATA_DIR / "ethnic_group_by_country_gender.csv"
LANG_PATH = DATA_DIR / "languages_by_country.csv"
OCC_PATH = DATA_DIR / "gender_occupation_with_isco_refined.csv"
AGE_PATH = DATA_DIR / "age_groups_by_country.csv"


st.set_page_config(layout="wide")

@st.cache_data
def load_age() -> pd.DataFrame:
    df = pd.read_csv(AGE_PATH)
    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)
    df["ageGroup"] = df.get("ageGroup", "").astype(str).str.strip()
    df["country"] = df.get("country", "").astype(str)
    return df

@st.cache_data
def load_occupations() -> pd.DataFrame:
    df = pd.read_csv(OCC_PATH)

    # Make sure types are clean
    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)
    df["genderCategory"] = df.get("genderCategory", "").astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["occupationLabel"] = df.get("occupationLabel", "").astype(str).str.strip()
    df["sector"] = df.get("sector", "").astype(str).str.strip()
    df["isco_major_title"] = df.get("isco_major_title", "").astype(str).str.strip()
    df["isco_sub_major_title"] = df.get("isco_sub_major_title", "").astype(str).str.strip()

    # country_qid may be missing in some rows; keep robust
    if "country_qid" in df.columns:
        df["country_qid"] = df["country_qid"].astype(str).str.strip()
    df["country"] = df.get("country", "").astype(str).str.strip()

    return df

@st.cache_data
def load_languages() -> pd.DataFrame:
    df = pd.read_csv(LANG_PATH)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["type"] = df["type"].astype(str).str.strip().str.lower()
    df["languageLabel"] = df["languageLabel"].astype(str).str.strip()
    df["country"] = df["country"].astype(str)
    return df


@st.cache_data
def load_ethnicity() -> pd.DataFrame:
    df = pd.read_csv(ETHNIC_PATH)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["genderCategory"] = df["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["ethnicGroupLabel"] = df["ethnicGroupLabel"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["country"] = df["country"].astype(str)
    return df


@st.cache_data
def load_decades() -> pd.DataFrame:
    df = pd.read_csv(DECADES_PATH)
    df["qid"] = df["country"].astype(str).str.rsplit("/", n=1).str[-1]
    df["genderCategory"] = df["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["decade"] = pd.to_numeric(df["decade"], errors="coerce")
    df = df.dropna(subset=["decade"]).copy()
    df["decade"] = df["decade"].astype(int)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_data
def load_gender_total() -> pd.DataFrame:
    df = pd.read_csv(GENDER_TOTAL_PATH)
    df["qid"] = df["country"].astype(str).str.rsplit("/", n=1).str[-1]
    df["genderCategory"] = df["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    return df


# -------------------------
# Page content
# -------------------------
st.markdown("# Country profile")

cd = st.session_state.get("selected_country")
if cd is None:
    st.info("Go back to the globe, click a country, then press Explore country.")
    st.stop()

qid = str(cd[0])
country_label = str(cd[1])
iso3 = str(cd[2])

st.markdown(f"## {country_label} ({iso3})")

views = [
    "Gender over decades",
    "Gender breakdown (country total)",
    "Languages",
    "Ethnic groups",
    "Occupations",
    "Age Representation",
]
selected_view = st.session_state.get("country_view", "Gender over decades")
if selected_view not in views:
    selected_view = "Gender over decades"


# -------------------------
# Gender breakdown (country total)
# -------------------------
if selected_view == "Gender breakdown (country total)":
    df_total = load_gender_total()
    render_gender_breakdown_country_total(df_total, qid, country_label, iso3)
    st.stop()


# -------------------------
# Languages
# -------------------------
if selected_view == "Languages":
    st.markdown("## Languages")

    langs = load_languages()
    country_url = f"http://www.wikidata.org/entity/{qid}"
    df_country_lang = langs[langs["country"] == country_url].copy()

    lang_type = st.radio("Language type", ["spoken", "native"], horizontal=True, index=0)

    # -------------------------
    # KPI row
    # -------------------------
    kpis = compute_language_kpis(df_country_lang, lang_type=lang_type)

    st.markdown(
        """
        <style>
          .kpi-row { display:flex; gap:10px; margin: 8px 0 14px 0; }
          .kpi-box{
            flex:1;
            background:#ffffff;
            border:1px solid #E6E6E6;
            border-radius:14px;
            padding:12px 14px;
            box-shadow:0 1px 2px rgba(0,0,0,0.04);
          }
          .kpi-l{ font-size:12px; color:#6B7280; margin:0 0 4px 0; }
          .kpi-v{ font-size:20px; font-weight:900; color:#111827; margin:0; }
          .kpi-s{ font-size:12px; color:#6B7280; margin-top:4px; }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        f"""
        <div class="kpi-row">
          <div class="kpi-box">
            <div class="kpi-l">Total language claims ({lang_type})</div>
            <div class="kpi-v">{kpis["total"]:,}</div>
          </div>
          <div class="kpi-box">
            <div class="kpi-l">Unique languages ({lang_type})</div>
            <div class="kpi-v">{kpis["unique"]:,}</div>
          </div>
          <div class="kpi-box">
            <div class="kpi-l">Top language share</div>
            <div class="kpi-v">{kpis["top_share"]:.1f}%</div>
            <div class="kpi-s">{kpis["top_label"]}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # -------------------------
    # Top 15 bar chart
    # -------------------------
    fig = make_language_bar_top_n(
        df_country_lang,
        country_label=country_label,
        lang_type=lang_type,
        top_n=15,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # -------------------------
    # Details toggle: FULL data
    # -------------------------
    show_details = st.checkbox("Show details (all language data)", value=False)

    if show_details:
        full = make_language_details_table(df_country_lang, lang_type=lang_type)

        if full.empty:
            st.info("No language data available for this selection.")
        else:
            c1, c2 = st.columns([2, 1])
            with c1:
                q = st.text_input("Search language", value="", placeholder="Type to filter…")
            with c2:
                sort_by = st.selectbox("Sort by", ["Count (desc)", "Count (asc)", "A–Z"], index=0)

            if q.strip():
                full = full[full["Language"].str.contains(q.strip(), case=False, na=False)].copy()

            if sort_by == "Count (desc)":
                full = full.sort_values("Count", ascending=False)
            elif sort_by == "Count (asc)":
                full = full.sort_values("Count", ascending=True)
            else:
                full = full.sort_values("Language", ascending=True)

            styled = (
                full.style
                .format({"Count": "{:,.0f}", "Share (%)": "{:.2f}"})
                .bar(subset=["Share (%)"], color="#93C5FD")
            )

            st.dataframe(styled, use_container_width=True, hide_index=True)

    st.stop()




# -------------------------
# Ethnic groups
# -------------------------
if selected_view == "Ethnic groups":
    st.markdown("## Ethnic groups")

    ethnic = load_ethnicity()
    country_url = f"http://www.wikidata.org/entity/{qid}"
    df_country_ethnic = ethnic[ethnic["country"] == country_url].copy()

    title_prefix = f"Ethnic group composition — {country_label}"
    show_details = st.checkbox("Show details", value=False)

    items = []
    for gender in ["Male", "Female", "Non-binary or other", "Unknown / not stated"]:
        fig, n = make_gender_ethnicity_donut(df_country_ethnic, gender, title_prefix, top_n=10)
        if fig is not None and n > 0:
            items.append((gender, fig, n))

    if not items:
        st.info("No ethnicity data available for this country.")
        st.stop()

    for i in range(0, len(items), 2):
        cols = st.columns(2)

        gender_l, fig_l, n_l = items[i]
        with cols[0]:
            st.caption(f"{gender_l} (n = {n_l:,})")
            st.plotly_chart(fig_l, use_container_width=True, config={"displayModeBar": False})
            if show_details:
                tbl = make_gender_ethnicity_table(df_country_ethnic, gender_l, top_n=10)
                if not tbl.empty:
                    st.dataframe(tbl, use_container_width=True, hide_index=True)

        if i + 1 < len(items):
            gender_r, fig_r, n_r = items[i + 1]
            with cols[1]:
                st.caption(f"{gender_r} (n = {n_r:,})")
                st.plotly_chart(fig_r, use_container_width=True, config={"displayModeBar": False})
                if show_details:
                    tbl = make_gender_ethnicity_table(df_country_ethnic, gender_r, top_n=10)
                    if not tbl.empty:
                        st.dataframe(tbl, use_container_width=True, hide_index=True)

    st.stop()


# -------------------------
# Placeholders
# -------------------------
if selected_view == "Occupations":
    st.markdown("## Occupations")
    st.caption("Tip: click a block to zoom in. Double-click to go back.")

    occ_all = load_occupations()
    df_country_occ = filter_country_occ(occ_all, qid=qid)

    if df_country_occ.empty:
        st.info("No occupation data available for this country.")
        st.stop()

    c1, c2 = st.columns([1.2, 1.8])
    with c1:
        gender_filter = st.selectbox(
            "Gender",
            ["All", "Male", "Female", "Non-binary or other", "Unknown / not stated"],
            index=0,
        )
    with c2:
        group_mode = st.selectbox(
            "Group by",
            ["Sector → Occupation", "ISCO Major → Occupation"],
            index=1,  # matches your screenshot default
        )

    # Keep all data, but group tiny leaves so it’s readable
    fig = make_occupation_treemap(
        df_country_occ,
        country_label=country_label,
        gender_filter=gender_filter,
        group_mode=group_mode,
        min_share_within_parent=0.005,  # 0.5% of parent; tweak if needed
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    show_details = st.checkbox("Show details (full table)", value=False)
    if show_details:
        full = make_occupation_details_table(df_country_occ, gender_filter=gender_filter)

        if full.empty:
            st.info("No occupation data available for this selection.")
        else:
            left, right = st.columns([2, 1])
            with left:
                q = st.text_input("Search occupation", value="", placeholder="Type to filter…")
            with right:
                sort_by = st.selectbox("Sort by", ["Count (desc)", "Count (asc)", "A–Z"], index=0)

            if q.strip():
                full = full[full["Occupation"].str.contains(q.strip(), case=False, na=False)].copy()

            if sort_by == "Count (desc)":
                full = full.sort_values("Count", ascending=False)
            elif sort_by == "Count (asc)":
                full = full.sort_values("Count", ascending=True)
            else:
                full = full.sort_values("Occupation", ascending=True)

            styled = (
                full.style
                .format({"Count": "{:,.0f}", "Share (%)": "{:.2f}"})
                .bar(subset=["Share (%)"], color="#93C5FD")
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

    st.stop()



if selected_view == "Age Representation":
    st.markdown("## Age Representation")

    ages_all = load_age()
    df_country_age = filter_country_age(ages_all, qid=qid)

    if df_country_age.empty:
        st.info("No age-group data available for this country.")
        st.stop()

    mode = st.radio("Display", ["Counts", "Share (%)"], horizontal=True, index=0)

    fig, base = make_age_histogram(
        df_country_age,
        country_label=country_label,
        mode=mode,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    show_details = st.checkbox("Show details", value=False)
    if show_details:
        tbl = make_age_table(base)
        styled = (
            tbl.style
            .format({"Count": "{:,.0f}", "Share (%)": "{:.2f}"})
            .bar(subset=["Share (%)"], color="#93C5FD")
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

    st.stop()


# -------------------------
# Gender over decades (default)
# -------------------------
all_decades = load_decades()
render_gender_over_decades(all_decades, qid)



