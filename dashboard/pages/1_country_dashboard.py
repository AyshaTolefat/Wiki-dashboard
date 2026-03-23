import pandas as pd
import streamlit as st
from pathlib import Path

# --- Components (yours, unchanged) ---
from components.gender_over_decades import render_gender_over_decades
from components.gender_breakdown_country_total import render_gender_breakdown_country_total

from components.language_bar_chart import (
    make_language_bar_top_n,
    make_language_details_table,
)
from components.ethnicity_bubble import make_gender_ethnicity_donut, make_gender_ethnicity_table

from components.occupation_treemap import (
    filter_country_occ,
    make_occupation_treemap,
    make_occupation_details_table,
)
from components.age_histogram import (
    filter_country_age,
    make_age_histogram,
    make_age_table,
)

# -------------------------
# Page config
# -------------------------
st.set_page_config(layout="wide")

# -------------------------
# Full-width + tighter spacing (professional)
# -------------------------
st.markdown(
    """
    <style>
      /* Use full width of screen */
      .block-container {
        max-width: 100% !important;
        padding-left: 2.2rem;
        padding-right: 2.2rem;
        padding-top: 1.2rem;
        padding-bottom: 2rem;
      }

      /* Reduce default vertical whitespace */
      div[data-testid="stVerticalBlock"] { gap: 0.75rem; }

      /* Nice section headers */
      .section-title{
        font-size: 28px;
        font-weight: 900;
        letter-spacing: -0.02em;
        margin: 0.2rem 0 0.2rem 0;
      }
      .section-sub{
        color:#6B7280;
        font-size: 13px;
        margin: 0 0 0.6rem 0;
      }

      /* Section card */
      .section-card{
        background:#ffffff;
        border:1px solid #E6E6E6;
        border-radius:16px;
        padding:16px 16px 10px 16px;
        box-shadow:0 1px 2px rgba(0,0,0,0.04);
      }

      /* Soft divider between big blocks */
      .soft-divider {height: 1px; background: #EEF2F7; margin: 0.8rem 0 0.6rem 0;}
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# Paths
# -------------------------
BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/
DATA_DIR = BASE_DIR.parent / "data"

DECADES_PATH = DATA_DIR / "gender_decades_by_country.csv"
GENDER_TOTAL_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"
LANG_PATH = DATA_DIR / "languages_by_country.csv"
ETHNIC_PATH = DATA_DIR / "ethnic_group_by_country_gender.csv"
OCC_PATH = DATA_DIR / "gender_occupation_with_isco_refined.csv"
AGE_PATH = DATA_DIR / "age_groups_by_country.csv"

# -------------------------
# Loaders
# -------------------------
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
def load_occupations() -> pd.DataFrame:
    df = pd.read_csv(OCC_PATH)
    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)
    return df

@st.cache_data
def load_age() -> pd.DataFrame:
    df = pd.read_csv(AGE_PATH)
    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)
    df["country"] = df.get("country", "").astype(str)
    df["ageGroup"] = df.get("ageGroup", "").astype(str).str.strip()
    return df

# -------------------------
# Selected country (from globe)
# -------------------------
cd = st.session_state.get("selected_country")
if cd is None:
    st.info("Go back to the globe, click a country, then press View details.")
    st.stop()

qid = str(cd[0])
country_label = str(cd[1])
iso3 = str(cd[2])
country_url = f"http://www.wikidata.org/entity/{qid}"

# -------------------------
# Page header
# -------------------------
st.markdown("# Country dashboard")
st.markdown(f"## {country_label} ({iso3})")
st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

# =========================================================
# SECTION 1 — Gender (full width, no blank columns)
# =========================================================
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Gender</div>', unsafe_allow_html=True)
st.markdown('<div class="section-sub">Over decades + country-total breakdown (kept exactly as your components).</div>', unsafe_allow_html=True)

# Put BOTH gender sections full-width to avoid column blank space:
# (gender_over_decades is tall; putting it next to anything causes ugly empty area)
all_decades = load_decades()
render_gender_over_decades(all_decades, qid)

df_total = load_gender_total()
render_gender_breakdown_country_total(df_total, qid, country_label, iso3)

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

# =========================================================
# SECTION 2 — Languages + Ethnic groups (side-by-side works)
# =========================================================
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Culture</div>', unsafe_allow_html=True)
st.markdown('<div class="section-sub">Languages and ethnic group composition.</div>', unsafe_allow_html=True)

left, right = st.columns([1.05, 1.25], gap="large")

with left:
    st.markdown("### Languages")

    langs = load_languages()
    df_country_lang = langs[langs["country"] == country_url].copy()

    lang_type = st.radio(
        "Language type",
        ["spoken", "native"],
        horizontal=True,
        index=0,
        key="lang_type_radio_dashboard",
    )

    fig_lang = make_language_bar_top_n(
        df_country_lang,
        country_label=country_label,
        lang_type=lang_type,
        top_n=15,
    )
    st.plotly_chart(fig_lang, use_container_width=True, config={"displayModeBar": False})

    show_lang_details = st.checkbox(
        "Show details (all languages)",
        value=False,
        key="lang_details_checkbox_dashboard",
    )
    if show_lang_details:
        tbl = make_language_details_table(df_country_lang, lang_type=lang_type)
        if tbl.empty:
            st.info("No language data available for this selection.")
        else:
            q = st.text_input(
                "Search language",
                value="",
                placeholder="Type to filter…",
                key="lang_search_input_dashboard",
            )
            if q.strip():
                tbl = tbl[tbl["Language"].str.contains(q.strip(), case=False, na=False)].copy()

            styled = (
                tbl.style
                .format({"Count": "{:,.0f}", "Share (%)": "{:.2f}"})
                .bar(subset=["Share (%)"], color="#93C5FD")
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

with right:
    st.markdown("### Ethnic groups")

    ethnic = load_ethnicity()
    df_country_eth = ethnic[ethnic["country"] == country_url].copy()

    show_eth_details = st.checkbox(
        "Show details",
        value=False,
        key="eth_details_checkbox_dashboard",
    )

    title_prefix = f"Ethnic group composition — {country_label}"

    items = []
    for gender in ["Male", "Female", "Non-binary or other", "Unknown / not stated"]:
        fig, n = make_gender_ethnicity_donut(df_country_eth, gender, title_prefix, top_n=10)
        if fig is not None and n > 0:
            items.append((gender, fig, n))

    if not items:
        st.info("No ethnicity data available for this country.")
    else:
        for i in range(0, len(items), 2):
            cols = st.columns(2, gap="large")

            gender_l, fig_l, n_l = items[i]
            with cols[0]:
                st.caption(f"{gender_l} (n = {n_l:,})")
                st.plotly_chart(fig_l, use_container_width=True, config={"displayModeBar": False})
                if show_eth_details:
                    t = make_gender_ethnicity_table(df_country_eth, gender_l, top_n=10)
                    if not t.empty:
                        st.dataframe(t, use_container_width=True, hide_index=True)

            if i + 1 < len(items):
                gender_r, fig_r, n_r = items[i + 1]
                with cols[1]:
                    st.caption(f"{gender_r} (n = {n_r:,})")
                    st.plotly_chart(fig_r, use_container_width=True, config={"displayModeBar": False})
                    if show_eth_details:
                        t = make_gender_ethnicity_table(df_country_eth, gender_r, top_n=10)
                        if not t.empty:
                            st.dataframe(t, use_container_width=True, hide_index=True)

st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

# =========================================================
# SECTION 3 — Occupations + Age representation (side-by-side works)
# =========================================================
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.markdown('<div class="section-title">Workforce & age</div>', unsafe_allow_html=True)
st.markdown('<div class="section-sub">Occupations treemap and age histogram.</div>', unsafe_allow_html=True)

left, right = st.columns([1.25, 1.0], gap="large")

with left:
    st.markdown("### Occupations")
    st.caption("Tip: click a block to zoom in. Double-click to go back.")

    occ_all = load_occupations()
    df_occ_country = filter_country_occ(occ_all, qid)

    gender_occ = st.selectbox(
        "Gender (occupations)",
        ["All", "Male", "Female", "Non-binary or other", "Unknown / not stated"],
        index=0,
        key="occ_gender_select_dashboard",
    )

    group_mode = st.selectbox(
        "Group by (occupations)",
        ["ISCO Major → Occupation", "Sector → Occupation"],
        index=0,
        key="occ_group_select_dashboard",
    )

    fig_occ = make_occupation_treemap(
        df_occ_country,
        country_label=country_label,
        gender_filter=gender_occ,
        group_mode=group_mode,
    )
    st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})

    show_occ = st.checkbox(
        "Show details (full table)",
        value=False,
        key="occ_details_checkbox_dashboard",
    )
    if show_occ:
        t = make_occupation_details_table(df_occ_country, gender_filter=gender_occ)
        if t.empty:
            st.info("No occupation data available for this selection.")
        else:
            q = st.text_input(
                "Search occupation",
                value="",
                placeholder="Type to filter…",
                key="occ_search_input_dashboard",
            )
            if q.strip():
                t = t[t["Occupation"].str.contains(q.strip(), case=False, na=False)].copy()

            styled = (
                t.style
                .format({"Count": "{:,.0f}", "Share (%)": "{:.2f}"})
                .bar(subset=["Share (%)"], color="#BBF7D0")
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

with right:
    st.markdown("### Age representation")

    age_all = load_age()
    df_age_country = filter_country_age(age_all, qid)

    mode = st.radio(
        "Display (age)",
        ["Counts", "Share (%)"],
        horizontal=True,
        index=0,
        key="age_mode_radio_dashboard",
    )

    fig_age, base_age = make_age_histogram(df_age_country, country_label=country_label, mode=mode)
    st.plotly_chart(fig_age, use_container_width=True, config={"displayModeBar": False})

    show_age = st.checkbox(
        "Show details",
        value=False,
        key="age_details_checkbox_dashboard",  # ✅ fixes duplicate checkbox id
    )
    if show_age:
        t = make_age_table(base_age)
        styled = t.style.format({"Count": "{:,.0f}", "Share (%)": "{:.2f}"})
        st.dataframe(styled, use_container_width=True, hide_index=True)

st.markdown("</div>", unsafe_allow_html=True)