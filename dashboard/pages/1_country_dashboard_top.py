import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

# --- Components (your existing functions; we will call chart builders directly) ---
from components.gender_over_decades import build_country_decade_df, add_line
from components.gender_breakdown_country_total import (
    get_gender_totals_for_country,
    classify_category,
    donut_gender_breakdown,
)

from components.language_bar_chart import (
    compute_language_kpis,
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
# Styles (full width + dashboard look)
# -------------------------
st.markdown(
    """
    <style>
      .block-container {
        max-width: 100% !important;
        padding-left: 2.2rem;
        padding-right: 2.2rem;
        padding-top: 1.2rem;
        padding-bottom: 2rem;
      }
      div[data-testid="stVerticalBlock"] { gap: 0.7rem; }

      .soft-divider {height: 1px; background: #EEF2F7; margin: 0.8rem 0 0.8rem 0;}

      .kpi-grid{
        display:grid;
        grid-template-columns: repeat(4, minmax(180px, 1fr));
        gap: 12px;
      }
      .kpi-card{
        background:#ffffff;
        border:1px solid #E6E6E6;
        border-radius:16px;
        padding:14px 14px;
        box-shadow:0 1px 2px rgba(0,0,0,0.04);
        min-height: 86px;
      }
      .kpi-label{
        color:#6B7280;
        font-size:12px;
        margin:0 0 6px 0;
      }
      .kpi-value{
        font-size:22px;
        font-weight:900;
        margin:0;
        color:#111827;
        line-height:1.1;
      }
      .kpi-sub{
        margin-top:6px;
        color:#6B7280;
        font-size:12px;
      }

      .section-card{
        background:#ffffff;
        border:1px solid #E6E6E6;
        border-radius:16px;
        padding:14px 14px 10px 14px;
        box-shadow:0 1px 2px rgba(0,0,0,0.04);
      }
      .section-title{
        font-size:20px;
        font-weight:900;
        margin:0 0 8px 0;
      }
      .mini-title{
        font-size:14px;
        font-weight:800;
        margin:0 0 6px 0;
      }
      .muted{ color:#6B7280; font-size:12px; margin:0 0 10px 0; }
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
# Helpers
# -------------------------
COLOR_MALE = "#2B6CB0"
COLOR_FEMALE = "#D53F8C"
COLOR_NB = "#805AD5"
COLOR_GAP = "#6B7280"

def _kpi(label: str, value: str, sub: str = ""):
    st.markdown(
        f"""
        <div class="kpi-card">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
          <div class="kpi-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _fmt_int(x) -> str:
    return f"{int(x):,}"

def _fmt_pp(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:.1f} pp"

# -------------------------
# Selected country
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
# Header
# -------------------------
st.markdown("# Country dashboard (KPIs at top)")
st.markdown(f"## {country_label} ({iso3})")

# =========================================================
# KPI HEADER (single place)
# =========================================================
df_total = load_gender_total()
totals = get_gender_totals_for_country(df_total, qid)
male = int(totals["Male"])
female = int(totals["Female"])
nb = int(totals["Non-binary or other"])
unk = int(totals["Unknown / not stated"])
total_all = male + female + nb + unk

mf_total = male + female
male_pct = (male / mf_total * 100.0) if mf_total > 0 else 0.0
female_pct = (female / mf_total * 100.0) if mf_total > 0 else 0.0
gap_pp = abs(male_pct - female_pct)
category = classify_category(male, female, gap_pp_threshold=10.0)

langs = load_languages()
df_country_lang = langs[langs["country"] == country_url].copy()
lang_kpi = compute_language_kpis(df_country_lang, lang_type="spoken")  # spoken default
top_lang_label = lang_kpi.get("top_label", "—")
top_lang_share = float(lang_kpi.get("top_share", 0.0))

ethnic = load_ethnicity()
df_country_eth = ethnic[ethnic["country"] == country_url].copy()
top_eth_label = "—"
if not df_country_eth.empty:
    # quick top ethnic group overall (across genders) for KPI
    tmp = df_country_eth.copy()
    tmp["count"] = pd.to_numeric(tmp["count"], errors="coerce").fillna(0).astype(int)
    tmp = tmp[tmp["count"] > 0]
    if not tmp.empty:
        g = tmp.groupby("ethnicGroupLabel", as_index=False)["count"].sum().sort_values("count", ascending=False)
        if not g.empty:
            top_eth_label = str(g.iloc[0]["ethnicGroupLabel"])

st.markdown('<div class="kpi-grid">', unsafe_allow_html=True)
c = st.columns(4, gap="large")
with c[0]:
    _kpi("Total biographies", _fmt_int(total_all))
with c[1]:
    _kpi("Male", _fmt_int(male), f"{male_pct:.1f}% of Male+Female")
with c[2]:
    _kpi("Female", _fmt_int(female), f"{female_pct:.1f}% of Male+Female")
with c[3]:
    _kpi("Gender gap", _fmt_pp(gap_pp), category)

c2 = st.columns(4, gap="large")
with c2[0]:
    _kpi("Non-binary / other", _fmt_int(nb))
with c2[1]:
    _kpi("Unknown / not stated", _fmt_int(unk))
with c2[2]:
    _kpi("Top language (spoken)", top_lang_label, f"{top_lang_share:.1f}% share (spoken)")
with c2[3]:
    _kpi("Top ethnic group", top_eth_label)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

# =========================================================
# SMALLER GRAPHS GRID (3 per row)
# =========================================================

# --- Gender over decades small version (one chart only) ---
all_decades = load_decades()
df_dec = build_country_decade_df(all_decades, qid)

# decade control (global, affects the mini charts)
if df_dec.empty:
    decades = []
    selected_decade = None
else:
    decades = df_dec["decade"].tolist()
    selected_decade = st.selectbox("Select decade (affects gender mini-charts)", decades, index=len(decades) - 1, key="dash2_decade_select")

# Row 1: Gender over decades (MF), NB, Gap  (3 charts)
row1 = st.columns(3, gap="large")

with row1[0]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Gender over decades</div>', unsafe_allow_html=True)
    st.markdown('<div class="muted">Male + Female (counts)</div>', unsafe_allow_html=True)

    fig = go.Figure()
    if not df_dec.empty:
        add_line(fig, df_dec["decade"], df_dec["Male"], "Male", COLOR_MALE)
        add_line(fig, df_dec["decade"], df_dec["Female"], "Female", COLOR_FEMALE)
        if selected_decade is not None:
            fig.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#9CA3AF")

    fig.update_layout(template="simple_white", height=320, margin=dict(l=10, r=10, t=10, b=10), hovermode=False, legend=dict(orientation="h", y=1.1))
    fig.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig.update_yaxes(title="Biographies")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

with row1[1]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="mini-title">Non-binary over decades</div>', unsafe_allow_html=True)
    fig = go.Figure()
    if not df_dec.empty:
        fig.add_trace(go.Scatter(
            x=df_dec["decade"], y=df_dec["Non-binary or other"],
            mode="lines+markers", line=dict(color=COLOR_NB, width=3),
            marker=dict(size=6, color=COLOR_NB),
            hoverinfo="skip", hovertemplate=None
        ))
        if selected_decade is not None:
            fig.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#9CA3AF")
    fig.update_layout(template="simple_white", height=320, margin=dict(l=10, r=10, t=10, b=10), hovermode=False, showlegend=False)
    fig.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig.update_yaxes(title="Count")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

with row1[2]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="mini-title">Gender gap over decades</div>', unsafe_allow_html=True)
    fig = go.Figure()
    if not df_dec.empty:
        fig.add_trace(go.Scatter(
            x=df_dec["decade"], y=df_dec["gap_pp"],
            mode="lines+markers", line=dict(color=COLOR_GAP, width=3),
            marker=dict(size=6, color=COLOR_GAP),
            hoverinfo="skip", hovertemplate=None
        ))
        if selected_decade is not None:
            fig.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#9CA3AF")
    fig.update_layout(template="simple_white", height=320, margin=dict(l=10, r=10, t=10, b=10), hovermode=False, showlegend=False)
    fig.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig.update_yaxes(title="pp")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

# Row 2: Gender donut, Languages, Age (3 charts)
row2 = st.columns(3, gap="large")

with row2[0]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Gender breakdown</div>', unsafe_allow_html=True)
    donut = donut_gender_breakdown(male, female, nb, min_visible_share=0.008)
    donut.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(donut, use_container_width=True, config={"displayModeBar": False})
    st.markdown("</div>", unsafe_allow_html=True)

with row2[1]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Languages</div>', unsafe_allow_html=True)

    lang_type = st.radio("Type", ["spoken", "native"], horizontal=True, index=0, key="dash2_lang_radio")
    fig_lang = make_language_bar_top_n(df_country_lang, country_label=country_label, lang_type=lang_type, top_n=15)
    fig_lang.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(fig_lang, use_container_width=True, config={"displayModeBar": False})

    show_lang_details = st.checkbox("Show language details", value=False, key="dash2_lang_details")
    if show_lang_details:
        tbl = make_language_details_table(df_country_lang, lang_type=lang_type)
        st.dataframe(tbl, use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)

with row2[2]:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Age</div>', unsafe_allow_html=True)

    age_all = load_age()
    df_age_country = filter_country_age(age_all, qid)
    age_mode = st.radio("Mode", ["Counts", "Share (%)"], horizontal=True, index=0, key="dash2_age_radio")
    fig_age, base_age = make_age_histogram(df_age_country, country_label=country_label, mode=age_mode)
    fig_age.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=10))
    st.plotly_chart(fig_age, use_container_width=True, config={"displayModeBar": False})

    show_age = st.checkbox("Show age details", value=False, key="dash2_age_details")
    if show_age:
        st.dataframe(make_age_table(base_age), use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)

# Row 3: Occupations (wide) + Ethnicity (wide) so it doesn't become unreadable
st.markdown('<div class="soft-divider"></div>', unsafe_allow_html=True)

wide1, wide2 = st.columns([1.3, 1.0], gap="large")

with wide1:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Occupations</div>', unsafe_allow_html=True)

    occ_all = load_occupations()
    df_occ_country = filter_country_occ(occ_all, qid)

    gender_occ = st.selectbox(
        "Gender (occupations)",
        ["All", "Male", "Female", "Non-binary or other", "Unknown / not stated"],
        index=0,
        key="dash2_occ_gender",
    )
    group_mode = st.selectbox(
        "Group by (occupations)",
        ["ISCO Major → Occupation", "Sector → Occupation"],
        index=0,
        key="dash2_occ_group",
    )

    fig_occ = make_occupation_treemap(
        df_occ_country,
        country_label=country_label,
        gender_filter=gender_occ,
        group_mode=group_mode,
    )
    fig_occ.update_layout(height=560, margin=dict(l=10, r=10, t=40, b=10))
    st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})

    show_occ = st.checkbox("Show occupation details", value=False, key="dash2_occ_details")
    if show_occ:
        t = make_occupation_details_table(df_occ_country, gender_filter=gender_occ)
        st.dataframe(t, use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)

with wide2:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Ethnic groups</div>', unsafe_allow_html=True)

    show_eth = st.checkbox("Show details", value=False, key="dash2_eth_details")
    title_prefix = f"Ethnic group composition — {country_label}"

    items = []
    for gender in ["Male", "Female", "Non-binary or other", "Unknown / not stated"]:
        fig, n = make_gender_ethnicity_donut(df_country_eth, gender, title_prefix, top_n=10)
        if fig is not None and n > 0:
            items.append((gender, fig, n))

    if not items:
        st.info("No ethnicity data available.")
    else:
        for gender, fig, n in items:
            st.caption(f"{gender} (n = {n:,})")
            fig.update_layout(height=340, margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            if show_eth:
                t = make_gender_ethnicity_table(df_country_eth, gender, top_n=10)
                if not t.empty:
                    st.dataframe(t, use_container_width=True, hide_index=True)

    st.markdown("</div>", unsafe_allow_html=True)