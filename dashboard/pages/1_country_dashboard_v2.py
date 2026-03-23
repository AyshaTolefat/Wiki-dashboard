# pages/1_country_dashboard_v2.py
import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
from contextlib import contextmanager
import plotly.graph_objects as go

# Components you already have
from components.gender_over_decades import build_country_decade_df
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
from components.ethnicity_bubble import (
    make_gender_ethnicity_donut,
    make_gender_ethnicity_table,
    make_gender_ethnicity_legend_df,
)
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

st.set_page_config(layout="wide")


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
# Data loaders
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
    return pd.read_csv(OCC_PATH)


@st.cache_data
def load_age() -> pd.DataFrame:
    df = pd.read_csv(AGE_PATH)
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["ageGroup"] = df["ageGroup"].astype(str).str.strip()
    df["country"] = df["country"].astype(str)
    return df


# -------------------------
# UI helpers
# -------------------------
def inject_global_css():
    st.markdown(
        """
        <style>
          .section-title{
            font-size: 20px;
            font-weight: 800;
            margin: 0 0 6px 0;
          }
          .desc{
            color:#6B7280;
            font-size: 13px;
            margin: 0 0 12px 0;
            line-height: 1.35;
          }
          .muted{
            color:#6B7280;
            font-size: 12px;
          }
          .kpi{
            background:#ffffff;
            border:1px solid #E6E6E6;
            border-radius:16px;
            padding:14px 14px;
            box-shadow:0 1px 2px rgba(0,0,0,0.04);
            height: 100%;
          }
          .kpi-label{
            color:#6B7280;
            font-size:12px;
            margin-bottom:6px;
          }
          .kpi-value{
            font-size:22px;
            font-weight:900;
            color:#111827;
            line-height:1.1;
          }
          .kpi-sub{
            margin-top:6px;
            color:#6B7280;
            font-size:12px;
          }
          hr{
            border:none;
            border-top:1px solid #EFEFEF;
            margin: 18px 0;
          }
          .block-container { padding-top: 1.4rem; padding-bottom: 2rem; }

          .mini-title{
            font-size: 14px;
            font-weight: 800;
            margin: 0 0 4px 0;
          }
          .mini-desc{
            font-size: 12px;
            color: #6B7280;
            margin: 0 0 8px 0;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card(label: str, value: str, sub: str = ""):
    st.markdown(
        f"""
        <div class="kpi">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{value}</div>
          {f'<div class="kpi-sub">{sub}</div>' if sub else ''}
        </div>
        """,
        unsafe_allow_html=True,
    )


@contextmanager
def card():
    """
    Streamlit-native card container.
    This fixes the "empty rounded bubbles" caused by trying to wrap st.plotly_chart with raw HTML.
    """
    try:
        with st.container(border=True):
            yield
    except TypeError:
        # Older Streamlit: no border=True support
        with st.container():
            yield

def render_ethnicity_legend(df_legend: pd.DataFrame):
    if df_legend is None or df_legend.empty:
        return

    items = []
    for _, row in df_legend.iterrows():
        label = str(row["Ethnic group"])
        color = str(row["Color"])
        pct = str(row["Percent"])
        items.append(
            f"""
            <div style="display:flex; align-items:center; gap:8px; margin:4px 0;">
              <span style="
                  display:inline-block;
                  width:10px;
                  height:10px;
                  border-radius:50%;
                  background:{color};
                  flex:0 0 10px;
              "></span>
              <span style="font-size:12px; color:#374151;">{label}</span>
              <span style="font-size:12px; color:#6B7280; margin-left:auto;">{pct}</span>
            </div>
            """
        )

    st.markdown(
        """
        <div style="margin-top:8px;">
          <div style="font-size:12px; font-weight:700; color:#6B7280; margin-bottom:6px;">
            Ethnic group legend
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("".join(items), unsafe_allow_html=True)

def compute_top_ethnic_group(df_country_eth: pd.DataFrame) -> str:
    if df_country_eth is None or df_country_eth.empty:
        return "—"
    d = df_country_eth.copy()
    d["count"] = pd.to_numeric(d["count"], errors="coerce").fillna(0).astype(int)
    d["ethnicGroupLabel"] = d["ethnicGroupLabel"].astype(str).str.strip()
    bad = d["ethnicGroupLabel"].isna() | d["ethnicGroupLabel"].isin(["", "nan", "None"])
    d = d[~bad].copy()
    if d.empty:
        return "—"
    agg = d.groupby("ethnicGroupLabel", as_index=False)["count"].sum().sort_values("count", ascending=False)
    return str(agg.iloc[0]["ethnicGroupLabel"]) if not agg.empty else "—"


# -------------------------
# Page start
# -------------------------
inject_global_css()

cd = st.session_state.get("selected_country")
if cd is None:
    st.info("Go back to the globe, click a country, then press Explore / View details.")
    st.stop()

st.markdown(
    """
    <style>
    div[data-testid="stButton"] > button[kind="secondary"] {
        margin-left: 2.2rem;
        margin-top: 2.2rem;
        width: 170px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if st.button("← Back to globe"):
    st.switch_page("pages/0_Globe_Overview.py")

st.markdown("# Country dashboard")

qid = str(cd[0])
country_label = str(cd[1])
iso3 = str(cd[2])

st.markdown(f"## {country_label} ({iso3})")



# -------------------------
# Load data (once)
# -------------------------
df_total = load_gender_total()
df_decades_all = load_decades()
df_lang_all = load_languages()
df_eth_all = load_ethnicity()
df_occ_all = load_occupations()
df_age_all = load_age()

country_url = f"http://www.wikidata.org/entity/{qid}"

df_country_lang = df_lang_all[df_lang_all["country"] == country_url].copy()
df_country_eth = df_eth_all[df_eth_all["country"] == country_url].copy()
df_country_occ = filter_country_occ(df_occ_all, qid)
df_country_age = filter_country_age(df_age_all, qid)

# Gender totals KPIs
totals = get_gender_totals_for_country(df_total, qid)
male = int(totals["Male"])
female = int(totals["Female"])
nb = int(totals["Non-binary or other"])
unk = int(totals["Unknown / not stated"])
total_all = male + female + nb + unk

mf_total = male + female
male_pct = (male / mf_total * 100.0) if mf_total > 0 else 0.0
female_pct = (female / mf_total * 100.0) if mf_total > 0 else 0.0
gap_pp_total = abs(male_pct - female_pct)
category_total = classify_category(male, female, gap_pp_threshold=10.0)

# Language KPI (default spoken)
lang_kpis_spoken = compute_language_kpis(df_country_lang, lang_type="spoken")
top_lang_label = lang_kpis_spoken.get("top_label", "—")
top_lang_share = float(lang_kpis_spoken.get("top_share", 0.0))

# Ethnic KPI (overall)
top_eth = compute_top_ethnic_group(df_country_eth)


# -------------------------
# KPI row(s) at top
# -------------------------
k1, k2, k3, k4 = st.columns(4)
with k1:
    kpi_card("Total biographies", f"{total_all:,}")
with k2:
    kpi_card("Male", f"{male:,}", f"{male_pct:.1f}% of Male+Female" if mf_total > 0 else "—")
with k3:
    kpi_card("Female", f"{female:,}", f"{female_pct:.1f}% of Male+Female" if mf_total > 0 else "—")
with k4:
    kpi_card("Gender gap", f"{gap_pp_total:.1f} pp", category_total)

k5, k6, k7, k8 = st.columns(4)
with k5:
    kpi_card("Non-binary / other", f"{nb:,}")
with k6:
    kpi_card("Unknown / not stated", f"{unk:,}")
with k7:
    kpi_card(
        "Top language (spoken)",
        f"{top_lang_label}",
        f"{top_lang_share:.1f}% share (spoken)" if lang_kpis_spoken.get("total", 0) > 0 else "—",
    )
with k8:
    kpi_card("Top ethnic group", f"{top_eth}")

st.markdown("<hr/>", unsafe_allow_html=True)


# -------------------------
# Gender over decades (3 charts in one row)
# -------------------------
st.markdown('<div class="section-title">Gender over decades</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="desc">This section shows how the number of biographies linked to this country changes over time by gender. The data includes items recorded as human, linked to the country through place of birth and country. Gender is grouped from sex or gender, and decades are calculated from date of birth. Only biographies with a recorded birth date are included here, and only entries from 1900 onwards are counted.</div>',
    unsafe_allow_html=True,
)

df_dec_country = build_country_decade_df(df_decades_all, qid)

if df_dec_country.empty:
    st.warning("No decade data found for this country.")
else:
    decades = df_dec_country["decade"].tolist()
    selected_decade = st.selectbox(
        "Select decade",
        options=decades,
        index=len(decades) - 1,
        key="dash_decade_select",
    )

    COLOR_MALE = "#2B6CB0"
    COLOR_FEMALE = "#D53F8C"
    COLOR_NB = "#805AD5"
    COLOR_GAP = "#6B7280"

    def _add_line(fig, x, y, name, color):
        fig.add_trace(
            go.Scatter(
                x=x, y=y,
                mode="lines+markers",
                name=name,
                line=dict(color=color, width=3),
                marker=dict(size=6, color=color),
                hoverinfo="skip",
                hovertemplate=None,
            )
        )

    x = df_dec_country["decade"]

    # (1) Male+Female
    fig_mf = go.Figure()
    _add_line(fig_mf, x, df_dec_country["Male"], "Male", COLOR_MALE)
    _add_line(fig_mf, x, df_dec_country["Female"], "Female", COLOR_FEMALE)
    fig_mf.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#D1D5DB")
    fig_mf.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig_mf.update_layout(
        template="simple_white",
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="left", x=0),
        yaxis=dict(title="Biographies"),
        hovermode=False,
        title_text="",   # ✅ fixes "undefined"
    )

    # (2) Non-binary
    fig_nb = go.Figure()
    fig_nb.add_trace(
        go.Scatter(
            x=x,
            y=df_dec_country["Non-binary or other"],
            mode="lines+markers",
            name="Non-binary / other",
            line=dict(color=COLOR_NB, width=3),
            marker=dict(size=6, color=COLOR_NB),
            hoverinfo="skip",
            hovertemplate=None,
        )
    )
    fig_nb.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#D1D5DB")
    fig_nb.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig_nb.update_layout(
        template="simple_white",
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(title="Count"),
        showlegend=False,
        hovermode=False,
        title_text="",   # ✅ fixes "undefined"
    )

    # (3) Gap
    fig_gap = go.Figure()
    fig_gap.add_trace(
        go.Scatter(
            x=x,
            y=df_dec_country["gap_pp"],
            mode="lines+markers",
            name="Gap (pp)",
            line=dict(color=COLOR_GAP, width=3),
            marker=dict(size=6, color=COLOR_GAP),
            hoverinfo="skip",
            hovertemplate=None,
        )
    )
    fig_gap.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#D1D5DB")
    fig_gap.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig_gap.update_layout(
        template="simple_white",
        height=360,
        margin=dict(l=10, r=10, t=10, b=10),
        yaxis=dict(title="pp"),
        showlegend=False,
        hovermode=False,
        title_text="",   # ✅ fixes "undefined"
    )

    cA, cB, cC = st.columns(3)
    with cA:
        st.markdown('<div class="mini-title">Male vs Female</div>', unsafe_allow_html=True)
        st.markdown('<div class="mini-desc">Counts of biographies per decade.</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_mf, use_container_width=True, config={"displayModeBar": False})
    with cB:
        st.markdown('<div class="mini-title">Non-binary / other</div>', unsafe_allow_html=True)
        st.markdown('<div class="mini-desc">Non-binary / other biographies per decade.</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_nb, use_container_width=True, config={"displayModeBar": False})
    with cC:
        st.markdown('<div class="mini-title">Gender gap</div>', unsafe_allow_html=True)
        st.markdown('<div class="mini-desc">Absolute difference between Male% and Female%.</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_gap, use_container_width=True, config={"displayModeBar": False})

st.markdown("<hr/>", unsafe_allow_html=True)


# -------------------------
# Row: Gender breakdown donut + Languages + Age
# -------------------------
col1, col2, col3 = st.columns(3)

with col1:
    with card():
        st.markdown('<div class="section-title">Gender breakdown</div>', unsafe_allow_html=True)
        st.markdown('<div class="desc">This chart summarises the overall gender distribution of biographies linked to this country. The data includes items recorded as human, linked through place of birth and country. Gender is grouped from sex or gender, with separate categories for Male, Female, Non-binary or other, and Unknown / not stated when no value is recorded. Entries are limited to people born in or after 1900 when a birth date is available.</div>', unsafe_allow_html=True)

        donut = donut_gender_breakdown(male, female, nb, min_visible_share=0.008)
        donut.update_layout(height=360, margin=dict(l=10, r=10, t=10, b=10), title_text="")
        st.plotly_chart(donut, use_container_width=True, config={"displayModeBar": False})
        st.markdown(f'<div class="muted">{category_total} • Gap: {gap_pp_total:.1f} pp</div>', unsafe_allow_html=True)

with col2:
    with card():
        st.markdown('<div class="section-title">Languages</div>', unsafe_allow_html=True)
        st.markdown('<div class="desc">This section shows languages associated with biographies linked to this country. The data includes items recorded as human, linked through place of birth and country. Languages are counted from native language and languages spoken, written or signed. Entries are limited to people born in or after 1900 when a birth date is available. “Native” and “spoken” are shown separately because they come from different Wikidata properties.</div>', unsafe_allow_html=True)

        lang_type = st.radio("Type", ["spoken", "native"], index=0, horizontal=True, key="dash_lang_type")

        fig_lang = make_language_bar_top_n(df_country_lang, country_label=country_label, lang_type=lang_type, top_n=15)
        fig_lang.update_layout(height=360, margin=dict(l=10, r=10, t=40, b=10), title_text="")
        st.plotly_chart(fig_lang, use_container_width=True, config={"displayModeBar": False})

        show_lang = st.checkbox("Show language details", value=False, key="dash_show_lang_details")
        if show_lang:
            tbl = make_language_details_table(df_country_lang, lang_type=lang_type)
            st.dataframe(tbl, use_container_width=True, hide_index=True)

with col3:
    with card():
        st.markdown('<div class="section-title">Age</div>', unsafe_allow_html=True)
        st.markdown('<div class="desc">This section groups biographies into broad age ranges based on recorded birth dates. The data includes items recorded as human, linked through place of birth and country. Age is calculated from date of birth using the current year, and only entries with a known birth date are included. Items with a recorded date of death are excluded here, so this view reflects living biographies with plausible ages between 0 and 120. The age groups follow four broad bands: 0-14, 15-24, 25-59, and 60+.</div>', unsafe_allow_html=True)

        age_mode = st.radio("Mode", ["Counts", "Share (%)"], index=0, horizontal=True, key="dash_age_mode")

        fig_age, base_age = make_age_histogram(df_country_age, country_label=country_label, mode=age_mode)
        fig_age.update_layout(height=360, margin=dict(l=10, r=10, t=40, b=10), title_text="")
        st.plotly_chart(fig_age, use_container_width=True, config={"displayModeBar": False})

        show_age = st.checkbox("Show age details", value=False, key="dash_show_age_details")
        if show_age:
            age_tbl = make_age_table(base_age)
            st.dataframe(age_tbl, use_container_width=True, hide_index=True)

st.markdown("<hr/>", unsafe_allow_html=True)


# -------------------------
# Ethnic groups
# -------------------------
st.markdown('<div class="section-title">Ethnic groups</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="desc">This section shows ethnic group labels attached to biographies linked to this country, separated by gender where available. The data includes items recorded as human, linked through place of birth and country. Ethnic group values come from ethnic group, and gender categories are grouped from sex or gender. Entries are limited to people born in or after 1900 when a birth date is available. This chart shows how ethnic-group labels are represented in Wikidata biographies, not the real-world ethnic composition of the country.</div>',
    unsafe_allow_html=True,
)

show_eth_details = st.checkbox("Show ethnicity details", value=False, key="dash_show_eth_details")

gender_order = ["Male", "Female", "Non-binary or other", "Unknown / not stated"]
eth_items = []
title_prefix = f"Ethnic group composition — {country_label}"

for g in gender_order:
    fig, n = make_gender_ethnicity_donut(df_country_eth, g, title_prefix, top_n=10)
    if fig is not None and n > 0:
        fig.update_layout(height=320, margin=dict(l=10, r=10, t=45, b=10), title_text="")
        eth_items.append((g, fig, n))

if not eth_items:
    st.info("No ethnicity data available for this country.")
else:
    cols = st.columns(4)
    for i, (g, fig, n) in enumerate(eth_items[:4]):
        with cols[i]:
            with card():
                st.caption(f"{g} (n = {n:,})")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

                legend_df = make_gender_ethnicity_legend_df(df_country_eth, g, top_n=10)
                render_ethnicity_legend(legend_df)

                if show_eth_details:
                    tbl = make_gender_ethnicity_table(df_country_eth, g, top_n=10)
                    if not tbl.empty:
                        st.dataframe(tbl, use_container_width=True, hide_index=True)

st.markdown("<hr/>", unsafe_allow_html=True)


# -------------------------
# Occupations
# -------------------------
# -------------------------
# Occupations
# -------------------------
st.markdown('<div class="section-title">Occupations</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="desc">This section shows occupations attached to biographies linked to this country, grouped into major ISCO categories for readability. Unlike the other panels, this data was prepared from a Wikidata dump rather than a live SPARQL query, then mapped into ISCO major groups in the processing pipeline. The treemap shows how occupations are represented in the biography dataset, not the country real-world labour-force distribution.</div>',
    unsafe_allow_html=True,
)

gender_filter = st.selectbox(
    "Gender (occupations)",
    options=["All", "Male", "Female", "Non-binary or other", "Unknown / not stated"],
    index=0,
    key="dash_occ_gender",
)

fig_occ = make_occupation_treemap(
    df_country_occ,
    country_label=country_label,
    gender_filter=gender_filter,
    group_mode="ISCO Major → Occupation",
    min_share_within_parent=0.005,
)
fig_occ.update_layout(height=650, margin=dict(l=10, r=10, t=55, b=10), title_text="")

with card():
    st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})

show_occ = st.checkbox("Show occupation details", value=False, key="dash_show_occ_details")
if show_occ:
    occ_tbl = make_occupation_details_table(df_country_occ, gender_filter=gender_filter)
    st.dataframe(occ_tbl, use_container_width=True, hide_index=True)