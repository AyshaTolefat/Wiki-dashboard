# pages/1_country_dashboard_v2.py
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from components.age_histogram import (
    filter_country_age,
    make_age_histogram,
    make_age_table,
)
from components.ethnicity_bubble import (
    make_gender_ethnicity_donut,
    make_gender_ethnicity_legend_df,
    make_gender_ethnicity_table,
)
from components.gender_breakdown_country_total import (
    classify_category,
    donut_gender_breakdown,
    get_gender_totals_for_country,
)
from components.gender_over_decades import build_country_decade_df
from components.language_bar_chart import (
    compute_language_kpis,
    make_language_bar_top_n,
    make_language_details_table,
)
from components.occupation_treemap import (
    filter_country_occ,
    make_occupation_details_table,
    make_occupation_treemap,
)

st.set_page_config(layout="wide")

# -------------------------
# Paths
# -------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR.parent / "data"

DECADES_PATH      = DATA_DIR / "gender_decades_by_country.csv"
GENDER_TOTAL_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"
LANG_PATH         = DATA_DIR / "languages_by_country.csv"
ETHNIC_PATH       = DATA_DIR / "ethnic_group_by_country_gender.csv"
OCC_PATH          = DATA_DIR / "gender_occupation_with_isco_refined.csv"
AGE_PATH          = DATA_DIR / "age_groups_by_country.csv"


# -------------------------
# Raw data loaders
# cache_resource → zero-copy object reference, no pickle overhead
# -------------------------

@st.cache_resource(show_spinner=False)
def load_decades() -> pd.DataFrame:
    df = pd.read_csv(DECADES_PATH)
    df["qid"]            = df["country"].astype(str).str.rsplit("/", n=1).str[-1]
    df["genderCategory"] = df["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["decade"]         = pd.to_numeric(df["decade"], errors="coerce")
    df = df.dropna(subset=["decade"]).copy()
    df["decade"] = df["decade"].astype(int)
    df["count"]  = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_resource(show_spinner=False)
def load_gender_total() -> pd.DataFrame:
    df = pd.read_csv(GENDER_TOTAL_PATH)
    df["qid"]            = df["country"].astype(str).str.rsplit("/", n=1).str[-1]
    df["genderCategory"] = df["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["count"]          = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    return df


@st.cache_resource(show_spinner=False)
def load_languages() -> pd.DataFrame:
    df = pd.read_csv(LANG_PATH)
    df["count"]         = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["type"]          = df["type"].astype(str).str.strip().str.lower()
    df["languageLabel"] = df["languageLabel"].astype(str).str.strip()
    df["country"]       = df["country"].astype(str)
    return df


@st.cache_resource(show_spinner=False)
def load_ethnicity() -> pd.DataFrame:
    df = pd.read_csv(ETHNIC_PATH)
    df["count"]            = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["genderCategory"]   = df["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["ethnicGroupLabel"] = df["ethnicGroupLabel"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    df["country"]          = df["country"].astype(str)
    return df


@st.cache_resource(show_spinner=False)
def load_occupations() -> pd.DataFrame:
    return pd.read_csv(OCC_PATH)


@st.cache_resource(show_spinner=False)
def load_age() -> pd.DataFrame:
    df = pd.read_csv(AGE_PATH)
    df["count"]    = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df["ageGroup"] = df["ageGroup"].astype(str).str.strip()
    df["country"]  = df["country"].astype(str)
    return df


# -------------------------
# Per-country data slices (cached by qid)
# -------------------------

@st.cache_data(show_spinner=False)
def get_country_decades(qid: str) -> pd.DataFrame:
    return build_country_decade_df(load_decades(), qid)


@st.cache_data(show_spinner=False)
def get_country_languages(qid: str) -> pd.DataFrame:
    url = f"http://www.wikidata.org/entity/{qid}"
    return load_languages()[load_languages()["country"] == url].copy()


@st.cache_data(show_spinner=False)
def get_country_ethnicity(qid: str) -> pd.DataFrame:
    url = f"http://www.wikidata.org/entity/{qid}"
    return load_ethnicity()[load_ethnicity()["country"] == url].copy()


@st.cache_data(show_spinner=False)
def get_country_occupations(qid: str) -> pd.DataFrame:
    return filter_country_occ(load_occupations(), qid)


@st.cache_data(show_spinner=False)
def get_country_age(qid: str) -> pd.DataFrame:
    return filter_country_age(load_age(), qid)


@st.cache_data(show_spinner=False)
def compute_top_ethnic_group(df_eth: pd.DataFrame) -> str:
    if df_eth is None or df_eth.empty:
        return "—"
    d = df_eth.copy()
    d["count"]            = pd.to_numeric(d["count"], errors="coerce").fillna(0).astype(int)
    d["ethnicGroupLabel"] = d["ethnicGroupLabel"].astype(str).str.strip()
    bad = d["ethnicGroupLabel"].isna() | d["ethnicGroupLabel"].isin(["", "nan", "None"])
    d = d[~bad]
    if d.empty:
        return "—"
    agg = d.groupby("ethnicGroupLabel", as_index=False)["count"].sum().sort_values("count", ascending=False)
    return str(agg.iloc[0]["ethnicGroupLabel"]) if not agg.empty else "—"


@st.cache_data(show_spinner=False)
def get_country_kpis(qid: str) -> dict:
    df_total    = load_gender_total()
    df_lang     = get_country_languages(qid)
    df_eth      = get_country_ethnicity(qid)

    totals    = get_gender_totals_for_country(df_total, qid)
    male      = int(totals["Male"])
    female    = int(totals["Female"])
    nb        = int(totals["Non-binary or other"])
    unk       = int(totals["Unknown / not stated"])
    total_all = male + female + nb + unk
    mf_total  = male + female

    male_pct   = (male   / mf_total * 100.0) if mf_total > 0 else 0.0
    female_pct = (female / mf_total * 100.0) if mf_total > 0 else 0.0
    gap_pp     = abs(male_pct - female_pct)
    cat        = classify_category(male, female, gap_pp_threshold=10.0)

    lang_kpis  = compute_language_kpis(df_lang, lang_type="spoken")
    top_eth    = compute_top_ethnic_group(df_eth)

    return {
        "male": male, "female": female, "nb": nb, "unk": unk,
        "total_all": total_all, "mf_total": mf_total,
        "male_pct": male_pct, "female_pct": female_pct,
        "gap_pp_total": gap_pp, "category_total": cat,
        "lang_kpis_spoken": lang_kpis,
        "top_lang_label": lang_kpis.get("top_label", "—"),
        "top_lang_share": float(lang_kpis.get("top_share", 0.0)),
        "top_eth": top_eth,
    }


# -------------------------
# Cached Plotly figure builders
#
# Each chart is built exactly once per (qid, user-selection) combination.
# Fragment widget interactions call these; the second call for the same
# arguments returns the cached Figure in microseconds rather than
# re-running the component function.
#
# Figures that depend on a widget value are keyed on that value so every
# unique selection is cached separately (e.g. "spoken" vs "native" for
# languages, each gender filter for occupations).
# -------------------------

@st.cache_data(show_spinner=False)
def cached_gender_donut(qid: str, male: int, female: int, nb: int) -> go.Figure:
    return donut_gender_breakdown(male, female, nb, min_visible_share=0.008)


@st.cache_data(show_spinner=False)
def cached_language_figure(qid: str, lang_type: str, country_label: str) -> go.Figure:
    df_lang = get_country_languages(qid)
    return make_language_bar_top_n(df_lang, country_label=country_label, lang_type=lang_type, top_n=15)


@st.cache_data(show_spinner=False)
def cached_age_figure(qid: str, mode: str, country_label: str):
    df_age = get_country_age(qid)
    return make_age_histogram(df_age, country_label=country_label, mode=mode)


@st.cache_data(show_spinner=False)
def cached_ethnicity_items(qid: str, country_label: str) -> list:
    """
    Pre-build all four ethnicity donuts and return them as a list of
    (gender, figure, n, legend_df) tuples.  Cached per qid so the section
    fragment never rebuilds figures on widget interaction.
    """
    df_eth      = get_country_ethnicity(qid)
    gender_order = ["Male", "Female", "Non-binary or other", "Unknown / not stated"]
    title_prefix = f"Ethnic group composition — {country_label}"
    items = []
    for g in gender_order:
        fig, n = make_gender_ethnicity_donut(df_eth, g, title_prefix, top_n=10)
        if fig is not None and n > 0:
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=45, b=10), title_text="")
            legend_df = make_gender_ethnicity_legend_df(df_eth, g, top_n=10)
            items.append((g, fig, n, legend_df))
    return items


@st.cache_data(show_spinner=False)
def cached_occupation_figure(qid: str, gender_filter: str, country_label: str) -> go.Figure:
    """
    The treemap is the most expensive chart to build.  Cached per
    (qid, gender_filter) so switching the dropdown is instant after the
    first selection.
    """
    df_occ = get_country_occupations(qid)
    return make_occupation_treemap(
        df_occ,
        country_label=country_label,
        gender_filter=gender_filter,
        group_mode="ISCO Major → Occupation",
        min_share_within_parent=0.005,
    )


# -------------------------
# UI helpers
# -------------------------

def inject_global_css():
    st.markdown(
        """
        <style>
          .section-title{ font-size:20px; font-weight:800; margin:0 0 6px 0; }
          .desc{ color:#6B7280; font-size:13px; margin:0 0 12px 0; line-height:1.35; }
          .muted{ color:#6B7280; font-size:12px; }
          .kpi{
            background:#ffffff; border:1px solid #E6E6E6; border-radius:16px;
            padding:14px; box-shadow:0 1px 2px rgba(0,0,0,0.04); height:100%;
          }
          .kpi-label{ color:#6B7280; font-size:12px; margin-bottom:6px; }
          .kpi-value{ font-size:22px; font-weight:900; color:#111827; line-height:1.1; }
          .kpi-sub{ margin-top:6px; color:#6B7280; font-size:12px; }
          hr{ border:none; border-top:1px solid #EFEFEF; margin:18px 0; }
          .block-container{ padding-top:1.4rem; padding-bottom:2rem; }
          .mini-title{ font-size:14px; font-weight:800; margin:0 0 4px 0; }
          .mini-desc{ font-size:12px; color:#6B7280; margin:0 0 8px 0; }
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
    try:
        with st.container(border=True):
            yield
    except TypeError:
        with st.container():
            yield


def render_ethnicity_legend(df_legend: pd.DataFrame):
    if df_legend is None or df_legend.empty:
        return
    items = []
    for _, row in df_legend.iterrows():
        items.append(
            f'<div style="display:flex;align-items:center;gap:8px;margin:4px 0;">'
            f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;'
            f'background:{row["Color"]};flex:0 0 10px;"></span>'
            f'<span style="font-size:12px;color:#374151;">{row["Ethnic group"]}</span>'
            f'<span style="font-size:12px;color:#6B7280;margin-left:auto;">{row["Percent"]}</span>'
            f'</div>'
        )
    st.markdown(
        '<div style="margin-top:8px;">'
        '<div style="font-size:12px;font-weight:700;color:#6B7280;margin-bottom:6px;">'
        'Ethnic group legend</div></div>',
        unsafe_allow_html=True,
    )
    st.markdown("".join(items), unsafe_allow_html=True)


# st.fragment shim: graceful fallback on Streamlit < 1.33
_fragment = getattr(st, "fragment", lambda f: f)


# =========================================================================
# FRAGMENTS  —  each section is independent.
#
# When a widget inside a fragment fires, ONLY that fragment re-executes.
# All cached figure getters return instantly on subsequent calls (the heavy
# component functions have already run and their output is cached by qid +
# widget value).
# =========================================================================

@_fragment
def section_decades(qid: str, color_male: str, color_female: str,
                    color_nb: str, color_gap: str):
    st.markdown('<div class="section-title">Gender over decades</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="desc">This section shows how the number of biographies linked to this country '
        'changes over time by gender. The data includes items recorded as human, linked to the country '
        'through place of birth and country. Gender is grouped from sex or gender, and decades are '
        'calculated from date of birth. Only biographies with a recorded birth date are included here, '
        'and only entries from 1900 onwards are counted.</div>',
        unsafe_allow_html=True,
    )

    df_dec = get_country_decades(qid)
    if df_dec.empty:
        st.warning("No decade data found for this country.")
        return

    decades = df_dec["decade"].tolist()
    selected_decade = st.selectbox(
        "Select decade", options=decades, index=len(decades)-1, key="dash_decade_select",
    )

    def _add_line(fig, x, y, name, color):
        fig.add_trace(go.Scatter(
            x=x, y=y, mode="lines+markers", name=name,
            line=dict(color=color, width=3), marker=dict(size=6, color=color),
            hoverinfo="skip", hovertemplate=None,
        ))

    x = df_dec["decade"]

    # Build base figures — these are cheap (simple scatter, <20 points).
    # vlines depend on the selectbox value so they cannot be pre-cached;
    # they are added after figure construction (add_vline is ~0 ms).
    fig_mf = go.Figure()
    _add_line(fig_mf, x, df_dec["Male"],   "Male",   color_male)
    _add_line(fig_mf, x, df_dec["Female"], "Female", color_female)
    fig_mf.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#D1D5DB")
    fig_mf.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig_mf.update_layout(
        template="simple_white", height=360, margin=dict(l=10,r=10,t=10,b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="left", x=0),
        yaxis=dict(title="Biographies"), hovermode=False, title_text="",
        uirevision=f"mf_{qid}",
    )

    fig_nb = go.Figure()
    fig_nb.add_trace(go.Scatter(
        x=x, y=df_dec["Non-binary or other"],
        mode="lines+markers", name="Non-binary / other",
        line=dict(color=color_nb, width=3), marker=dict(size=6, color=color_nb),
        hoverinfo="skip", hovertemplate=None,
    ))
    fig_nb.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#D1D5DB")
    fig_nb.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig_nb.update_layout(
        template="simple_white", height=360, margin=dict(l=10,r=10,t=10,b=10),
        yaxis=dict(title="Count"), showlegend=False, hovermode=False, title_text="",
        uirevision=f"nb_{qid}",
    )

    fig_gap = go.Figure()
    fig_gap.add_trace(go.Scatter(
        x=x, y=df_dec["gap_pp"],
        mode="lines+markers", name="Gap (pp)",
        line=dict(color=color_gap, width=3), marker=dict(size=6, color=color_gap),
        hoverinfo="skip", hovertemplate=None,
    ))
    fig_gap.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#D1D5DB")
    fig_gap.update_xaxes(title=None, tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    fig_gap.update_layout(
        template="simple_white", height=360, margin=dict(l=10,r=10,t=10,b=10),
        yaxis=dict(title="pp"), showlegend=False, hovermode=False, title_text="",
        uirevision=f"gap_{qid}",
    )

    cA, cB, cC = st.columns(3)
    with cA:
        st.markdown('<div class="mini-title">Male vs Female</div>', unsafe_allow_html=True)
        st.markdown('<div class="mini-desc">Counts of biographies per decade.</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_mf,  use_container_width=True, config={"displayModeBar": False})
    with cB:
        st.markdown('<div class="mini-title">Non-binary / other</div>', unsafe_allow_html=True)
        st.markdown('<div class="mini-desc">Non-binary / other biographies per decade.</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_nb,  use_container_width=True, config={"displayModeBar": False})
    with cC:
        st.markdown('<div class="mini-title">Gender gap</div>', unsafe_allow_html=True)
        st.markdown('<div class="mini-desc">Absolute difference between Male% and Female%.</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_gap, use_container_width=True, config={"displayModeBar": False})


@_fragment
def section_gender_languages_age(
    qid: str, country_label: str,
    male: int, female: int, nb: int,
    mf_total: int, gap_pp_total: float, category_total: str,
):
    col1, col2, col3 = st.columns(3)

    with col1:
        with card():
            st.markdown('<div class="section-title">Gender breakdown</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="desc">This chart summarises the overall gender distribution of biographies '
                'linked to this country. The data includes items recorded as human, linked through place '
                'of birth and country. Gender is grouped from sex or gender, with separate categories for '
                'Male, Female, Non-binary or other, and Unknown / not stated when no value is recorded. '
                'Entries are limited to people born in or after 1900 when a birth date is available.</div>',
                unsafe_allow_html=True,
            )
            # Cached: built once per qid, returned instantly on every fragment rerun
            donut = cached_gender_donut(qid, male, female, nb)
            donut.update_layout(height=360, margin=dict(l=10,r=10,t=10,b=10), title_text="")
            st.plotly_chart(donut, use_container_width=True, config={"displayModeBar": False})
            st.markdown(f'<div class="muted">{category_total} • Gap: {gap_pp_total:.1f} pp</div>', unsafe_allow_html=True)

    with col2:
        with card():
            st.markdown('<div class="section-title">Languages</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="desc">This section shows languages associated with biographies linked to '
                'this country. The data includes items recorded as human, linked through place of birth '
                'and country. Languages are counted from native language and languages spoken, written or '
                'signed. Entries are limited to people born in or after 1900 when a birth date is '
                'available. "Native" and "spoken" are shown separately because they come from different '
                'Wikidata properties.</div>',
                unsafe_allow_html=True,
            )
            lang_type = st.radio("Type", ["spoken","native"], index=0, horizontal=True, key="dash_lang_type")
            # Cached per (qid, lang_type): switching spoken↔native is instant after first view
            fig_lang = cached_language_figure(qid, lang_type, country_label)
            fig_lang.update_layout(height=360, margin=dict(l=10,r=10,t=40,b=10), title_text="")
            st.plotly_chart(fig_lang, use_container_width=True, config={"displayModeBar": False})
            if st.checkbox("Show language details", value=False, key="dash_show_lang_details"):
                df_lang = get_country_languages(qid)
                tbl = make_language_details_table(df_lang, lang_type=lang_type)
                st.dataframe(tbl, use_container_width=True, hide_index=True)

    with col3:
        with card():
            st.markdown('<div class="section-title">Age</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="desc">This section groups biographies into broad age ranges based on '
                'recorded birth dates. The data includes items recorded as human, linked through place '
                'of birth and country. Age is calculated from date of birth using the current year, and '
                'only entries with a known birth date are included. Items with a recorded date of death '
                'are excluded here, so this view reflects living biographies with plausible ages between '
                '0 and 120. The age groups follow four broad bands: 0-14, 15-24, 25-59, and 60+.</div>',
                unsafe_allow_html=True,
            )
            age_mode = st.radio("Mode", ["Counts","Share (%)"], index=0, horizontal=True, key="dash_age_mode")
            # Cached per (qid, mode): switching Counts↔Share is instant after first view
            fig_age, base_age = cached_age_figure(qid, age_mode, country_label)
            fig_age.update_layout(height=360, margin=dict(l=10,r=10,t=40,b=10), title_text="")
            st.plotly_chart(fig_age, use_container_width=True, config={"displayModeBar": False})
            if st.checkbox("Show age details", value=False, key="dash_show_age_details"):
                age_tbl = make_age_table(base_age)
                st.dataframe(age_tbl, use_container_width=True, hide_index=True)


@_fragment
def section_ethnicity(qid: str, country_label: str):
    st.markdown('<div class="section-title">Ethnic groups</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="desc">This section shows ethnic group labels attached to biographies linked to this '
        'country, separated by gender where available. The data includes items recorded as human, linked '
        'through place of birth and country. Ethnic group values come from ethnic group, and gender '
        'categories are grouped from sex or gender. Entries are limited to people born in or after 1900 '
        'when a birth date is available. This chart shows how ethnic-group labels are represented in '
        'Wikidata biographies, not the real-world ethnic composition of the country.</div>',
        unsafe_allow_html=True,
    )

    show_eth_details = st.checkbox("Show ethnicity details", value=False, key="dash_show_eth_details")

    # Cached: all four donuts built once per qid
    eth_items = cached_ethnicity_items(qid, country_label)

    if not eth_items:
        st.info("No ethnicity data available for this country.")
        return

    cols = st.columns(4)
    for i, (g, fig, n, legend_df) in enumerate(eth_items[:4]):
        with cols[i]:
            with card():
                st.caption(f"{g} (n = {n:,})")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                render_ethnicity_legend(legend_df)
                if show_eth_details:
                    df_eth = get_country_ethnicity(qid)
                    tbl = make_gender_ethnicity_table(df_eth, g, top_n=10)
                    if not tbl.empty:
                        st.dataframe(tbl, use_container_width=True, hide_index=True)


@_fragment
def section_occupations(qid: str, country_label: str):
    st.markdown('<div class="section-title">Occupations</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="desc">This section shows occupations attached to biographies linked to this '
        'country, grouped into major ISCO categories for readability. Unlike the other panels, this '
        'data was prepared from a Wikidata dump rather than a live SPARQL query, then mapped into ISCO '
        'major groups in the processing pipeline. The treemap shows how occupations are represented in '
        'the biography dataset, not the country real-world labour-force distribution.</div>',
        unsafe_allow_html=True,
    )

    gender_filter = st.selectbox(
        "Gender (occupations)",
        options=["All","Male","Female","Non-binary or other","Unknown / not stated"],
        index=0,
        key="dash_occ_gender",
    )

    # Cached per (qid, gender_filter): the treemap is the most expensive chart
    # to build.  Switching the dropdown is instant after the first selection.
    fig_occ = cached_occupation_figure(qid, gender_filter, country_label)
    fig_occ.update_layout(height=650, margin=dict(l=10,r=10,t=55,b=10), title_text="")

    with card():
        st.plotly_chart(fig_occ, use_container_width=True, config={"displayModeBar": False})

    if st.checkbox("Show occupation details", value=False, key="dash_show_occ_details"):
        df_occ = get_country_occupations(qid)
        occ_tbl = make_occupation_details_table(df_occ, gender_filter=gender_filter)
        st.dataframe(occ_tbl, use_container_width=True, hide_index=True)


# =========================================================================
# Page bootstrap
# =========================================================================
inject_global_css()

cd = st.session_state.get("selected_country")
if cd is None:
    st.info("Go back to the globe, click a country, then press Explore / View details.")
    st.stop()

st.markdown(
    """
    <style>
    div[data-testid="stButton"] > button[kind="secondary"] {
        margin-left: 2.2rem; margin-top: 2.2rem; width: 170px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if st.button("← Back to globe"):
    st.switch_page("pages/0_Globe_Overview.py")

st.markdown("# Country dashboard")

qid           = str(cd[0])
country_label = str(cd[1])
iso3          = str(cd[2])

st.markdown(f"## {country_label} ({iso3})")

# Load KPIs — all cached, instantaneous after first visit
kpis = get_country_kpis(qid)

male           = kpis["male"]
female         = kpis["female"]
nb             = kpis["nb"]
unk            = kpis["unk"]
total_all      = kpis["total_all"]
mf_total       = kpis["mf_total"]
male_pct       = kpis["male_pct"]
female_pct     = kpis["female_pct"]
gap_pp_total   = kpis["gap_pp_total"]
category_total = kpis["category_total"]
lang_kpis      = kpis["lang_kpis_spoken"]
top_lang_label = kpis["top_lang_label"]
top_lang_share = kpis["top_lang_share"]
top_eth        = kpis["top_eth"]

# -------------------------
# KPI cards — pure HTML, renders instantly
# -------------------------
k1, k2, k3, k4 = st.columns(4)
with k1: kpi_card("Total biographies",   f"{total_all:,}")
with k2: kpi_card("Male",   f"{male:,}",   f"{male_pct:.1f}% of Male+Female"   if mf_total>0 else "—")
with k3: kpi_card("Female", f"{female:,}", f"{female_pct:.1f}% of Male+Female" if mf_total>0 else "—")
with k4: kpi_card("Gender gap", f"{gap_pp_total:.1f} pp", category_total)

k5, k6, k7, k8 = st.columns(4)
with k5: kpi_card("Non-binary / other",   f"{nb:,}")
with k6: kpi_card("Unknown / not stated", f"{unk:,}")
with k7: kpi_card(
    "Top language (spoken)", f"{top_lang_label}",
    f"{top_lang_share:.1f}% share (spoken)" if lang_kpis.get("total",0)>0 else "—",
)
with k8: kpi_card("Top ethnic group", f"{top_eth}")

st.markdown("<hr/>", unsafe_allow_html=True)

# -------------------------
# Independent fragments — widget interactions inside each section
# only re-run that section; all cached figures return instantly.
# -------------------------
section_decades(
    qid,
    color_male="#2B6CB0", color_female="#D53F8C",
    color_nb="#805AD5",   color_gap="#6B7280",
)

st.markdown("<hr/>", unsafe_allow_html=True)

section_gender_languages_age(
    qid, country_label,
    male, female, nb, mf_total, gap_pp_total, category_total,
)

st.markdown("<hr/>", unsafe_allow_html=True)

section_ethnicity(qid, country_label)

st.markdown("<hr/>", unsafe_allow_html=True)

section_occupations(qid, country_label)