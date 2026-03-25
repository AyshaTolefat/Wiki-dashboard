import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------- Paths ----------
BASE_DIR       = Path(__file__).resolve().parents[1]
DATA_DIR       = BASE_DIR.parent / "data"

GENDER_PATH    = DATA_DIR / "gender_country_1900_present_per_country.csv"
ISO_PATH       = DATA_DIR / "allowed_countries_iso3.csv"
GEOJSON_PATH   = DATA_DIR / "ne_50m_admin_0_countries_patched.geojson"
CENTROIDS_PATH = DATA_DIR / "country_centroids_iso3.csv"

# ---------- Colors ----------
COLOR_MALE_LIGHT   = "#93C5FD"
COLOR_MALE_MED     = "#2563EB"
COLOR_MALE_DARK    = "#082F49"
COLOR_FEMALE_LIGHT = "#F9A8D4"
COLOR_FEMALE_MED   = "#EC4899"
COLOR_FEMALE_DARK  = "#9D174D"
COLOR_NB_LIGHT     = "#C4B5FD"
COLOR_NB_MED       = "#7C3AED"
COLOR_NB_DARK      = "#4C1D95"
COLOR_BALANCED     = "#FDE68A"
COLOR_NODATA       = "#D1D5DB"
COLOR_HIGHLIGHT    = "#DC2626"

BALANCE_GAP = 0.10

CATEGORY_LABELS = {
    "More male":       "Male-dominated",
    "More female":     "Female-dominated",
    "More non-binary": "Non-binary-dominated",
    "Balanced":        "Balanced representation",
    "No data":         "No data",
}

st.set_page_config(layout="wide")

st.markdown(
    f"""
    <style>
      .legend-wrap {{
        display: flex; flex-wrap: wrap; gap: 18px;
        align-items: center; margin-bottom: 10px; padding: 8px 0 2px 0;
      }}
      .legend-item {{
        display: flex; align-items: center; gap: 8px;
        font-size: 14px; color: #111827; font-weight: 600;
      }}
      .legend-swatch {{
        width: 18px; height: 18px; border-radius: 4px;
        border: 1px solid rgba(0,0,0,0.2); display: inline-block;
      }}
      .panel-wrap  {{ padding-top: 6px; }}
      .panel-title {{ font-size: 22px; font-weight: 800; margin: 0 0 10px 0; color: #1F2937; }}
      .panel-subtle {{ font-size: 14px; color: #4B5563; line-height: 1.6; margin: 0 0 16px 0; }}
      .section-title {{ font-size: 15px; font-weight: 800; color: #1F2937; margin: 0 0 8px 0; }}
      .country-card {{
        background: #ffffff; border: 1px solid #E5E7EB;
        border-radius: 16px; padding: 16px;
        box-shadow: 0 1px 2px rgba(0,0,0,0.03);
      }}
      .country-name  {{ font-size: 20px; font-weight: 800; margin: 0 0 4px 0; line-height: 1.2; color: #111827; }}
      .country-meta  {{ font-size: 14px; color: #6B7280; margin: 0; }}
      .country-status {{ font-size: 14px; color: #374151; margin: 8px 0 0 0; line-height: 1.5; }}
      .empty-card {{
        background: #ffffff; border: 1px dashed #D1D5DB;
        border-radius: 16px; padding: 16px; color: #6B7280;
        font-size: 14px; line-height: 1.6;
      }}
      .divider {{ height: 1px; background: #E5E7EB; margin: 18px 0; }}
      .stRadio > div {{ gap: 0.5rem; }}
    </style>
    <div class="legend-wrap">
      <div class="legend-item"><span class="legend-swatch" style="background:{COLOR_MALE_DARK};"></span> Strongly male-dominated</div>
      <div class="legend-item"><span class="legend-swatch" style="background:{COLOR_MALE_LIGHT};"></span> Slightly male-dominated</div>
      <div class="legend-item"><span class="legend-swatch" style="background:{COLOR_FEMALE_DARK};"></span> Female-dominated</div>
      <div class="legend-item"><span class="legend-swatch" style="background:{COLOR_NB_DARK};"></span> Non-binary-dominated</div>
      <div class="legend-item"><span class="legend-swatch" style="background:{COLOR_BALANCED};"></span> Balanced</div>
      <div class="legend-item"><span class="legend-swatch" style="background:{COLOR_NODATA};"></span> No data</div>
    </div>
    """,
    unsafe_allow_html=True,
)


# -----------------------------------------------------------------------
# Color helpers
# -----------------------------------------------------------------------

def _hex_to_rgb(h: str):
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _lerp_hex(c1: str, c2: str, t: float) -> str:
    t = max(0.0, min(1.0, t))
    r1, g1, b1 = _hex_to_rgb(c1)
    r2, g2, b2 = _hex_to_rgb(c2)
    return f"#{int(r1+(r2-r1)*t):02x}{int(g1+(g2-g1)*t):02x}{int(b1+(b2-b1)*t):02x}"


def _country_color(category: str, shade: float) -> str:
    if category == "Balanced":
        return COLOR_BALANCED
    if category == "More male":
        return (_lerp_hex(COLOR_MALE_LIGHT,   COLOR_MALE_MED,    shade / 0.35)
                if shade <= 0.35 else
                _lerp_hex(COLOR_MALE_MED,     COLOR_MALE_DARK,   (shade-0.35)/0.65))
    if category == "More female":
        return (_lerp_hex(COLOR_FEMALE_LIGHT, COLOR_FEMALE_MED,  shade / 0.35)
                if shade <= 0.35 else
                _lerp_hex(COLOR_FEMALE_MED,   COLOR_FEMALE_DARK, (shade-0.35)/0.65))
    if category == "More non-binary":
        return (_lerp_hex(COLOR_NB_LIGHT,     COLOR_NB_MED,      shade / 0.35)
                if shade <= 0.35 else
                _lerp_hex(COLOR_NB_MED,       COLOR_NB_DARK,     (shade-0.35)/0.65))
    return COLOR_NODATA


# -----------------------------------------------------------------------
# Data loaders  (cache_resource = zero-copy object reference, no pickle)
# -----------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def load_geojson() -> dict:
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(f"Missing geojson:\n{GEOJSON_PATH}")
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        geo = json.load(f)
    for feat in geo.get("features", []):
        props = feat.get("properties", {}) or {}
        name = (props.get("NAME") or props.get("ADMIN") or props.get("NAME_EN")
                or props.get("SOVEREIGNT") or props.get("GEONUNIT") or "")
        name = str(name).strip()
        if name == "Kosovo":                          props["ISO3_FIX"] = "XKX"
        elif name in {"Northern Cyprus","North Cyprus"}: props["ISO3_FIX"] = "CYN"
        elif name == "Somaliland":                    props["ISO3_FIX"] = "SOL"
    return geo


@st.cache_resource(show_spinner=False)
def load_centroids() -> dict:
    if not CENTROIDS_PATH.exists():
        return {}
    c = pd.read_csv(CENTROIDS_PATH)
    if not {"iso3","lat","lon"}.issubset(set(c.columns)):
        return {}
    c["iso3"] = c["iso3"].astype(str).str.strip().str.upper()
    c["lat"]  = pd.to_numeric(c["lat"], errors="coerce")
    c["lon"]  = pd.to_numeric(c["lon"], errors="coerce")
    c = c.dropna(subset=["iso3","lat","lon"]).copy()
    return {r["iso3"]: (float(r["lon"]), float(r["lat"])) for _, r in c.iterrows()}


@st.cache_data(show_spinner=False)
def build_df(balance_gap: float) -> pd.DataFrame:
    gender = pd.read_csv(GENDER_PATH)
    iso    = pd.read_csv(ISO_PATH)

    gender["qid"] = gender["country"].astype(str).str.rsplit("/", n=1).str[-1].astype(str).str.strip()
    if "qid" in iso.columns:
        iso["qid"] = iso["qid"].astype(str).str.strip()
    else:
        iso["qid"] = iso["country"].astype(str).str.rsplit("/", n=1).str[-1].astype(str).str.strip()

    if "iso3" not in iso.columns:
        raise RuntimeError(f"'iso3' missing in {ISO_PATH.name}")
    iso["iso3"] = iso["iso3"].astype(str).str.strip().str.upper()

    gender["genderCategory"] = gender["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    piv = (
        gender.pivot_table(index=["qid","countryLabel"], columns="genderCategory",
                           values="count", aggfunc="sum", fill_value=0)
        .reset_index()
    )
    for col in ["Male","Female","Unknown / not stated","Non-binary or other"]:
        if col not in piv.columns:
            piv[col] = 0

    merged = iso.merge(piv, on="qid", how="left", suffixes=("_iso","_gender"))
    if "countryLabel" not in merged.columns:
        for cand in ["countryLabel_iso","countryLabel_gender","countryLabel_x","countryLabel_y"]:
            if cand in merged.columns:
                merged = merged.rename(columns={cand: "countryLabel"}); break
    if "countryLabel" not in merged.columns:
        merged["countryLabel"] = merged.get("qid","").astype(str)

    for col in ["Male","Female","Unknown / not stated","Non-binary or other"]:
        merged[col] = merged[col].fillna(0).astype(int)

    merged["total"]              = merged["Male"]+merged["Female"]+merged["Unknown / not stated"]+merged["Non-binary or other"]
    merged["known_gender_total"] = merged["Male"]+merged["Female"]+merged["Non-binary or other"]

    kgt = merged["known_gender_total"]
    merged["male_pct"]   = np.where(kgt>0, merged["Male"]                  /kgt, np.nan)
    merged["female_pct"] = np.where(kgt>0, merged["Female"]                /kgt, np.nan)
    merged["nb_pct"]     = np.where(kgt>0, merged["Non-binary or other"]   /kgt, np.nan)

    shares        = merged[["male_pct","female_pct","nb_pct"]].fillna(0).to_numpy()
    sorted_shares = np.sort(shares, axis=1)
    merged["imbalance"] = sorted_shares[:,2] - sorted_shares[:,1]
    merged["category"]  = "No data"

    has = merged["known_gender_total"] > 0
    merged.loc[has & (merged["imbalance"] <= balance_gap), "category"] = "Balanced"
    merged.loc[has & (merged["imbalance"]>balance_gap) & (merged["Male"]>=merged["Female"]) & (merged["Male"]>=merged["Non-binary or other"]), "category"] = "More male"
    merged.loc[has & (merged["imbalance"]>balance_gap) & (merged["Female"]>=merged["Male"]) & (merged["Female"]>=merged["Non-binary or other"]), "category"] = "More female"
    merged.loc[has & (merged["imbalance"]>balance_gap) & (merged["Non-binary or other"]>=merged["Male"]) & (merged["Non-binary or other"]>=merged["Female"]), "category"] = "More non-binary"

    merged["shade_strength"] = np.where(
        merged["category"].isin(["More male","More female","More non-binary"]),
        np.clip((merged["imbalance"]-balance_gap)/(1-balance_gap), 0, 1), 0,
    )
    merged["category_label"] = merged["category"].map(CATEGORY_LABELS).fillna("No data")
    merged = merged[merged["iso3"].notna() & (merged["iso3"].astype(str).str.len()==3)].copy()
    merged["iso3"] = merged["iso3"].astype(str).str.strip().str.upper()
    merged = merged.sort_values(["iso3","total"], ascending=[True,False]).drop_duplicates(subset=["iso3"], keep="first")
    return merged


@st.cache_data(show_spinner=False)
def build_search_df(df: pd.DataFrame) -> pd.DataFrame:
    s = df[["countryLabel","iso3"]].dropna().copy()
    s["countryLabel"] = s["countryLabel"].astype(str)
    s["iso3"]         = s["iso3"].astype(str).str.upper()
    s = s.sort_values("countryLabel").reset_index(drop=True)
    s["option"] = s["countryLabel"] + " (" + s["iso3"] + ")"
    return s


# -----------------------------------------------------------------------
# Globe figure — three-tier caching strategy
#
# Tier 1  build_trace_params()     — cached: lightweight per-country data, no GeoJSON
# Tier 2  get_base_figure_dict()   — cached: full figure dict with GeoJSON embedded,
#                                    built ONCE, stored as plain Python dict.
#                                    On every rerun we only shallow-patch rotation
#                                    and the optional highlight trace — no Plotly
#                                    figure object construction, no GeoJSON re-serialisation.
# Tier 3  uirevision="globe"       — browser side: tells Plotly.js to DIFF-UPDATE
#                                    rather than tear down and fully re-project all
#                                    200+ country polygons on every rerun.
#                                    This is the single biggest remaining win.
# -----------------------------------------------------------------------

@st.cache_data(show_spinner=False)
def build_trace_params(df: pd.DataFrame) -> dict:
    """Lightweight serialisable data — no GeoJSON."""
    dff = df.reset_index(drop=True).copy()
    n   = len(dff)
    colors = [_country_color(str(r["category"]), float(r["shade_strength"])) for _, r in dff.iterrows()]
    if n == 0:
        colorscale = [[0.0, COLOR_NODATA], [1.0, COLOR_NODATA]]
    elif n == 1:
        colorscale = [[0.0, colors[0]], [1.0, colors[0]]]
    else:
        colorscale = [[i/(n-1), c] for i, c in enumerate(colors)]
    return {
        "locations":  dff["iso3"].tolist(),
        "z":          list(range(n)),
        "zmin":       0,
        "zmax":       max(n-1, 1),
        "colorscale": colorscale,
        "customdata": np.stack([
            dff["qid"].to_numpy(), dff["countryLabel"].to_numpy(),
            dff["iso3"].to_numpy(), dff["category"].to_numpy(),
            dff["category_label"].to_numpy(),
        ], axis=-1).tolist(),
        "hovertext": dff["countryLabel"].tolist(),
    }


@st.cache_resource(show_spinner=False)
def get_base_figure_dict(_geo: dict, _tp: dict) -> dict:
    """
    Build the full Plotly figure (with GeoJSON embedded) exactly ONCE per server
    process and cache it as a plain Python dict.

    cache_resource  → same dict object returned on every hit, zero copy, zero pickle.
    Underscore args → Streamlit skips hashing these (they are large objects; the
                      cache is effectively keyed on 'always one entry').

    The returned dict is treated as READ-ONLY.  Rotation and highlight are patched
    cheaply on each rerun by building new shallow wrapper dicts (see patch_figure).
    """
    fig = go.Figure()
    fig.add_trace(go.Choropleth(
        geojson=_geo,
        featureidkey="properties.ISO3_FIX",
        locations=_tp["locations"],
        z=_tp["z"],
        zmin=_tp["zmin"],
        zmax=_tp["zmax"],
        colorscale=_tp["colorscale"],
        showscale=False,
        marker_line_color="#111827",
        marker_line_width=1.0,
        customdata=_tp["customdata"],
        hovertext=_tp["hovertext"],
        hovertemplate="%{hovertext}<extra></extra>",
    ))
    fig.update_geos(
        projection_type="orthographic",
        projection_rotation=dict(lon=0.0, lat=0.0, roll=0),
        showocean=True, oceancolor="#4B9CD3",
        showland=True,  landcolor="#FAFAFA",
        showcountries=True, countrycolor="white",
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=720,
        showlegend=False,
        # KEY: keeps Plotly.js from tearing down and fully re-projecting
        # all country polygons on every rerun.  With this set to a constant,
        # the browser receives diffs instead of a full redraw instruction.
        uirevision="globe",
    )
    return json.loads(fig.to_json())


def patch_figure(
    base_dict: dict,
    geo: dict,
    df: pd.DataFrame,
    focus_iso3: str | None,
    rotation: dict,
) -> go.Figure:
    """
    Assemble the final per-rerun figure in ~1 ms by shallow-patching
    the cached base dict rather than rebuilding from scratch.

    Only two things ever change between reruns:
      1. projection rotation  → patch layout.geo.projection.rotation
      2. highlight trace      → append one extra Choropleth trace

    The large data (GeoJSON + colorscale + country lists) is never copied —
    the inner lists/dicts from base_dict are reused by reference.
    """
    rot = rotation or {"lon": 0.0, "lat": 0.0}

    # Shallow-patch layout: create new dicts only for the path we need to change.
    # Nothing inside base_dict["layout"]["geo"]["projection"]["rotation"] is large,
    # so this is effectively free.
    layout = base_dict["layout"]
    geo_layout = layout["geo"]
    patched_layout = {
        **layout,
        "geo": {
            **geo_layout,
            "projection": {
                **geo_layout["projection"],
                "rotation": {
                    "lon": float(rot.get("lon", 0.0)),
                    "lat": float(rot.get("lat", 0.0)),
                    "roll": 0,
                },
            },
        },
    }

    # Data list: usually just the one base trace; append highlight if needed.
    data = base_dict["data"]   # read-only reference — no copy

    if focus_iso3:
        iso3       = str(focus_iso3).strip().upper()
        focus_rows = df[df["iso3"] == iso3]
        if not focus_rows.empty:
            # Build highlight trace dict directly — avoids go.Choropleth overhead
            highlight = {
                "type": "choropleth",
                "geojson": geo,                         # pass by reference
                "featureidkey": "properties.ISO3_FIX",
                "locations": focus_rows["iso3"].tolist(),
                "z": [0],
                "zmin": 0, "zmax": 1,
                "colorscale": [[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
                "showscale": False,
                "marker": {"line": {"color": COLOR_HIGHLIGHT, "width": 2.8}},
                "hoverinfo": "skip",
            }
            data = list(data) + [highlight]   # new list, inner items still by reference

    return go.Figure({"data": data, "layout": patched_layout})


# -----------------------------------------------------------------------
# UI helpers
# -----------------------------------------------------------------------

def _set_focus_from_option(chosen_option: str, centroids: dict):
    if not chosen_option:
        return
    iso3 = chosen_option.split("(")[-1].replace(")", "").strip().upper()
    st.session_state.globe_focus_iso3 = iso3
    if iso3 in centroids:
        lon, lat = centroids[iso3]
        st.session_state.globe_rotation = {"lon": float(lon), "lat": float(lat)}
    else:
        st.session_state.globe_rotation = {"lon": 0.0, "lat": 0.0}


def _clear_focus():
    st.session_state.globe_focus_iso3 = None
    st.session_state.globe_rotation   = {"lon": 0.0, "lat": 0.0}


# -----------------------------------------------------------------------
# Load data
# -----------------------------------------------------------------------
geo       = load_geojson()
centroids = load_centroids()
df        = build_df(BALANCE_GAP)
search_df = build_search_df(df)
options   = search_df["option"].tolist()

# -----------------------------------------------------------------------
# Session state
# -----------------------------------------------------------------------
if "selected_country"  not in st.session_state: st.session_state.selected_country  = None
if "globe_focus_iso3"  not in st.session_state: st.session_state.globe_focus_iso3  = None
if "globe_rotation"    not in st.session_state: st.session_state.globe_rotation    = {"lon": 0.0, "lat": 0.0}

# -----------------------------------------------------------------------
# Layout
# -----------------------------------------------------------------------
left, right = st.columns([3, 1], vertical_alignment="top")

# ---------- Right panel ----------
with right:
    st.markdown('<div class="panel-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Country details</div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="panel-subtle">Use the search box to move the globe to a country, '
        'then click that country on the map to open its summary here.</p>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="section-title">Find a country</div>', unsafe_allow_html=True)

    default_index = 0
    current_focus = st.session_state.get("globe_focus_iso3")
    if current_focus:
        matches = search_df.index[search_df["iso3"] == current_focus].tolist()
        if matches:
            default_index = matches[0]

    with st.form("country_search_form", clear_on_submit=False):
        chosen = st.selectbox(
            "Type to search", options=options, index=default_index,
            key="country_search_select", label_visibility="collapsed",
        )
        b1, b2 = st.columns([1, 1])
        with b1: go_btn    = st.form_submit_button("Go to country",  use_container_width=True)
        with b2: clear_btn = st.form_submit_button("Clear",           use_container_width=True)

    if clear_btn:          _clear_focus()
    if go_btn and chosen:  _set_focus_from_option(chosen, centroids)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

# -----------------------------------------------------------------------
# Build and render globe
# -----------------------------------------------------------------------
trace_params   = build_trace_params(df)
base_dict      = get_base_figure_dict(geo, trace_params)   # instant after first build
fig            = patch_figure(
    base_dict, geo, df,
    focus_iso3=st.session_state.globe_focus_iso3,
    rotation=st.session_state.globe_rotation,
)

with left:
    event = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        selection_mode="points",
        config={"displayModeBar": False},
        key="globe_chart",
    )

# ---------- Click handling ----------
try:
    pts = event.selection.get("points", []) if event and hasattr(event, "selection") else []
except Exception:
    pts = []

if pts:
    cd = pts[0].get("customdata")
    if cd is not None:
        st.session_state.selected_country = cd

# -----------------------------------------------------------------------
# Right panel: country card
# -----------------------------------------------------------------------
with right:
    cd = st.session_state.selected_country

    if cd is None:
        st.markdown(
            '<div class="empty-card">Select a country on the globe to view its '
            'summary and open its dashboard page.</div>',
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    qid, label, iso3, category, category_label = cd
    qid            = str(qid).strip()
    label          = str(label)
    iso3           = str(iso3).strip().upper()
    category_label = str(category_label)

    status_text = {
        "Male-dominated":          "This country's recorded entries are predominantly male.",
        "Female-dominated":        "This country's recorded entries are predominantly female.",
        "Non-binary-dominated":    "This country's recorded entries are predominantly non-binary or other.",
        "Balanced representation": "This country shows a relatively balanced gender distribution.",
        "No data":                 "There is not enough data available to classify this country.",
    }.get(category_label, category_label)

    st.markdown(
        f"""
        <div class="country-card">
          <p class="country-name">{label} <span style="color:#6B7280;font-weight:700;">({iso3})</span></p>
          <p class="country-meta">{category_label}</p>
          <p class="country-status">{status_text}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("")

    if st.button("View dashboard", use_container_width=True):
        st.session_state.selected_country = [qid, label, iso3, str(category), str(category_label)]
        st.switch_page("pages/1_country_dashboard_v2.py")

    st.markdown("</div>", unsafe_allow_html=True)