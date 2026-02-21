import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path
import json

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parents[1]  # dashboard/
DATA_DIR = BASE_DIR.parent / "data"

GENDER_PATH = DATA_DIR / "gender_country_1900_present_per_country.csv"
ISO_PATH = DATA_DIR / "allowed_countries_iso3.csv"

# GeoJSON for better polygons (Bahrain will render)
GEOJSON_PATH = DATA_DIR / "ne_50m_admin_0_countries.geojson"

# Precomputed centroids for fast + correct rotation
CENTROIDS_PATH = DATA_DIR / "country_centroids_iso3.csv"

# ---------- Colors ----------
COLOR_MALE = "#2B6CB0"
COLOR_FEMALE = "#D53F8C"
COLOR_BALANCED = "#805AD5"
COLOR_NODATA = "#D1D5DB"

# Highlight outline for searched country
COLOR_HIGHLIGHT = "#DC2626" 

BALANCE_GAP = 0.10  # 10 percentage points

st.set_page_config(layout="wide")


@st.cache_data
def load_geojson() -> dict:
    if not GEOJSON_PATH.exists():
        raise FileNotFoundError(
            f"Missing geojson file:\n{GEOJSON_PATH}\n\n"
            "Place this file in /data:\n"
            "  ne_50m_admin_0_countries.geojson"
        )
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@st.cache_data
def load_centroids() -> dict:
    """
    Returns dict: iso3 -> (lon, lat)
    """
    if not CENTROIDS_PATH.exists():
        return {}

    c = pd.read_csv(CENTROIDS_PATH)
    if not {"iso3", "lat", "lon"}.issubset(set(c.columns)):
        return {}

    c["iso3"] = c["iso3"].astype(str).str.strip().str.upper()
    c["lat"] = pd.to_numeric(c["lat"], errors="coerce")
    c["lon"] = pd.to_numeric(c["lon"], errors="coerce")
    c = c.dropna(subset=["iso3", "lat", "lon"]).copy()

    return {r["iso3"]: (float(r["lon"]), float(r["lat"])) for _, r in c.iterrows()}


@st.cache_data
def build_df(balance_gap: float) -> tuple[pd.DataFrame, dict]:
    gender = pd.read_csv(GENDER_PATH)
    iso = pd.read_csv(ISO_PATH)

    # Keys
    gender["qid"] = gender["country"].astype(str).str.rsplit("/", n=1).str[-1].astype(str).str.strip()

    if "qid" in iso.columns:
        iso["qid"] = iso["qid"].astype(str).str.strip()
    else:
        iso["qid"] = iso["country"].astype(str).str.rsplit("/", n=1).str[-1].astype(str).str.strip()

    if "iso3" not in iso.columns:
        raise RuntimeError(
            f"'iso3' column missing in {ISO_PATH.name}. Columns: {list(iso.columns)}"
        )
    iso["iso3"] = iso["iso3"].astype(str).str.strip().str.upper()

    # Normalize categories
    gender["genderCategory"] = (
        gender["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    )

    piv = (
        gender.pivot_table(
            index=["qid", "countryLabel"],
            columns="genderCategory",
            values="count",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )

    for col in ["Male", "Female", "Unknown / not stated", "Non-binary or other"]:
        if col not in piv.columns:
            piv[col] = 0

    merged = iso.merge(piv, on="qid", how="left", suffixes=("_iso", "_gender"))

    # Fix label name after merge if needed
    if "countryLabel" not in merged.columns:
        for cand in ["countryLabel_iso", "countryLabel_gender", "countryLabel_x", "countryLabel_y"]:
            if cand in merged.columns:
                merged = merged.rename(columns={cand: "countryLabel"})
                break

    # Counts
    for col in ["Male", "Female", "Unknown / not stated", "Non-binary or other"]:
        merged[col] = merged[col].fillna(0).astype(int)

    merged["total"] = merged["Male"] + merged["Female"] + merged["Unknown / not stated"] + merged["Non-binary or other"]

    # Male vs Female only
    merged["mf_total"] = merged["Male"] + merged["Female"]
    merged["male_pct"] = np.where(merged["mf_total"] > 0, merged["Male"] / merged["mf_total"], np.nan)
    merged["female_pct"] = np.where(merged["mf_total"] > 0, merged["Female"] / merged["mf_total"], np.nan)
    merged["gap"] = np.abs(merged["male_pct"] - merged["female_pct"])

    merged["category"] = "No data"
    has_mf = merged["mf_total"] > 0
    merged.loc[has_mf & (merged["gap"] <= balance_gap), "category"] = "Balanced"
    merged.loc[has_mf & (merged["gap"] > balance_gap) & (merged["Male"] > merged["Female"]), "category"] = "More male"
    merged.loc[has_mf & (merged["gap"] > balance_gap) & (merged["Female"] > merged["Male"]), "category"] = "More female"

    # Keep only valid ISO3 codes
    merged = merged[merged["iso3"].notna() & (merged["iso3"].astype(str).str.len() == 3)].copy()
    merged["iso3"] = merged["iso3"].astype(str).str.strip().str.upper()

    # Deduplicate ISO3 if needed
    merged = merged.sort_values(["iso3", "total"], ascending=[True, False]).drop_duplicates(subset=["iso3"], keep="first")

    # Audit info
    iso_set = set(iso["iso3"].astype(str).str.strip().str.upper().tolist())
    merged_set = set(merged["iso3"].astype(str).str.strip().str.upper().tolist())
    audit = {
        "iso_rows": int(len(iso)),
        "merged_rows": int(len(merged)),
        "missing_from_plot_df": sorted(list(iso_set - merged_set)),
        "no_data_iso3": sorted(merged.loc[merged["category"] == "No data", "iso3"].astype(str).tolist()),
    }
    return merged, audit


def make_globe_figure(df: pd.DataFrame, geo: dict, focus_iso3: str | None) -> go.Figure:
    cat_order = ["More male", "Balanced", "More female", "No data"]
    cat_to_z = {c: i for i, c in enumerate(cat_order)}

    dff = df.copy()
    dff["z"] = dff["category"].map(cat_to_z).fillna(cat_to_z["No data"]).astype(int)

    colorscale = [
        [0.00, COLOR_MALE],
        [0.33, COLOR_BALANCED],
        [0.66, COLOR_FEMALE],
        [1.00, COLOR_NODATA],
    ]

    fig = go.Figure()

    # Base choropleth
    fig.add_trace(
        go.Choropleth(
            geojson=geo,
            featureidkey="properties.ISO_A3",
            locations=dff["iso3"],
            z=dff["z"],
            zmin=0,
            zmax=3,
            colorscale=colorscale,
            showscale=False,
            marker_line_color="white" ,
            marker_line_width=2.0,
            customdata=np.stack(
                [dff["qid"].to_numpy(), dff["countryLabel"].to_numpy(), dff["iso3"].to_numpy(), dff["category"].to_numpy()],
                axis=-1
            ),
            hovertext=dff["countryLabel"],
            hovertemplate="%{hovertext}<extra></extra>",
        )
    )

    # Optional highlight outline for focus country
    if focus_iso3:
        iso3 = str(focus_iso3).strip().upper()
        df_focus = dff[dff["iso3"] == iso3].copy()
        if not df_focus.empty:
            fig.add_trace(
                go.Choropleth(
                    geojson=geo,
                    featureidkey="properties.ISO_A3",
                    locations=df_focus["iso3"],
                    z=[0],
                    colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
                    showscale=False,
                    marker_line_color=COLOR_HIGHLIGHT,
                    marker_line_width=2.6,
                    hoverinfo="skip",
                )
            )

    fig.update_geos(
        projection_type="orthographic",
        showocean=True,
        oceancolor="#4B9CD3",
        showland=True,
        landcolor="#FAFAFA",
        showcountries=True,
        countrycolor="white",
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=720,
        showlegend=False,
    )

    return fig


# ---------- Load data ----------
geo = load_geojson()
centroids = load_centroids()
df, audit = build_df(BALANCE_GAP)

# ---------- Session state ----------
if "selected_country" not in st.session_state:
    st.session_state.selected_country = None

if "globe_focus_iso3" not in st.session_state:
    st.session_state.globe_focus_iso3 = None

if "globe_rotation" not in st.session_state:
    st.session_state.globe_rotation = {"lon": 0.0, "lat": 0.0}

# ---------- Layout ----------
left, right = st.columns([3, 1], vertical_alignment="top")

# ---------- Right panel: Search UI ----------
with right:
    st.markdown(
        """
        <style>
          .panel-wrap{ padding-top: 6px; }
          .panel-title{ font-size: 22px; font-weight: 800; margin: 0 0 10px 0; }
          .panel-card{
            background: #ffffff;
            border: 1px solid #E6E6E6;
            border-radius: 14px;
            padding: 14px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
          }
          .country-name{ font-size: 18px; font-weight: 800; margin: 0; line-height: 1.2; }
          .helper{ font-size: 13px; color: #6B7280; margin: 0; }
          .subtle{ font-size: 12px; color: #6B7280; margin-top: 6px; }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown('<div class="panel-wrap">', unsafe_allow_html=True)
    st.markdown('<div class="panel-title">Country details</div>', unsafe_allow_html=True)

    # Search options
    search_df = df[["countryLabel", "iso3"]].dropna().copy()
    search_df["countryLabel"] = search_df["countryLabel"].astype(str)
    search_df["iso3"] = search_df["iso3"].astype(str).str.upper()
    search_df = search_df.sort_values("countryLabel").reset_index(drop=True)
    search_df["option"] = search_df["countryLabel"] + " (" + search_df["iso3"] + ")"
    options = search_df["option"].tolist()

    st.markdown("**Search country**")

    # ✅ Form = no lag while typing (only reruns when you press Go/Clear)
    with st.form("country_search_form", clear_on_submit=False):
        chosen = st.selectbox(
            "Type to search",
            options=options,
            index=0,
            key="country_search_select",
            label_visibility="collapsed",
        )

        b1, b2 = st.columns([1, 1])
        with b1:
            go_btn = st.form_submit_button("Go to country", use_container_width=True)
        with b2:
            clear_btn = st.form_submit_button("Clear", use_container_width=True)

    if clear_btn:
        st.session_state.globe_focus_iso3 = None
        st.session_state.globe_rotation = {"lon": 0.0, "lat": 0.0}
        st.rerun()

    if go_btn and chosen:
        iso3 = chosen.split("(")[-1].replace(")", "").strip().upper()
        st.session_state.globe_focus_iso3 = iso3

        # ✅ Fast, accurate rotation using precomputed centroids
        if iso3 in centroids:
            lon, lat = centroids[iso3]
            st.session_state.globe_rotation = {"lon": float(lon), "lat": float(lat)}
        else:
            # If centroids file missing or iso not found, don't rotate (still highlight)
            st.session_state.globe_rotation = {"lon": 0.0, "lat": 0.0}

        st.rerun()

    st.markdown(
        '<div class="subtle">Tip: after rotating, click the country on the globe to load details.</div>',
        unsafe_allow_html=True
    )
    st.markdown("---")

# ---------- Build figure ----------
focus_iso3 = st.session_state.globe_focus_iso3
fig = make_globe_figure(df, geo, focus_iso3)

rot = st.session_state.globe_rotation or {"lon": 0.0, "lat": 0.0}

fig.update_layout(
    geo=dict(
        projection=dict(
            type="orthographic",
            rotation=dict(
                lon=float(rot["lon"]),
                lat=float(rot["lat"]),
                roll=0,
            )
        )
    )
)


# ---------- Left (globe) ----------
with left:
    event = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        selection_mode="points",
    )

    with st.expander("Audit: globe coverage (debug)", expanded=False):
        st.write(f"ISO rows: {audit['iso_rows']:,}")
        st.write(f"Rows plotted (unique ISO3): {audit['merged_rows']:,}")
        missing = audit["missing_from_plot_df"]
        if missing:
            st.warning(f"ISO3 in allowed list but missing from plot df ({len(missing)}):")
            st.code(", ".join(missing))
        else:
            st.success("No missing ISO3 codes: every allowed ISO3 appears in the plotted dataframe.")

# ---------- Click selection handling ----------
try:
    pts = event.selection.get("points", []) if event and hasattr(event, "selection") else []
except Exception:
    pts = []

if pts:
    cd = pts[0].get("customdata")
    if cd is not None:
        st.session_state.selected_country = cd

# ---------- Right panel: selected country card + routing ----------
with right:
    cd = st.session_state.selected_country

    if cd is None:
        st.markdown(
            """
            <div class="panel-card">
              <p class="helper">Click a country on the globe to see its details here.</p>
            </div>
            """,
            unsafe_allow_html=True
        )
        st.markdown("</div>", unsafe_allow_html=True)
        st.stop()

    qid, label, iso3, category = cd
    qid = str(qid).strip()
    label = str(label)
    iso3 = str(iso3).strip().upper()

    st.markdown(
        f"""
        <div class="panel-card">
          <p class="country-name">{label} <span style="color:#6B7280;font-weight:700;">({iso3})</span></p>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("")

    view_map = {
        "Gender breakdown": "Gender breakdown (country total)",
        "Gender over decades": "Gender over decades",
        "Languages spoken": "Languages",
        "Ethnic group counts": "Ethnic groups",
        "Age representation": "Age Representation",
        "Occupations": "Occupations",
    }

    if "country_view" not in st.session_state:
        st.session_state.country_view = view_map["Gender breakdown"]

    inv = {v: k for k, v in view_map.items()}
    default_label = inv.get(st.session_state.country_view, "Gender breakdown")

    view_label = st.selectbox(
        "View country details",
        options=list(view_map.keys()),
        index=list(view_map.keys()).index(default_label),
        key="globe_country_view_select",
    )

    if st.button("View details", use_container_width=True):
        st.session_state.country_view = view_map[view_label]
        st.session_state.selected_country = [qid, label, iso3, str(category)]
        st.switch_page("pages/1_country_profile.py")

    st.markdown("</div>", unsafe_allow_html=True)




