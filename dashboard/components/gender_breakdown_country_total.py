import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


COLOR_MALE = "#2B6CB0"
COLOR_FEMALE = "#D53F8C"
COLOR_NB = "#805AD5"


def get_gender_totals_for_country(df_total: pd.DataFrame, qid: str) -> dict:
    c = df_total[df_total["qid"] == qid].copy()
    if c.empty:
        return {"Male": 0, "Female": 0, "Non-binary or other": 0, "Unknown / not stated": 0}

    sums = c.groupby("genderCategory", as_index=False)["count"].sum()
    m = int(sums.loc[sums["genderCategory"] == "Male", "count"].sum())
    f = int(sums.loc[sums["genderCategory"] == "Female", "count"].sum())
    nb = int(sums.loc[sums["genderCategory"] == "Non-binary or other", "count"].sum())
    unk = int(sums.loc[sums["genderCategory"] == "Unknown / not stated", "count"].sum())
    return {"Male": m, "Female": f, "Non-binary or other": nb, "Unknown / not stated": unk}


def classify_category(male: int, female: int, gap_pp_threshold: float = 10.0) -> str:
    mf = male + female
    if mf <= 0:
        return "No data"
    gap_pp = abs((male / mf) - (female / mf)) * 100.0
    if gap_pp <= gap_pp_threshold:
        return "Balanced"
    return "More male" if male > female else "More female"


def donut_gender_breakdown(male: int, female: int, nb: int, min_visible_share: float = 0.008) -> go.Figure:
    labels = ["Male", "Female", "Non-binary or other"]
    real_vals = np.array([male, female, nb], dtype=float)
    real_total = float(real_vals.sum())

    if real_total <= 0:
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=[1, 1, 1],
                    hole=0.62,
                    sort=False,
                    textinfo="percent",
                    textposition="outside",
                    hoverinfo="skip",
                    marker=dict(
                        colors=[COLOR_MALE, COLOR_FEMALE, COLOR_NB],
                        line=dict(color="white", width=2),
                    ),
                )
            ]
        )
        fig.update_layout(
            template="simple_white",
            height=420,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
            showlegend=True,
        )
        return fig

    shares = real_vals / real_total

    vis_vals = real_vals.copy()
    if nb > 0 and shares[2] < min_visible_share:
        target = min_visible_share
        diff = target - shares[2]
        mf_sum = shares[0] + shares[1]
        if mf_sum > 0:
            take_m = diff * (shares[0] / mf_sum)
            take_f = diff * (shares[1] / mf_sum)
            vis_shares = shares.copy()
            vis_shares[2] = target
            vis_shares[0] = max(0, vis_shares[0] - take_m)
            vis_shares[1] = max(0, vis_shares[1] - take_f)
            vis_vals = vis_shares * real_total

    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=vis_vals,
                hole=0.62,
                sort=False,
                textinfo="percent",
                textposition="outside",
                hoverinfo="skip",
                marker=dict(
                    colors=[COLOR_MALE, COLOR_FEMALE, COLOR_NB],
                    line=dict(color="white", width=2),
                ),
            )
        ]
    )
    fig.update_layout(
        template="simple_white",
        height=420,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="top", y=-0.05, xanchor="center", x=0.5),
        showlegend=True,
    )
    return fig


def render_gender_breakdown_country_total(
    df_total: pd.DataFrame,
    qid: str,
    country_label: str,
    iso3: str,
) -> None:
    st.markdown("## Gender breakdown (country total)")

    totals = get_gender_totals_for_country(df_total, qid)
    male = int(totals["Male"])
    female = int(totals["Female"])
    nb = int(totals["Non-binary or other"])
    unknown = int(totals["Unknown / not stated"])
    total_all = male + female + nb + unknown

    mf_total = male + female
    male_pct = (male / mf_total * 100.0) if mf_total > 0 else 0.0
    female_pct = (female / mf_total * 100.0) if mf_total > 0 else 0.0
    gap_pp = abs(male_pct - female_pct)

    category = classify_category(male, female, gap_pp_threshold=10.0)

    badge_bg = "#F9FAFB"
    badge_border = "#E6E6E6"
    if category == "More male":
        badge_bg = "#EFF6FF"
        badge_border = "#BFDBFE"
    elif category == "Balanced":
        badge_bg = "#F5F3FF"
        badge_border = "#DDD6FE"
    elif category == "More female":
        badge_bg = "#FDF2F8"
        badge_border = "#FBCFE8"

    st.markdown(
        """
        <style>
        .card {
            background: #ffffff;
            border: 1px solid #E6E6E6;
            border-radius: 14px;
            padding: 14px 14px;
            box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        .title {
            font-size: 18px;
            font-weight: 700;
            margin: 0;
            line-height: 1.2;
        }
        .badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 600;
            border: 1px solid #E6E6E6;
            background: #F9FAFB;
        }
        .row {
            display: flex;
            gap: 10px;
            margin-top: 12px;
        }
        .metric {
            flex: 1;
            background: #F9FAFB;
            border: 1px solid #EFEFEF;
            border-radius: 12px;
            padding: 10px 10px;
        }
        .metric-label {
            color: #6B7280;
            font-size: 12px;
            margin-bottom: 4px;
        }
        .metric-value {
            font-size: 18px;
            font-weight: 700;
            margin: 0;
        }
        .kv {
            margin-top: 12px;
            font-size: 13px;
        }
        .kv div {
            display: flex;
            justify-content: space-between;
            padding: 6px 0;
            border-bottom: 1px dashed #EEE;
        }
        .kv div:last-child {
            border-bottom: none;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        f"""
        <div class="card">
          <div style="display:flex;justify-content:space-between;gap:10px;align-items:flex-start;">
            <div>
              <p class="title">{country_label} <span style="color:#6B7280;font-weight:600;">({iso3})</span></p>
            </div>
            <div class="badge" style="background:{badge_bg};border-color:{badge_border};">{category}</div>
          </div>

          <div class="row">
            <div class="metric">
              <div class="metric-label">Male</div>
              <div class="metric-value">{male:,}</div>
            </div>
            <div class="metric">
              <div class="metric-label">Female</div>
              <div class="metric-value">{female:,}</div>
            </div>
            <div class="metric">
              <div class="metric-label">Gap</div>
              <div class="metric-value">{gap_pp:.1f}<span style="font-size:12px;color:#6B7280;"> pp</span></div>
            </div>
          </div>

          <div class="kv">
            <div><span>Total biographies</span><span><b>{total_all:,}</b></span></div>
            <div><span>Unknown / not stated</span><span>{unknown:,}</span></div>
            <div><span>Non-binary / other</span><span>{nb:,}</span></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("### Donut view")
    donut = donut_gender_breakdown(male, female, nb, min_visible_share=0.008)
    st.plotly_chart(donut, use_container_width=True)
