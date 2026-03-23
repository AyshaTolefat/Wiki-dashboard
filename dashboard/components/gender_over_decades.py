import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


COLOR_MALE = "#2B6CB0"
COLOR_FEMALE = "#D53F8C"
COLOR_NB = "#805AD5"
COLOR_GAP = "#6B7280"


def build_country_decade_df(all_decades: pd.DataFrame, qid: str) -> pd.DataFrame:
    c = all_decades[all_decades["qid"] == qid].copy()
    if c.empty:
        return pd.DataFrame()

    piv = (
        c.pivot_table(
            index=["qid", "countryLabel", "decade"],
            columns="genderCategory",
            values="count",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .sort_values("decade")
        .reset_index(drop=True)
    )

    for col in ["Male", "Female", "Non-binary or other", "Unknown / not stated"]:
        if col not in piv.columns:
            piv[col] = 0

    piv["mf_total"] = piv["Male"] + piv["Female"]
    piv["female_share"] = np.where(piv["mf_total"] > 0, piv["Female"] / piv["mf_total"], np.nan)
    piv["male_share"] = np.where(piv["mf_total"] > 0, piv["Male"] / piv["mf_total"], np.nan)
    piv["gap_pp"] = np.where(
        piv["mf_total"] > 0,
        np.abs(piv["male_share"] - piv["female_share"]) * 100.0,
        np.nan
    )
    return piv


def add_line(fig, x, y, name, color):
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=3),
            marker=dict(size=7, color=color),
            hoverinfo="skip",
            hovertemplate=None,
        )
    )


def fmt_pct(p):
    return "—" if p is None or (isinstance(p, float) and np.isnan(p)) else f"{p*100:.1f}%"


def fmt_pp(x):
    return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.1f} pp"


def fmt_pp_signed(x):
    return "—" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:+.1f} pp"


def render_gender_over_decades(all_decades: pd.DataFrame, qid: str) -> None:
    df = build_country_decade_df(all_decades, qid)

    if df.empty:
        st.warning("No decade data found for this country.")
        return

    decades = df["decade"].tolist()

    # --- Title + simple description ---
    st.markdown("## Gender representation over decades")
    st.caption(
        "These charts show how biographies are distributed by gender across birth decades (1900–present). "
        "Use the decade selector to update the KPIs and highlight that decade on each chart."
    )

    # --- Controls (clean: no extra bubbles/empty labels) ---
    c1, c2 = st.columns([2, 1], vertical_alignment="bottom")
    with c1:
        view = st.selectbox(
            "View",
            ["All (Male + Female)", "Male only", "Female only"],
            index=0,
            label_visibility="collapsed",
        )
    with c2:
        selected_decade = st.selectbox(
            "Select decade",
            decades,
            index=len(decades) - 1,
            label_visibility="visible",
        )

    row = df[df["decade"] == selected_decade].iloc[0]
    prev_df = df[df["decade"] == (selected_decade - 10)]
    prev_row = prev_df.iloc[0] if not prev_df.empty else None

    male = int(row["Male"])
    female = int(row["Female"])
    nb = int(row["Non-binary or other"])

    mf_total = int(row["mf_total"])
    male_share = None if mf_total <= 0 else float(row["male_share"])
    female_share = None if mf_total <= 0 else float(row["female_share"])
    gap_pp = None if mf_total <= 0 else float(row["gap_pp"])

    prev_male_share = None
    prev_female_share = None
    if prev_row is not None and int(prev_row["mf_total"]) > 0:
        prev_male_share = float(prev_row["male_share"])
        prev_female_share = float(prev_row["female_share"])

    delta_male_pp = None if (male_share is None or prev_male_share is None) else (male_share - prev_male_share) * 100.0
    delta_female_pp = None if (female_share is None or prev_female_share is None) else (female_share - prev_female_share) * 100.0

    # --- KPI styles ---
    st.markdown(
        """
        <style>
          .kpi-card{
            background:#ffffff;
            border:1px solid #E6E6E6;
            border-radius:14px;
            padding:14px;
            box-shadow:0 1px 2px rgba(0,0,0,0.04);
            height: 100%;
          }
          .kpi-label{
            font-size:12px;
            color:#6B7280;
            margin-bottom:6px;
          }
          .kpi-value{
            font-size:24px;
            font-weight:900;
            color:#111827;
            line-height:1.1;
          }
          .kpi-sub{
            margin-top:8px;
            font-size:12px;
            color:#6B7280;
          }
        </style>
        """,
        unsafe_allow_html=True
    )

    top = st.columns(4)
    bottom = st.columns(4)

    with top[0]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Selected decade</div>
              <div class="kpi-value">{selected_decade}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    with top[1]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Male (count)</div>
              <div class="kpi-value">{male:,}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    with top[2]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Female (count)</div>
              <div class="kpi-value">{female:,}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    with top[3]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Non-binary (count)</div>
              <div class="kpi-value">{nb:,}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    with bottom[0]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Male % (of Male+Female)</div>
              <div class="kpi-value">{fmt_pct(male_share)}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    with bottom[1]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Female % (of Male+Female)</div>
              <div class="kpi-value">{fmt_pct(female_share)}</div>
            </div>
            """,
            unsafe_allow_html=True
        )
    with bottom[2]:
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Gender gap (pp)</div>
              <div class="kpi-value">{fmt_pp(gap_pp)}</div>
              <div class="kpi-sub">Gap = |Male% − Female%|</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    # ✅ Split change into TWO KPI cards (cleaner)
    with bottom[3]:
        prev_label = (selected_decade - 10) if prev_row is not None else "—"
        st.markdown(
            f"""
            <div class="kpi-card">
              <div class="kpi-label">Change from previous decade</div>
              <div class="kpi-sub">Compared to {prev_label}</div>
              <div style="height:8px;"></div>
              <div class="kpi-label">Male % change</div>
              <div class="kpi-value">{fmt_pp_signed(delta_male_pp)}</div>
              <div style="height:10px;"></div>
              <div class="kpi-label">Female % change</div>
              <div class="kpi-value">{fmt_pp_signed(delta_female_pp)}</div>
            </div>
            """,
            unsafe_allow_html=True
        )

    st.markdown("")

    # ----------------------
    # Charts + descriptions
    # ----------------------

    # Male/Female chart
    st.markdown("### Gender over decades")
    st.caption("Counts of male and female biographies by decade. The dotted line marks the selected decade.")

    mf_fig = go.Figure()
    x = df["decade"]

    if view in ["All (Male + Female)", "Male only"]:
        add_line(mf_fig, x, df["Male"], "Male", COLOR_MALE)
    if view in ["All (Male + Female)", "Female only"]:
        add_line(mf_fig, x, df["Female"], "Female", COLOR_FEMALE)

    mf_fig.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#9CA3AF")

    mf_fig.update_xaxes(title="Decade", tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])

    # ✅ Fix legend overlap with title by giving it real space + moving it higher
    mf_fig.update_layout(
        height=520,
        margin=dict(l=10, r=10, t=70, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.15,   # higher than before
            xanchor="left",
            x=0.0,
            title_text="",
        ),
        yaxis=dict(title="Biographies"),
        template="simple_white",
        hovermode=False,
    )

    st.plotly_chart(mf_fig, use_container_width=True)

    # Non-binary chart
    st.markdown("### Non-binary over decades")
    st.caption("Counts of non-binary / other biographies by decade. The dotted line marks the selected decade.")

    nb_fig = go.Figure()
    nb_fig.add_trace(
        go.Scatter(
            x=df["decade"],
            y=df["Non-binary or other"],
            mode="lines+markers",
            name="Non-binary / other",
            line=dict(color=COLOR_NB, width=3),
            marker=dict(size=7, color=COLOR_NB),
            hoverinfo="skip",
            hovertemplate=None,
        )
    )
    nb_fig.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#9CA3AF")
    nb_fig.update_xaxes(title="Decade", tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    nb_fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=40, b=10),
        template="simple_white",
        yaxis=dict(title="Count"),
        showlegend=False,
        hovermode=False,
    )
    st.plotly_chart(nb_fig, use_container_width=True)

    # Gap chart
    st.markdown("### Gender gap over decades")
    st.caption("Absolute difference between male and female share (percentage points) by decade.")

    gap_fig = go.Figure()
    gap_fig.add_trace(
        go.Scatter(
            x=df["decade"],
            y=df["gap_pp"],
            mode="lines+markers",
            name="Gap (pp)",
            line=dict(color=COLOR_GAP, width=3),
            marker=dict(size=7, color=COLOR_GAP),
            hoverinfo="skip",
            hovertemplate=None,
        )
    )
    gap_fig.add_vline(x=selected_decade, line_width=2, line_dash="dot", line_color="#9CA3AF")
    gap_fig.update_xaxes(title="Decade", tickmode="array", tickvals=decades, ticktext=[str(d) for d in decades])
    gap_fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=40, b=10),
        template="simple_white",
        yaxis=dict(title="pp"),
        showlegend=False,
        hovermode=False,
    )
    st.plotly_chart(gap_fig, use_container_width=True)