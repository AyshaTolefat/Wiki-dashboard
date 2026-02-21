# dashboard/components/ethnicity_gender_pies.py

import pandas as pd
import plotly.graph_objects as go

GENDER_ORDER = ["Male", "Female", "Non-binary or other", "Unknown / not stated"]

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    d["count"] = pd.to_numeric(d["count"], errors="coerce").fillna(0).astype(int)
    d["genderCategory"] = (
        d["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    )
    d["ethnicGroupLabel"] = (
        d["ethnicGroupLabel"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    )

    # Fix missing ethnic group labels (prevents "nan")
    bad = d["ethnicGroupLabel"].isna() | d["ethnicGroupLabel"].isin(["", "nan", "None"])
    d.loc[bad, "ethnicGroupLabel"] = "Unknown ethnic group"

    # Keep only known gender buckets
    d = d[d["genderCategory"].isin(GENDER_ORDER)]

    # Remove zero counts (noise)
    d = d[d["count"] > 0]

    return d

def make_gender_ethnicity_table(
    df_country: pd.DataFrame,
    gender: str,
    top_n: int = 10,
) -> pd.DataFrame:
    """
    Table of ethnic groups for ONE gender:
    Ethnic group | Count | Percent

    Matches the donut logic (Top N + Other).
    Returns empty df if no data.
    """
    if df_country is None or df_country.empty:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent"])

    d = _clean(df_country)
    d = d[d["genderCategory"] == gender].copy()
    if d.empty:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent"])

    totals = (
        d.groupby("ethnicGroupLabel", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
    )

    total_gender = int(totals["count"].sum())
    if total_gender <= 0:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent"])

    top = totals.head(top_n).copy()
    other_sum = int(total_gender - top["count"].sum())

    rows = top.rename(columns={"ethnicGroupLabel": "Ethnic group", "count": "Count"}).copy()
    if other_sum > 0:
        rows = pd.concat(
            [rows, pd.DataFrame([{"Ethnic group": "Other", "Count": other_sum}])],
            ignore_index=True,
        )

    rows["Percent"] = rows["Count"].apply(lambda v: (v / total_gender) * 100.0)
    rows["Percent"] = rows["Percent"].map(lambda p: f"{p:.1f}%")

    return rows[["Ethnic group", "Count", "Percent"]]


def make_gender_ethnicity_donut(
    df_country: pd.DataFrame,
    gender: str,
    title_prefix: str,   # keep param name but we’ll pass the new title text
    top_n: int = 10,
    min_pct_label: float = 3.0,  # ✅ only show % labels if slice >= this
) -> tuple[go.Figure | None, int]:
    """
    Returns (figure_or_None, total_count_for_gender)

    - If total is 0 -> returns (None, 0)
    - Legend used as key (no leader lines)
    - Hover disabled
    - Only show inside % labels for slices >= min_pct_label
    """

    if df_country is None or df_country.empty:
        return None, 0

    d = _clean(df_country)
    d = d[d["genderCategory"] == gender].copy()

    if d.empty:
        return None, 0

    totals = (
        d.groupby("ethnicGroupLabel", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
    )

    total_gender = int(totals["count"].sum())
    if total_gender <= 0:
        return None, 0

    # Top N + Other (kept for readability; title no longer mentions it)
    top = totals.head(top_n).copy()
    other_sum = int(total_gender - top["count"].sum())

    labels = top["ethnicGroupLabel"].tolist()
    values = top["count"].tolist()

    if other_sum > 0:
        labels.append("Other")
        values.append(other_sum)

    # ✅ Custom text: show percent only when slice is big enough
    pct = [(v / total_gender) * 100.0 for v in values]
    text = [f"{p:.1f}%" if p >= float(min_pct_label) else "" for p in pct]

    fig = go.Figure()
    fig.add_trace(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.58,
            sort=False,

            # ✅ use our custom text instead of Plotly’s default that becomes unreadable
            text=text,
            textinfo="text",
            textposition="inside",

            # ✅ legend is the key
            showlegend=True,

            # ✅ no hover
            hoverinfo="skip",
            hovertemplate=None,
        )
    )

    fig.update_layout(
        template="simple_white",
        title=f"{title_prefix} — {gender}",
        height=460,
        margin=dict(l=10, r=10, t=60, b=10),
        legend=dict(
            orientation="v",
            title="Ethnic group",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
        ),
    )

    return fig, total_gender


    return fig, total_gender






