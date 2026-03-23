import pandas as pd
import plotly.graph_objects as go

GENDER_ORDER = ["Male", "Female", "Non-binary or other", "Unknown / not stated"]

# Longer palette so colors do not repeat quickly
ETHNIC_COLORS = [
    "#4F46E5",  # indigo
    "#EF553B",  # red-orange
    "#10B981",  # emerald
    "#8B5CF6",  # violet
    "#F59E0B",  # amber
    "#06B6D4",  # cyan
    "#F472B6",  # pink
    "#A3E635",  # lime
    "#E879F9",  # fuchsia
    "#FBBF24",  # yellow
    "#14B8A6",  # teal
    "#FB7185",  # rose
    "#84CC16",  # green
    "#38BDF8",  # sky
    "#F97316",  # orange
    "#6366F1",  # blue-indigo
    "#22C55E",  # green
    "#EC4899",  # magenta
    "#A855F7",  # purple
    "#0EA5E9",  # blue
]

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    d["count"] = pd.to_numeric(d["count"], errors="coerce").fillna(0).astype(int)
    d["genderCategory"] = (
        d["genderCategory"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    )
    d["ethnicGroupLabel"] = (
        d["ethnicGroupLabel"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    )

    bad = d["ethnicGroupLabel"].isna() | d["ethnicGroupLabel"].isin(["", "nan", "None"])
    d.loc[bad, "ethnicGroupLabel"] = "Unknown ethnic group"

    d = d[d["genderCategory"].isin(GENDER_ORDER)]
    d = d[d["count"] > 0]

    return d


def _top_ethnicity_rows(
    df_country: pd.DataFrame,
    gender: str,
    top_n: int = 10,
) -> tuple[pd.DataFrame, int]:
    if df_country is None or df_country.empty:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent", "Color"]), 0

    d = _clean(df_country)
    d = d[d["genderCategory"] == gender].copy()
    if d.empty:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent", "Color"]), 0

    totals = (
        d.groupby("ethnicGroupLabel", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    total_gender = int(totals["count"].sum())
    if total_gender <= 0:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent", "Color"]), 0

    top = totals.head(top_n).copy()
    other_sum = int(total_gender - top["count"].sum())

    rows = top.rename(columns={"ethnicGroupLabel": "Ethnic group", "count": "Count"}).copy()
    if other_sum > 0:
        rows = pd.concat(
            [rows, pd.DataFrame([{"Ethnic group": "Other", "Count": other_sum}])],
            ignore_index=True,
        )

    rows["PercentValue"] = rows["Count"].apply(lambda v: (v / total_gender) * 100.0)
    rows["Percent"] = rows["PercentValue"].map(lambda p: f"{p:.1f}%")
    rows["Color"] = [ETHNIC_COLORS[i % len(ETHNIC_COLORS)] for i in range(len(rows))]

    return rows[["Ethnic group", "Count", "Percent", "PercentValue", "Color"]], total_gender


def make_gender_ethnicity_table(
    df_country: pd.DataFrame,
    gender: str,
    top_n: int = 10,
) -> pd.DataFrame:
    rows, _ = _top_ethnicity_rows(df_country, gender, top_n=top_n)
    if rows.empty:
        return pd.DataFrame(columns=["Ethnic group", "Count", "Percent"])
    return rows[["Ethnic group", "Count", "Percent"]]


def make_gender_ethnicity_legend_df(
    df_country: pd.DataFrame,
    gender: str,
    top_n: int = 10,
) -> pd.DataFrame:
    rows, _ = _top_ethnicity_rows(df_country, gender, top_n=top_n)
    if rows.empty:
        return pd.DataFrame(columns=["Ethnic group", "Color", "Percent"])
    return rows[["Ethnic group", "Color", "Percent"]]


def make_gender_ethnicity_donut(
    df_country: pd.DataFrame,
    gender: str,
    title_prefix: str,
    top_n: int = 10,
    min_pct_label: float = 3.0,
) -> tuple[go.Figure | None, int]:
    rows, total_gender = _top_ethnicity_rows(df_country, gender, top_n=top_n)

    if rows.empty or total_gender <= 0:
        return None, 0

    labels = rows["Ethnic group"].tolist()
    values = rows["Count"].tolist()
    colors = rows["Color"].tolist()
    pct = rows["PercentValue"].tolist()
    text = [f"{p:.1f}%" if p >= float(min_pct_label) else "" for p in pct]

    fig = go.Figure()

    fig.add_trace(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.58,
            sort=False,
            marker=dict(colors=colors),
            text=text,
            textinfo="text",
            textposition="inside",
            insidetextorientation="radial",
            showlegend=False,
            hoverinfo="skip",
            hovertemplate=None,
            domain=dict(x=[0.10, 0.90], y=[0.08, 0.92]),
        )
    )

    fig.update_layout(
        template="simple_white",
        title="",
        height=320,
        margin=dict(l=10, r=10, t=10, b=10),
        uniformtext_minsize=10,
        uniformtext_mode="hide",
    )

    return fig, total_gender