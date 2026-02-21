import numpy as np
import pandas as pd
import plotly.graph_objects as go


AGE_ORDER = ["0-14 child", "15-24 youth", "25-59 adult", "60+ senior"]


def prep_age_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)
    df["ageGroup"] = df.get("ageGroup", "").astype(str).str.strip()
    df["country"] = df.get("country", "").astype(str)

    # ensure consistent ordering even if a group is missing
    df["ageGroup"] = pd.Categorical(df["ageGroup"], categories=AGE_ORDER, ordered=True)
    return df


def filter_country_age(df_all: pd.DataFrame, qid: str) -> pd.DataFrame:
    qid = str(qid).strip()
    url = f"http://www.wikidata.org/entity/{qid}"
    df = prep_age_df(df_all)
    if df.empty:
        return df

    out = df[df["country"] == url].copy()
    return out


def make_age_histogram(
    df_country: pd.DataFrame,
    *,
    country_label: str,
    mode: str = "Counts",   # "Counts" or "Share (%)"
):
    """
    Histogram-style binned bar chart: one bar per age group.
    """
    df = prep_age_df(df_country)

    # build full set of bins (so missing bins show as 0)
    base = pd.DataFrame({"ageGroup": AGE_ORDER})
    if df.empty:
        base["count"] = 0
    else:
        sums = df.groupby("ageGroup", as_index=False)["count"].sum()
        base = base.merge(sums, on="ageGroup", how="left")
        base["count"] = base["count"].fillna(0).astype(int)

    total = float(base["count"].sum())
    base["share"] = np.where(total > 0, (base["count"] / total) * 100.0, 0.0)

    y = base["count"] if mode == "Counts" else base["share"]
    y_title = "Biographies (count)" if mode == "Counts" else "Share of biographies (%)"
    title = f"Age representation — {country_label}"

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=base["ageGroup"].astype(str),
            y=y,
            text=[f"{v:,.0f}" if mode == "Counts" else f"{v:.1f}%" for v in y],
            textposition="outside",
            cliponaxis=False,
            hoverinfo="skip",   # ✅ remove hover popup
        )
    )

    fig.update_layout(
        template="simple_white",
        height=520,
        margin=dict(l=10, r=10, t=50, b=10),
        title=title,
        xaxis=dict(title="Age group"),
        yaxis=dict(title=y_title),
        showlegend=False,
    )

    # make labels readable
    fig.update_xaxes(tickangle=0)
    return fig, base


def make_age_table(base: pd.DataFrame) -> pd.DataFrame:
    """
    base is output from make_age_histogram(): contains ageGroup, count, share
    """
    out = base.copy()
    out = out.rename(columns={"ageGroup": "Age group", "count": "Count", "share": "Share (%)"})
    out["Share (%)"] = out["Share (%)"].astype(float)
    return out[["Age group", "Count", "Share (%)"]]