import numpy as np
import pandas as pd
import plotly.express as px


def _prep_occ_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if df.empty:
        return df

    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)

    # ✅ critical: fill NaN BEFORE astype(str) so it never becomes "nan"
    df["genderCategory"] = (
        df.get("genderCategory", "")
        .fillna("")
        .astype(str).str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )

    df["occupationLabel"] = (
        df.get("occupationLabel", "")
        .fillna("")
        .astype(str).str.strip()
        .replace({"": "Unknown occupation", "nan": "Unknown occupation", "None": "Unknown occupation"})
    )

    df["sector"] = (
        df.get("sector", "")
        .fillna("")
        .astype(str).str.strip()
        .replace({"": "Other / Unclassified", "nan": "Other / Unclassified", "None": "Other / Unclassified"})
    )

    df["isco_major_title"] = (
        df.get("isco_major_title", "")
        .fillna("")
        .astype(str).str.strip()
        .replace({"": "Unmapped ISCO major", "nan": "Unmapped ISCO major", "None": "Unmapped ISCO major"})
    )

    return df


def filter_country_occ(df_all: pd.DataFrame, qid: str) -> pd.DataFrame:
    df = _prep_occ_df(df_all)
    if df.empty:
        return df

    qid = str(qid).strip()
    url = f"http://www.wikidata.org/entity/{qid}"

    # match robustly by either country_qid OR country URL
    if "country_qid" in df.columns:
        m = (df["country_qid"].astype(str).str.strip() == qid) | (df.get("country", "").astype(str) == url)
    else:
        m = (df.get("country", "").astype(str) == url)

    return df[m].copy()


def _group_small_leaves_within_parent(
    agg: pd.DataFrame,
    parent_col: str,
    leaf_col: str,
    value_col: str,
    min_share_within_parent: float,
) -> pd.DataFrame:
    """
    Keeps ALL data, but collapses tiny leaves into "Other (small occupations)"
    within each parent group so the treemap stays readable.

    min_share_within_parent = 0.005 means <0.5% of its parent's total gets grouped.
    """
    out_parts = []

    for parent_value, g in agg.groupby(parent_col, sort=False):
        g = g.copy().sort_values(value_col, ascending=False)
        parent_total = float(g[value_col].sum())

        if parent_total <= 0:
            out_parts.append(g)
            continue

        g["share_in_parent"] = g[value_col] / parent_total
        small = g[g["share_in_parent"] < min_share_within_parent].copy()
        big = g[g["share_in_parent"] >= min_share_within_parent].copy()

        if not small.empty:
            other_sum = int(small[value_col].sum())
            other_row = pd.DataFrame([{
                parent_col: parent_value,
                leaf_col: "Other (small occupations)",
                value_col: other_sum,
            }])
            big = pd.concat([big[[parent_col, leaf_col, value_col]], other_row], ignore_index=True)
        else:
            big = big[[parent_col, leaf_col, value_col]]

        out_parts.append(big)

    return pd.concat(out_parts, ignore_index=True)


def make_occupation_treemap(
    df_country: pd.DataFrame,
    *,
    country_label: str,
    gender_filter: str = "All",
    group_mode: str = "Sector → Occupation",
    min_share_within_parent: float = 0.005,  # 0.5% default
):
    """
    Treemap that uses ALL data, but makes it readable by grouping tiny leaves
    into "Other (small occupations)" within each parent.

    Users can click blocks to zoom into a sector/ISCO major.
    """
    df = _prep_occ_df(df_country)
    if df.empty:
        fig = px.treemap(title=f"Occupations — {country_label}")
        fig.update_layout(template="simple_white", height=520)
        return fig

    if gender_filter and gender_filter != "All":
        df = df[df["genderCategory"] == gender_filter].copy()

    if df.empty:
        fig = px.treemap(title=f"Occupations — {country_label}")
        fig.update_layout(template="simple_white", height=520)
        return fig

    # aggregate to leaf level (occupation)
    agg = (
        df.groupby(["sector", "isco_major_title", "occupationLabel"], as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    if group_mode == "ISCO Major → Occupation":
        parent_col = "isco_major_title"
        title = f"Occupations by ISCO major — {country_label}"
        agg2 = agg[[parent_col, "occupationLabel", "count"]].copy()
        agg2 = _group_small_leaves_within_parent(
            agg2,
            parent_col=parent_col,
            leaf_col="occupationLabel",
            value_col="count",
            min_share_within_parent=min_share_within_parent,
        )
        plot_df = agg2.rename(columns={"occupationLabel": "Occupation"})
        path = [parent_col, "Occupation"]

    else:
        parent_col = "sector"
        title = f"Occupations by sector — {country_label}"
        agg2 = agg[[parent_col, "occupationLabel", "count"]].copy()
        agg2 = _group_small_leaves_within_parent(
            agg2,
            parent_col=parent_col,
            leaf_col="occupationLabel",
            value_col="count",
            min_share_within_parent=min_share_within_parent,
        )
        plot_df = agg2.rename(columns={"occupationLabel": "Occupation"})
        path = [parent_col, "Occupation"]

    fig = px.treemap(
        plot_df,
        path=path,
        values="count",
        title=title,
    )

    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>Count: %{value:,}<extra></extra>",
        marker=dict(line=dict(width=1, color="white")),
        textinfo="label",
    )
    fig.update_layout(
        template="simple_white",
        height=700,
        margin=dict(l=10, r=10, t=60, b=10),
    )
    return fig


def make_occupation_details_table(
    df_country: pd.DataFrame,
    *,
    gender_filter: str = "All",
) -> pd.DataFrame:
    df = _prep_occ_df(df_country)
    if df.empty:
        return pd.DataFrame(columns=["Occupation", "Count", "Share (%)", "Sector", "ISCO major", "Gender"])

    if gender_filter and gender_filter != "All":
        df = df[df["genderCategory"] == gender_filter].copy()

    if df.empty:
        return pd.DataFrame(columns=["Occupation", "Count", "Share (%)", "Sector", "ISCO major", "Gender"])

    agg = (
        df.groupby(["occupationLabel", "sector", "isco_major_title", "genderCategory"], as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    total = float(agg["count"].sum())
    agg["Share (%)"] = np.where(total > 0, (agg["count"] / total) * 100.0, 0.0)

    out = agg.rename(columns={
        "occupationLabel": "Occupation",
        "count": "Count",
        "sector": "Sector",
        "isco_major_title": "ISCO major",
        "genderCategory": "Gender",
    })

    out = out[["Occupation", "Count", "Share (%)", "Sector", "ISCO major", "Gender"]]
    return out

