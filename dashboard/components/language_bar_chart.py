import numpy as np
import pandas as pd
import plotly.express as px


def _prep_language_df(df_country_lang: pd.DataFrame) -> pd.DataFrame:
    df = df_country_lang.copy()
    if df.empty:
        return df

    df["count"] = pd.to_numeric(df.get("count", 0), errors="coerce").fillna(0).astype(int)
    df["type"] = df.get("type", "").astype(str).str.strip().str.lower()
    df["languageLabel"] = df.get("languageLabel", "").astype(str).str.strip()

    # Keep only valid labels
    df = df[df["languageLabel"].notna() & (df["languageLabel"].astype(str).str.len() > 0)].copy()
    return df


def compute_language_kpis(
    df_country_lang: pd.DataFrame,
    *,
    lang_type: str,
) -> dict:
    df = _prep_language_df(df_country_lang)
    lang_type = str(lang_type).strip().lower()
    df = df[df["type"] == lang_type].copy()

    if df.empty:
        return {"total": 0, "unique": 0, "top_label": "—", "top_share": 0.0}

    agg = df.groupby("languageLabel", as_index=False)["count"].sum()
    total = int(agg["count"].sum())
    unique = int(agg.shape[0])

    agg = agg.sort_values("count", ascending=False)
    top_label = str(agg.iloc[0]["languageLabel"]) if not agg.empty else "—"
    top_count = int(agg.iloc[0]["count"]) if not agg.empty else 0
    top_share = (top_count / total * 100.0) if total > 0 else 0.0

    return {"total": total, "unique": unique, "top_label": top_label, "top_share": top_share}


def make_language_bar_top_n(
    df_country_lang: pd.DataFrame,
    *,
    country_label: str,
    lang_type: str,
    top_n: int = 15,
):
    df = _prep_language_df(df_country_lang)
    lang_type = str(lang_type).strip().lower()
    df = df[df["type"] == lang_type].copy()

    # Empty figure (clean)
    if df.empty:
        fig = px.bar(title=f"Top {top_n} languages ({lang_type}) — {country_label}")
        fig.update_layout(template="simple_white", height=520, hovermode=False)
        return fig

    agg = (
        df.groupby("languageLabel", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
        .head(top_n)
        .copy()
    )

    fig = px.bar(
        agg,
        x="count",
        y="languageLabel",
        orientation="h",
        text="count",
        title=f"Top {min(top_n, len(agg))} languages ({lang_type}) — {country_label}",
    )

    # Softer modern blue + remove hover completely
    fig.update_traces(
        marker_color="#3B82F6",
        texttemplate="%{text:,}",
        textposition="outside",
        cliponaxis=False,
        hoverinfo="skip",
        hovertemplate=None,
    )
    # Extra-safe: nuke hover on every trace
    for tr in fig.data:
        tr.hovertemplate = None
        tr.hoverinfo = "skip"

    fig.update_layout(
        template="simple_white",
        height=max(520, 28 * len(agg) + 220),
        margin=dict(l=10, r=10, t=70, b=10),
        xaxis_title="Count",
        yaxis_title=None,      # remove redundant "Language" label
        showlegend=False,
        hovermode=False,       # disables hover globally
    )
    fig.update_xaxes(tickformat=",")  # 12,000 not 12k
    fig.update_yaxes(autorange="reversed")  # biggest at top

    return fig


def make_language_details_table(
    df_country_lang: pd.DataFrame,
    *,
    lang_type: str,
) -> pd.DataFrame:
    df = _prep_language_df(df_country_lang)
    lang_type = str(lang_type).strip().lower()
    df = df[df["type"] == lang_type].copy()

    if df.empty:
        return pd.DataFrame(columns=["Language", "Count", "Share (%)"])

    agg = (
        df.groupby("languageLabel", as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
        .reset_index(drop=True)
    )

    total = float(agg["count"].sum())
    agg["Share (%)"] = np.where(total > 0, (agg["count"] / total) * 100.0, 0.0)

    out = agg.rename(columns={"languageLabel": "Language", "count": "Count"}).copy()
    out = out[["Language", "Count", "Share (%)"]]
    return out



