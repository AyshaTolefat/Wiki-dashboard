import streamlit as st
import pandas as pd
from pathlib import Path

# =========================
# PAGE CONFIG
# =========================

st.set_page_config(page_title="Tabular Data Interface", layout="wide")

# =========================
# PATHS
# =========================

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR.parent / "data"
TABLE_DIR = BASE_DIR.parent / "user_study_tables"

# =========================
# HELPER
# =========================

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

# =========================
# TITLE / INSTRUCTIONS
# =========================

st.title("Tabular Data Interface")

st.markdown(
    """
    Use the tables below to complete the tasks.  
    Select the relevant dataset, then scroll through the table to find the country and values you need.

    This interface presents the same underlying data as the dashboard, but in tabular form only.
    """
)

# =========================
# DATASET SELECTION
# =========================

dataset_name = st.selectbox(
    "Select dataset",
    [
        "Gender Overall",
        "Gender Over Decades",
        "Age Groups",
        "Languages",
        "Ethnic Groups",
        "Occupation Categories",
    ]
)

# =========================
# LOAD CORRECT TABLE
# =========================

if dataset_name == "Gender Overall":
    file_path = TABLE_DIR / "study_gender_overall_all_countries.csv"
    description = "Overall gender counts by country."

elif dataset_name == "Gender Over Decades":
    file_path = TABLE_DIR / "study_gender_over_decades_all_countries.csv"
    description = "Gender counts by country and decade."

elif dataset_name == "Age Groups":
    file_path = TABLE_DIR / "study_age_groups_all_countries.csv"
    description = "Age-group counts by country."

elif dataset_name == "Languages":
    file_path = TABLE_DIR / "study_languages_spoken_all_countries.csv"
    description = "Most common spoken languages by country."

elif dataset_name == "Ethnic Groups":
    file_path = TABLE_DIR / "study_ethnic_groups_all_countries.csv"
    description = "Ethnic-group counts by country and gender."

elif dataset_name == "Occupation Categories":
    file_path = TABLE_DIR / "study_occupation_categories_all_countries.csv"
    description = "Occupation-category counts by country."

else:
    st.error("Unknown dataset selected.")
    st.stop()

# =========================
# DISPLAY
# =========================

if not file_path.exists():
    st.error(f"File not found: {file_path}")
    st.info("Make sure you have generated the cleaned user-study tables first.")
    st.stop()

df = load_csv(file_path)

st.caption(description)

# Optional: show row count
st.write(f"Rows: {len(df):,}")

# Show plain table
st.dataframe(
    df,
    use_container_width=True,
    hide_index=True,
)

# Optional note
st.markdown(
    """
    ---
    **Note:** No charts or visual summaries are provided in this interface.
    """
)