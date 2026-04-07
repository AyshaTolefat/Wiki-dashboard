import streamlit as st

st.set_page_config(
    page_title="Wikidata Demographic Bias Explorer",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------
# Styling
# -----------------------------
st.markdown(
    """
    <style>
        .main {
            padding-top: 2rem;
        }

        .hero-title {
            font-size: 2.8rem;
            font-weight: 800;
            margin-bottom: 0.5rem;
            color: #1f2937;
        }

        .hero-subtitle {
            font-size: 1.15rem;
            color: #4b5563;
            margin-bottom: 2rem;
            max-width: 850px;
            line-height: 1.6;
        }

        .choice-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 1.5rem 1.25rem;
            box-shadow: 0 6px 18px rgba(0,0,0,0.05);
            height: 100%;
        }

        .choice-title {
            font-size: 1.35rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: #111827;
        }

        .choice-text {
            color: #4b5563;
            font-size: 1rem;
            line-height: 1.6;
            margin-bottom: 1rem;
        }

        .footer-note {
            margin-top: 2rem;
            color: #6b7280;
            font-size: 0.95rem;
        }

        div.stButton > button {
            width: 100%;
            border-radius: 12px;
            padding: 0.75rem 1rem;
            font-weight: 600;
            font-size: 1rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Header
# -----------------------------
st.markdown('<div class="hero-title">Wikidata Demographic Bias Explorer</div>', unsafe_allow_html=True)
st.markdown(
    """
    <div class="hero-subtitle">
        Choose the interface assigned to you. The dashboard interface provides
        interactive visualisations, while the tabular data interface presents the same
        underlying data in table form only.
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------
# Two-column choice layout
# -----------------------------
col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown(
        """
        <div class="choice-card">
            <div class="choice-title">📊 Dashboard Interface</div>
            <div class="choice-text">
                Explore the data through interactive visualisations, including country-level
                summaries, gender trends over time, age groups, languages, ethnic groups,
                and occupation categories.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("Enter Dashboard Interface", key="dashboard_btn"):
        st.switch_page("pages/0_Globe_Overview.py")

with col2:
    st.markdown(
        """
        <div class="choice-card">
            <div class="choice-title">📋 Tabular Data Interface</div>
            <div class="choice-text">
                Explore the same underlying data using tables only. This interface is intended
                for users completing tasks through tabular data rather than visualisations.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("Enter Tabular Data Interface", key="table_btn"):
        st.switch_page("pages/2_tabular_data_interface.py")

# -----------------------------
# Optional note
# -----------------------------
st.markdown(
    """
    <div class="footer-note">
        Please select only the interface assigned to you.
    </div>
    """,
    unsafe_allow_html=True,
)