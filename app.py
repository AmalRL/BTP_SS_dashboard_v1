import streamlit as st
import pandas as pd
import redshift_connector
import os
from dotenv import load_dotenv
import plotly.express as px
import datetime

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

# -----------------------------
# CONNECTION
# -----------------------------
@st.cache_resource
def get_connection():
    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )

# -----------------------------
# MAIN METRICS QUERY (REUSABLE)
# -----------------------------
@st.cache_data(ttl=600)
def fetch_onboarding_metrics(district_ids):

    district_filter = ",".join(map(str, district_ids))

    query = f"""
    WITH base_users AS (
        SELECT g.id AS guardian_id, g.phone
        FROM rl_dwh_prod.live.guardians_btp g
        JOIN rl_dwh_prod.live.covered_guardians_btp c
            ON g.id = c.guardian_id
        WHERE c.district_product_id IN ({district_filter})
    ),

    onboarding_data AS (
        SELECT
            guardian_phone,
            MIN(responded_datetime) AS first_onboarding_date
        FROM rl_dwh_prod.live.individual_interactions
        WHERE campaign_name ILIKE '%onboarding%'
        GROUP BY guardian_phone
    ),

    final_base AS (
        SELECT
            b.phone,
            b.guardian_id,
            o.first_onboarding_date
        FROM base_users b
        LEFT JOIN onboarding_data o
            ON b.phone = o.guardian_phone
    ),

    kids_data AS (
        SELECT
            guardian_id,
            MIN(DATEDIFF(month, birthday, GETDATE())) AS age_months
        FROM rl_dwh_prod.live.kids_btp
        WHERE birthday IS NOT NULL
        GROUP BY guardian_id
    ),

    age_36_users AS (
        SELECT f.phone
        FROM final_base f
        JOIN kids_data k
            ON f.guardian_id = k.guardian_id
        WHERE k.age_months <= 36
    ),

    interactions AS (
        SELECT
            guardian_phone,
            responded_datetime,
            DATE_TRUNC('week', responded_datetime) AS week_start
        FROM rl_dwh_prod.live.individual_interactions
        WHERE responded_datetime IS NOT NULL
    ),

    user_active_weeks AS (
        SELECT
            f.phone,
            COUNT(DISTINCT i.week_start) AS active_weeks
        FROM final_base f
        LEFT JOIN interactions i
            ON f.phone = i.guardian_phone
           AND i.week_start >= DATE_TRUNC('week', f.first_onboarding_date)
        WHERE f.first_onboarding_date IS NOT NULL
        GROUP BY f.phone
    ),

    wau_eligible AS (
        SELECT f.phone
        FROM final_base f
        JOIN kids_data k
            ON f.guardian_id = k.guardian_id
        WHERE k.age_months <= 36
          AND f.first_onboarding_date IS NOT NULL
    ),

    wau_users AS (
        SELECT DISTINCT i.guardian_phone
        FROM interactions i
        JOIN wau_eligible w
            ON i.guardian_phone = w.phone
        WHERE i.responded_datetime >= DATE_TRUNC('week', GETDATE()) - INTERVAL '7 day'
          AND i.responded_datetime <= DATE_TRUNC('week', GETDATE()) - INTERVAL '1 day'
    ),

    power_user_base AS (
        SELECT
            f.phone,
            f.first_onboarding_date,
            COALESCE(u.active_weeks, 0) AS active_weeks,
            DATEDIFF(
                week,
                DATE_TRUNC('week', f.first_onboarding_date),
                DATE_TRUNC('week', GETDATE())
            ) AS weeks_since_onboarding
        FROM final_base f
        LEFT JOIN user_active_weeks u
            ON f.phone = u.phone
        WHERE f.first_onboarding_date IS NOT NULL
    ),

    power_users AS (
        SELECT phone
        FROM power_user_base
        WHERE weeks_since_onboarding > 0
          AND (active_weeks * 1.0 / weeks_since_onboarding) >= 0.5
    )

    SELECT
        COUNT(DISTINCT phone) AS total_users,

        COUNT(DISTINCT CASE
            WHEN first_onboarding_date IS NOT NULL THEN phone
        END) AS onboarding_users,

        ROUND(
            COUNT(DISTINCT CASE
                WHEN first_onboarding_date IS NOT NULL THEN phone
            END) * 100.0 / COUNT(DISTINCT phone),
        2) AS onboarding_percentage,

        ROUND((SELECT AVG(active_weeks) FROM user_active_weeks), 2) AS avg_weeks_active,

        (SELECT COUNT(DISTINCT phone) FROM age_36_users) AS age_36_users,

        (SELECT COUNT(DISTINCT guardian_phone) FROM wau_users) AS wau_users,

        ROUND(
            (SELECT COUNT(DISTINCT guardian_phone) FROM wau_users) * 100.0 /
            NULLIF((SELECT COUNT(DISTINCT phone) FROM wau_eligible), 0),
        2) AS wau_percentage,

        (SELECT COUNT(DISTINCT phone) FROM power_users) AS power_users,

        ROUND(
            (SELECT COUNT(DISTINCT phone) FROM power_users) * 100.0 /
            NULLIF(
                COUNT(DISTINCT CASE
                    WHEN first_onboarding_date IS NOT NULL THEN phone
                END), 0
            ),
        2) AS power_user_percentage

    FROM final_base;
    """

    return pd.read_sql(query, get_connection())


# -----------------------------
# INTERVENTION QUERY (FILTERED BY USERS)
# -----------------------------
@st.cache_data(ttl=600)
def fetch_intervention_metrics(district_ids, start_date, end_date):

    district_filter = ",".join(map(str, district_ids))

    query = f"""
    WITH base_users AS (
        SELECT g.phone
        FROM rl_dwh_prod.live.guardians_btp g
        JOIN rl_dwh_prod.live.covered_guardians_btp c
            ON g.id = c.guardian_id
        WHERE c.district_product_id IN ({district_filter})
    )

    SELECT
        CASE
            WHEN campaign_template ILIKE '%bonus%' THEN 'Wednesday Video'
            WHEN campaign_template ILIKE '%activity_utility%' THEN 'Sunday Video'
            WHEN campaign_template ILIKE '%reel%' THEN 'Recap Reel'
            WHEN campaign_template ILIKE '%weeklycerti%' THEN 'Saturday Certificate'
            ELSE 'Others'
        END AS campaign_type,

        COUNT(*) AS total_interventions,

        COUNT(CASE WHEN delivered_time IS NOT NULL THEN 1 END) AS delivered_interventions,

        ROUND(
            COUNT(CASE WHEN delivered_time IS NOT NULL THEN 1 END) * 100.0
            / COUNT(*),
        2) AS delivery_percentage

    FROM rl_dwh_prod.live.individual_interventions i
    JOIN base_users b
        ON i.guardian_phone = b.phone

    WHERE created_at >= '{start_date}'
      AND created_at < DATEADD(day, 1, '{end_date}')

    GROUP BY 1
    ORDER BY 1;
    """

    return pd.read_sql(query, get_connection())


# -----------------------------
# PAGE NAVIGATION
# -----------------------------
st.set_page_config(page_title="Analytics Dashboard", layout="wide")

page = st.sidebar.selectbox(
    "Select Dashboard",
    ["BTP Analytics", "SS Analytics"]
)

# -----------------------------
# PAGE CONFIG
# -----------------------------
if page == "BTP Analytics":
    st.title("📊 BTP Analytics Dashboard")
    district_ids = [288, 289]

else:
    st.title("📊 SS Analytics Dashboard")
    district_ids = [415]  # 🔁 CHANGE THIS

# -----------------------------
# MAIN METRICS
# -----------------------------
st.subheader(":pushpin: Onboarding, WAU & Power Users")

df1 = fetch_onboarding_metrics(district_ids)

if not df1.empty:
    total = int(df1.loc[0, "total_users"])
    onboarded = int(df1.loc[0, "onboarding_users"])
    pct = float(df1.loc[0, "onboarding_percentage"])
    avg_weeks = float(df1.loc[0, "avg_weeks_active"] or 0)
    age_36 = int(df1.loc[0, "age_36_users"])
    wau = int(df1.loc[0, "wau_users"])
    wau_pct = float(df1.loc[0, "wau_percentage"] or 0)
    power = int(df1.loc[0, "power_users"])
    power_pct = float(df1.loc[0, "power_user_percentage"] or 0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Users", total)
    col2.metric("Onboarded Users", onboarded)
    col3.metric("% Onboarding", f"{pct}%")
    col4.metric("Avg Weeks Active", f"{avg_weeks} weeks")

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Age ≤ 36 Months", age_36)
    col6.metric("WAU (Last Week)", wau)
    col7.metric("% WAU", f"{wau_pct}%")
    col8.metric("Power Users", power)

    col9, col10 = st.columns(2)
    col9.metric("Power Users %", f"{power_pct}%")

# -----------------------------
# INTERVENTION
# -----------------------------
st.subheader(":package: Intervention Delivery Rate")

today = datetime.date.today()
last_sunday = today - datetime.timedelta(days=(today.weekday() + 1) % 7 + 7)

selected_sunday = st.date_input("Select Week Start (Sunday only)", value=last_sunday)

if selected_sunday.weekday() != 6:
    st.warning("⚠️ Please select a Sunday")
    st.stop()

selected_saturday = selected_sunday + datetime.timedelta(days=6)

st.write(f"Selected Range: {selected_sunday} → {selected_saturday}")

df2 = fetch_intervention_metrics(district_ids, selected_sunday, selected_saturday)

st.dataframe(df2)

fig = px.bar(
    df2,
    x="campaign_type",
    y="delivery_percentage",
    text="delivery_percentage",
    title="Delivery % by Campaign Type"
)

fig.update_traces(textposition='outside')

st.plotly_chart(fig, use_container_width=True)

st.caption("Source: Redshift")