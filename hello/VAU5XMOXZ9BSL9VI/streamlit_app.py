import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Snowflake Usage Dashboard", layout="wide")
st.title("Snowflake Account Usage Dashboard")

session = get_active_session()

days = st.sidebar.slider("過去何日間のデータを表示", min_value=7, max_value=90, value=30)

tab1, tab2, tab3 = st.tabs(["ウェアハウス クレジット使用量", "ストレージ使用量", "サービス別 クレジット使用量"])

with tab1:
    st.subheader("ウェアハウス別 日次クレジット使用量")
    wh_data = session.sql(f"""
        SELECT start_time::date AS usage_date,
               warehouse_name,
               SUM(credits_used) AS total_credits_used
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_DATE)
        GROUP BY 1, 2
        ORDER BY 1
    """).to_pandas()

    if not wh_data.empty:
        pivot = wh_data.pivot_table(
            index="USAGE_DATE", columns="WAREHOUSE_NAME",
            values="TOTAL_CREDITS_USED", aggfunc="sum"
        ).fillna(0)
        st.area_chart(pivot)

        st.subheader("ウェアハウス別 合計クレジット")
        wh_total = wh_data.groupby("WAREHOUSE_NAME")["TOTAL_CREDITS_USED"].sum().sort_values(ascending=False).reset_index()
        st.bar_chart(wh_total, x="WAREHOUSE_NAME", y="TOTAL_CREDITS_USED")
        st.dataframe(wh_total, use_container_width=True)
    else:
        st.info("データがありません。")

with tab2:
    st.subheader("日次ストレージ使用量 (TB)")
    storage_data = session.sql(f"""
        SELECT usage_date,
               ROUND(storage_bytes / POWER(1024, 4), 4) AS storage_tb,
               ROUND(stage_bytes / POWER(1024, 4), 4) AS stage_tb,
               ROUND(failsafe_bytes / POWER(1024, 4), 4) AS failsafe_tb
        FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
        WHERE usage_date >= DATEADD(day, -{days}, CURRENT_DATE)
        ORDER BY usage_date
    """).to_pandas()

    if not storage_data.empty:
        chart_df = storage_data.set_index("USAGE_DATE")[["STORAGE_TB", "STAGE_TB", "FAILSAFE_TB"]]
        st.area_chart(chart_df)
        st.dataframe(storage_data, use_container_width=True)
    else:
        st.info("データがありません。")

with tab3:
    st.subheader("サービス別 日次クレジット使用量")
    svc_data = session.sql(f"""
        SELECT usage_date,
               service_type,
               credits_billed
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE usage_date >= DATEADD(day, -{days}, CURRENT_DATE)
        ORDER BY usage_date
    """).to_pandas()

    if not svc_data.empty:
        svc_total = svc_data.groupby("SERVICE_TYPE")["CREDITS_BILLED"].sum().sort_values(ascending=False).reset_index()
        st.bar_chart(svc_total, x="SERVICE_TYPE", y="CREDITS_BILLED")

        pivot_svc = svc_data.pivot_table(
            index="USAGE_DATE", columns="SERVICE_TYPE",
            values="CREDITS_BILLED", aggfunc="sum"
        ).fillna(0)
        st.area_chart(pivot_svc)
        st.dataframe(svc_data, use_container_width=True)
    else:
        st.info("データがありません。")
