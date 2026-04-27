import os
import duckdb
import streamlit as st
import plotly.express as px

DB_PATH = os.path.join(os.path.dirname(__file__), "../dbt/churnguard/churnguard.duckdb")

st.set_page_config(page_title="ChurnGuard", page_icon="📉", layout="wide")

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()

COLOR_SCALE = ["#3498db", "#e74c3c"]

# ── KPI row (always full dataset) ─────────────────────────────────────────────

st.title("ChurnGuard — Customer Retention Analytics")

kpi = con.sql("""
    select
        count(*)                                       as total,
        sum(churn_flag)                                as churned,
        round(sum(churn_flag) * 100.0 / count(*), 1)  as churn_pct,
        round(sum(revenue_lost), 0)                    as monthly_lost,
        round(sum(revenue_lost) * 12, 0)               as annual_lost
    from main.fact_churn
""").fetchone()

total, churned, churn_pct, monthly_lost, annual_lost = kpi

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Customers",      f"{int(total):,}")
col2.metric("Churned",              f"{int(churned):,}")
col3.metric("Churn Rate",           f"{churn_pct}%")
col4.metric("Monthly Revenue Lost", f"${int(monthly_lost):,}")
col5.metric("Annualized Loss",      f"${int(annual_lost):,}")

st.divider()

# ── overview charts (unfiltered) ──────────────────────────────────────────────

st.subheader("Overall Breakdown")

c1, c2, c3 = st.columns(3)

with c1:
    df = con.sql("""
        select dc.contract,
               round(sum(f.churn_flag) * 100.0 / count(*), 1) as churn_pct
        from main.fact_churn f
        join main.dim_contract dc on f.contract_key = dc.contract_key
        group by dc.contract order by churn_pct desc
    """).df()
    fig = px.bar(df, x="churn_pct", y="contract", orientation="h",
                 color="churn_pct", color_continuous_scale=COLOR_SCALE,
                 text="churn_pct", title="Churn Rate by Contract Type",
                 labels={"churn_pct": "Churn Rate (%)", "contract": ""})
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=280, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    df = con.sql("""
        select dc.tenure_bucket,
               round(sum(f.churn_flag) * 100.0 / count(*), 1) as churn_pct
        from main.fact_churn f
        join main.dim_contract dc on f.contract_key = dc.contract_key
        group by dc.tenure_bucket order by churn_pct desc
    """).df()
    order = {"New": 0, "Growing": 1, "Loyal": 2}
    df = df.sort_values("tenure_bucket", key=lambda s: s.map(order))
    fig = px.bar(df, x="tenure_bucket", y="churn_pct",
                 color="churn_pct", color_continuous_scale=COLOR_SCALE,
                 text="churn_pct", title="Churn Rate by Tenure",
                 labels={"churn_pct": "Churn Rate (%)", "tenure_bucket": ""})
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=280, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

with c3:
    df = con.sql("""
        select dp.payment_method,
               round(sum(f.churn_flag) * 100.0 / count(*), 1) as churn_pct
        from main.fact_churn f
        join main.dim_payment dp on f.payment_key = dp.payment_key
        group by dp.payment_method order by churn_pct desc
    """).df()
    fig = px.bar(df, x="churn_pct", y="payment_method", orientation="h",
                 color="churn_pct", color_continuous_scale=COLOR_SCALE,
                 text="churn_pct", title="Churn Rate by Payment Method",
                 labels={"churn_pct": "Churn Rate (%)", "payment_method": ""})
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=280, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── segment deep-dive ─────────────────────────────────────────────────────────

st.subheader("Segment Deep-Dive")
st.caption("Pick a contract type to see how tenure and payment method drive churn *within that segment*.")

contracts = con.sql("select distinct contract from main.dim_contract order by 1").df()["contract"].tolist()
selected = st.radio("Contract segment", contracts, horizontal=True)

where = f"dc.contract = '{selected}'"

seg_kpi = con.sql(f"""
    select count(*) as total,
           sum(f.churn_flag) as churned,
           round(sum(f.churn_flag) * 100.0 / count(*), 1) as churn_pct,
           round(sum(f.revenue_lost), 0) as monthly_lost
    from main.fact_churn f
    join main.dim_contract dc on f.contract_key = dc.contract_key
    where {where}
""").fetchone()

s_total, s_churned, s_churn_pct, s_monthly_lost = seg_kpi

k1, k2, k3, k4 = st.columns(4)
k1.metric("Customers in segment",  f"{int(s_total):,}")
k2.metric("Churned",               f"{int(s_churned):,}")
k3.metric("Churn Rate",            f"{s_churn_pct}%",
          delta=f"{round(s_churn_pct - churn_pct, 1)}pp vs overall",
          delta_color="inverse")
k4.metric("Monthly Revenue Lost",  f"${int(s_monthly_lost):,}")

d1, d2 = st.columns(2)

with d1:
    df_t = con.sql(f"""
        select dc.tenure_bucket,
               count(*) as customers,
               sum(f.churn_flag) as churned,
               round(sum(f.churn_flag) * 100.0 / count(*), 1) as churn_pct
        from main.fact_churn f
        join main.dim_contract dc on f.contract_key = dc.contract_key
        where {where}
        group by dc.tenure_bucket order by churn_pct desc
    """).df()
    order = {"New": 0, "Growing": 1, "Loyal": 2}
    df_t = df_t.sort_values("tenure_bucket", key=lambda s: s.map(order))
    fig = px.bar(df_t, x="tenure_bucket", y="churn_pct",
                 color="churn_pct", color_continuous_scale=COLOR_SCALE,
                 text="churn_pct",
                 title=f"Tenure Breakdown — {selected}",
                 labels={"churn_pct": "Churn Rate (%)", "tenure_bucket": ""},
                 hover_data={"customers": True, "churned": True})
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=320)
    st.plotly_chart(fig, use_container_width=True)

with d2:
    df_p = con.sql(f"""
        select dp.payment_method,
               count(*) as customers,
               sum(f.churn_flag) as churned,
               round(sum(f.churn_flag) * 100.0 / count(*), 1) as churn_pct
        from main.fact_churn f
        join main.dim_contract dc on f.contract_key = dc.contract_key
        join main.dim_payment  dp on f.payment_key  = dp.payment_key
        where {where}
        group by dp.payment_method order by churn_pct desc
    """).df()
    fig = px.bar(df_p, x="churn_pct", y="payment_method", orientation="h",
                 color="churn_pct", color_continuous_scale=COLOR_SCALE,
                 text="churn_pct",
                 title=f"Payment Method Breakdown — {selected}",
                 labels={"churn_pct": "Churn Rate (%)", "payment_method": ""},
                 hover_data={"customers": True, "churned": True})
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=320)
    st.plotly_chart(fig, use_container_width=True)
