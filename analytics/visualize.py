import duckdb
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../dbt/churnguard/churnguard.duckdb")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "charts")
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect(DB_PATH)

sns.set_theme(style="whitegrid", palette="muted")
COLORS = {"highlight": "#e74c3c", "base": "#3498db", "neutral": "#95a5a6"}

# ── helpers ───────────────────────────────────────────────────────────────────

def save(fig, name):
    path = os.path.join(OUTPUT_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  saved → {path}")


# ── 1. Overall churn rate (donut) ─────────────────────────────────────────────

row = con.sql("""
    select
        sum(churn_flag)                              as churned,
        count(*) - sum(churn_flag)                   as retained
    from main.fact_churn
""").fetchone()

churned, retained = int(row[0]), int(row[1])
total = churned + retained
churn_pct = churned / total * 100

fig, ax = plt.subplots(figsize=(6, 6))
wedges, _, autotexts = ax.pie(
    [churned, retained],
    labels=["Churned", "Retained"],
    colors=[COLORS["highlight"], COLORS["base"]],
    autopct="%1.1f%%",
    startangle=90,
    wedgeprops={"width": 0.5},
    textprops={"fontsize": 13},
)
ax.set_title(f"Overall Churn Rate\n({total:,} customers)", fontsize=14, fontweight="bold")
save(fig, "01_churn_rate.png")


# ── 2. Churn rate by contract type ───────────────────────────────────────────

df = con.sql("""
    select dc.contract, round(sum(f.churn_flag)*100.0/count(*), 1) as churn_pct
    from main.fact_churn f
    join main.dim_contract dc on f.contract_key = dc.contract_key
    group by dc.contract
    order by churn_pct desc
""").df()

fig, ax = plt.subplots(figsize=(7, 4))
bars = ax.barh(df["contract"], df["churn_pct"],
               color=[COLORS["highlight"], COLORS["neutral"], COLORS["base"]])
ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=11)
ax.set_xlabel("Churn Rate (%)")
ax.set_title("Churn Rate by Contract Type", fontsize=14, fontweight="bold")
ax.set_xlim(0, df["churn_pct"].max() * 1.2)
save(fig, "02_churn_by_contract.png")


# ── 3. Churn rate by tenure bucket ───────────────────────────────────────────

df = con.sql("""
    select dc.tenure_bucket, round(sum(f.churn_flag)*100.0/count(*), 1) as churn_pct
    from main.fact_churn f
    join main.dim_contract dc on f.contract_key = dc.contract_key
    group by dc.tenure_bucket
    order by churn_pct desc
""").df()

order = ["New", "Growing", "Loyal"]
df["tenure_bucket"] = df["tenure_bucket"].astype("category").cat.set_categories(order, ordered=True)
df = df.sort_values("tenure_bucket")

fig, ax = plt.subplots(figsize=(7, 4))
bar_colors = [COLORS["highlight"], COLORS["neutral"], COLORS["base"]]
bars = ax.bar(df["tenure_bucket"], df["churn_pct"], color=bar_colors, width=0.5)
ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=11)
ax.set_ylabel("Churn Rate (%)")
ax.set_title("Churn Rate by Tenure Bucket", fontsize=14, fontweight="bold")
ax.set_ylim(0, df["churn_pct"].max() * 1.2)
save(fig, "03_churn_by_tenure.png")


# ── 4. Churn rate by payment method ──────────────────────────────────────────

df = con.sql("""
    select dp.payment_method, round(sum(f.churn_flag)*100.0/count(*), 1) as churn_pct
    from main.fact_churn f
    join main.dim_payment dp on f.payment_key = dp.payment_key
    group by dp.payment_method
    order by churn_pct desc
""").df()

fig, ax = plt.subplots(figsize=(8, 4))
palette = [COLORS["highlight"]] + [COLORS["base"]] * (len(df) - 1)
bars = ax.barh(df["payment_method"], df["churn_pct"], color=palette)
ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=11)
ax.set_xlabel("Churn Rate (%)")
ax.set_title("Churn Rate by Payment Method", fontsize=14, fontweight="bold")
ax.set_xlim(0, df["churn_pct"].max() * 1.25)
save(fig, "04_churn_by_payment.png")


# ── 5. Monthly revenue lost by contract type ─────────────────────────────────

df = con.sql("""
    select dc.contract, round(sum(f.revenue_lost), 2) as monthly_lost
    from main.fact_churn f
    join main.dim_contract dc on f.contract_key = dc.contract_key
    group by dc.contract
    order by monthly_lost desc
""").df()

fig, ax = plt.subplots(figsize=(7, 4))
bars = ax.barh(df["contract"], df["monthly_lost"],
               color=[COLORS["highlight"], COLORS["neutral"], COLORS["base"]])
ax.bar_label(bars, fmt="$%.0f", padding=4, fontsize=11)
ax.set_xlabel("Monthly Revenue Lost ($)")
ax.set_title("Monthly Revenue Lost by Contract Type", fontsize=14, fontweight="bold")
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))
ax.set_xlim(0, df["monthly_lost"].max() * 1.2)
save(fig, "05_revenue_lost_by_contract.png")

con.close()
print("\nAll charts saved to analytics/charts/")
